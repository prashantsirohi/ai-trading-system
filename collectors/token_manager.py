import os
import json
import requests
import time
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from core.runtime_config import DhanRuntimeConfig
from utils.env import find_project_env, load_project_env
from core.logging import logger

try:
    import pyotp

    HAS_PYOTP = True
except ImportError:
    HAS_PYOTP = False


class DhanTokenManager:
    """
    Manages Dhan API access token lifecycle - generation and renewal.
    Token is valid for 24 hours and can be renewed before expiry.
    """

    def __init__(self, env_path: str = ".env"):
        resolved_env = find_project_env(env_path) if env_path == ".env" else Path(env_path).resolve()
        self.env_path = str(resolved_env) if resolved_env is not None else env_path
        load_project_env(self.env_path, override=False)
        load_dotenv(self.env_path, override=False)

        runtime = DhanRuntimeConfig.from_env()
        self.client_id = runtime.client_id
        self.access_token = runtime.access_token
        self.pin = runtime.pin
        self.api_key = runtime.api_key

        self.base_url = "https://api.dhan.co/v2"
        self.auth_url = "https://auth.dhan.co"
        self.enable_renew_token = self._env_flag("DHAN_ENABLE_RENEW_TOKEN", default=False)

    @staticmethod
    def _env_flag(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return bool(default)
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    def save_token(self, new_token: str, expiry_time: str = None):
        """Save the new access token to .env file"""
        env_content = []
        if os.path.exists(self.env_path):
            with open(self.env_path, "r") as f:
                for line in f:
                    if line.startswith("DHAN_ACCESS_TOKEN="):
                        env_content.append(f"DHAN_ACCESS_TOKEN={new_token}\n")
                    elif line.startswith("DHAN_TOKEN_EXPIRY="):
                        if expiry_time:
                            env_content.append(f"DHAN_TOKEN_EXPIRY={expiry_time}\n")
                        else:
                            env_content.append(line)
                    else:
                        env_content.append(line)
        else:
            env_content.append(f"DHAN_ACCESS_TOKEN={new_token}\n")
            if expiry_time:
                env_content.append(f"DHAN_TOKEN_EXPIRY={expiry_time}\n")

        with open(self.env_path, "w") as f:
            f.writelines(env_content)

        logger.info(f"Token saved to {self.env_path}")

    def get_token_expiry(self) -> datetime:
        """Get token expiry from .env or calculate from current token"""
        expiry = os.getenv("DHAN_TOKEN_EXPIRY", "")
        if expiry:
            try:
                return datetime.strptime(expiry, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                try:
                    return datetime.strptime(expiry, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass

        if self.access_token:
            return datetime.now() + timedelta(hours=23)
        return datetime.now()

    def is_token_expiring_soon(self, hours_threshold: int = 1) -> bool:
        """Check if token will expire within the specified hours"""
        expiry = self.get_token_expiry()
        threshold = datetime.now() + timedelta(hours=hours_threshold)
        return expiry <= threshold

    def is_token_expired(self) -> bool:
        """Check if token is expired by attempting a lightweight API call."""
        if not self.access_token:
            return True

        url = f"{self.base_url}/profile"
        headers = {
            "access-token": self.access_token,
            "dhanClientId": self.client_id,
        }
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code in (401, 403):
                return True
            result = resp.json()
            if isinstance(result, dict) and result.get("status") == "failure":
                err_code = result.get("remarks", {}).get("error_code", "")
                if err_code in ("DH-901", "DH-905"):
                    return True
        except Exception:
            pass

        expiry = self.get_token_expiry()
        return datetime.now() >= expiry

    @staticmethod
    def _safe_json(response: requests.Response) -> dict:
        """Parse JSON response defensively."""
        try:
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _extract_error_code(payload: dict) -> str:
        """Extract Dhan-style error code from response payload."""
        if not isinstance(payload, dict):
            return ""
        return str(
            payload.get("errorCode")
            or payload.get("error_code")
            or (payload.get("remarks") or {}).get("error_code")
            or ""
        )

    def renew_token(self) -> dict:
        """
        Renew the access token.
        If the existing token is expired (user-generated), falls back to
        OAuth token generation using client_id + PIN + TOTP.
        """
        if not self.client_id:
            logger.error("Client ID is required")
            return {"status": "error", "message": "Client ID is required"}

        url = f"{self.base_url}/RenewToken"
        headers = {
            "access-token": self.access_token,
            "dhanClientId": self.client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            logger.info("Attempting to renew access token...")
            response = requests.post(url, headers=headers, timeout=30)
            result = self._safe_json(response)
            if response.status_code == 200 and (
                result.get("status") == "success" or "accessToken" in result
            ):
                new_token = result.get("accessToken", result.get("access_token"))
                expiry_time = result.get("expiryTime", result.get("expiry_time"))
                self.save_token(new_token, expiry_time)
                self.access_token = new_token
                logger.info(f"Token renewed successfully. Expires at: {expiry_time}")
                return {
                    "status": "success",
                    "access_token": new_token,
                    "expiry_time": expiry_time,
                    "source": "renew_token",
                }

            error_code = self._extract_error_code(result)
            # Observed in production: RenewToken can return DH-905 even when
            # the current token is still valid. In that case keep using it.
            if error_code == "DH-905" and self.access_token and not self.is_token_expired():
                logger.warning(
                    "RenewToken returned DH-905 but existing token is still valid. "
                    "Continuing with current token."
                )
                return {
                    "status": "success",
                    "access_token": self.access_token,
                    "expiry_time": self.get_token_expiry().isoformat(),
                    "source": "existing_token_after_dh905",
                }

            logger.warning(
                f"Token renewal failed ({response.status_code}): {response.text}. "
                f"Falling back to OAuth token generation..."
            )
            generated = self.generate_token()
            if generated.get("status") == "success":
                generated.setdefault("source", "generate_access_token")
                return generated
            # If generation is rate-limited but token remains usable, preserve continuity.
            if self.access_token and not self.is_token_expired():
                logger.warning(
                    "Token generation failed after renewal fallback, but existing token "
                    "is still valid. Continuing with current token."
                )
                return {
                    "status": "success",
                    "access_token": self.access_token,
                    "expiry_time": self.get_token_expiry().isoformat(),
                    "source": "existing_token_after_generate_failure",
                    "message": generated.get("message"),
                }
            return generated

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Request error during token renewal: {e}. "
                f"Falling back to OAuth token generation..."
            )
            generated = self.generate_token()
            if generated.get("status") == "success":
                generated.setdefault("source", "generate_access_token")
                return generated
            if self.access_token and not self.is_token_expired():
                logger.warning(
                    "Token generation failed after request error, but existing token "
                    "is still valid. Continuing with current token."
                )
                return {
                    "status": "success",
                    "access_token": self.access_token,
                    "expiry_time": self.get_token_expiry().isoformat(),
                    "source": "existing_token_after_request_error",
                    "message": generated.get("message"),
                }
            return generated

    def generate_token(self) -> dict:
        """
        Generate a new access token using client ID, PIN, and TOTP.
        If DHAN_TOTP is a base32 secret, auto-generates the current 6-digit code.
        If DHAN_TOTP is a 6-digit code, uses it directly.
        """
        if not self.client_id or not self.pin:
            logger.error("Client ID and PIN are required for token generation")
            return {"status": "error", "message": "Client ID and PIN are required"}

        totp_secret = os.getenv("DHAN_TOTP", "")
        if not totp_secret:
            logger.error("DHAN_TOTP is required but not set in .env")
            return {"status": "error", "message": "DHAN_TOTP not set in .env"}

        if (
            HAS_PYOTP
            and len(totp_secret) >= 16
            and all(
                c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567=" for c in totp_secret.upper()
            )
        ):
            totp_code = pyotp.TOTP(totp_secret).now()
            logger.info(f"Auto-generated TOTP code from base32 secret")
        elif len(totp_secret) == 6 and totp_secret.isdigit():
            totp_code = totp_secret
            logger.info("Using provided 6-digit TOTP code")
        else:
            logger.error(
                f"DHAN_TOTP format not recognized: '{totp_secret[:10]}...'. "
                f"Provide either a 6-digit code or a base32 secret."
            )
            return {
                "status": "error",
                "message": f"Invalid TOTP format: {totp_secret[:10]}",
            }

        url = f"{self.auth_url}/app/generateAccessToken"
        params = {
            "dhanClientId": self.client_id,
            "pin": self.pin,
            "totp": totp_code,
        }

        try:
            logger.info("Attempting to generate new access token...")
            response = requests.post(url, params=params, timeout=30)

            if response.status_code == 200:
                result = response.json()
                if "accessToken" in result or "access_token" in result:
                    new_token = result.get("accessToken", result.get("access_token"))
                    expiry_time = result.get("expiryTime", result.get("expiry_time"))

                    self.save_token(new_token, expiry_time)
                    self.access_token = new_token

                    logger.info(
                        f"Token generated successfully. Expires at: {expiry_time}"
                    )
                    return {
                        "status": "success",
                        "access_token": new_token,
                        "expiry_time": expiry_time,
                        "client_name": result.get("dhanClientName", ""),
                    }
                else:
                    logger.error(f"Token generation failed: {result}")
                    return {"status": "error", "message": result}
            else:
                logger.error(
                    f"HTTP error during token generation: {response.status_code} - {response.text}"
                )
                return {
                    "status": "error",
                    "message": f"HTTP {response.status_code}: {response.text}",
                }

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error during token generation: {e}")
            return {"status": "error", "message": str(e)}

    def ensure_valid_token(self, hours_before_expiry: int = 1) -> str:
        """
        Ensure we have a valid access token.
        Default policy uses fresh TOTP generation for expired/expiring tokens.
        RenewToken is only attempted when DHAN_ENABLE_RENEW_TOKEN=1.
        Returns the current access token.
        """
        def _generate_fresh_token(reason: str) -> str | None:
            logger.info(reason)
            result = self.generate_token()
            if result.get("status") == "success":
                self.access_token = result["access_token"]
                return result["access_token"]
            return None

        if not self.access_token:
            return _generate_fresh_token("No access token found. Generating new token...")

        if self.is_token_expired():
            return _generate_fresh_token(
                "Token is expired or rejected by profile check. Generating fresh token via TOTP..."
            )

        if self.is_token_expiring_soon(hours_before_expiry):
            if self.enable_renew_token:
                logger.info(
                    "Token is expiring soon (within %s hour(s)). "
                    "Trying RenewToken because DHAN_ENABLE_RENEW_TOKEN is enabled...",
                    hours_before_expiry,
                )
                result = self.renew_token()
                if result.get("status") == "success":
                    self.access_token = result["access_token"]
                    return result["access_token"]
                logger.warning(
                    "Token renewal failed: %s. Falling back to fresh token generation...",
                    result.get("message"),
                )
            return _generate_fresh_token(
                f"Token is expiring soon (within {hours_before_expiry} hour(s)). "
                "Generating fresh token via TOTP..."
            )

        return self.access_token

    def get_profile(self) -> dict:
        """Check if token is valid by fetching user profile"""
        if not self.access_token:
            return {"status": "error", "message": "No access token"}

        url = f"{self.base_url}/profile"
        headers = {"access-token": self.access_token, "dhanClientId": self.client_id}

        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "error", "message": f"HTTP {response.status_code}"}
        except Exception as e:
            return {"status": "error", "message": str(e)}


def main():
    """Main function to demonstrate token management"""
    import argparse

    parser = argparse.ArgumentParser(description="Dhan API Token Manager")
    parser.add_argument("--renew", action="store_true", help="Force renew token")
    parser.add_argument("--check", action="store_true", help="Check token validity")
    parser.add_argument("--profile", action="store_true", help="Get user profile")
    parser.add_argument(
        "--auto", action="store_true", help="Auto-renew if expiring within 1 hour"
    )

    args = parser.parse_args()

    manager = DhanTokenManager()

    if not manager.client_id:
        logger.error("DHAN_CLIENT_ID not found in .env")
        return

    print(f"Client ID: {manager.client_id}")
    print(
        f"Current token: {manager.access_token[:50]}..."
        if manager.access_token
        else "No token found"
    )
    print(f"Token expiry: {manager.get_token_expiry()}")
    print(f"Is expired: {manager.is_token_expired()}")
    print(f"Is expiring soon: {manager.is_token_expiring_soon()}")
    print()

    if args.renew:
        print("Forcing token renewal...")
        result = manager.renew_token()
        print(f"Result: {json.dumps(result, indent=2)}")

    elif args.check:
        print("Checking token validity...")
        if manager.is_token_expired():
            print("Token is EXPIRED!")
            result = manager.renew_token()
            print(f"Renewal result: {json.dumps(result, indent=2)}")
        elif manager.is_token_expiring_soon():
            print("Token is expiring soon!")
            result = manager.renew_token()
            print(f"Renewal result: {json.dumps(result, indent=2)}")
        else:
            print("Token is valid")

    elif args.profile:
        print("Fetching user profile...")
        result = manager.get_profile()
        print(json.dumps(result, indent=2))

    elif args.auto:
        print("Ensuring valid token...")
        token = manager.ensure_valid_token()
        if token:
            print(f"Valid token obtained: {token[:50]}...")
        else:
            print("Failed to obtain valid token")

    else:
        print("Use --help for usage information")


if __name__ == "__main__":
    main()
