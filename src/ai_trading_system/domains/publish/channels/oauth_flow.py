"""
Google Sheets OAuth2 Authentication Helper

This script performs the OAuth2 flow to generate tokens for Google Sheets access.
Run this once to authenticate, then use google_sheets_manager.py for all operations.

Steps:
1. Go to https://console.cloud.google.com/apis/credentials
2. Create OAuth2 Client ID (Desktop app or Web application)
3. Download the JSON file as 'client_secret.json'
4. Run this script: python oauth_flow.py
5. Follow the instructions to authorize
6. Token will be saved to 'token.json'
"""

import os
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.json"


def run_oauth_flow():
    if not Path(CLIENT_SECRETS_FILE).exists():
        print(f"ERROR: {CLIENT_SECRETS_FILE} not found!")
        print("\n1. Go to https://console.cloud.google.com/apis/credentials")
        print("2. Create OAuth 2.0 Client IDs > Desktop app (or Web application)")
        print("3. Download the JSON file and save as 'client_secret.json'")
        print("4. Run this script again")
        return None

    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)

    print("Starting OAuth2 flow...")
    print("A browser window will open for authorization.")

    credentials = flow.run_local_server(port=0, access_type="offline", prompt="consent")

    with open(TOKEN_FILE, "w") as f:
        f.write(credentials.to_json())

    print(f"\nSUCCESS! Token saved to {TOKEN_FILE}")
    print(f"Refresh token: {credentials.refresh_token[:20]}...")

    return credentials


def check_token():
    if Path(TOKEN_FILE).exists():
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        if creds and creds.valid:
            print("Token is valid!")
            return creds
        elif creds and creds.refresh_token:
            print("Token expired. Refreshing...")
            creds.refresh(Request())
            with open(TOKEN_FILE, "w") as f:
                f.write(creds.to_json())
            print("Token refreshed!")
            return creds
        else:
            print("Token invalid. Please re-authenticate.")
    else:
        print("No token found. Please run OAuth flow first.")

    return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Google Sheets OAuth Helper")
    parser.add_argument("--check", action="store_true", help="Check existing token")
    args = parser.parse_args()

    if args.check:
        check_token()
    else:
        run_oauth_flow()
