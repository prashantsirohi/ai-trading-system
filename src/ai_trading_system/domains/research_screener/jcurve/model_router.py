from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import requests

from ai_trading_system.domains.research_screener.store import canonical_json, content_hash

from .claim_contract import JCurveClaimContract
from .models import AgentCallRecord, AgentResult, EvidencePacket, ModelRoute


class ModelRouterError(RuntimeError):
    pass


@dataclass(frozen=True)
class RouterPolicy:
    raw: dict[str, Any]
    policy_hash: str

    @classmethod
    def load(cls, path: str | Path) -> "RouterPolicy":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        required = {
            "policy_version", "transport", "base_url", "models",
            "extraction_prompt_version", "verification_prompt_version",
            "max_context_characters", "max_pages_per_request", "max_output_tokens",
            "max_retries", "max_requests_per_run", "halt_new_requests_at_reported_cost_usd",
            "temperature", "full_document_prompting_forbidden",
            "require_structured_outputs", "require_different_verifier_family",
        }
        missing = required - set(raw)
        if missing:
            raise ValueError(f"model policy missing fields: {sorted(missing)}")
        if raw["transport"] != "OPENROUTER_CHAT_COMPLETIONS_V1":
            raise ValueError("unsupported J-curve transport")
        if not raw["full_document_prompting_forbidden"]:
            raise ValueError("J-curve policy must forbid full-document prompting")
        if not raw["require_structured_outputs"]:
            raise ValueError("J-curve policy must require structured outputs")
        if not raw["require_different_verifier_family"]:
            raise ValueError("J-curve policy must require a different verifier family")
        return cls(raw=raw, policy_hash=content_hash(raw))


class OpenRouterModelRouter:
    """Bounded OpenRouter transport for auditable extraction and verification."""

    def __init__(
        self,
        *,
        api_key: str,
        policy: RouterPolicy,
        extraction_schema: dict[str, Any],
        verification_schema: dict[str, Any],
        post: Callable[..., Any] | None = None,
        timeout_seconds: int = 60,
    ):
        if not api_key:
            raise ValueError("OpenRouter API key is required")
        self.api_key = api_key
        self.policy = policy
        self.extraction_schema = extraction_schema
        self.verification_schema = verification_schema
        self.post = post or requests.post
        self.timeout_seconds = timeout_seconds
        self.contract = JCurveClaimContract()

    @classmethod
    def from_project_root(
        cls, *, project_root: Path, api_key: str, post: Callable[..., Any] | None = None
    ) -> "OpenRouterModelRouter":
        config_root = project_root / "configs/research_screener"
        return cls(
            api_key=api_key,
            policy=RouterPolicy.load(config_root / "jcurve/model_policy.json"),
            extraction_schema=json.loads(
                (config_root / "schemas/jcurve_claim_batch_v1.schema.json").read_text(encoding="utf-8")
            ),
            verification_schema=json.loads(
                (config_root / "schemas/jcurve_review_batch_v1.schema.json").read_text(encoding="utf-8")
            ),
            post=post,
        )

    def extract(self, packet: EvidencePacket, *, route: ModelRoute) -> AgentResult:
        self._validate_packet(packet, route=route)
        model = self._model_for(route, role="EXTRACTION")
        prompt_version = self.policy.raw["extraction_prompt_version"]
        messages = self._extraction_messages(packet, route=route)
        result = self._call(
            packet=packet,
            role="EXTRACTION",
            route=route,
            model=model,
            prompt_version=prompt_version,
            messages=messages,
            schema_name="jcurve_claim_batch_v1",
            schema=self.extraction_schema,
        )
        errors = self.contract.validate_batch(result.payload)
        if errors:
            raise ModelRouterError("extraction response failed deterministic validation: " + ",".join(errors))
        return result

    def verify(
        self,
        packet: EvidencePacket,
        *,
        claims: list[dict[str, Any]],
        extraction_route: ModelRoute,
    ) -> AgentResult:
        verifier_route = ModelRoute.TEXT
        self._validate_packet(packet, route=verifier_route)
        extraction_model = self._model_for(extraction_route, role="EXTRACTION")
        verifier_model = (
            self.policy.raw["models"]["text_verifier"]
            if extraction_route == ModelRoute.TEXT
            else self.policy.raw["models"]["vision_verifier"]
        )
        if self.policy.raw["require_different_verifier_family"] and self._family(extraction_model) == self._family(verifier_model):
            raise ModelRouterError("extractor and verifier must use different model families")
        messages = self._verification_messages(packet, claims=claims, route=verifier_route)
        result = self._call(
            packet=packet,
            role="VERIFICATION",
            route=verifier_route,
            model=verifier_model,
            prompt_version=self.policy.raw["verification_prompt_version"],
            messages=messages,
            schema_name="jcurve_review_batch_v1",
            schema=self.verification_schema,
        )
        errors = self.contract.validate_review_batch(result.payload, claim_count=len(claims))
        if errors:
            raise ModelRouterError("verification response failed deterministic validation: " + ",".join(errors))
        return result

    def _call(
        self,
        *,
        packet: EvidencePacket,
        role: str,
        route: ModelRoute,
        model: str,
        prompt_version: str,
        messages: list[dict[str, Any]],
        schema_name: str,
        schema: dict[str, Any],
    ) -> AgentResult:
        prompt_hash = content_hash(messages)
        request_id = f"jcurve-request:{uuid.uuid4()}"
        body: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": self.policy.raw["temperature"],
            "max_tokens": self.policy.raw["max_output_tokens"],
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": schema_name, "strict": True, "schema": schema},
            },
        }
        providers = list(self.policy.raw.get("allowed_provider_slugs") or [])
        if providers:
            body["provider"] = {"only": providers, "allow_fallbacks": False}
        history: list[dict[str, Any]] = []
        response_data: dict[str, Any] | None = None
        error_code: str | None = None
        max_attempts = int(self.policy.raw["max_retries"]) + 1
        for attempt in range(1, max_attempts + 1):
            started = time.monotonic()
            try:
                response = self.post(
                    f"{self.policy.raw['base_url'].rstrip('/')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=body,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                response_data = response.json()
                history.append({"attempt": attempt, "status": "SUCCESS", "elapsed_ms": round((time.monotonic() - started) * 1000)})
                break
            except Exception as exc:
                error_code = type(exc).__name__
                history.append({"attempt": attempt, "status": "FAILED", "error_code": error_code, "elapsed_ms": round((time.monotonic() - started) * 1000)})
        if response_data is None:
            raise ModelRouterError(f"OpenRouter request failed after {max_attempts} attempts: {error_code}")
        try:
            content = response_data["choices"][0]["message"]["content"]
            payload = json.loads(content) if isinstance(content, str) else content
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise ModelRouterError("OpenRouter response did not contain valid structured JSON") from exc
        usage = response_data.get("usage") or {}
        source_hashes = tuple(self._page_hash(page.page, page.text, page.image_data_url) for page in packet.pages)
        call = AgentCallRecord(
            request_id=str(response_data.get("id") or request_id), role=role, route=route.value,
            model_id=str(response_data.get("model") or model), provider=response_data.get("provider"),
            prompt_version=prompt_version, prompt_hash=prompt_hash, source_page_hashes=source_hashes,
            input_tokens=int(usage.get("prompt_tokens") or 0), output_tokens=int(usage.get("completion_tokens") or 0),
            cost_usd=self._cost(usage), schema_valid=True, attempt_count=len(history),
            retry_history=tuple(history), response_hash=content_hash(payload), status="COMPLETED", error_code=None,
        )
        return AgentResult(payload=payload, call=call)

    def _validate_packet(self, packet: EvidencePacket, *, route: ModelRoute) -> None:
        if not packet.pages:
            raise ValueError("evidence packet must contain at least one page")
        if len(packet.pages) > int(self.policy.raw["max_pages_per_request"]):
            raise ValueError("evidence packet exceeds page limit")
        chars = sum(len(page.text) for page in packet.pages)
        if chars > int(self.policy.raw["max_context_characters"]):
            raise ValueError("evidence packet exceeds context character limit")
        if len(packet.source_content_hash) != 64 or any(
            char not in "0123456789abcdef" for char in packet.source_content_hash
        ):
            raise ValueError("source_content_hash must be a lowercase SHA-256 digest")
        if route != ModelRoute.TEXT and not any(page.image_data_url for page in packet.pages) and not packet.pdf_data_url:
            raise ValueError("vision routes require selected page images or a bounded PDF")

    def _model_for(self, route: ModelRoute, *, role: str) -> str:
        models = self.policy.raw["models"]
        return {
            ModelRoute.TEXT: models["text_extractor"],
            ModelRoute.VISION: models["vision_extractor"],
            ModelRoute.VISION_FALLBACK: models["vision_fallback"],
        }[route]

    @staticmethod
    def _family(model: str) -> str:
        return model.split("/", 1)[0].lower()

    def _extraction_messages(self, packet: EvidencePacket, *, route: ModelRoute) -> list[dict[str, Any]]:
        system = (
            "Extract only directly supported capex lifecycle facts from the supplied bounded evidence. "
            "Never estimate a missing number. Use NOT_DISCLOSED with null values when a requested fact is absent. "
            "Every PRESENT claim must reproduce an exact excerpt and page number. Return only schema-valid JSON."
        )
        return [{"role": "system", "content": system}, {"role": "user", "content": self._content(packet, route, prefix=packet.title)}]

    def _verification_messages(
        self, packet: EvidencePacket, *, claims: list[dict[str, Any]], route: ModelRoute
    ) -> list[dict[str, Any]]:
        system = (
            "Independently verify each proposed claim against the supplied source context. "
            "Accept only when the exact excerpt, page, value, unit, and claim meaning are supported. "
            "Return one review for every claim index and no additional prose."
        )
        prefix = "PROPOSED CLAIMS:\n" + canonical_json(claims) + "\n\nSOURCE:"
        return [{"role": "system", "content": system}, {"role": "user", "content": self._content(packet, route, prefix=prefix)}]

    @staticmethod
    def _content(packet: EvidencePacket, route: ModelRoute, *, prefix: str) -> Any:
        parts: list[dict[str, Any]] = [{"type": "text", "text": prefix}]
        for page in packet.pages:
            parts.append({"type": "text", "text": f"\n--- PAGE {page.page} ---\n{page.text}"})
            if route != ModelRoute.TEXT and page.image_data_url:
                parts.append({"type": "image_url", "image_url": {"url": page.image_data_url}})
        if route != ModelRoute.TEXT and packet.pdf_data_url:
            parts.append({"type": "file", "file": {"filename": "selected-evidence.pdf", "file_data": packet.pdf_data_url}})
        return parts if route != ModelRoute.TEXT else "".join(part.get("text", "") for part in parts)

    @staticmethod
    def _page_hash(page: int, text: str, image_data_url: str | None) -> str:
        return content_hash({"page": page, "text": text, "image": image_data_url})

    @staticmethod
    def _cost(usage: dict[str, Any]) -> float | None:
        for key in ("cost", "total_cost", "cost_usd"):
            if usage.get(key) is not None:
                try:
                    return float(usage[key])
                except (TypeError, ValueError):
                    return None
        return None
