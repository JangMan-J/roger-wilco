# Meta Provider Route & Model Availability Diagnosis

**Date:** 2026-10-13  
**Status:** Authentication succeeds. The tested inference requests return model-not-found errors.
**Scope:** `meta/muse-spark-1.3`, catalog IDs (`1.1`, `1.2`, `1.2-contributor`, `1.3`, `1.3-contributor`), Pi provider configuration and live endpoint behavior.

---

## 1. Executive Summary

- **Provider Endpoint & Routing:** `https://api.meta.ai/v1` is reachable and functional. The routing header returns `x-route: model-api-rust`.
- **Authentication & Subscription:** OAuth refresh / key minting via `https://api.meta.ai/muse-code/key` succeeds with HTTP 200. The account holds an active `Muse Code Power Usage` tier (`is_subs_active: true`, `used_percent: 0`).
- **Diagnosis (Stale ID vs Unavailable Provider):** Neither a simple typo in model ID nor a dead provider URL. Rather, **the provider's model catalog is currently empty for this account/endpoint (`GET /v1/models` returns `[]`)**.
- **Model Invocations:** Every catalog model (`muse-spark-1.1`, `muse-spark-1.2`, `muse-spark-1.2-contributor`, `muse-spark-1.3`, `muse-spark-1.3-contributor`) returns `404` (`code: model_not_found`, message: `"The requested model was not found."` or `"The model ... does not exist or is not available"`).
- **Cause remains unresolved:** These responses do not establish an outage, catalog purge, backend migration, or account restriction.
  A successful request under matching conditions or provider-side evidence would distinguish those explanations.

---

## 2. Source Code & Configuration Inspection

### A. Provider Definition (`packages/ai/src/providers/meta.ts`)
- Provider ID: `meta`
- Base URL: `https://api.meta.ai/v1`
- API format: `openai-responses` (backed by `OpenAI` client wrapper pointing to `baseUrl`)
- Auth: Environment variable `META_API_KEY` or OAuth device code flow via `https://auth.meta.com` with key minting at `https://api.meta.ai/muse-code/key`.

### B. Catalog Definition (`packages/ai/src/providers/data/meta.json`)
The static catalog in Pi defines:
- `chat:muse-spark-1.1`
- `chat:muse-spark-1.2`
- `chat:muse-spark-1.2-contributor`
- `chat:muse-spark-1.3` (default fallback for `meta` in `model-resolver.ts`)
- `chat:muse-spark-1.3-contributor`

All models specify `api: "openai-responses"`, `baseUrl: "https://api.meta.ai/v1"`, and `provider: "meta"`.

### C. Models Cache (`~/.pi/agent/models-store.json`)
The cached `models-store.json` matches `packages/ai` exactly.

---

## 3. Live Empirical Probe Results

Probes executed using in-process credential loading without exposing secret tokens:

| Probe Target | Method | Response Status | Response Body Summary |
|---|---|---|---|
| `https://api.meta.ai/muse-code/key` | `POST` | `200 OK` | Key minted; active subscription tier `Muse Code Power Usage` |
| `https://api.meta.ai/v1/models` | `GET` | `200 OK` | `{"object":"list","data":[]}` |
| `https://api.meta.ai/muse-code/models` | `GET` | `200 OK` | `{"object":"list","data":[]}` |
| `https://api.meta.ai/v1/models/muse-spark-1.3` | `GET` | `404 Not Found` | `{"error":{"code":"model_not_found","message":"The model ... does not exist or is not available"}}` |
| `https://api.meta.ai/v1/models/muse-spark-1.2` | `GET` | `404 Not Found` | `{"error":{"code":"model_not_found","message":"The model ... does not exist or is not available"}}` |
| `https://api.meta.ai/v1/models/muse-spark-1.1` | `GET` | `404 Not Found` | `{"error":{"code":"model_not_found","message":"The model ... does not exist or is not available"}}` |
| `https://api.meta.ai/v1/responses` (`muse-spark-1.3`) | `POST` | `404 Not Found` | `{"error":{"code":"model_not_found","message":"The requested model was not found."}}` |
| `https://api.meta.ai/v1/chat/completions` (`muse-spark-1.3`) | `POST` | `404 Not Found` | `{"error":{"code":"model_not_found","message":"The requested model was not found."}}` |

---

## 4. Conclusion & Operational Recommendation

The tested endpoint accepts authentication but rejects the listed inference requests.
The empty catalog and 404 responses establish this request failure, not its root cause.
The local provider and catalog match each other. That agreement does not prove compatibility with the current remote service.
No credential or provider configuration changed during this investigation.
The requested Meta review did not complete. Other explicitly selected model routes handle the implementation work.
