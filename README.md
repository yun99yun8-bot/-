# TRON Monitor V8.1.4

TRON collection stability release.

- Shared process-wide request governor prevents independent engines from flooding public TRON APIs.
- HTTP 429 activates an 18-second global cooldown instead of immediate retry storms.
- Group-20 fast path no longer races multiple providers on every attempt.
- Startup hydrates recent blocks from PostgreSQL first, then repairs only a small missing window from upstream.
- Ongoing collector preserves missing-block order and retries gaps rather than skipping them.
- Health exposes `runtime.tronRateLimit` counters for request/429 diagnostics.
- Existing V8.1.3 lifecycle tracing and no-retroactive-prediction rule remain intact.

Render commands remain unchanged: web service uses the existing web start command; Background Worker uses `python worker.py`.
