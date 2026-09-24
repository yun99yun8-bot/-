# V8.1.5 — Event-driven G17 + G20 Barrier + DB-first Repair

This build keeps the V8.1.4 throttling and lifecycle tracing, and adds:

- Event-driven G17 prediction lock when block `target20-3` is published.
- A G20 pre-publication barrier: before G20 becomes visible to the rest of the app, RAM + PostgreSQL are reconstructed and a complete G1..G17 snapshot is locked if available.
- DB/RAM-only barrier path: it never performs upstream HTTP backfill, so it does not worsen TRON 429 throttling.
- Idempotent locking: an existing durable prediction is never overwritten.
- Health diagnostics under `runtime.eventDrivenLifecycle` and a dedicated `g17-event` engine heartbeat.
- No post-result backfill of predictions.

Render configuration remains unchanged: Web + Background Worker share the same PostgreSQL database; Worker start command is `python worker.py`.
