# Job Runner (interview-friendly)

Упрощённый вариант для live-coding: in-memory repo + worker pool с high/normal приоритетом, resize, retry (до 3), idempotency и graceful shutdown.

Политика shutdown: новые jobs отклоняются, queued jobs помечаются `canceled`, running jobs дожидаются до таймаута.
