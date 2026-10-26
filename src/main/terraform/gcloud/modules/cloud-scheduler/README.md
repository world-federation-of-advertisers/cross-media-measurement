
# EDP Aggregator System

Edp Aggregator components for Cloud Scheduler.
## HTTP retries

Optional `scheduler_config.retry_config` sets `retry_count`,
`min_backoff_duration`, `max_backoff_duration`, and `max_doublings`. It uses a
retry-count limit without configuring an additional retry-duration window.
Omitting it retains Cloud Scheduler's default retry behavior.

Retries cover failed HTTP requests. An application that returns HTTP 200 for an
internal failure must provide its own recovery. A long-running request or its
retries can delay or skip a subsequent scheduled execution.
