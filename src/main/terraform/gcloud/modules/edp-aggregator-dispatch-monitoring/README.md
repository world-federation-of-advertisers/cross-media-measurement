# VID dispatch failure monitoring

Creates two log-based DELTA counters and alerts for terminal dispatch failure and
confirmed retry exhaustion. The generic listener's exhaustion event is emitted
after FAILED state is recorded and before its DLQ message is acknowledged, so
the signal remains observable when the DLQ drains.

The dispatcher emits `edpa.vid_labeling.dispatch.work_item` with outcome `failed`.
The listener emits `secure_computation.work_item.retry_exhausted` with outcome
`retry_exhausted` and `xmm.pubsub.subscription.id`. Counters keep individual
WorkItem names, generations, attempts, source hashes and errors in diagnostic
logs rather than metric labels. Stale/terminal ignored deliveries and transient
attempt errors do not match these filters.

Both alerts sum the DELTA counts over five minutes and open an incident when at
least one event is observed. Logging ingestion can be delayed; no exact delivery
or notification deadline is promised.

Pass approved notification channels when configured. Empty channels create
incidents without notifications. Channel ownership, configuration, receipt
verification and dispatch cutover readiness remain tracked by
[issue #4912](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/4912).
Do not activate queued VID dispatch until that readiness is verified.
