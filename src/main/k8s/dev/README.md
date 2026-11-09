# Dev Configuration

Configuration for Halo-managed cloud environments. This configuration can be
adapted and used as the basis for other environments.

## Kingdom internal API availability

The GKE Kingdom deployment runs two internal data-server replicas and prefers
placing them on different hostnames. A PodDisruptionBudget retains one available
replica during voluntary node drains. Provide capacity for the second replica
and for a replacement before a planned drain. The budget does not prevent
involuntary node loss.

## Access internal API availability

The development Reporting deployment runs two Access internal API replicas and
spreads them across hostnames, requiring two eligible node domains. A
PodDisruptionBudget retains one available replica during voluntary node drains.
Provide at least two eligible nodes; a replacement replica remains pending when
that capacity is unavailable. The budget does not prevent involuntary node loss.

Before rollout, validate the rendered Access Deployment and PodDisruptionBudget
with `kubectl apply --dry-run=server` against the target cluster. After rollout,
confirm two ready replicas on different nodes and one allowed disruption in the
budget. During a planned node drain, watch that one Access replica remains ready
and an authorization request through the public API succeeds. This exercises the
deployed readiness, eviction, and topology-spread behavior.

Reporting simulator BasicReport GETs retry HTTP 429, 500, 502, 503, and 504
responses with up to six calls to OkHttp. Delays double from the configured initial
polling delay and are capped by the maximum polling delay. Connection recovery
uses OkHttp's built-in policy; any final I/O failure propagates immediately.
Successful responses resume normal report polling; malformed responses and other
HTTP errors fail immediately. Final HTTP failures include the status and response
body when readable. An unreadable error body preserves the HTTP status and keeps
the read failure as the cause. Report creation is not retried by this read policy.
