# Dev Configuration

Configuration for Halo-managed cloud environments. This configuration can be
adapted and used as the basis for other environments.

## Access internal API availability

The development Reporting deployment runs two Access internal API replicas and
spreads them across hostnames, requiring two eligible node domains. A
PodDisruptionBudget retains one available replica during voluntary node drains.
Provide at least two eligible nodes; a replacement replica remains pending when
that capacity is unavailable. The budget does not prevent involuntary node loss.

Reporting simulator BasicReport GETs retry HTTP 429, 500, 502, 503, and 504
responses with up to six calls to OkHttp. Delays double from the configured initial
polling delay and are capped by the maximum polling delay. Connection recovery
uses OkHttp's built-in policy; any final I/O failure propagates immediately.
Successful responses resume normal report polling; malformed responses and other
HTTP errors fail immediately. Report creation is not retried by this read policy.
