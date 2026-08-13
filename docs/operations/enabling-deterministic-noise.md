# Enabling Deterministic Noise for TrusTEE

This guide describes how to enable the `DETERMINISTIC_TRUNCATED_LAPLACE` noise
mechanism for TrusTEE reach and frequency measurements.

The noise is drawn inside the TEE from a seed derived from the aggregated
frequency vector and the number of contributions to it. Repeating a query over
unchanged data returns the same result rather than a fresh draw, so the noise
cannot be averaged away. Changing the underlying data changes the seed, and with
it the result.

The privacy parameters are compiled into the attested TrusTEE image rather than
taken from the `MeasurementSpec`. `reach_dp_params` and `frequency_dp_params`
are still carried on the `MeasurementSpec` and still reach the Duchy, but the
TEE discards them for this mechanism, so the epsilon a Measurement Consumer sets
has no effect on the result.

## Steps

### Prerequisites

TrusTEE must already be enabled. See
[Enabling TrusTEE in Kingdom and Duchy](enabling-trustee-in-kingdom-and-duchy.md)
and [Enabling TrusTEE on EDP](enabling-trustee-on-edp.md).

The mechanism is gated on EDP capability. The Kingdom offers it only when
*every* `DataProvider` on a `Measurement` declares support, so a single EDP that
has not been updated moves the whole `Measurement` onto another mechanism or
another protocol.

### 1. Upgrade Duchies

Upgrade all Duchies to a release that includes the mechanism before configuring
the Kingdom to select it.

The mechanism is carried to the Duchy as a new value in the system API
`NoiseMechanism` enum. A Duchy running an older build fails with
`Invalid system NoiseMechanism` when it receives a computation that names it.

### 2. Accept the mechanism in requisition fulfillment

The mechanism does not change what an EDP sends. For TrusTEE the EDP encrypts
the same frequency vector to the TEE, which draws the noise itself. What changes
is whether the EDP's fulfillment path accepts a requisition carrying the
mechanism.

Complete this step before step 3.

#### EDPs using the EDP Aggregator

An EDP that populates `multi_party_config.supported_noise_types` in its
`ResultsFulfillerParams` must add the mechanism to that list:

```textproto
multi_party_config {
  supported_noise_types: CONTINUOUS_GAUSSIAN
  supported_noise_types: DETERMINISTIC_TRUNCATED_LAPLACE
}
```

When the list is populated, the EDP Aggregator refuses any HMSS or TrusTEE
requisition whose stamped mechanism is not in it. When the list is empty, no
multi-party mechanism validation is performed and nothing needs to change.

Redeploy the fulfiller after changing this.

Note that `noise_params.noise_type` is a different setting that applies only to
direct (single EDP) measurements. It does not affect TrusTEE.

#### EDPs with their own implementation

An EDP that fulfills requisitions with its own implementation has no
`ResultsFulfillerParams`. Nothing in the protocol requires a change: the
frequency vector sent for a TrusTEE requisition is the same under every
mechanism.

If the implementation validates the mechanism on
`ProtocolConfig.Protocol.TrusTee.noise_mechanism` before fulfilling, it must
accept `DETERMINISTIC_TRUNCATED_LAPLACE`, otherwise it will refuse every
affected requisition once the Kingdom starts selecting it.

### 3. Declare the EDP capability

Set the capability on the `DataProvider` resource with
`ReplaceDataProviderCapabilities`, or from the CLI:

```shell
MeasurementSystem \
  --tls-cert-file=secretfiles/edp1_tls.pem \
  --tls-key-file=secretfiles/edp1_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  data-providers --name=dataProviders/AAAAAAAAAHs \
  update-capabilities --noise-mechanism-deterministic-truncated-laplace-supported
```

Only the capabilities named on the command line are changed.

Steps 2 and 3 must agree. The capability is a commitment to the Kingdom, which
starts routing measurements as soon as every EDP on them has made it. Declaring
it while the fulfillment path still rejects the mechanism, whether through a
populated `supported_noise_types` that omits it or a custom implementation that
validates against it, causes every affected requisition to be refused.

### 4. Configure the mechanism in the Kingdom

Set `noise_mechanism` in the Kingdom's `TrusTeeProtocolConfigConfig`. In the
`dev` configuration this is read from the
`trustee_protocol_config_config.textproto` file in the `certs-and-configs`
Kubernetes secret.

```textproto
protocol_config {
  noise_mechanism: DETERMINISTIC_TRUNCATED_LAPLACE
  result_minimum_thresholds {
    min_users: 100
    min_impressions: 500
  }
}
duchy_id: "aggregator"
```

With this configuration, a `Measurement` whose EDPs do not all declare the
capability is not offered TrusTEE at all, and selection falls through to HMSS
and then LLv2.

To offer TrusTEE under a different mechanism in that case, configure an ordered
fallback:

```textproto
protocol_config {
  noise_mechanism: DETERMINISTIC_TRUNCATED_LAPLACE
  result_minimum_thresholds {
    min_users: 100
    min_impressions: 500
  }
}
duchy_id: "aggregator"
fallback_noise_mechanisms: CONTINUOUS_GAUSSIAN
```

The Kingdom tries `protocol_config.noise_mechanism` first, then each entry in
`fallback_noise_mechanisms` in order, and stamps the first one every EDP
supports. Only the mechanism varies; `result_minimum_thresholds` is stamped as
written, so it does not depend on EDP capabilities.

`CONTINUOUS_GAUSSIAN` is assumed to be supported by every EDP and needs no
capability. It still has to appear in each EDP's `supported_noise_types` if that
list is populated: a fallback requisition is refused by any EDP whose list omits
the mechanism the Kingdom stamped, including EDPs that do have the deterministic
capability.

The config is read once at startup. Restart the Kingdom to apply a change.

## Verification

The selected mechanism is stamped on the `Measurement`:

```kotlin
val trusTeeProtocol: ProtocolConfig.Protocol =
    requireNotNull(requisition.protocolConfig.protocolsList.find { it.hasTrusTee() })
val noiseMechanism = trusTeeProtocol.trusTee.noiseMechanism
```

`noiseMechanism` should be `DETERMINISTIC_TRUNCATED_LAPLACE`.

The `ProtocolConfig` can also be inspected in `MeasurementDetails` in the
`Measurements` table in the Kingdom.

A `Measurement` that did not get this mechanism failed one of the gates, in the
order the Kingdom applies them:

1.  TrusTEE is not enabled for the `MeasurementConsumer`, via `--enable-trustee`
    or `--trustee-enabled-measurement-consumers`. The `Measurement` lands on
    HMSS or LLv2.
2.  Some EDP has `trus_tee_supported` false. Same outcome.
3.  Some EDP has `noise_mechanism_deterministic_truncated_laplace_supported`
    false. The `Measurement` lands on TrusTEE under a
    `fallback_noise_mechanisms` entry, or on the next protocol if no fallback is
    configured.

Compare `DataProvider.capabilities` across every EDP on the `Measurement` to
find which one is missing a capability.

## Emergency Rollback

Roll back in the reverse order of enablement, so that nothing is selecting the
mechanism before support for it is withdrawn.

1.  Change `noise_mechanism` in the Kingdom's `TrusTeeProtocolConfigConfig` back
    to the previous mechanism and restart the Kingdom. In-flight computations
    that already carry the mechanism continue under it.
2.  EDPs withdraw the capability with `update-capabilities
    --noise-mechanism-deterministic-truncated-laplace-supported=false`.
3.  EDPs using the EDP Aggregator remove the mechanism from
    `supported_noise_types` and redeploy. EDPs with their own implementation
    revert whatever they changed in step 2, if anything.

Duchies do not need to be rolled back. A Duchy that understands the mechanism
handles the previous mechanisms unchanged.
