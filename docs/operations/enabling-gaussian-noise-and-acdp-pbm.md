# Enabling Gaussian Noise and ACDP Composition in PBM

This guide describes how to enable Gaussian Differential Privacy(DP) noise for
Multi-Party Computation(MPC) and direct measurements, and Almost Concentrated
Differential Privacy(ACDP) composition mechanism in Privacy Budget Manager(PBM)
to improve privacy accounting. Gaussian noise and ACDP composition can support
more than 2x queries with the same privacy budget compared to Laplace noise and
advanced composition in some parameter settings. It also enables the Measurement
Consumer to change their per-query epsilon, delta values arbitrarily, which
improves flexibility of privacy budgeting.

## Steps

### Prerequisites

Before enabling Gaussian noise in the Kingdom, it is important to ensure that
all EDPs who implement a PBM are capable of accounting for Gaussian noise. They
should also have implemented ACDP, but if they have not, they should be
comfortable with Gaussian noise allowing fewer queries than Laplace noise.

All EDP with PBM using ACDP composition should support adding continuous
Gaussian noise to their direct measurements. The Gaussian noiser implementation
is provided in
`src/main/kotlin/org/wfanet/measurement/eventdataprovider/noiser/GaussianNoiser.kt`

### Configure MPC protocols to use discrete Gaussian noise in Kingdom

1.  Change `noise_mechanism` to `DISCRETE_GAUSSIAN` in the Kingdom's
    `Llv2ProtocolConfigConfig` for LLV2 based MPC reach and frequency
    measurement. In the `dev` configuration, this is read from the
    `llv2_protocol_config_config.textproto` file in the `certs-and-configs`
    Kubernetes secret.
2.  Change `noise_mechanism` to `DISCRETE_GAUSSIAN` in the Kingdom's
    `RoLlv2ProtocolConfigConfig` for LLV2 based MPC reach only measurement. In
    the `dev` configuration, this is read from the
    `ro_llv2_protocol_config_config.textproto` file in the `certs-and-configs`
    Kubernetes secret.

### Ensure continuous Gaussian noise for direct protocol in Kingdom

For direct measurements, ensure `CONTINUOUS_GAUSSIAN` is enabled for the Direct
protocol in Kingdom. By default, both `CONTINUOUS_GAUSSIAN` and
`CONTINUOUS_LAPLACE` are enabled. The prerequisite is EDPs reading possible
noise mechanisms from ProtocolConfig.

### EDP with ACDP PBM

1.  When Kingdom is configured to use discrete Gaussian noise for MPC
    requisitions, EDP with PBM should start using ACDP composition in their
    deployment. In EDP simulator, change `compositionMechanism` flag to `ACDP`
    in
    [EDPSimulatorFlags.kt](../../src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider/EdpSimulatorFlags.kt)
    to simulate using ACDP composition in a test environment.
2.  Changing to ACDP composition also requires EDP to use continuous Gaussian
    noise for direct measurements.
3.  Note that in the PBM ACDP reference implementation, ACDP composition will
    write privacy charges to a new table(PrivacyBucketAcdpCharges) which is
    effectively resetting the ledger.

## Verification

When PBM is using ACDP composition, and the `noise_mechanism` passed in
`ProtocolConfig.Protocol.LiquidLegionsV2` in MPC requisition is not
`DISCRETE_GAUSSIAN`, the requisition will be refused. The following code snippet
can be used to verify the `noise_mechanism`:

```kotlin
val llv2Protocol: ProtocolConfig.Protocol =
        requireNotNull(requisition.protocolConfig.protocolsList.find { protocol -> protocol.hasLiquidLegionsV2() })
val liquidLegionsV2: ProtocolConfig.LiquidLegionsV2 = llv2Protocol.liquidLegionsV2
val llv2NoiseMechanism = liquidLegionsV2.noiseMechanism
```

`llv2NoiseMechanism` should be `DISCRETE_GAUSSIAN`.

The noise and composition mechanisms changes can also be validated with logs in
EDP simulator through integration tests:

```shell
INFO:  chargeLiquidLegionsV2PrivacyBudget with ACDP composition mechanism for requisition with DISCRETE_GAUSSIAN noise mechanism...
```

```shell
INFO:  chargeDirectPrivacyBudget with ACDP composition mechanism...
```

## Kingdom Operator and EDP with PBM

The Kingdom operators need to update their own config files
llv2_protocol_config_config.textproto and
ro_llv2_protocol_config_config.textproto files in their K8s settings(Configure
MPC protocols to use discrete Gaussian noise in Kingdom in Steps).

For EDP running their own PBM, they need to follow the reference implementation
provided in the PBM library to implement their own ACDP PBM. EDP using ACDP
composition should support adding continuous Gaussian noise to their direct
measurements. The Gaussian noiser implementation is provided in
`src/main/kotlin/org/wfanet/measurement/eventdataprovider/noiser/GaussianNoiser.kt`

## Emergency Rollback

The Gaussian noise and PBM ACDP composition features are designed with backward
compatibility. If there’s a need to roll back the changes, follow Steps to
revert back the configs.

Note that in the PBM ACDP reference implementation, ACDP composition will write
privacy charges to a different table(PrivacyBucketAcdpCharges) from the current
PrivacyBucketCharges table. Rolling back on the EDP side has the effect of
dropping all the ACDP privacy charges.
