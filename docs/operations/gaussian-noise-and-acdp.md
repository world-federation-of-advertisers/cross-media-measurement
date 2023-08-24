# Gaussian Noise and ACDP Composition in PBM

This guide describes how to enable Gaussian Differential Privacy(DP) noise for
Multi-Party Computation(MPC) and direct measurements, and Almost Concentrated
Differential Privacy(ACDP) composition mechanism in Privacy Budget Manager(PBM)
to improve privacy accounting. Gaussian noise and ACDP composition can support
more than 2x queries with the same privacy budget compared to Laplace noise and
Advanced composition in some parameter settings. It also enables the Measurement
Consumer to change their per-query epsilon, delta values arbitrarily, which
improves flexibility of privacy budgeting.

## Rollout Steps

Before starting the Kingdom rollout of MPC Gaussian noise, all EDP with PBM in
the deployment should support full ACDP implementation. Using the old
DP_ADVANCED composition method for Gaussian noise could incorrectly estimate the
privacy charges. All EDP with PBM using ACDP composition should support adding
continuous Gaussian noise to their direct measurements. The Gaussian noiser
implementation is provided in
`src/main/kotlin/org/wfanet/measurement/eventdataprovider/noiser/GaussianNoiser.kt`

To enable Gaussian noise and ACDP composition in the system:

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
3.  ACDP composition can only support continuous Gaussian noise for direct
    measurement. Ensure `CONTINUOUS_GAUSSIAN` is enabled for the Direct protocol
    in Kingdom. By default, both `CONTINUOUS_GAUSSIAN` and `CONTINUOUS_LAPLACE`
    are enabled.
4.  EDP with PBM to start using ACDP composition in their deployment. In the
    current PBM reference implementation, change `compositionMechanism` flag to
    `ACDP` in
    [EDPSimulatorFlags.kt](../../src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider/EdpSimulatorFlags.kt)
    to start using ACDP composition. This change also requires EDP to use
    Gaussian noise for direct measurements.

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

The changes can also be validated with logs in EDP simulator through integration
tests:

```shell
INFO:  chargeLiquidLegionsV2PrivacyBudget with ACDP composition mechanism for requisition with DISCRETE_GAUSSIAN noise mechanism...
```

```shell
INFO:  chargeDirectPrivacyBudget with ACDP composition mechanism...
```

## Kingdom Operator and EDP with PBM

For MPC Gaussian noise, the config files in the above Rollout Steps are part of
the testing secrets and are only used by the Halo dev environment. The Kingdom
operators write/maintain these config files themselves don't automatically get
updates when upgrading to a new release. The Kingdom operators need to update
their own llv2_protocol_config_config.textproto and
ro_llv2_protocol_config_config.textproto files in their K8s settings(1. and 2.
in Rollout Steps).

For EDP running their own PBM, they need to follow the reference implementation
provided in the PBM library and EDP simulator to implement the Gaussian noiser
for direct measurements and their own ACDP PBM.

## Emergency Rollback

The Gaussian DP noise and PBM ACDP composition features are designed with
backward compatibility. If there’s a need to roll back the changes, follow
Rollout Steps to revert back the config flags.

Note that in the PBM ACDP reference implementation, ACDP composition will write
privacy charges to a different table(PrivacyBucketAcdpCharges) from the current
PrivacyBucketCharges table. Rolling back on the EDP side has the effect of
dropping all the ACDP privacy charges.
