# Enabling Reach-Only LiquidLegionsV2 Protocol

This guide describes steps to enable Reach-Only LLV2 protocol. Kingdom, Duchies,
and EDPs need to deploy these operations.

## EDP Fulfills Reach-Only Sketches

Reach-only llv2 protocol uses different format of sketches. EDPs should generate
sketches according to the protocol.

**Note: if an EDP ignores the protocol thus fulfills reach-only llv2 protocol
with old llv2 sketches, the result will be wrong and this error is unable to be
detected.**

### Check Requisition Protocol

For each Requisition, EDP should check the protocol to determine whether it is a
direct requisition, LiquidLegionsV2 protocol or ReachOnlyLiquidLegionsV2
protocol.

A new format of sketch is required for the ReachOnlyLiquidLegionsV2 protocol
described in the next section.

#### Generate Reach Only Sketches

The difference between llv2 sketch and reach-only llv2 sketch is that reach-only
sketches only have 1 field in a register, but llv2 sketches have two extra
fields for each register.

The change can be easily done by using an
[updated `SketchConfig`](../../src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider/EdpSimulator.kt#L1029).
Deleting the extra fields from LiquidLegionsV2’s SketchConfig can make the
ReachOnlyLiquidLegionsV2’s SketchConfig. The example of updated SketchConfig
[here](../../src/main/kotlin/org/wfanet/measurement/loadtest/dataprovider/SketchGenerator.kt#L100).
The logic of code does not need to change because given the updated
SketchConfig, the extra fields of LiquidLegionsV2’s sketch will be ignored.
However, it is welcomed to update the code to reduce unnecessary processing.
Since the `index` already has the attribution of `UNIQUE`, it will automatically
resolve duplicate insertion.

## Duchies Update Deployment

All duchies update the binary image to support the reach-only llv2 protocol.
Release 0.4.0 or later version contains the new feature.

## Kingdom Flips the Feature Flag

Kingdom updates the binary image to support the reach-only llv2 protocol.
Release 0.4.0 or later version contains the new feature.

Add flags
[`--enable-ro-llv2-protocol`](../../src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server/V2alphaPublicApiServer.kt#L223)
into deployment
[`v2alpha-public-api-server`](../../src/main/k8s/kingdom.cue#L172). Restarting
the kingdom will apply the new protocol for new Reach measurements.
