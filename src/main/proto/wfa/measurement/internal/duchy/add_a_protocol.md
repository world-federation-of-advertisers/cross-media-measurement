# How to Add a Prootcol

Definitions

*   **Protocol**: A Protocol is the end to end unit of work spread across all
    the MPC workers. Think of this as the algorithm described in a white-paper
*   **Computation**: A computation is a single run of the protocol.

## Overview

The computations backend system is implemented in a way that allows it many
pieces to be reused in support of a new protocol. This document lays out the
steps needed to add a new protocol to the system.

## Components

### Stages of a Computation

Protocols are divided into stages (which roughly correspond to rounds in an MPC
protocol spec). The stages of a protocol are defined in a proto3 enum. When
adding a new protocol, a new enum will need to be created. Typically there are
two types of stages WAIT_FOR_SOMETHING and DO_SOMETHING; where the local duchy
is either waiting on input from another duchy or executing a portiong of the
computation.

Source:

*   `src/main/proto/wfa/measurement/internal/sketch_aggregation_stage.proto`

### Computation Control gRPC Service

This is the service used for duchy to duchy communication to control the flow of
a computation. Being that a protocol is multi-party, this is the way in which
the parties running a computation interact with one another.

When adding a new protocol, any messages needed to encode rounds of
communication need to be added to the proto3 service definition, and then need
to be implemented in Kotlin.

Source:

*   `src/main/proto/wfa/measurement/system/v1alpha/computation_control_service.proto`
*   `src/main/kotlin/org/wfanet/measurement/duchy/service/system/v1alpha/LiquidLegionsComputationControlService.kt`

### Computation Storage gRPC Service

This is the service which sits on top of a duchy's relational database used for
tracking the progress of computations. Protocols can be added to this service by
adding the stages (as defined earlier) into ProtocolStage and defining the name
of the new Protocol in ProtocolType. The Kotlin implementation would need to
handle request of the given type.

Source:

*   `src/main/proto/wfa/measurement/internal/duchy/computation_protocols.proto`

```protobuf
// Stage of one of the supported protocols.
message ProtocolStage {
  oneof stage { // Stage of a sketch aggregation multi party computation.
    SketchAggregationStage sketch_aggregation = 1;

    // ...Add your new stages...
    NewProtocolStage new_protocol = 2;
  }
}

// The type of a protocol.
enum ProtocolType {
  // Not set intentionally.
  UNSPECIFIED = 0;
  SKETCH_AGGREGATION = 1;
  // ...Add your new protocol... NEW_PROTOCOL_NAME = 2;
}
```

*   `src/main/kotlin/org/wfanet/measurement/duchy/service/internal/computation/ComputationsService.kt`
    *   Should not require changing

### Mills

Mills are responsible for the CPU intensive part of a protocol. These read from
a local work queue via a Computations.ClaimWork RPC. This RPC is typed on
protocol so only work items for the given protocol will be returned.

When adding a new protocol, a new Mill will need to be created.

Example:

*   `src/main/kotlin/org/wfanet/measurement/duchy/mill/liquidlegionsv1/LiquidLegionsV1Mill.kt`

### Database Layers

The database abstraction layer is set up to take multiple protocols into
account. That is to say the computations backend database can support multiple
protocols. To use the backend class you will need to implement both

*   `interface ProtocolStageEnumHelper<StageT>`
*   `interface ProtocolStageDetails<StageT, StageDetailsT>`

for the new protocol. `StageT` is the stages enum defined earlier.
`StageDetailsT` is an object which encapsulates and details specific to a single
stage type. Both of these interfaces are pretty simple. Through these the valid
start and ending stages of the computation; which transitions are allowed; and
how to convert `StageT` to/from ByteArrays.

Example:

*   `src/main/kotlin/org/wfanet/measurement/db/duchy/SketchAggregationStages.kt`
*   `src/main/kotlin/org/wfanet/measurement/db/duchy/SketchAggregationStageDetails.kt`
