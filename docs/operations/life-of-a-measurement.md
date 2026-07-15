# Life of a Measurement

A high-level description of the lifecycle of a
[`Measurement`](https://github.com/world-federation-of-advertisers/cross-media-measurement-api/blob/main/src/main/proto/wfa/measurement/api/v2alpha/measurement.proto)
resource in the Cross-Media Measurement System (CMMS).

## Terminology

Kingdom
:   The component of the CMMS that orchestrates communication between parties.

Duchy
:   The component of the CMMS that is responsible for computation of results for
    protocols other than the Direct protocol. There may be more than one Duchy
    instance participating in a given computation.

Measurement Consumer
:   An entity which creates `Measurement`s and consumes their results. For
    example, a marketer or advertiser.

Data Provider
:   An entity which provides data to the CMMS in order to compute `Measurement`
    results. A Data Provider can either be an Event Data Provider or a
    Population Data Provider.

Event Data Provider
:   A Data Provider which provides data about specific events, such as
    impression events.

Population Data Provider
:   A Data Provider which provides data about populations.

Computation Protocol
:   How entities communicate to produce `Measurement` results.

## Creation

The process starts when a Measurement Consumer (MC) creates a `Measurement`
resource. This is commonly handled by some agent acting on the MC's behalf
triggered by some other action, such as an MC user creating a Report in a
Reporting user interface or via the Reporting API.

On creation, the Kingdom selects a computation protocol for the `Measurement`
which then starts in the `AWAITING_REQUISITION_FULFILLMENT` state.

## Cancellation

At any point after creation but before the `Measurement` enters a terminal
state, a Measurement Consumer may cancel a `Measurement`. This transitions the
`Measurement` to the `CANCELLED` state and all unfulfilled `Requisition`s for
that `Measurement` to the `WITHDRAWN` state.

## Ready for Fulfillment

### Direct Protocol

If the `Measurement` is using the Direct protocol, the resulting
[`Requisition`](https://github.com/world-federation-of-advertisers/cross-media-measurement-api/blob/main/src/main/proto/wfa/measurement/api/v2alpha/requisition.proto)
resources are immediately made available to Data Providers to fulfill. These may
be either Event Data Providers (EDPs) or Population Data Providers (PDPs)
depending on the type of `Measurement`.

### Duchy Protocols

Protocols other than the Direct protocol all involve the creation of a
`Computation`. This is the Duchy view of a `Measurement`. Depending on the
protocol, Duchies may need to do some initial Computation setup before
`Requisition`s can be made available for fulfillment.

## Fulfillment

Data Provider systems can see `Requisition` resources and either fulfill or
refuse them. For the Direct protocol, a Data Provider fulfills by sending data
to the Kingdom. For Duchy protocols, a Data Provider fulfills by sending data to
one or more Duchy instances.

### Refusal

When a Data Provider refuses a `Requisition`, they indicate a
[`Justification`](https://github.com/search?q=repo%3Aworld-federation-of-advertisers%2Fcross-media-measurement-api+symbol%3ARequisition%3A%3ARefusal%3A%3AJustification&type=code)
as well as a human-readable message. This results in the `Requisition` state
being transitioned to `REFUSED`. The `Measurement` state will be transitioned to
`FAILED` with a failure reason of `REQUISITION_REFUSED`, and all other
unfulfilled `Requisition`s for the `Measurement` will be transitioned to the
`WITHDRAWN` state.

## Computation

When all `Requisition`s for a `Measurement` have been fulfilled, the CMMS can
compute `Measurement` results. For the Direct protocol, there is no additional
work to be done as Data Providers compute results directly before fulfilling.
For Duchy protocols, the set of Duchy participants works together to transition
the `Computation` through all the stages of the protocol. Once computation
succeeds, the `Measurement` is transitioned to the `SUCCEEDED` state and will
have its `results` field set.

### Computation Failure

If there is a failure during computation at a participant Duchy, the
`Measurement` will be transitioned to the `FAILED` state with a failure reason
of `COMPUTATION_PARTICIPANT_FAILED`.

## Appendix

### Diagnosing Failure

When a `Measurement` is in the `FAILED` state, its `failure` field will be set.
This includes a `reason` field which provides insight into the reason the
`Measurement` failed, as well as a human-readable `message` field. The
conditions for each reason are described above.

If the reason is `REQUISITION_REFUSED`, an API caller can get more information
by listing the `Requisition`s for the `Measurement` in question and seeing which
one is in the `REFUSED` state. This `Requisition` will have its `failure` field
set as described above in the "Refusal" section.

If the reason is `COMPUTATION_PARTICIPANT_FAILED`, the CMMS operator may be able
to investigate further.
