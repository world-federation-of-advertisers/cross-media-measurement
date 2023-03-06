// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.service.api.v2alpha

import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKt
import org.wfanet.measurement.api.v2alpha.ExchangeStepKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry
import org.wfanet.measurement.api.v2alpha.Measurement.Failure
import org.wfanet.measurement.api.v2alpha.Measurement.State
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultPair
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.Direct
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKey
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.direct
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.protocol
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.exchange
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.api.v2alpha.exchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.internal.kingdom.DifferentialPrivacyParams as InternalDifferentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.EventGroup as InternalEventGroup
import org.wfanet.measurement.internal.kingdom.Exchange as InternalExchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt as InternalExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow as InternalExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflowKt
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.Measurement.DataProviderValue
import org.wfanet.measurement.internal.kingdom.MeasurementKt.details
import org.wfanet.measurement.internal.kingdom.ProtocolConfig as InternalProtocolConfig
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.exchangeWorkflow
import org.wfanet.measurement.internal.kingdom.measurement as internalMeasurement
import org.wfanet.measurement.internal.kingdom.protocolConfig as internalProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig

/** Converts an internal [InternalMeasurement.State] to a public [State]. */
fun InternalMeasurement.State.toState(): State =
  when (this) {
    InternalMeasurement.State.PENDING_REQUISITION_PARAMS,
    InternalMeasurement.State.PENDING_REQUISITION_FULFILLMENT ->
      State.AWAITING_REQUISITION_FULFILLMENT
    InternalMeasurement.State.PENDING_PARTICIPANT_CONFIRMATION,
    InternalMeasurement.State.PENDING_COMPUTATION -> State.COMPUTING
    InternalMeasurement.State.SUCCEEDED -> State.SUCCEEDED
    InternalMeasurement.State.FAILED -> State.FAILED
    InternalMeasurement.State.CANCELLED -> State.CANCELLED
    InternalMeasurement.State.STATE_UNSPECIFIED,
    InternalMeasurement.State.UNRECOGNIZED -> State.STATE_UNSPECIFIED
  }

/** Convert a public [State] to an internal [InternalMeasurement.State]. */
fun State.toInternalState(): List<InternalMeasurement.State> {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (this) {
    State.AWAITING_REQUISITION_FULFILLMENT -> {
      listOf(
        InternalMeasurement.State.PENDING_REQUISITION_PARAMS,
        InternalMeasurement.State.PENDING_REQUISITION_FULFILLMENT
      )
    }
    State.COMPUTING -> {
      listOf(
        InternalMeasurement.State.PENDING_PARTICIPANT_CONFIRMATION,
        InternalMeasurement.State.PENDING_COMPUTATION
      )
    }
    State.SUCCEEDED -> listOf(InternalMeasurement.State.SUCCEEDED)
    State.FAILED -> listOf(InternalMeasurement.State.FAILED)
    State.CANCELLED -> listOf(InternalMeasurement.State.CANCELLED)
    State.STATE_UNSPECIFIED,
    State.UNRECOGNIZED -> listOf(InternalMeasurement.State.STATE_UNSPECIFIED)
  }
}

/** Converts an internal [InternalMeasurement.Failure.Reason] to a public [Failure.Reason]. */
fun InternalMeasurement.Failure.Reason.toReason(): Failure.Reason =
  when (this) {
    InternalMeasurement.Failure.Reason.CERTIFICATE_REVOKED -> Failure.Reason.CERTIFICATE_REVOKED
    InternalMeasurement.Failure.Reason.REQUISITION_REFUSED -> Failure.Reason.REQUISITION_REFUSED
    InternalMeasurement.Failure.Reason.COMPUTATION_PARTICIPANT_FAILED ->
      Failure.Reason.COMPUTATION_PARTICIPANT_FAILED
    InternalMeasurement.Failure.Reason.REASON_UNSPECIFIED,
    InternalMeasurement.Failure.Reason.UNRECOGNIZED -> Failure.Reason.REASON_UNSPECIFIED
  }

fun InternalDifferentialPrivacyParams.toDifferentialPrivacyParams(): DifferentialPrivacyParams {
  val source = this
  return differentialPrivacyParams {
    epsilon = source.epsilon
    delta = source.delta
  }
}

fun InternalProtocolConfig.toProtocolConfig(
  measurementTypeCase: MeasurementSpec.MeasurementTypeCase,
  dataProviderCount: Int,
): ProtocolConfig {
  require(dataProviderCount >= 1) { "Expected at least one DataProvider" }

  val source = this
  return protocolConfig {
    name =
      if (source.externalProtocolConfigId.isNotEmpty())
        ProtocolConfigKey(source.externalProtocolConfigId).toName()
      else ProtocolConfigKey(Direct.getDescriptor().name).toName()

    measurementType =
      when (measurementTypeCase) {
        MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
          throw IllegalArgumentException("Measurement type not specified")
        MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
          ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
        MeasurementSpec.MeasurementTypeCase.IMPRESSION -> ProtocolConfig.MeasurementType.IMPRESSION
        MeasurementSpec.MeasurementTypeCase.DURATION -> ProtocolConfig.MeasurementType.DURATION
      }

    when (measurementType) {
      ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY -> {
        if (dataProviderCount == 1) {
          protocols += protocol { direct = direct {} }
        } else {
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields are never null.
          when (source.protocolCase) {
            InternalProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 -> {
              protocols += protocol {
                liquidLegionsV2 = liquidLegionsV2 {
                  if (source.liquidLegionsV2.hasSketchParams()) {
                    val sourceSketchParams = source.liquidLegionsV2.sketchParams
                    sketchParams = liquidLegionsSketchParams {
                      decayRate = sourceSketchParams.decayRate
                      maxSize = sourceSketchParams.maxSize
                      samplingIndicatorSize = sourceSketchParams.samplingIndicatorSize
                    }
                  }
                  if (source.liquidLegionsV2.hasDataProviderNoise()) {
                    dataProviderNoise =
                      source.liquidLegionsV2.dataProviderNoise.toDifferentialPrivacyParams()
                  }
                  ellipticCurveId = source.liquidLegionsV2.ellipticCurveId
                  maximumFrequency = source.liquidLegionsV2.maximumFrequency
                }
              }
            }
            InternalProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Protocol not specified")
          }
        }
      }
      ProtocolConfig.MeasurementType.IMPRESSION,
      ProtocolConfig.MeasurementType.DURATION -> {
        protocols += protocol { direct = direct {} }
      }
      ProtocolConfig.MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED,
      ProtocolConfig.MeasurementType.UNRECOGNIZED -> error("Invalid MeasurementType")
    }
  }
}

/** Converts an internal [InternalMeasurement] to a public [Measurement]. */
fun InternalMeasurement.toMeasurement(): Measurement {
  val source = this
  check(Version.fromString(source.details.apiVersion) == Version.V2_ALPHA) {
    "Incompatible API version ${source.details.apiVersion}"
  }
  return measurement {
    name =
      MeasurementKey(
          externalIdToApiId(source.externalMeasurementConsumerId),
          externalIdToApiId(source.externalMeasurementId)
        )
        .toName()
    measurementConsumerCertificate =
      MeasurementConsumerCertificateKey(
          externalIdToApiId(source.externalMeasurementConsumerId),
          externalIdToApiId(source.externalMeasurementConsumerCertificateId)
        )
        .toName()
    measurementSpec = signedData {
      data = source.details.measurementSpec
      signature = source.details.measurementSpecSignature
    }
    dataProviders +=
      source.dataProvidersMap.entries.map(Map.Entry<Long, DataProviderValue>::toDataProviderEntry)

    val measurementTypeCase =
      MeasurementSpec.parseFrom(this.measurementSpec.data).measurementTypeCase

    protocolConfig =
      source.details.protocolConfig.toProtocolConfig(
        measurementTypeCase,
        dataProvidersCount,
      )

    state = source.state.toState()
    results +=
      source.resultsList.map {
        val certificateApiId = externalIdToApiId(it.externalCertificateId)
        resultPair {
          if (it.externalAggregatorDuchyId.isNotBlank()) {
            certificate =
              DuchyCertificateKey(it.externalAggregatorDuchyId, certificateApiId).toName()
          } else if (it.externalDataProviderId != 0L) {
            certificate =
              DataProviderCertificateKey(
                  externalIdToApiId(it.externalDataProviderId),
                  certificateApiId
                )
                .toName()
          }
          encryptedResult = it.encryptedResult
        }
      }
    measurementReferenceId = source.providedMeasurementId
    failure = failure {
      reason = source.details.failure.reason.toReason()
      message = source.details.failure.message
    }
  }
}

/** Converts an internal [DataProviderValue] to a public [DataProviderEntry.Value]. */
fun DataProviderValue.toDataProviderEntryValue(dataProviderId: String): DataProviderEntry.Value {
  val dataProviderValue = this
  return DataProviderEntryKt.value {
    dataProviderCertificate =
      DataProviderCertificateKey(
          dataProviderId,
          externalIdToApiId(externalDataProviderCertificateId)
        )
        .toName()
    dataProviderPublicKey = signedData {
      data = dataProviderValue.dataProviderPublicKey
      signature = dataProviderPublicKeySignature
    }
    encryptedRequisitionSpec = dataProviderValue.encryptedRequisitionSpec
    nonceHash = dataProviderValue.nonceHash
  }
}

/** Converts an internal data provider map entry to a public [DataProviderEntry]. */
fun Map.Entry<Long, DataProviderValue>.toDataProviderEntry(): DataProviderEntry {
  val mapEntry = this
  return dataProviderEntry {
    key = DataProviderKey(externalIdToApiId(mapEntry.key)).toName()
    value = mapEntry.value.toDataProviderEntryValue(externalIdToApiId(mapEntry.key))
  }
}

/**
 * Converts a public [Measurement] to an internal [InternalMeasurement] for creation.
 *
 * @throws [IllegalStateException] if MeasurementType not specified
 */
fun Measurement.toInternal(
  measurementConsumerCertificateKey: MeasurementConsumerCertificateKey,
  dataProvidersMap: Map<Long, DataProviderValue>,
  measurementSpecProto: MeasurementSpec
): InternalMeasurement {
  val publicMeasurement = this

  return internalMeasurement {
    providedMeasurementId = measurementReferenceId
    externalMeasurementConsumerId =
      apiIdToExternalId(measurementConsumerCertificateKey.measurementConsumerId)
    externalMeasurementConsumerCertificateId =
      apiIdToExternalId(measurementConsumerCertificateKey.certificateId)
    dataProviders.putAll(dataProvidersMap)
    details = details {
      apiVersion = Version.V2_ALPHA.string
      measurementSpec = publicMeasurement.measurementSpec.data
      measurementSpecSignature = publicMeasurement.measurementSpec.signature

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (measurementSpecProto.measurementTypeCase) {
        MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
          if (dataProvidersCount > 1) {
            protocolConfig = internalProtocolConfig {
              externalProtocolConfigId = Llv2ProtocolConfig.name
              liquidLegionsV2 = Llv2ProtocolConfig.protocolConfig
            }
            duchyProtocolConfig = duchyProtocolConfig {
              liquidLegionsV2 = Llv2ProtocolConfig.duchyProtocolConfig
            }
          }
        }
        MeasurementSpec.MeasurementTypeCase.IMPRESSION,
        MeasurementSpec.MeasurementTypeCase.DURATION, -> {}
        MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
          error("MeasurementType not set.")
      }
    }
  }
}

/** @throws [IllegalStateException] if InternalExchange.State not specified */
fun InternalExchange.toV2Alpha(): Exchange {
  val exchangeKey =
    ExchangeKey(
      dataProviderId = null,
      modelProviderId = null,
      recurringExchangeId = externalIdToApiId(externalRecurringExchangeId),
      exchangeId = date.toLocalDate().toString()
    )
  return exchange {
    name = exchangeKey.toName()
    date = this@toV2Alpha.date
    state = v2AlphaState
    auditTrailHash = details.auditTrailHash
    // TODO(@yunyeng): Add graphvizRepresentation to Exchange proto.
    graphvizRepresentation = ""
  }
}

private val InternalExchange.v2AlphaState: Exchange.State
  get() {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (this.state) {
      InternalExchange.State.ACTIVE -> Exchange.State.ACTIVE
      InternalExchange.State.SUCCEEDED -> Exchange.State.SUCCEEDED
      InternalExchange.State.FAILED -> Exchange.State.FAILED
      InternalExchange.State.STATE_UNSPECIFIED,
      InternalExchange.State.UNRECOGNIZED -> error("Invalid InternalExchange state.")
    }
  }

/** @throws [IllegalStateException] if InternalExchangeStep.State not specified */
fun InternalExchangeStep.toV2Alpha(): ExchangeStep {
  val exchangeStepKey =
    ExchangeStepKey(
      recurringExchangeId = externalIdToApiId(externalRecurringExchangeId),
      exchangeId = date.toLocalDate().toString(),
      exchangeStepId = stepIndex.toString()
    )
  return exchangeStep {
    name = exchangeStepKey.toName()
    state = v2AlphaState
    stepIndex = this@toV2Alpha.stepIndex
    exchangeDate = date
    serializedExchangeWorkflow = this@toV2Alpha.serializedExchangeWorkflow
  }
}

/** @throws [IllegalStateException] if State not specified */
fun ExchangeStepAttempt.State.toInternal(): InternalExchangeStepAttempt.State {
  return when (this) {
    ExchangeStepAttempt.State.STATE_UNSPECIFIED,
    ExchangeStepAttempt.State.UNRECOGNIZED -> error("Invalid ExchangeStepAttempt state: $this")
    ExchangeStepAttempt.State.ACTIVE -> InternalExchangeStepAttempt.State.ACTIVE
    ExchangeStepAttempt.State.SUCCEEDED -> InternalExchangeStepAttempt.State.SUCCEEDED
    ExchangeStepAttempt.State.FAILED -> InternalExchangeStepAttempt.State.FAILED
    ExchangeStepAttempt.State.FAILED_STEP -> InternalExchangeStepAttempt.State.FAILED_STEP
  }
}

fun Iterable<ExchangeStepAttempt.DebugLog>.toInternal():
  Iterable<ExchangeStepAttemptDetails.DebugLog> {
  return map { apiProto ->
    ExchangeStepAttemptDetailsKt.debugLog {
      time = apiProto.time
      message = apiProto.message
    }
  }
}

/** @throws [IllegalStateException] if InternalExchangeStepAttempt.State not specified */
fun InternalExchangeStepAttempt.toV2Alpha(): ExchangeStepAttempt {
  val key =
    ExchangeStepAttemptKey(
      recurringExchangeId = externalIdToApiId(externalRecurringExchangeId),
      exchangeId = date.toLocalDate().toString(),
      exchangeStepId = stepIndex.toString(),
      exchangeStepAttemptId = this@toV2Alpha.attemptNumber.toString()
    )
  return exchangeStepAttempt {
    name = key.toName()
    attemptNumber = this@toV2Alpha.attemptNumber
    state = this@toV2Alpha.state.toV2Alpha()
    debugLogEntries += details.debugLogEntriesList.map { it.toV2Alpha() }
    startTime = details.startTime
    updateTime = details.updateTime
  }
}

/** @throws [IllegalStateException] if State not specified */
fun ExchangeStep.State.toInternal(): InternalExchangeStep.State {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (this) {
    ExchangeStep.State.BLOCKED -> InternalExchangeStep.State.BLOCKED
    ExchangeStep.State.READY -> InternalExchangeStep.State.READY
    ExchangeStep.State.READY_FOR_RETRY -> InternalExchangeStep.State.READY_FOR_RETRY
    ExchangeStep.State.IN_PROGRESS -> InternalExchangeStep.State.IN_PROGRESS
    ExchangeStep.State.SUCCEEDED -> InternalExchangeStep.State.SUCCEEDED
    ExchangeStep.State.FAILED -> InternalExchangeStep.State.FAILED
    ExchangeStep.State.STATE_UNSPECIFIED,
    ExchangeStep.State.UNRECOGNIZED -> error("Invalid ExchangeStep state: $this")
  }
}

private fun ExchangeStepAttemptDetails.DebugLog.toV2Alpha(): ExchangeStepAttempt.DebugLog {
  return ExchangeStepAttemptKt.debugLog {
    time = this@toV2Alpha.time
    message = this@toV2Alpha.message
  }
}

private fun InternalExchangeStepAttempt.State.toV2Alpha(): ExchangeStepAttempt.State {
  return when (this) {
    InternalExchangeStepAttempt.State.STATE_UNSPECIFIED,
    InternalExchangeStepAttempt.State.UNRECOGNIZED ->
      error("Invalid InternalExchangeStepAttempt state: $this")
    InternalExchangeStepAttempt.State.ACTIVE -> ExchangeStepAttempt.State.ACTIVE
    InternalExchangeStepAttempt.State.SUCCEEDED -> ExchangeStepAttempt.State.SUCCEEDED
    InternalExchangeStepAttempt.State.FAILED -> ExchangeStepAttempt.State.FAILED
    InternalExchangeStepAttempt.State.FAILED_STEP -> ExchangeStepAttempt.State.FAILED_STEP
  }
}

private val InternalExchangeStep.v2AlphaState: ExchangeStep.State
  get() {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (this.state) {
      InternalExchangeStep.State.BLOCKED -> ExchangeStep.State.BLOCKED
      InternalExchangeStep.State.READY -> ExchangeStep.State.READY
      InternalExchangeStep.State.READY_FOR_RETRY -> ExchangeStep.State.READY_FOR_RETRY
      InternalExchangeStep.State.IN_PROGRESS -> ExchangeStep.State.IN_PROGRESS
      InternalExchangeStep.State.SUCCEEDED -> ExchangeStep.State.SUCCEEDED
      InternalExchangeStep.State.FAILED -> ExchangeStep.State.FAILED
      InternalExchangeStep.State.STATE_UNSPECIFIED,
      InternalExchangeStep.State.UNRECOGNIZED -> error("Invalid InternalExchangeStep state: $this")
    }
  }

/** @throws [IllegalArgumentException] if step dependencies invalid */
fun ExchangeWorkflow.toInternal(): InternalExchangeWorkflow {
  val labelsMap = mutableMapOf<String, MutableSet<Pair<String, Int>>>()
  for ((index, step) in stepsList.withIndex()) {
    for (outputLabel in step.outputLabelsMap.values) {
      labelsMap.getOrPut(outputLabel) { mutableSetOf() }.add(step.stepId to index)
    }
  }
  val internalSteps =
    stepsList.mapIndexed { index, step ->
      ExchangeWorkflowKt.step {
        stepIndex = index
        party = step.party.toInternal()
        prerequisiteStepIndices +=
          step.inputLabelsMap.values
            .flatMap { value ->
              val prerequisites = labelsMap.getOrDefault(value, emptyList())
              prerequisites.forEach { (stepId, stepIndex) ->
                require(step.hasCopyFromPreviousExchangeStep() || stepIndex < index) {
                  "Step ${step.stepId} with index $index cannot depend on step $stepId with" +
                    " index $stepIndex. To depend on another step, the index must be greater than" +
                    " the prerequisite step"
                }
              }
              prerequisites.map { it.second }
            }
            .filter { it < index }
            .toSet()
      }
    }

  return exchangeWorkflow { steps += internalSteps }
}

/** @throws [IllegalArgumentException] if Provider not specified */
fun ExchangeWorkflow.Party.toInternal(): InternalExchangeWorkflow.Party {
  return when (this) {
    ExchangeWorkflow.Party.DATA_PROVIDER -> InternalExchangeWorkflow.Party.DATA_PROVIDER
    ExchangeWorkflow.Party.MODEL_PROVIDER -> InternalExchangeWorkflow.Party.MODEL_PROVIDER
    ExchangeWorkflow.Party.PARTY_UNSPECIFIED,
    ExchangeWorkflow.Party.UNRECOGNIZED ->
      throw IllegalArgumentException("Provider is not set for the Exchange Step.")
  }
}

fun InternalEventGroup.State.toV2Alpha(): EventGroup.State {
  return when (this) {
    InternalEventGroup.State.STATE_UNSPECIFIED -> EventGroup.State.STATE_UNSPECIFIED
    InternalEventGroup.State.ACTIVE -> EventGroup.State.ACTIVE
    InternalEventGroup.State.DELETED -> EventGroup.State.DELETED
    InternalEventGroup.State.UNRECOGNIZED -> error("Invalid InternalEventGroup state: $this")
  }
}
