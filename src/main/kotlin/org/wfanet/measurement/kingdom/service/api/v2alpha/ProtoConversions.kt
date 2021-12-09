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

import io.grpc.Status
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
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
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKey
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.exchange
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.api.v2alpha.exchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.internal.kingdom.DifferentialPrivacyParams as InternalDifferentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Exchange as InternalExchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt as ImternalExchangeStepAttempt
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
    InternalMeasurement.State.STATE_UNSPECIFIED, InternalMeasurement.State.UNRECOGNIZED ->
      State.STATE_UNSPECIFIED
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
    State.STATE_UNSPECIFIED, State.UNRECOGNIZED ->
      listOf(InternalMeasurement.State.STATE_UNSPECIFIED)
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

fun InternalProtocolConfig.toProtocolConfig(): ProtocolConfig {
  val source = this
  return protocolConfig {
    name = ProtocolConfigKey(source.externalProtocolConfigId).toName()
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    measurementType =
      when (source.measurementType) {
        InternalProtocolConfig.MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED ->
          ProtocolConfig.MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED
        InternalProtocolConfig.MeasurementType.REACH_AND_FREQUENCY ->
          ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
        InternalProtocolConfig.MeasurementType.UNRECOGNIZED ->
          error("MeasurementType unrecognized.")
      }
    if (source.hasLiquidLegionsV2()) {
      liquidLegionsV2 =
        liquidLegionsV2 {
          if (source.liquidLegionsV2.hasSketchParams()) {
            val sourceSketchParams = source.liquidLegionsV2.sketchParams
            sketchParams =
              liquidLegionsSketchParams {
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
    measurementSpec =
      signedData {
        data = source.details.measurementSpec
        signature = source.details.measurementSpecSignature
      }
    dataProviders +=
      source.dataProvidersMap.entries.map(Map.Entry<Long, DataProviderValue>::toDataProviderEntry)
    protocolConfig = source.details.protocolConfig.toProtocolConfig()
    state = source.state.toState()
    aggregatorCertificate = source.details.aggregatorCertificate
    encryptedResult = source.details.encryptedResult
    measurementReferenceId = source.providedMeasurementId
    failure =
      failure {
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
    dataProviderPublicKey =
      signedData {
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

/** Converts a public [Measurement] to an internal [InternalMeasurement] for creation. */
fun Measurement.toInternal(
  measurementConsumerCertificateKey: MeasurementConsumerCertificateKey,
  dataProvidersMap: Map<Long, DataProviderValue>,
  measurementSpecProto: MeasurementSpec
): InternalMeasurement {
  val publicMeasurement = this
  val internalProtocolConfig: InternalProtocolConfig
  val internalDuchyProtocolConfig: DuchyProtocolConfig
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  when (measurementSpecProto.measurementTypeCase) {
    MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
      internalProtocolConfig =
        internalProtocolConfig {
          externalProtocolConfigId = Llv2ProtocolConfig.name
          measurementType = InternalProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
          liquidLegionsV2 = Llv2ProtocolConfig.protocolConfig
        }
      internalDuchyProtocolConfig =
        duchyProtocolConfig { liquidLegionsV2 = Llv2ProtocolConfig.duchyProtocolConfig }
    }
    MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> error("MeasurementType not set.")
  }
  return internalMeasurement {
    providedMeasurementId = measurementReferenceId
    externalMeasurementConsumerId =
      apiIdToExternalId(measurementConsumerCertificateKey.measurementConsumerId)
    externalMeasurementConsumerCertificateId =
      apiIdToExternalId(measurementConsumerCertificateKey.certificateId)
    dataProviders.putAll(dataProvidersMap)
    details =
      details {
        apiVersion = Version.V2_ALPHA.string
        measurementSpec = publicMeasurement.measurementSpec.data
        measurementSpecSignature = publicMeasurement.measurementSpec.signature
        protocolConfig = internalProtocolConfig
        duchyProtocolConfig = internalDuchyProtocolConfig
      }
  }
}

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
      InternalExchange.State.STATE_UNSPECIFIED, InternalExchange.State.UNRECOGNIZED ->
        failGrpc(Status.INTERNAL) { "Invalid state: $this" }
    }
  }

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

fun ExchangeStepAttempt.State.toInternal(): ImternalExchangeStepAttempt.State {
  return when (this) {
    ExchangeStepAttempt.State.STATE_UNSPECIFIED, ExchangeStepAttempt.State.UNRECOGNIZED ->
      failGrpc { "Invalid State: $this" }
    ExchangeStepAttempt.State.ACTIVE -> ImternalExchangeStepAttempt.State.ACTIVE
    ExchangeStepAttempt.State.SUCCEEDED -> ImternalExchangeStepAttempt.State.SUCCEEDED
    ExchangeStepAttempt.State.FAILED -> ImternalExchangeStepAttempt.State.FAILED
    ExchangeStepAttempt.State.FAILED_STEP -> ImternalExchangeStepAttempt.State.FAILED_STEP
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

fun ImternalExchangeStepAttempt.toV2Alpha(): ExchangeStepAttempt {
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

fun ExchangeStep.State.toInternal(): InternalExchangeStep.State {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (this) {
    ExchangeStep.State.BLOCKED -> InternalExchangeStep.State.BLOCKED
    ExchangeStep.State.READY -> InternalExchangeStep.State.READY
    ExchangeStep.State.READY_FOR_RETRY -> InternalExchangeStep.State.READY_FOR_RETRY
    ExchangeStep.State.IN_PROGRESS -> InternalExchangeStep.State.IN_PROGRESS
    ExchangeStep.State.SUCCEEDED -> InternalExchangeStep.State.SUCCEEDED
    ExchangeStep.State.FAILED -> InternalExchangeStep.State.FAILED
    ExchangeStep.State.STATE_UNSPECIFIED, ExchangeStep.State.UNRECOGNIZED ->
      failGrpc(Status.INVALID_ARGUMENT) { "Invalid state: $this" }
  }
}

private fun ExchangeStepAttemptDetails.DebugLog.toV2Alpha(): ExchangeStepAttempt.DebugLog {
  return ExchangeStepAttemptKt.debugLog {
    time = this@toV2Alpha.time
    message = this@toV2Alpha.message
  }
}

private fun ImternalExchangeStepAttempt.State.toV2Alpha(): ExchangeStepAttempt.State {
  return when (this) {
    ImternalExchangeStepAttempt.State.STATE_UNSPECIFIED,
    ImternalExchangeStepAttempt.State.UNRECOGNIZED ->
      failGrpc(Status.INTERNAL) { "Invalid State: $this" }
    ImternalExchangeStepAttempt.State.ACTIVE -> ExchangeStepAttempt.State.ACTIVE
    ImternalExchangeStepAttempt.State.SUCCEEDED -> ExchangeStepAttempt.State.SUCCEEDED
    ImternalExchangeStepAttempt.State.FAILED -> ExchangeStepAttempt.State.FAILED
    ImternalExchangeStepAttempt.State.FAILED_STEP -> ExchangeStepAttempt.State.FAILED_STEP
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
      InternalExchangeStep.State.STATE_UNSPECIFIED, InternalExchangeStep.State.UNRECOGNIZED ->
        failGrpc(Status.INTERNAL) { "Invalid state: $this" }
    }
  }

fun ExchangeWorkflow.toInternal(): InternalExchangeWorkflow {
  val labelsMap = mutableMapOf<String, MutableSet<Int>>()
  for ((index, step) in stepsList.withIndex()) {
    for (outputLabel in step.outputLabelsMap.values) {
      labelsMap.getOrPut(outputLabel) { mutableSetOf() }.add(index)
    }
  }
  val internalSteps =
    stepsList.mapIndexed { index, step ->
      ExchangeWorkflowKt.step {
        stepIndex = index
        party = step.party.toInternal()
        prerequisiteStepIndices +=
          step
            .inputLabelsMap
            .values
            .flatMap { value -> labelsMap.getOrDefault(value, emptyList()) }
            .toSet()
      }
    }

  return exchangeWorkflow { steps += internalSteps }
}

fun ExchangeWorkflow.Party.toInternal(): InternalExchangeWorkflow.Party {
  return when (this) {
    ExchangeWorkflow.Party.DATA_PROVIDER -> InternalExchangeWorkflow.Party.DATA_PROVIDER
    ExchangeWorkflow.Party.MODEL_PROVIDER -> InternalExchangeWorkflow.Party.MODEL_PROVIDER
    else -> throw IllegalArgumentException("Provider is not set for the Exchange Step.")
  }
}
