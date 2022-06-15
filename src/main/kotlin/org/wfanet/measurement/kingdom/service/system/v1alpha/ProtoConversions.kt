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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ComputationParticipant as InternalComputationParticipant
import org.wfanet.measurement.internal.kingdom.DifferentialPrivacyParams as InternalDifferentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry.StageAttempt as InternalStageAttempt
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig as InternalDuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.ProtocolConfig as InternalProtocolConfig
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.LiquidLegionsV2Kt.liquidLegionsSketchParams
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.LiquidLegionsV2Kt.mpcNoise
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.system.v1alpha.ComputationKt.mpcProtocolConfig
import org.wfanet.measurement.system.v1alpha.ComputationLogEntry
import org.wfanet.measurement.system.v1alpha.ComputationLogEntryKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.DifferentialPrivacyParams
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.RequisitionKey
import org.wfanet.measurement.system.v1alpha.StageAttempt
import org.wfanet.measurement.system.v1alpha.computation

/** Converts a kingdom internal Requisition to system Api Requisition. */
fun InternalRequisition.toSystemRequisition(): Requisition {
  return Requisition.newBuilder()
    .also {
      it.name =
        RequisitionKey(
            externalIdToApiId(externalComputationId),
            externalIdToApiId(externalRequisitionId)
          )
          .toName()
      it.requisitionSpecHash = hashSha256(details.encryptedRequisitionSpec)
      it.nonceHash = details.nonceHash
      it.state = state.toSystemRequisitionState()
      it.nonce = details.nonce
      if (externalFulfillingDuchyId.isNotBlank()) {
        it.fulfillingComputationParticipant =
          ComputationParticipantKey(
              externalIdToApiId(externalComputationId),
              externalFulfillingDuchyId
            )
            .toName()
      }
    }
    .build()
}

/** Converts a kingdom internal Requisition.State to system Api Requisition.State. */
fun InternalRequisition.State.toSystemRequisitionState(): Requisition.State {
  return when (this) {
    InternalRequisition.State.PENDING_PARAMS,
    InternalRequisition.State.UNFULFILLED -> Requisition.State.UNFULFILLED
    InternalRequisition.State.FULFILLED -> Requisition.State.FULFILLED
    InternalRequisition.State.REFUSED -> Requisition.State.REFUSED
    InternalRequisition.State.STATE_UNSPECIFIED,
    InternalRequisition.State.UNRECOGNIZED -> error("Invalid requisition state.")
  }
}

/** Converts a kingdom internal ComputationParticipant to system Api ComputationParticipant. */
fun InternalComputationParticipant.toSystemComputationParticipant(): ComputationParticipant {
  return ComputationParticipant.newBuilder()
    .also {
      it.name =
        ComputationParticipantKey(externalIdToApiId(externalComputationId), externalDuchyId)
          .toName()
      it.state = state.toSystemRequisitionState()
      it.updateTime = updateTime
      it.requisitionParamsBuilder.apply {
        if (hasDuchyCertificate()) {
          val duchyCertificate = this@toSystemComputationParticipant.duchyCertificate

          this.duchyCertificate =
            when (Version.fromString(apiVersion)) {
              Version.V2_ALPHA ->
                DuchyCertificateKey(
                    externalDuchyId,
                    externalIdToApiId(duchyCertificate.externalCertificateId)
                  )
                  .toName()
              Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
            }
          duchyCertificateDer = duchyCertificate.details.x509Der
        }
        if (details.hasLiquidLegionsV2()) {
          liquidLegionsV2Builder.apply {
            elGamalPublicKey = details.liquidLegionsV2.elGamalPublicKey
            elGamalPublicKeySignature = details.liquidLegionsV2.elGamalPublicKeySignature
          }
        }
      }
      if (hasFailureLogEntry()) {
        it.failureBuilder.apply {
          participantChildReferenceId = failureLogEntry.details.duchyChildReferenceId
          errorMessage = failureLogEntry.logEntry.details.logMessage
          errorTime = failureLogEntry.logEntry.details.error.errorTime
          stageAttempt = failureLogEntry.details.stageAttempt.toSystemStageAttempt()
        }
      }
    }
    .build()
}

/** Converts a kingdom internal StageAttempt to system Api StageAttempt. */
fun InternalStageAttempt.toSystemStageAttempt(): StageAttempt {
  return StageAttempt.newBuilder()
    .also {
      it.stage = stage
      it.stageName = stageName
      it.stageStartTime = stageStartTime
      it.attemptNumber = attemptNumber
    }
    .build()
}

/** Converts a system Api StageAttempt to kingdom internal StageAttempt. */
fun StageAttempt.toInternalStageAttempt(): InternalStageAttempt {
  return InternalStageAttempt.newBuilder()
    .also {
      it.stage = stage
      it.stageName = stageName
      it.stageStartTime = stageStartTime
      it.attemptNumber = attemptNumber
    }
    .build()
}

/**
 * Converts a kingdom internal ComputationParticipant.State to system Api
 * ComputationParticipant.State.
 */
fun InternalComputationParticipant.State.toSystemRequisitionState(): ComputationParticipant.State {
  return when (this) {
    InternalComputationParticipant.State.CREATED -> ComputationParticipant.State.CREATED
    InternalComputationParticipant.State.REQUISITION_PARAMS_SET ->
      ComputationParticipant.State.REQUISITION_PARAMS_SET
    InternalComputationParticipant.State.READY -> ComputationParticipant.State.READY
    InternalComputationParticipant.State.FAILED -> ComputationParticipant.State.FAILED
    InternalComputationParticipant.State.STATE_UNSPECIFIED,
    InternalComputationParticipant.State.UNRECOGNIZED ->
      error("Invalid computationParticipant state.")
  }
}

/** Converts a kingdom internal Measurement to system Api Computation. */
fun InternalMeasurement.toSystemComputation(): Computation {
  val source = this
  val apiVersion = Version.fromString(details.apiVersion)
  return computation {
    name = ComputationKey(externalIdToApiId(externalComputationId)).toName()
    publicApiVersion = details.apiVersion
    measurementSpec = details.measurementSpec
    state = source.state.toSystemComputationState()
    val resultsList = source.resultsList
    if (resultsList.isNotEmpty() && resultsList[0].externalAggregatorDuchyId.isNotBlank()) {
      aggregatorCertificate =
        when (apiVersion) {
          Version.V2_ALPHA ->
            DuchyCertificateKey(
                resultsList[0].externalAggregatorDuchyId,
                externalIdToApiId(resultsList[0].externalCertificateId)
              )
              .toName()
          Version.VERSION_UNSPECIFIED -> error("Public API version is invalid or unspecified.")
        }
      encryptedResult = resultsList[0].encryptedResult
    }
    computationParticipants +=
      computationParticipantsList.map { participant ->
        participant.toSystemComputationParticipant()
      }
    requisitions += requisitionsList.map { requisition -> requisition.toSystemRequisition() }
    mpcProtocolConfig = buildMpcProtocolConfig(details.duchyProtocolConfig, details.protocolConfig)
  }
}

/**
 * Builds a [Computation.MpcProtocolConfig] using the [InternalDuchyProtocolConfig] and
 * [InternalProtocolConfig].
 */
fun buildMpcProtocolConfig(
  duchyProtocolConfig: InternalDuchyProtocolConfig,
  protocolConfig: InternalProtocolConfig
): Computation.MpcProtocolConfig {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (duchyProtocolConfig.protocolCase) {
    InternalDuchyProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 -> {
      require(protocolConfig.hasLiquidLegionsV2()) {
        "Public API ProtocolConfig type doesn't match DuchyProtocolConfig type."
      }
      mpcProtocolConfig {
        liquidLegionsV2 = liquidLegionsV2 {
          sketchParams = liquidLegionsSketchParams {
            decayRate = protocolConfig.liquidLegionsV2.sketchParams.decayRate
            maxSize = protocolConfig.liquidLegionsV2.sketchParams.maxSize
          }
          mpcNoise = mpcNoise {
            blindedHistogramNoise =
              duchyProtocolConfig.liquidLegionsV2.mpcNoise.blindedHistogramNoise
                .toSystemDifferentialPrivacyParams()
            noiseForPublisherNoise =
              duchyProtocolConfig.liquidLegionsV2.mpcNoise.noiseForPublisherNoise
                .toSystemDifferentialPrivacyParams()
          }
          ellipticCurveId = protocolConfig.liquidLegionsV2.ellipticCurveId
          maximumFrequency = protocolConfig.liquidLegionsV2.maximumFrequency
        }
      }
    }
    InternalDuchyProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Protocol not set")
  }
}

/**
 * Converts a kingdom internal DifferentialPrivacyParams to system Api DifferentialPrivacyParams.
 */
fun InternalDifferentialPrivacyParams.toSystemDifferentialPrivacyParams():
  DifferentialPrivacyParams {
  return DifferentialPrivacyParams.newBuilder()
    .also {
      it.epsilon = epsilon
      it.delta = delta
    }
    .build()
}

/** Converts a kingdom internal Measurement.State to system Api Computation.State. */
fun InternalMeasurement.State.toSystemComputationState(): Computation.State {
  return when (this) {
    InternalMeasurement.State.PENDING_REQUISITION_PARAMS ->
      Computation.State.PENDING_REQUISITION_PARAMS
    InternalMeasurement.State.PENDING_REQUISITION_FULFILLMENT ->
      Computation.State.PENDING_REQUISITION_FULFILLMENT
    InternalMeasurement.State.PENDING_PARTICIPANT_CONFIRMATION ->
      Computation.State.PENDING_PARTICIPANT_CONFIRMATION
    InternalMeasurement.State.PENDING_COMPUTATION -> Computation.State.PENDING_COMPUTATION
    InternalMeasurement.State.SUCCEEDED -> Computation.State.SUCCEEDED
    InternalMeasurement.State.FAILED -> Computation.State.FAILED
    InternalMeasurement.State.CANCELLED -> Computation.State.CANCELLED
    InternalMeasurement.State.STATE_UNSPECIFIED,
    InternalMeasurement.State.UNRECOGNIZED -> error("Invalid measurement state.")
  }
}

/**
 * Converts an internal MeasurementLogEntry.ErrorDetails to system ComputationLogEntry.ErrorDetails.
 */
fun MeasurementLogEntry.ErrorDetails.toSystemLogErrorDetails(): ComputationLogEntry.ErrorDetails {
  return ComputationLogEntry.ErrorDetails.newBuilder()
    .also {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      it.type =
        when (this.type) {
          MeasurementLogEntry.ErrorDetails.Type.PERMANENT ->
            ComputationLogEntry.ErrorDetails.Type.PERMANENT
          MeasurementLogEntry.ErrorDetails.Type.TRANSIENT ->
            ComputationLogEntry.ErrorDetails.Type.TRANSIENT
          MeasurementLogEntry.ErrorDetails.Type.TYPE_UNSPECIFIED,
          MeasurementLogEntry.ErrorDetails.Type.UNRECOGNIZED -> error("Invalid error type.")
        }
      it.errorTime = this.errorTime
    }
    .build()
}

/**
 * Converts a system ComputationLogEntry.ErrorDetails to internal MeasurementLogEntry.ErrorDetails.
 */
fun ComputationLogEntry.ErrorDetails.toInternalLogErrorDetails(): MeasurementLogEntry.ErrorDetails {
  return MeasurementLogEntry.ErrorDetails.newBuilder()
    .also {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      it.type =
        when (this.type) {
          ComputationLogEntry.ErrorDetails.Type.PERMANENT ->
            MeasurementLogEntry.ErrorDetails.Type.PERMANENT
          ComputationLogEntry.ErrorDetails.Type.TRANSIENT ->
            MeasurementLogEntry.ErrorDetails.Type.TRANSIENT
          ComputationLogEntry.ErrorDetails.Type.TYPE_UNSPECIFIED,
          ComputationLogEntry.ErrorDetails.Type.UNRECOGNIZED -> error("Invalid error type.")
        }
      it.errorTime = this.errorTime
    }
    .build()
}

/** Converts a kingdom internal DuchyMeasurementLogEntry to system ComputationLogEntry. */
fun DuchyMeasurementLogEntry.toSystemComputationLogEntry(
  apiComputationId: String
): ComputationLogEntry {
  return ComputationLogEntry.newBuilder()
    .apply {
      name =
        ComputationLogEntryKey(
            apiComputationId,
            externalDuchyId,
            externalIdToApiId(externalComputationLogEntryId)
          )
          .toName()
      participantChildReferenceId = details.duchyChildReferenceId
      logMessage = logEntry.details.logMessage
      if (details.hasStageAttempt()) {
        stageAttempt = details.stageAttempt.toSystemStageAttempt()
      }
      if (logEntry.details.hasError()) {
        errorDetails = logEntry.details.error.toSystemLogErrorDetails()
      }
    }
    .build()
}
