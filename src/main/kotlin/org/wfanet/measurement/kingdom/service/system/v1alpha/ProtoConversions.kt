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
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ComputationParticipant as InternalComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantDetails as InternalComputationParticipantDetails
import org.wfanet.measurement.internal.kingdom.DifferentialPrivacyParams as InternalDifferentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntryStageAttempt as InternalStageAttempt
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig as InternalDuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryError
import org.wfanet.measurement.internal.kingdom.ProtocolConfig as InternalProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfig.NoiseMechanism as InternalNoiseMechanism
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.measurementLogEntryError
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.Computation.MpcProtocolConfig.NoiseMechanism
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.LiquidLegionsV2Kt.liquidLegionsSketchParams
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.LiquidLegionsV2Kt.mpcNoise
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.honestMajorityShareShuffle
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.trusTee
import org.wfanet.measurement.system.v1alpha.ComputationKt.mpcProtocolConfig
import org.wfanet.measurement.system.v1alpha.ComputationLogEntry
import org.wfanet.measurement.system.v1alpha.ComputationLogEntryKey
import org.wfanet.measurement.system.v1alpha.ComputationLogEntryKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.DifferentialPrivacyParams
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.RequisitionKey
import org.wfanet.measurement.system.v1alpha.RequisitionKt
import org.wfanet.measurement.system.v1alpha.StageAttempt
import org.wfanet.measurement.system.v1alpha.computation
import org.wfanet.measurement.system.v1alpha.computationParticipant
import org.wfanet.measurement.system.v1alpha.requisition

/** Converts a kingdom internal Requisition to system Api Requisition. */
fun InternalRequisition.toSystemRequisition(): Requisition {
  val source = this
  return requisition {
    name =
      RequisitionKey(
          externalIdToApiId(source.externalComputationId),
          externalIdToApiId(source.externalRequisitionId),
        )
        .toName()
    requisitionSpecHash = Hashing.hashSha256(source.details.encryptedRequisitionSpec)
    nonceHash = source.details.nonceHash
    state = source.state.toSystemRequisitionState()
    nonce = source.details.nonce
    if (source.externalFulfillingDuchyId.isNotBlank()) {
      fulfillingComputationParticipant =
        ComputationParticipantKey(
            externalIdToApiId(source.externalComputationId),
            source.externalFulfillingDuchyId,
          )
          .toName()
    }
    if (source.details.hasFulfillmentContext()) {
      fulfillmentContext =
        RequisitionKt.fulfillmentContext {
          buildLabel = source.details.fulfillmentContext.buildLabel
          warnings += source.details.fulfillmentContext.warningsList
        }
    }
  }
}

/** Converts a kingdom internal Requisition.State to system Api Requisition.State. */
fun InternalRequisition.State.toSystemRequisitionState(): Requisition.State {
  return when (this) {
    InternalRequisition.State.PENDING_PARAMS,
    InternalRequisition.State.UNFULFILLED -> Requisition.State.UNFULFILLED
    InternalRequisition.State.FULFILLED -> Requisition.State.FULFILLED
    InternalRequisition.State.REFUSED -> Requisition.State.REFUSED
    InternalRequisition.State.WITHDRAWN -> Requisition.State.WITHDRAWN
    InternalRequisition.State.STATE_UNSPECIFIED,
    InternalRequisition.State.UNRECOGNIZED -> error("Invalid requisition state.")
  }
}

/** Converts a kingdom internal ComputationParticipant to system Api ComputationParticipant. */
fun InternalComputationParticipant.toSystemComputationParticipant(): ComputationParticipant {
  val source = this
  return computationParticipant {
    name =
      ComputationParticipantKey(
          externalIdToApiId(source.externalComputationId),
          source.externalDuchyId,
        )
        .toName()
    state = source.state.toSystemRequisitionState()
    updateTime = source.updateTime
    if (
      source.hasDuchyCertificate() ||
        source.details.protocolCase !=
          InternalComputationParticipantDetails.ProtocolCase.PROTOCOL_NOT_SET
    ) {
      requisitionParams =
        ComputationParticipantKt.requisitionParams {
          if (hasDuchyCertificate()) {
            duchyCertificate =
              when (Version.fromString(apiVersion)) {
                Version.V2_ALPHA ->
                  DuchyCertificateKey(
                      source.externalDuchyId,
                      externalIdToApiId(source.duchyCertificate.externalCertificateId),
                    )
                    .toName()
              }
            duchyCertificateDer = source.duchyCertificate.details.x509Der
          }
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
          when (source.details.protocolCase) {
            InternalComputationParticipantDetails.ProtocolCase.LIQUID_LEGIONS_V2 -> {
              liquidLegionsV2 =
                ComputationParticipantKt.RequisitionParamsKt.liquidLegionsV2 {
                  elGamalPublicKey = source.details.liquidLegionsV2.elGamalPublicKey
                  elGamalPublicKeySignature =
                    source.details.liquidLegionsV2.elGamalPublicKeySignature
                  elGamalPublicKeySignatureAlgorithmOid =
                    source.details.liquidLegionsV2.elGamalPublicKeySignatureAlgorithmOid
                }
            }
            InternalComputationParticipantDetails.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
              reachOnlyLiquidLegionsV2 =
                ComputationParticipantKt.RequisitionParamsKt.liquidLegionsV2 {
                  elGamalPublicKey = source.details.reachOnlyLiquidLegionsV2.elGamalPublicKey
                  elGamalPublicKeySignature =
                    source.details.reachOnlyLiquidLegionsV2.elGamalPublicKeySignature
                  elGamalPublicKeySignatureAlgorithmOid =
                    source.details.reachOnlyLiquidLegionsV2.elGamalPublicKeySignatureAlgorithmOid
                }
            }
            InternalComputationParticipantDetails.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
              honestMajorityShareShuffle =
                ComputationParticipantKt.RequisitionParamsKt.honestMajorityShareShuffle {
                  tinkPublicKey = source.details.honestMajorityShareShuffle.tinkPublicKey
                  tinkPublicKeySignature =
                    source.details.honestMajorityShareShuffle.tinkPublicKeySignature
                  tinkPublicKeySignatureAlgorithmOid =
                    source.details.honestMajorityShareShuffle.tinkPublicKeySignatureAlgorithmOid
                }
            }
            InternalComputationParticipantDetails.ProtocolCase.TRUS_TEE -> {
              trusTee = ComputationParticipant.RequisitionParams.TrusTee.getDefaultInstance()
            }
            InternalComputationParticipantDetails.ProtocolCase.PROTOCOL_NOT_SET -> Unit
          }
        }
    }
    if (hasFailureLogEntry()) {
      failure =
        ComputationParticipantKt.failure {
          participantChildReferenceId = source.failureLogEntry.details.duchyChildReferenceId
          errorMessage = source.failureLogEntry.logEntry.details.logMessage
          errorTime = source.failureLogEntry.logEntry.details.error.errorTime
          stageAttempt = source.failureLogEntry.details.stageAttempt.toSystemStageAttempt()
        }
    }
    etag = source.etag
  }
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
                externalIdToApiId(resultsList[0].externalCertificateId),
              )
              .toName()
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
private fun buildMpcProtocolConfig(
  duchyProtocolConfig: InternalDuchyProtocolConfig,
  protocolConfig: InternalProtocolConfig,
): Computation.MpcProtocolConfig {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (protocolConfig.protocolCase) {
    InternalProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2 -> {
      require(duchyProtocolConfig.hasLiquidLegionsV2()) {
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
            publisherNoise =
              duchyProtocolConfig.liquidLegionsV2.mpcNoise.noiseForPublisherNoise
                .toSystemDifferentialPrivacyParams()
          }
          ellipticCurveId = protocolConfig.liquidLegionsV2.ellipticCurveId
          @Suppress("DEPRECATION") // For legacy Measurements.
          maximumFrequency = protocolConfig.liquidLegionsV2.maximumFrequency
          // Use `GEOMETRIC` for unspecified InternalNoiseMechanism for old Measurements.
          noiseMechanism =
            if (
              protocolConfig.liquidLegionsV2.noiseMechanism ==
                InternalProtocolConfig.NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED
            ) {
              NoiseMechanism.GEOMETRIC
            } else {
              protocolConfig.liquidLegionsV2.noiseMechanism.toSystemNoiseMechanism()
            }
        }
      }
    }
    InternalProtocolConfig.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
      require(duchyProtocolConfig.hasReachOnlyLiquidLegionsV2()) {
        "Public API ProtocolConfig type doesn't match DuchyProtocolConfig type."
      }
      mpcProtocolConfig {
        reachOnlyLiquidLegionsV2 = liquidLegionsV2 {
          sketchParams = liquidLegionsSketchParams {
            decayRate = protocolConfig.reachOnlyLiquidLegionsV2.sketchParams.decayRate
            maxSize = protocolConfig.reachOnlyLiquidLegionsV2.sketchParams.maxSize
          }
          mpcNoise = mpcNoise {
            blindedHistogramNoise =
              duchyProtocolConfig.reachOnlyLiquidLegionsV2.mpcNoise.blindedHistogramNoise
                .toSystemDifferentialPrivacyParams()
            publisherNoise =
              duchyProtocolConfig.reachOnlyLiquidLegionsV2.mpcNoise.noiseForPublisherNoise
                .toSystemDifferentialPrivacyParams()
          }
          ellipticCurveId = protocolConfig.reachOnlyLiquidLegionsV2.ellipticCurveId
          // Use `GEOMETRIC` for unspecified InternalNoiseMechanism for old Measurements.
          noiseMechanism =
            if (
              protocolConfig.reachOnlyLiquidLegionsV2.noiseMechanism ==
                InternalProtocolConfig.NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED
            ) {
              NoiseMechanism.GEOMETRIC
            } else {
              protocolConfig.reachOnlyLiquidLegionsV2.noiseMechanism.toSystemNoiseMechanism()
            }
        }
      }
    }
    InternalProtocolConfig.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
      mpcProtocolConfig {
        honestMajorityShareShuffle = honestMajorityShareShuffle {
          reachAndFrequencyRingModulus =
            protocolConfig.honestMajorityShareShuffle.reachAndFrequencyRingModulus
          reachRingModulus = protocolConfig.honestMajorityShareShuffle.reachRingModulus
          noiseMechanism =
            protocolConfig.honestMajorityShareShuffle.noiseMechanism.toSystemNoiseMechanism()
        }
      }
    }
    InternalProtocolConfig.ProtocolCase.TRUS_TEE -> {
      mpcProtocolConfig {
        trusTee = trusTee {
          noiseMechanism = protocolConfig.trusTee.noiseMechanism.toSystemNoiseMechanism()
        }
      }
    }
    InternalProtocolConfig.ProtocolCase.DIRECT,
    InternalProtocolConfig.ProtocolCase.PROTOCOL_NOT_SET -> error("Invalid Protocol.")
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

/** Converts an internal MeasurementLogEntryErrorto system ComputationLogEntry.ErrorDetails. */
fun MeasurementLogEntryError.toSystemLogErrorDetails(): ComputationLogEntry.ErrorDetails {
  val source = this
  return ComputationLogEntryKt.errorDetails {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    type =
      when (source.type) {
        MeasurementLogEntryError.Type.PERMANENT -> ComputationLogEntry.ErrorDetails.Type.PERMANENT
        MeasurementLogEntryError.Type.TRANSIENT -> ComputationLogEntry.ErrorDetails.Type.TRANSIENT
        MeasurementLogEntryError.Type.TYPE_UNSPECIFIED,
        MeasurementLogEntryError.Type.UNRECOGNIZED -> error("Invalid error type.")
      }
    errorTime = source.errorTime
  }
}

/** Converts a system ComputationLogEntry.ErrorDetails to internal MeasurementLogEntryError. */
fun ComputationLogEntry.ErrorDetails.toInternalLogErrorDetails(): MeasurementLogEntryError {
  val source = this
  return measurementLogEntryError {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    type =
      when (source.type) {
        ComputationLogEntry.ErrorDetails.Type.PERMANENT -> MeasurementLogEntryError.Type.PERMANENT
        ComputationLogEntry.ErrorDetails.Type.TRANSIENT -> MeasurementLogEntryError.Type.TRANSIENT
        ComputationLogEntry.ErrorDetails.Type.TYPE_UNSPECIFIED,
        ComputationLogEntry.ErrorDetails.Type.UNRECOGNIZED -> error("Invalid error type.")
      }
    errorTime = source.errorTime
  }
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
            externalIdToApiId(externalComputationLogEntryId),
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

/** Converts an internal NoiseMechanism to a system NoiseMechanism. */
fun InternalNoiseMechanism.toSystemNoiseMechanism(): NoiseMechanism {
  return when (this) {
    InternalNoiseMechanism.GEOMETRIC -> NoiseMechanism.GEOMETRIC
    InternalNoiseMechanism.DISCRETE_GAUSSIAN -> NoiseMechanism.DISCRETE_GAUSSIAN
    InternalNoiseMechanism.CONTINUOUS_GAUSSIAN -> NoiseMechanism.CONTINUOUS_GAUSSIAN
    InternalNoiseMechanism.CONTINUOUS_LAPLACE,
    InternalNoiseMechanism.NOISE_MECHANISM_UNSPECIFIED,
    InternalNoiseMechanism.NONE,
    InternalNoiseMechanism.UNRECOGNIZED -> error("invalid internal noise mechanism.")
  }
}
