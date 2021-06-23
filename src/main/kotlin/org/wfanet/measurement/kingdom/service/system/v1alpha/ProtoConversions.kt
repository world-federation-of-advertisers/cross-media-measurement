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
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKey
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ComputationParticipant as InternalComputationParticipant
import org.wfanet.measurement.internal.kingdom.DifferentialPrivacyParams as InternalDifferentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry.StageAttempt as InternalStageAttempt
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfig as InternalDuchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.DifferentialPrivacyParams
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.RequisitionKey
import org.wfanet.measurement.system.v1alpha.StageAttempt

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
      it.dataProvider =
        when (Version.fromString(apiVersion)) {
          Version.V2_ALPHA -> DataProviderKey(externalIdToApiId(externalDataProviderId)).toName()
          Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
        }
      // TODO: get dataProviderCertificate from the external_data_provider_certificate_id
      // TODO: set the requisition_spec_hash
      it.state = state.toSystemRequisitionState()
      it.dataProviderParticipationSignature = details.dataProviderParticipationSignature
      it.fulfillingComputationParticipant =
        ComputationParticipantKey(
            externalIdToApiId(externalComputationId),
            externalFulfillingDuchyId
          )
          .toName()
    }
    .build()
}

/** Converts a kingdom internal Requisition.State to system Api Requisition.State. */
fun InternalRequisition.State.toSystemRequisitionState(): Requisition.State {
  return when (this) {
    InternalRequisition.State.UNFULFILLED -> Requisition.State.UNFULFILLED
    InternalRequisition.State.FULFILLED -> Requisition.State.FULFILLED
    InternalRequisition.State.REFUSED -> Requisition.State.REFUSED
    InternalRequisition.State.STATE_UNSPECIFIED, InternalRequisition.State.UNRECOGNIZED ->
      error("Invalid requisition state.")
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
        duchyCertificate =
          when (Version.fromString(apiVersion)) {
            Version.V2_ALPHA ->
              DuchyCertificateKey(externalDuchyId, externalIdToApiId(externalDuchyCertificateId))
                .toName()
            Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
          }
        // TODO: set duchy_certificate_der
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
  return Computation.newBuilder()
    .also {
      it.name = ComputationKey(externalIdToApiId(externalComputationId)).toName()
      it.publicApiVersion = details.apiVersion
      it.measurementSpec = details.measurementSpec
      it.dataProviderList = details.dataProviderList
      it.dataProviderListSalt = details.dataProviderListSalt
      it.protocolConfig =
        when (Version.fromString(details.apiVersion)) {
          Version.V2_ALPHA -> ProtocolConfigKey(externalProtocolConfigId).toName()
          Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
        }
      it.state = state.toSystemComputationState()
      it.aggregatorCertificate = details.aggregatorCertificate
      it.resultPublicKey = details.resultPublicKey
      it.encryptedResult = details.encryptedResult
      it.addAllComputationParticipants(
        computationParticipantsList.map { participant ->
          participant.toSystemComputationParticipant()
        }
      )
      it.addAllRequisitions(
        requisitionsList.map { requisition -> requisition.toSystemRequisition() }
      )
      it.duchyProtocolConfig = details.duchyProtocolConfig.toSystemDuchyProtocolConfig()
    }
    .build()
}

/** Converts a kingdom internal Requisition.State to system Api Requisition.State. */
fun InternalDuchyProtocolConfig.toSystemDuchyProtocolConfig(): Computation.DuchyProtocolConfig {
  return Computation.DuchyProtocolConfig.newBuilder()
    .also {
      if (hasLiquidLegionsV2()) {
        it.liquidLegionsV2Builder.apply {
          maximumFrequency = liquidLegionsV2.maximumFrequency
          mpcNoiseBuilder.apply {
            blindedHistogramNoise =
              liquidLegionsV2.mpcNoise.blindedHistogramNoise.toSystemDifferentialPrivacyParams()
            noiseForPublisherNoise =
              liquidLegionsV2.mpcNoise.noiseForPublisherNoise.toSystemDifferentialPrivacyParams()
          }
        }
      }
    }
    .build()
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

/** Converts a kingdom internal Requisition.State to system Api Requisition.State. */
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
    InternalMeasurement.State.STATE_UNSPECIFIED, InternalMeasurement.State.UNRECOGNIZED ->
      error("Invalid measurement state.")
  }
}
