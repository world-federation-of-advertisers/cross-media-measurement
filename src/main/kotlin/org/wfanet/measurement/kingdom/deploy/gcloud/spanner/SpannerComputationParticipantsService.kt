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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ConfirmComputationParticipant
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FailComputationParticipant
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SetParticipantRequisitionParams

class SpannerComputationParticipantsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ComputationParticipantsCoroutineImplBase() {
  override suspend fun setParticipantRequisitionParams(
    request: SetParticipantRequisitionParamsRequest
  ): ComputationParticipant {
    try {
      return SetParticipantRequisitionParams(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL ->
          failGrpc(Status.FAILED_PRECONDITION) { "Computation participant not in CREATED state." }
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL ->
          failGrpc(Status.FAILED_PRECONDITION) {
            "Measurement not in PENDING_REQUISITION_PARAMS state."
          }
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Computation participant not found." }
        KingdomInternalException.Code.DUCHY_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Duchy not found" }
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND ->
          failGrpc(Status.FAILED_PRECONDITION) {
            "Certificate for Computation participant not found."
          }
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID ->
          failGrpc(Status.FAILED_PRECONDITION) { "Certificate is invalid" }
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND,
        KingdomInternalException.Code.PERMISSION_DENIED,
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL -> throw e
      }
    }
  }

  override suspend fun failComputationParticipant(
    request: FailComputationParticipantRequest
  ): ComputationParticipant {
    try {
      return FailComputationParticipant(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Computation participant not found." }
        KingdomInternalException.Code.DUCHY_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Duchy not found" }
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL ->
          failGrpc(Status.FAILED_PRECONDITION) { "Measurement State is Illegal" }
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY,
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND,
        KingdomInternalException.Code.PERMISSION_DENIED,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL -> throw e
      }
    }
  }

  override suspend fun confirmComputationParticipant(
    request: ConfirmComputationParticipantRequest
  ): ComputationParticipant {
    try {
      return ConfirmComputationParticipant(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL ->
          failGrpc(Status.FAILED_PRECONDITION) { "Computation participant in illegal state." }
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Computation participant not found." }
        KingdomInternalException.Code.DUCHY_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Duchy not found" }
        KingdomInternalException.Code.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        KingdomInternalException.Code.DUPLICATE_ACCOUNT_IDENTITY,
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND,
        KingdomInternalException.Code.PERMISSION_DENIED,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL -> throw e
      }
    }
  }
}
