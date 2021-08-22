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
import java.time.Clock
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SetParticipantRequisitionParams

class SpannerComputationParticipantsService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ComputationParticipantsCoroutineImplBase() {
  override suspend fun setParticipantRequisitionParams(
    request: SetParticipantRequisitionParamsRequest
  ): ComputationParticipant {
    try {
      return SetParticipantRequisitionParams(request).execute(client, idGenerator, clock)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_IN_UNEXPECTED_STATE ->
          failGrpc(Status.FAILED_PRECONDITION) { "Computation participant not in CREATED state." }
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Computation participant not found." }
        KingdomInternalException.Code.DUCHY_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Duchy not found" }
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND ->
          failGrpc(Status.FAILED_PRECONDITION) {
            "Certificate for Computation participant not found."
          }
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS -> throw e
      }
    }
  }
  override suspend fun failComputationParticipant(
    request: FailComputationParticipantRequest
  ): ComputationParticipant {
    TODO("not implemented yet")
  }
  override suspend fun confirmComputationParticipant(
    request: ConfirmComputationParticipantRequest
  ): ComputationParticipant {
    TODO("not implemented yet")
  }
}
