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
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountActivationStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuplicateAccountIdentityException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
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
    } catch (e: ComputationParticipantStateIllegalException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "ComputationParticipant not in CREATED state."
      }
    } catch (e: MeasurementStateIllegalException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "Measurement not in PENDING_REQUISITION_PARAMS state."
      }
    } catch (e: ComputationParticipantNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "ComputationParticipant not found." }
    } catch (e: DuchyNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Duchy not found." }
    } catch (e: DuchyCertificateNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Duchy's Certificate not found." }
    } catch (e: CertificateIsInvalidException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Certificate is invalid." }
    } catch (e: DuplicateAccountIdentityException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Account Identity duplicated." }
    } catch (e: AccountActivationStateIllegalException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "Account activation state illegal."
      }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
    }
  }

  override suspend fun failComputationParticipant(
    request: FailComputationParticipantRequest
  ): ComputationParticipant {
    try {
      return FailComputationParticipant(request).execute(client, idGenerator)
    } catch (e: ComputationParticipantNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "ComputationParticipant not found." }
    } catch (e: DuchyNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Duchy not found." }
    } catch (e: MeasurementStateIllegalException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Measurement state is illegal." }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
    }
  }

  override suspend fun confirmComputationParticipant(
    request: ConfirmComputationParticipantRequest
  ): ComputationParticipant {
    try {
      return ConfirmComputationParticipant(request).execute(client, idGenerator)
    } catch (e: ComputationParticipantNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "ComputationParticipant not found." }
    } catch (e: ComputationParticipantStateIllegalException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "ComputationParticipant in illegal state."
      }
    } catch (e: DuchyNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Duchy not found." }
    } catch (e: MeasurementStateIllegalException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Measurement in illegal state." }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
    }
  }
}
