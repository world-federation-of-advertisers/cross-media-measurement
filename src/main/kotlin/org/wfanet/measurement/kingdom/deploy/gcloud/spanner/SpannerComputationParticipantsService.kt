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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.GetComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountActivationStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantETagMismatchException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantNotFoundByComputationException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ComputationParticipantStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuplicateAccountIdentityException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ComputationParticipantReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ConfirmComputationParticipant
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FailComputationParticipant
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SetParticipantRequisitionParams

class SpannerComputationParticipantsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ComputationParticipantsCoroutineImplBase(coroutineContext) {
  override suspend fun getComputationParticipant(
    request: GetComputationParticipantRequest
  ): ComputationParticipant {
    if (request.externalComputationId == 0L) {
      throw Status.INVALID_ARGUMENT.withDescription("external_computation_id not specified")
        .asRuntimeException()
    }
    if (request.externalDuchyId.isEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription("external_duchy_id not specified")
        .asRuntimeException()
    }

    val externalComputationId = ExternalId(request.externalComputationId)
    val externalDuchyId = request.externalDuchyId
    val result: ComputationParticipantReader.Result? =
      try {
        ComputationParticipantReader()
          .readByExternalComputationId(client.singleUse(), externalComputationId, externalDuchyId)
      } catch (e: DuchyNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: KingdomInternalException) {
        throw e.asStatusRuntimeException(Status.Code.INTERNAL)
      }
    if (result == null) {
      throw ComputationParticipantNotFoundByComputationException(
          externalComputationId,
          externalDuchyId,
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    return result.computationParticipant
  }

  override suspend fun setParticipantRequisitionParams(
    request: SetParticipantRequisitionParamsRequest
  ): ComputationParticipant {
    try {
      return SetParticipantRequisitionParams(request).execute(client, idGenerator)
    } catch (e: ComputationParticipantETagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    } catch (e: ComputationParticipantStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "ComputationParticipant not in CREATED state.",
      )
    } catch (e: MeasurementStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement not in PENDING_REQUISITION_PARAMS state.",
      )
    } catch (e: ComputationParticipantNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ComputationParticipant not found.")
    } catch (e: DuchyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
    } catch (e: DuchyCertificateNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Duchy's Certificate not found.",
      )
    } catch (e: CertificateIsInvalidException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Certificate is invalid.")
    } catch (e: DuplicateAccountIdentityException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Account Identity duplicated.",
      )
    } catch (e: AccountActivationStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Account activation state illegal.",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun failComputationParticipant(
    request: FailComputationParticipantRequest
  ): ComputationParticipant {
    try {
      return FailComputationParticipant(request).execute(client, idGenerator)
    } catch (e: ComputationParticipantNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ComputationParticipant not found.")
    } catch (e: ComputationParticipantETagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    } catch (e: DuchyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
    } catch (e: MeasurementStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement state is illegal.",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun confirmComputationParticipant(
    request: ConfirmComputationParticipantRequest
  ): ComputationParticipant {
    try {
      return ConfirmComputationParticipant(request).execute(client, idGenerator)
    } catch (e: ComputationParticipantNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ComputationParticipant not found.")
    } catch (e: ComputationParticipantETagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    } catch (e: ComputationParticipantStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "ComputationParticipant in illegal state.",
      )
    } catch (e: DuchyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
    } catch (e: MeasurementStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement in illegal state.",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }
}
