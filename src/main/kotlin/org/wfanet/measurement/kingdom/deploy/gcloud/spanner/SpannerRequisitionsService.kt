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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.GetRequisitionByDataProviderIdRequest
import org.wfanet.measurement.internal.kingdom.GetRequisitionRequest
import org.wfanet.measurement.internal.kingdom.RefuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.Refusal
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamRequisitions
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FulfillRequisition
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.RefuseRequisition

class SpannerRequisitionsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : RequisitionsCoroutineImplBase() {

  override suspend fun getRequisition(request: GetRequisitionRequest): Requisition {
    return RequisitionReader()
      .readByExternalId(
        client.singleUse(),
        externalMeasurementConsumerId = request.externalMeasurementConsumerId,
        externalMeasurementId = request.externalMeasurementId,
        externalRequisitionId = request.externalRequisitionId
      )
      ?.requisition
      ?: failGrpc(Status.NOT_FOUND) { "Requisition not found" }
  }

  override suspend fun getRequisitionByDataProviderId(
    request: GetRequisitionByDataProviderIdRequest
  ): Requisition {
    return RequisitionReader()
      .readByExternalDataProviderId(
        client.singleUse(),
        externalDataProviderId = request.externalDataProviderId,
        externalRequisitionId = request.externalRequisitionId
      )
      ?.requisition
      ?: failGrpc(Status.NOT_FOUND) { "Requisition not found" }
  }

  override fun streamRequisitions(request: StreamRequisitionsRequest): Flow<Requisition> {
    val requestFilter = request.filter
    if (requestFilter.externalMeasurementId != 0L) {
      grpcRequire(requestFilter.externalMeasurementConsumerId != 0L) {
        "external_measurement_consumer_id must be specified if external_measurement_id is specified"
      }
    }

    return StreamRequisitions(requestFilter, request.limit).execute(client.singleUse()).map {
      it.requisition
    }
  }

  override suspend fun fulfillRequisition(request: FulfillRequisitionRequest): Requisition {
    with(request) {
      grpcRequire(externalRequisitionId != 0L) { "external_requisition_id not specified" }
      grpcRequire(nonce != 0L) { "nonce not specified" }
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (request.paramsCase) {
        FulfillRequisitionRequest.ParamsCase.COMPUTED_PARAMS -> {
          grpcRequire(computedParams.externalComputationId != 0L) {
            "external_computation_id not specified"
          }
          grpcRequire(computedParams.externalFulfillingDuchyId.isNotEmpty()) {
            "external_fulfilling_duchy_id not specified"
          }
        }
        FulfillRequisitionRequest.ParamsCase.DIRECT_PARAMS -> {
          grpcRequire(!directParams.encryptedData.isEmpty) { "encrypted_data not specified" }
          grpcRequire(directParams.externalDataProviderId != 0L) {
            "data_provider_id not specified"
          }
        }
        FulfillRequisitionRequest.ParamsCase.PARAMS_NOT_SET ->
          failGrpc(Status.INVALID_ARGUMENT) { "params field not set" }
      }
    }

    try {
      return FulfillRequisition(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      val status: Status =
        when (e.code) {
          ErrorCode.REQUISITION_NOT_FOUND -> Status.NOT_FOUND
          ErrorCode.REQUISITION_STATE_ILLEGAL,
          ErrorCode.MEASUREMENT_STATE_ILLEGAL,
          ErrorCode.DUCHY_NOT_FOUND -> Status.FAILED_PRECONDITION
          ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
          ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
          ErrorCode.ACCOUNT_NOT_FOUND,
          ErrorCode.API_KEY_NOT_FOUND,
          ErrorCode.PERMISSION_DENIED,
          ErrorCode.MEASUREMENT_NOT_FOUND,
          ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
          ErrorCode.DATA_PROVIDER_NOT_FOUND,
          ErrorCode.MODEL_PROVIDER_NOT_FOUND,
          ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
          ErrorCode.CERTIFICATE_NOT_FOUND,
          ErrorCode.CERTIFICATE_IS_INVALID,
          ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
          ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
          ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
          ErrorCode.EVENT_GROUP_INVALID_ARGS,
          ErrorCode.EVENT_GROUP_NOT_FOUND,
          ErrorCode.UNKNOWN_ERROR,
          ErrorCode.UNRECOGNIZED -> throw e
        }
      throw status.withCause(e).asRuntimeException()
    }
  }

  override suspend fun refuseRequisition(request: RefuseRequisitionRequest): Requisition {
    with(request) {
      grpcRequire(externalDataProviderId != 0L) { "external_data_provider_id not specified" }
      grpcRequire(externalRequisitionId != 0L) { "external_requisition_id not specified" }
      grpcRequire(refusal.justification != Refusal.Justification.UNRECOGNIZED) {
        "Unrecognized refusal justification ${refusal.justificationValue}"
      }
      grpcRequire(refusal.justification != Refusal.Justification.JUSTIFICATION_UNSPECIFIED) {
        "refusal justification not specified"
      }
    }

    try {
      return RefuseRequisition(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      val status: Status =
        when (e.code) {
          ErrorCode.REQUISITION_NOT_FOUND -> Status.NOT_FOUND
          ErrorCode.REQUISITION_STATE_ILLEGAL, ErrorCode.MEASUREMENT_STATE_ILLEGAL ->
            Status.FAILED_PRECONDITION
          ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
          ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
          ErrorCode.ACCOUNT_NOT_FOUND,
          ErrorCode.API_KEY_NOT_FOUND,
          ErrorCode.PERMISSION_DENIED,
          ErrorCode.DUCHY_NOT_FOUND,
          ErrorCode.MEASUREMENT_NOT_FOUND,
          ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
          ErrorCode.DATA_PROVIDER_NOT_FOUND,
          ErrorCode.MODEL_PROVIDER_NOT_FOUND,
          ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
          ErrorCode.CERTIFICATE_NOT_FOUND,
          ErrorCode.CERTIFICATE_IS_INVALID,
          ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
          ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
          ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
          ErrorCode.EVENT_GROUP_INVALID_ARGS,
          ErrorCode.EVENT_GROUP_NOT_FOUND,
          ErrorCode.UNKNOWN_ERROR,
          ErrorCode.UNRECOGNIZED -> throw e
        }
      throw status.withCause(e).asRuntimeException()
    }
  }
}
