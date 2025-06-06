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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.GetRequisitionRequest
import org.wfanet.measurement.internal.kingdom.RefuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionRefusal
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionEtagMismatchException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionNotFoundByDataProviderException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequisitionStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamRequisitions
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FulfillRequisition
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.RefuseRequisition

class SpannerRequisitionsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RequisitionsCoroutineImplBase(coroutineContext) {

  override suspend fun getRequisition(request: GetRequisitionRequest): Requisition {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val externalRequisitionId = ExternalId(request.externalRequisitionId)
    val result: RequisitionReader.Result =
      RequisitionReader.readByExternalDataProviderId(
        client.singleUse(),
        externalDataProviderId = externalDataProviderId,
        externalRequisitionId = externalRequisitionId,
      )
        ?: throw RequisitionNotFoundByDataProviderException(
            externalDataProviderId,
            externalRequisitionId,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    return result.requisition
  }

  override fun streamRequisitions(request: StreamRequisitionsRequest): Flow<Requisition> {
    val requestFilter = request.filter
    if (requestFilter.externalMeasurementId != 0L) {
      if (requestFilter.externalMeasurementConsumerId == 0L) {
        throw RequiredFieldNotSetException("filter.external_measurement_consumer_id") { fieldName ->
            "$fieldName is required when filter.external_measurement_id is set"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }
    if (requestFilter.measurementStatesList.isNotEmpty()) {
      throw Status.INVALID_ARGUMENT.withDescription(
          "filter.measurement_states is no longer supported"
        )
        .asRuntimeException()
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
          grpcRequire(directParams.apiVersion.isNotEmpty()) { "api_version not specified" }
        }
        FulfillRequisitionRequest.ParamsCase.PARAMS_NOT_SET ->
          failGrpc(Status.INVALID_ARGUMENT) { "params field not specified" }
      }
    }

    try {
      return FulfillRequisition(request).execute(client, idGenerator)
    } catch (e: RequisitionNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Requisition not found.")
    } catch (e: RequisitionStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Requisition state illegal.",
      )
    } catch (e: MeasurementStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement state illegal.",
      )
    } catch (e: DuchyNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "Duchy not found.")
    } catch (e: RequisitionEtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED, "Requisition etags mismatch")
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override suspend fun refuseRequisition(request: RefuseRequisitionRequest): Requisition {
    with(request) {
      grpcRequire(externalDataProviderId != 0L) { "external_data_provider_id not specified" }
      grpcRequire(externalRequisitionId != 0L) { "external_requisition_id not specified" }
      grpcRequire(refusal.justification != RequisitionRefusal.Justification.UNRECOGNIZED) {
        "Unrecognized refusal justification ${refusal.justificationValue}"
      }
      grpcRequire(
        refusal.justification != RequisitionRefusal.Justification.JUSTIFICATION_UNSPECIFIED
      ) {
        "refusal justification not specified"
      }
    }

    try {
      return RefuseRequisition(request).execute(client, idGenerator)
    } catch (e: RequisitionNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Requisition not found.")
    } catch (e: RequisitionStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Requisition state illegal.",
      )
    } catch (e: RequisitionEtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED, "Requisition etags mismatch")
    } catch (e: MeasurementStateIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement state illegal.",
      )
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }
}
