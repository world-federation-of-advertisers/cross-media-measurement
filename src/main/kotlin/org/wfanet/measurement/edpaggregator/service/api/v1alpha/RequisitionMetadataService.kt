// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.api.v1alpha

import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FulfillRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LookupRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.QueueRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RefuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.StartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub as InternalRequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.createRequisitionMetadataRequest as internalCreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.deleteRequisitionMetadataRequest as internalDeleteRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.fetchLatestCmmsCreateTimeRequest as internalFetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.internal.edpaggregator.fulfillRequisitionMetadataRequest as internalFulfillRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest as internalGetRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.lookupRequisitionMetadataRequest as internalLookupRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.queueRequisitionMetadataRequest as internalQueueRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.refuseRequisitionMetadataRequest as internalRefuseRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.startProcessingRequisitionMetadataRequest as internalStartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey

class RequisitionMetadataService(
  private val internalClient: InternalRequisitionMetadataServiceCoroutineStub
) : RequisitionMetadataServiceCoroutineImplBase() {

  override suspend fun createRequisitionMetadata(
    request: CreateRequisitionMetadataRequest
  ): RequisitionMetadata {
    val parentKey =
      DataProviderKey.fromName(request.parent)
        ?: failGrpc(Status.INVALID_ARGUMENT) { "Parent is either unspecified or invalid." }

    grpcRequire(request.requisitionMetadata.name.isEmpty()) {
      "RequisitionMetadata name must be empty."
    }

    val cmmsRequisitionKey =
      CanonicalRequisitionKey.fromName(request.requisitionMetadata.cmmsRequisition)
        ?: failGrpc(Status.INVALID_ARGUMENT) {
          "cmms_requisition is either unspecified or invalid."
        }
    grpcRequire(cmmsRequisitionKey.dataProviderId == parentKey.dataProviderId) {
      "DataProvider in cmms_requisition ${cmmsRequisitionKey.dataProviderId} " +
        "does not match parent DataProvider ${parentKey.dataProviderId}."
    }

    // Validate report format.
    ReportKey.fromName(request.requisitionMetadata.report)
      ?: failGrpc(Status.INVALID_ARGUMENT) { "report is either unspecified or invalid." }

    val internalRequest = internalCreateRequisitionMetadataRequest {
      requisitionMetadata = request.requisitionMetadata.toInternal(parentKey)
      requestId = request.requestId
    }

    val internalRequisitionMetadata =
      try {
        internalClient.createRequisitionMetadata(internalRequest)
      } catch (ex: StatusException) {
        when (ex.status.code) {
          Status.Code.INVALID_ARGUMENT,
          Status.Code.FAILED_PRECONDITION ->
            throw ex.status.withDescription(ex.message).withCause(ex).asRuntimeException()
          Status.Code.ALREADY_EXISTS ->
            throw Status.ALREADY_EXISTS.withDescription(ex.message)
              .withCause(ex)
              .asRuntimeException()
          else ->
            throw Status.UNKNOWN.withDescription("Unknown internal error")
              .withCause(ex)
              .asRuntimeException()
        }
      }

    return internalRequisitionMetadata.toRequisitionMetadata()
  }

  override suspend fun getRequisitionMetadata(
    request: GetRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key = parseRequisitionMetadataKey(request.name)
    val internalRequest = internalGetRequisitionMetadataRequest {
      externalDataProviderId = key.dataProviderId.toLong()
      externalRequisitionMetadataId = key.requisitionMetadataId.toLong()
    }

    return internalClient.getRequisitionMetadata(internalRequest).toRequisitionMetadata()
  }

  override suspend fun lookupRequisitionMetadata(
    request: LookupRequisitionMetadataRequest
  ): RequisitionMetadata {
    val parentKey =
      DataProviderKey.fromName(request.parent)
        ?: failGrpc(Status.INVALID_ARGUMENT) { "Parent is either unspecified or invalid." }

    val internalRequest = internalLookupRequisitionMetadataRequest {
      externalDataProviderId = parentKey.dataProviderId.toLong()
      when (request.lookupKeyCase) {
        LookupRequisitionMetadataRequest.LookupKeyCase.CMMS_REQUISITION ->
          cmmsRequisition = request.cmmsRequisition
        LookupRequisitionMetadataRequest.LookupKeyCase.BLOB_URI -> blobUri = request.blobUri
        LookupRequisitionMetadataRequest.LookupKeyCase.LOOKUPKEY_NOT_SET ->
          failGrpc(Status.INVALID_ARGUMENT) { "Lookup key is not set." }
      }
    }
    return internalClient.lookupRequisitionMetadata(internalRequest).toRequisitionMetadata()
  }

  override suspend fun fetchLatestCmmsCreateTime(
    request: FetchLatestCmmsCreateTimeRequest
  ): Timestamp {
    val parentKey =
      DataProviderKey.fromName(request.parent)
        ?: failGrpc(Status.INVALID_ARGUMENT) { "Parent is either unspecified or invalid." }

    val internalRequest = internalFetchLatestCmmsCreateTimeRequest {
      externalDataProviderId = parentKey.dataProviderId.toLong()
    }

    return internalClient.fetchLatestCmmsCreateTime(internalRequest)
  }

  override suspend fun queueRequisitionMetadata(
    request: QueueRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key = parseRequisitionMetadataKey(request.name)
    val internalRequest = internalQueueRequisitionMetadataRequest {
      externalDataProviderId = key.dataProviderId.toLong()
      externalRequisitionMetadataId = key.requisitionMetadataId.toLong()
      etag = request.etag
      workItem = request.workItem
    }
    return internalClient.queueRequisitionMetadata(internalRequest).toRequisitionMetadata()
  }

  override suspend fun startProcessingRequisitionMetadata(
    request: StartProcessingRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key = parseRequisitionMetadataKey(request.name)
    val internalRequest = internalStartProcessingRequisitionMetadataRequest {
      externalDataProviderId = key.dataProviderId.toLong()
      externalRequisitionMetadataId = key.requisitionMetadataId.toLong()
      etag = request.etag
    }
    return internalClient
      .startProcessingRequisitionMetadata(internalRequest)
      .toRequisitionMetadata()
  }

  override suspend fun fulfillRequisitionMetadata(
    request: FulfillRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key = parseRequisitionMetadataKey(request.name)
    val internalRequest = internalFulfillRequisitionMetadataRequest {
      externalDataProviderId = key.dataProviderId.toLong()
      externalRequisitionMetadataId = key.requisitionMetadataId.toLong()
      etag = request.etag
    }
    return internalClient.fulfillRequisitionMetadata(internalRequest).toRequisitionMetadata()
  }

  override suspend fun refuseRequisitionMetadata(
    request: RefuseRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key = parseRequisitionMetadataKey(request.name)
    val internalRequest = internalRefuseRequisitionMetadataRequest {
      externalDataProviderId = key.dataProviderId.toLong()
      externalRequisitionMetadataId = key.requisitionMetadataId.toLong()
      etag = request.etag
      errorMessage = request.errorMessage
    }
    return internalClient.refuseRequisitionMetadata(internalRequest).toRequisitionMetadata()
  }

  override suspend fun deleteRequisitionMetadata(
    request: DeleteRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key = parseRequisitionMetadataKey(request.name)
    val internalRequest = internalDeleteRequisitionMetadataRequest {
      externalDataProviderId = key.dataProviderId.toLong()
      externalRequisitionMetadataId = key.requisitionMetadataId.toLong()
      etag = request.etag
    }
    return internalClient.deleteRequisitionMetadata(internalRequest).toRequisitionMetadata()
  }

  private fun parseRequisitionMetadataKey(name: String): RequisitionMetadataKey {
    return RequisitionMetadataKey.fromName(name)
      ?: failGrpc(Status.INVALID_ARGUMENT) { "Name is either unspecified or invalid." }
  }
}
