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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FulfillRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LookupRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.QueueRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RefuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.StartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata as InternalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub as InternalRequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState as InternalState
import org.wfanet.measurement.internal.edpaggregator.createRequisitionMetadataRequest as internalCreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.fetchLatestCmmsCreateTimeRequest as internalFetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.internal.edpaggregator.fulfillRequisitionMetadataRequest as internalFulfillRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest as internalGetRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.lookupRequisitionMetadataRequest as internalLookupRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.queueRequisitionMetadataRequest as internalQueueRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.refuseRequisitionMetadataRequest as internalRefuseRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata as internalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.startProcessingRequisitionMetadataRequest as internalStartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.securecomputation.service.WorkItemKey

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
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: failGrpc(Status.INVALID_ARGUMENT) { "Parent is either unspecified or invalid." }

    val internalRequest = internalGetRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
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
      dataProviderResourceId = parentKey.dataProviderId
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
      dataProviderResourceId = parentKey.dataProviderId
    }

    return internalClient.fetchLatestCmmsCreateTime(internalRequest)
  }

  override suspend fun queueRequisitionMetadata(
    request: QueueRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: failGrpc(Status.INVALID_ARGUMENT) {
          "RequisitionMetadata name is either unspecified or invalid."
        }
    WorkItemKey.fromName(request.workItem)
      ?: failGrpc(Status.INVALID_ARGUMENT) { "workItem is either unspecified or invalid." }
    grpcRequire(request.etag.isNotEmpty()) { "Etag is missing." }

    val internalRequest = internalQueueRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
      etag = request.etag
      workItem = request.workItem
    }
    return internalClient.queueRequisitionMetadata(internalRequest).toRequisitionMetadata()
  }

  override suspend fun startProcessingRequisitionMetadata(
    request: StartProcessingRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: failGrpc(Status.INVALID_ARGUMENT) {
          "RequisitionMetadata name is either unspecified or invalid."
        }
    grpcRequire(request.etag.isNotEmpty()) { "Etag is missing." }

    val internalRequest = internalStartProcessingRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
      etag = request.etag
    }
    return internalClient
      .startProcessingRequisitionMetadata(internalRequest)
      .toRequisitionMetadata()
  }

  override suspend fun fulfillRequisitionMetadata(
    request: FulfillRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: failGrpc(Status.INVALID_ARGUMENT) {
          "RequisitionMetadata name is either unspecified or invalid."
        }
    grpcRequire(request.etag.isNotEmpty()) { "Etag is missing." }

    val internalRequest = internalFulfillRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
      etag = request.etag
    }
    return internalClient.fulfillRequisitionMetadata(internalRequest).toRequisitionMetadata()
  }

  override suspend fun refuseRequisitionMetadata(
    request: RefuseRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: failGrpc(Status.INVALID_ARGUMENT) {
          "RequisitionMetadata name is either unspecified or invalid."
        }
    grpcRequire(request.etag.isNotEmpty()) { "Etag is missing." }

    val internalRequest = internalRefuseRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
      etag = request.etag
      refusalMessage = request.refusalMessage
    }
    return internalClient.refuseRequisitionMetadata(internalRequest).toRequisitionMetadata()
  }

  /** Converts an internal [InternalRequisitionMetadata] to a public [RequisitionMetadata]. */
  fun InternalRequisitionMetadata.toRequisitionMetadata(): RequisitionMetadata {
    val source = this
    return requisitionMetadata {
      name =
        RequisitionMetadataKey(source.dataProviderResourceId, source.requisitionMetadataResourceId)
          .toName()
      cmmsRequisition = source.cmmsRequisition
      blobUri = source.blobUri
      blobTypeUrl = source.blobTypeUrl
      groupId = source.groupId
      cmmsCreateTime = source.cmmsCreateTime
      report = source.report
      workItem = source.workItem
      state = source.state.toState()
      createTime = source.createTime
      updateTime = source.updateTime
      refusalMessage = source.refusalMessage
      etag = source.etag
    }
  }

  /**
   * Converts a public [RequisitionMetadata] to an internal [InternalRequisitionMetadata] for
   * creation.
   */
  fun RequisitionMetadata.toInternal(
    dataProviderKey: DataProviderKey
  ): InternalRequisitionMetadata {
    val source = this
    return internalRequisitionMetadata {
      this.dataProviderResourceId = dataProviderKey.dataProviderId
      cmmsRequisition = source.cmmsRequisition
      blobUri = source.blobUri
      blobTypeUrl = source.blobTypeUrl
      groupId = source.groupId
      cmmsCreateTime = source.cmmsCreateTime
      report = source.report
      workItem = source.workItem
      refusalMessage = source.refusalMessage
    }
  }

  /** Converts an [InternalState] to a public [RequisitionMetadata.State]. */
  internal fun InternalState.toState(): RequisitionMetadata.State {
    return when (this) {
      InternalState.REQUISITION_METADATA_STATE_STORED -> RequisitionMetadata.State.STORED
      InternalState.REQUISITION_METADATA_STATE_QUEUED -> RequisitionMetadata.State.QUEUED
      InternalState.REQUISITION_METADATA_STATE_PROCESSING -> RequisitionMetadata.State.PROCESSING
      InternalState.REQUISITION_METADATA_STATE_FULFILLED -> RequisitionMetadata.State.FULFILLED
      InternalState.REQUISITION_METADATA_STATE_REFUSED -> RequisitionMetadata.State.REFUSED
      InternalState.UNRECOGNIZED,
      InternalState.REQUISITION_METADATA_STATE_UNSPECIFIED -> error("Unrecognized state")
    }
  }
}
