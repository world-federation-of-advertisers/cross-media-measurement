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
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.RequisitionMetadataKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata as InternalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub as InternalRequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.createRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.fetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata as internalRequisitionMetadata

class RequisitionMetadataService(
  private val internalClient: InternalRequisitionMetadataServiceCoroutineStub,
  private val idGenerator: IdGenerator,
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

    val externalRequisitionMetadataId = idGenerator.generateExternalId()
    val internalRequisitionMetadata =
      request.requisitionMetadata.toInternal(
        parentKey.dataProviderId,
        externalRequisitionMetadataId.apiId.value,
      )

    val internalRequest = createRequisitionMetadataRequest {
      requisitionMetadata = internalRequisitionMetadata
    }

    return internalClient.createRequisitionMetadata(internalRequest).toPublic()
  }

  override suspend fun getRequisitionMetadata(
    request: GetRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: failGrpc(Status.INVALID_ARGUMENT) { "Name is either unspecified or invalid." }

    val internalRequest = getRequisitionMetadataRequest {
      externalDataProviderId = key.dataProviderId.toLong()
      externalRequisitionMetadataId = key.requisitionMetadataId.toLong()
    }

    return internalClient.getRequisitionMetadata(internalRequest).toPublic()
  }

  override suspend fun fetchLatestCmmsCreateTime(
    request: FetchLatestCmmsCreateTimeRequest
  ): Timestamp {
    val parentKey =
      DataProviderKey.fromName(request.parent)
        ?: failGrpc(Status.INVALID_ARGUMENT) { "Parent is either unspecified or invalid." }

    val internalRequest = fetchLatestCmmsCreateTimeRequest {
      externalDataProviderId = parentKey.dataProviderId.toLong()
    }

    return internalClient.fetchLatestCmmsCreateTime(internalRequest)
  }
}

/** Converts an internal [InternalRequisitionMetadata] to a public [RequisitionMetadata]. */
private fun InternalRequisitionMetadata.toPublic(): RequisitionMetadata {
  val source = this
  return requisitionMetadata {
    name =
      RequisitionMetadataKey(
        source.externalDataProviderId.toString(),
        source.externalRequisitionMetadataId.toString(),
      )
        .toName()
    cmmsRequisition = source.cmmsRequisition
    blobUri = source.blobUri
    groupId = source.groupId
    cmmsCreateTime = source.cmmsCreateTime
    cmmsReportId = source.cmmsReportId
    workItemId = source.workItemId
    requestId = source.requestId
    state = source.state.toPublic()
    createTime = source.createTime
    updateTime = source.updateTime
    errorMessage = source.errorMessage
    etag = source.etag
  }
}

/** Converts a public [RequisitionMetadata] to an internal [InternalRequisitionMetadata]. */
private fun RequisitionMetadata.toInternal(
  externalDataProviderId: String,
  externalRequisitionMetadataId: Long,
): InternalRequisitionMetadata {
  val source = this
  return internalRequisitionMetadata {
    this.externalDataProviderId = externalDataProviderId.toLong()
    this.externalRequisitionMetadataId = externalRequisitionMetadataId
    cmmsRequisition = source.cmmsRequisition
    blobUri = source.blobUri
    groupId = source.groupId
    cmmsCreateTime = source.cmmsCreateTime
    cmmsReportId = source.cmmsReportId
    workItemId = source.workItemId
    requestId = source.requestId
    // State is set by the backend, not by the client on creation.
  }
}

/**
 * Converts an internal [InternalRequisitionMetadata.State] to a public [RequisitionMetadata.State].
 */
private fun InternalRequisitionMetadata.State.toPublic(): RequisitionMetadata.State {
  return RequisitionMetadata.State.forNumber(this.number)
}
