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

import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.DataProviderMismatchException
import org.wfanet.measurement.edpaggregator.service.ImpressionMetadataAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.ImpressionMetadataKey
import org.wfanet.measurement.edpaggregator.service.ImpressionMetadataNotFoundException
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchDeleteImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.CreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.BatchCreateImpressionMetadataResponse as InternalBatchCreateImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.BatchDeleteImpressionMetadataResponse as InternalBatchDeleteImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.ComputeModelLineBoundsResponse as InternalComputeModelLineBoundsResponse
import org.wfanet.measurement.internal.edpaggregator.CreateImpressionMetadataRequest as InternalCreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.DeleteImpressionMetadataRequest as InternalDeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata as InternalImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub as InternalImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as InternalImpressionMetadataState
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageToken as InternalListImpressionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequest as InternalListImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequestKt.filter as internalListImpressionMetadataRequestFilter
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataResponse as InternalListImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.batchCreateImpressionMetadataRequest as internalBatchCreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.batchDeleteImpressionMetadataRequest as internalBatchDeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.computeModelLineBoundsRequest as internalComputeModelLineBoundsRequest
import org.wfanet.measurement.internal.edpaggregator.createImpressionMetadataRequest as internalCreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.deleteImpressionMetadataRequest as internalDeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getImpressionMetadataRequest as internalGetImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.impressionMetadata as internalImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.listImpressionMetadataRequest as internalListImpressionMetadataRequest

class ImpressionMetadataService(
  private val internalImpressionMetadataStub: InternalImpressionMetadataServiceCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ImpressionMetadataServiceCoroutineImplBase(coroutineContext) {
  override suspend fun getImpressionMetadata(
    request: GetImpressionMetadataRequest
  ): ImpressionMetadata {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val impressionMetadataKey =
      ImpressionMetadataKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalImpressionMetadata =
      try {
        internalImpressionMetadataStub.getImpressionMetadata(
          internalGetImpressionMetadataRequest {
            dataProviderResourceId = impressionMetadataKey.dataProviderId
            impressionMetadataResourceId = impressionMetadataKey.impressionMetadataId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND ->
            ImpressionMetadataNotFoundException(request.name, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toImpressionMetadata()
  }

  override suspend fun createImpressionMetadata(
    request: CreateImpressionMetadataRequest
  ): ImpressionMetadata {
    try {
      validateImpressionMetadataRequest(request, "")
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalResponse: InternalImpressionMetadata =
      try {
        val dataProviderKey: DataProviderKey = DataProviderKey.fromName(request.parent)!!

        internalImpressionMetadataStub.createImpressionMetadata(
          internalCreateImpressionMetadataRequest {
            this.requestId = request.requestId
            impressionMetadata = request.impressionMetadata.toInternal(dataProviderKey, null)
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS ->
            ImpressionMetadataAlreadyExistsException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.ETAG_MISMATCH,
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toImpressionMetadata()
  }

  override suspend fun batchCreateImpressionMetadata(
    request: BatchCreateImpressionMetadataRequest
  ): BatchCreateImpressionMetadataResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val blobUriSet = mutableSetOf<String>()
    val requestIdSet = mutableSetOf<String>()

    val internalRequests: List<InternalCreateImpressionMetadataRequest> =
      request.requestsList.mapIndexed { index, it ->
        if (it.parent.isNotEmpty() && it.parent != request.parent) {
          throw DataProviderMismatchException(request.parent, it.parent)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        val blobUri = it.impressionMetadata.blobUri
        if (!blobUriSet.add(blobUri)) {
          throw InvalidFieldValueException("requests.$index.blob_uri") {
              "blob uri $blobUri is duplicate in the batch of requests"
            }
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        val requestId = it.requestId
        if (requestId.isNotEmpty()) {
          if (!requestIdSet.add(requestId)) {
            throw InvalidFieldValueException("requests.$index.request_id") {
                "request Id $requestId is duplicate in the batch of requests"
              }
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
          }
        }

        try {
          validateImpressionMetadataRequest(it, "requests.$index.")
        } catch (e: RequiredFieldNotSetException) {
          throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        } catch (e: InvalidFieldValueException) {
          throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }

        internalCreateImpressionMetadataRequest {
          this.requestId = requestId
          impressionMetadata = it.impressionMetadata.toInternal(dataProviderKey, null)
        }
      }

    val internalResponse: InternalBatchCreateImpressionMetadataResponse =
      try {
        internalImpressionMetadataStub.batchCreateImpressionMetadata(
          internalBatchCreateImpressionMetadataRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
            requests += internalRequests
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS ->
            ImpressionMetadataAlreadyExistsException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return batchCreateImpressionMetadataResponse {
      impressionMetadata +=
        internalResponse.impressionMetadataList.map { it.toImpressionMetadata() }
    }
  }

  override suspend fun deleteImpressionMetadata(
    request: DeleteImpressionMetadataRequest
  ): ImpressionMetadata {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val key =
      ImpressionMetadataKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    return try {
      internalImpressionMetadataStub
        .deleteImpressionMetadata(
          internalDeleteImpressionMetadataRequest {
            dataProviderResourceId = key.dataProviderId
            impressionMetadataResourceId = key.impressionMetadataId
          }
        )
        .toImpressionMetadata()
    } catch (e: StatusException) {
      throw when (InternalErrors.getReason(e)) {
        InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND ->
          ImpressionMetadataNotFoundException(request.name, e)
            .asStatusRuntimeException(e.status.code)
        InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
        InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
        InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
        InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
        InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
        InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
        InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
        InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
        InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
        InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
        InternalErrors.Reason.INVALID_FIELD_VALUE,
        InternalErrors.Reason.ETAG_MISMATCH,
        null -> Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
  }

  override suspend fun batchDeleteImpressionMetadata(
    request: BatchDeleteImpressionMetadataRequest
  ): BatchDeleteImpressionMetadataResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.namesList.isEmpty()) {
      throw RequiredFieldNotSetException("names")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val nameSet = HashSet<String>()
    val internalDeleteRequests = mutableListOf<InternalDeleteImpressionMetadataRequest>()
    request.namesList.forEachIndexed { index, it ->
      if (it.isEmpty()) {
        throw RequiredFieldNotSetException("names.$index")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val impressionMetadataKey =
        ImpressionMetadataKey.fromName(it)
          ?: throw InvalidFieldValueException("names.$index")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

      if (dataProviderKey.dataProviderId != impressionMetadataKey.dataProviderId) {
        throw InvalidFieldValueException("names.$index")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (!nameSet.add(it)) {
        throw InvalidFieldValueException("names.$index")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      internalDeleteRequests.add(
        internalDeleteImpressionMetadataRequest {
          dataProviderResourceId = dataProviderKey.dataProviderId
          impressionMetadataResourceId = impressionMetadataKey.impressionMetadataId
        }
      )
    }

    val internalResponse: InternalBatchDeleteImpressionMetadataResponse =
      try {
        internalImpressionMetadataStub.batchDeleteImpressionMetadata(
          internalBatchDeleteImpressionMetadataRequest { requests += internalDeleteRequests }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND ->
            ImpressionMetadataNotFoundException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return batchDeleteImpressionMetadataResponse {
      impressionMetadata +=
        internalResponse.impressionMetadataList.map { it.toImpressionMetadata() }
    }
  }

  override suspend fun listImpressionMetadata(
    request: ListImpressionMetadataRequest
  ): ListImpressionMetadataResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val internalPageToken: InternalListImpressionMetadataPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListImpressionMetadataPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalFilter: InternalListImpressionMetadataRequest.Filter =
      internalListImpressionMetadataRequestFilter {
        if (request.filter.modelLine.isNotEmpty()) {
          cmmsModelLine = request.filter.modelLine
        }
        if (request.filter.eventGroupReferenceId.isNotEmpty()) {
          eventGroupReferenceId = request.filter.eventGroupReferenceId
        }
        if (request.filter.hasIntervalOverlaps()) {
          intervalOverlaps = request.filter.intervalOverlaps
        }
        if (request.filter.blobUriPrefix.isNotEmpty()) {
          blobUriPrefix = request.filter.blobUriPrefix
        }

        state =
          if (!request.showDeleted) {
            InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_ACTIVE
          } else {
            InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_DELETED
          }
      }

    val internalResponse: InternalListImpressionMetadataResponse =
      try {
        internalImpressionMetadataStub.listImpressionMetadata(
          internalListImpressionMetadataRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
            this.pageSize = pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
            filter = internalFilter
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return listImpressionMetadataResponse {
      impressionMetadata +=
        internalResponse.impressionMetadataList.map { it.toImpressionMetadata() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun computeModelLineBounds(
    request: ComputeModelLineBoundsRequest
  ): ComputeModelLineBoundsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val dataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalComputeModelLineBoundsResponse =
      try {
        internalImpressionMetadataStub.computeModelLineBounds(
          internalComputeModelLineBoundsRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return computeModelLineBoundsResponse {
      for ((key, value) in internalResponse.modelLineBoundsMap) {
        modelLineBounds += modelLineBoundMapEntry {
          this.key = key
          this.value = value
        }
      }
    }
  }

  /**
   * Checks whether the specified create impression metadata request is valid.
   *
   * @throws RequiredFieldNotSetException
   */
  private fun validateImpressionMetadataRequest(
    request: CreateImpressionMetadataRequest,
    fieldPathPrefix: String,
  ) {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}parent")
    }

    DataProviderKey.fromName(request.parent)
      ?: throw InvalidFieldValueException("${fieldPathPrefix}parent")

    val requestId = request.requestId
    if (requestId.isNotEmpty()) {
      try {
        UUID.fromString(requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("${fieldPathPrefix}request_id", e)
      }
    }

    if (!request.hasImpressionMetadata()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata")
    }

    if (request.impressionMetadata.blobUri.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata.blob_uri")
    }

    if (request.impressionMetadata.blobTypeUrl.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata.blob_type_url")
    }

    if (request.impressionMetadata.eventGroupReferenceId.isEmpty()) {
      throw RequiredFieldNotSetException(
        "${fieldPathPrefix}impression_metadata.event_group_reference_id"
      )
    }

    if (request.impressionMetadata.modelLine.isEmpty()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata.model_line")
    }

    ModelLineKey.fromName(request.impressionMetadata.modelLine)
      ?: throw InvalidFieldValueException("${fieldPathPrefix}impression_metadata.model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.impressionMetadata.hasInterval()) {
      throw RequiredFieldNotSetException("${fieldPathPrefix}impression_metadata.interval")
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}

/** Converts an internal [InternalImpressionMetadata] to a public [ImpressionMetadata]. */
fun InternalImpressionMetadata.toImpressionMetadata(): ImpressionMetadata {
  val source = this
  return impressionMetadata {
    name =
      ImpressionMetadataKey(source.dataProviderResourceId, source.impressionMetadataResourceId)
        .toName()
    blobUri = source.blobUri
    blobTypeUrl = source.blobTypeUrl
    eventGroupReferenceId = source.eventGroupReferenceId
    modelLine = source.cmmsModelLine
    interval = source.interval
    state = source.state.toState()
    createTime = source.createTime
    updateTime = source.updateTime
  }
}

/**
 * Converts a public [ImpressionMetadata] to an internal [InternalImpressionMetadata] for creation.
 */
fun ImpressionMetadata.toInternal(
  dataProviderKey: DataProviderKey,
  impressionMetadataKey: ImpressionMetadataKey?,
): InternalImpressionMetadata {
  val source = this
  return internalImpressionMetadata {
    dataProviderResourceId = dataProviderKey.dataProviderId
    if (impressionMetadataKey != null) {
      impressionMetadataResourceId = impressionMetadataKey.impressionMetadataId
    }
    blobUri = source.blobUri
    blobTypeUrl = source.blobTypeUrl
    eventGroupReferenceId = source.eventGroupReferenceId
    cmmsModelLine = source.modelLine
    interval = source.interval
  }
}

/**
 * Converts an internal [InternalImpressionMetadataState] to a public [ImpressionMetadata.State].
 */
internal fun InternalImpressionMetadataState.toState(): ImpressionMetadata.State {
  return when (this) {
    InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_ACTIVE ->
      ImpressionMetadata.State.ACTIVE
    InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_DELETED ->
      ImpressionMetadata.State.DELETED
    InternalImpressionMetadataState.UNRECOGNIZED,
    InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_UNSPECIFIED ->
      error("Unrecognized state")
  }
}

/**
 * Converts a public [ImpressionMetadata.State] to an internal [InternalImpressionMetadataState].
 */
internal fun ImpressionMetadata.State.toInternal(): InternalImpressionMetadataState {
  return when (this) {
    ImpressionMetadata.State.ACTIVE ->
      InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_ACTIVE
    ImpressionMetadata.State.DELETED ->
      InternalImpressionMetadataState.IMPRESSION_METADATA_STATE_DELETED
    ImpressionMetadata.State.UNRECOGNIZED,
    ImpressionMetadata.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
