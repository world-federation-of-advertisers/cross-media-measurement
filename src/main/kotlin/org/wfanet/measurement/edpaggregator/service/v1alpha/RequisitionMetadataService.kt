/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.DataProviderMismatchException
import org.wfanet.measurement.edpaggregator.service.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.RequisitionMetadataAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.RequisitionMetadataKey
import org.wfanet.measurement.edpaggregator.service.RequisitionMetadataNotFoundByCmmsRequisitionException
import org.wfanet.measurement.edpaggregator.service.RequisitionMetadataNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.CreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.edpaggregator.v1alpha.FulfillRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.LookupRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.QueueRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RefuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.StartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataPageToken as InternalListRequisitionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataRequest.Filter as InternalListRequisitionMetadataFilter
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataRequestKt.filter as internalListRequisitionMetadataRequestFilter
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataResponse as InternalListRequisitionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata as InternalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub as InternalRequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState as InternalState
import org.wfanet.measurement.internal.edpaggregator.createRequisitionMetadataRequest as internalCreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.fetchLatestCmmsCreateTimeRequest as internalFetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.internal.edpaggregator.fulfillRequisitionMetadataRequest as internalFulfillRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest as internalGetRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.listRequisitionMetadataRequest as internalListRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.lookupRequisitionMetadataRequest as internalLookupRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.queueRequisitionMetadataRequest as internalQueueRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.refuseRequisitionMetadataRequest as internalRefuseRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata as internalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.startProcessingRequisitionMetadataRequest as internalStartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.securecomputation.service.WorkItemKey

class RequisitionMetadataService(
  private val internalClient: InternalRequisitionMetadataServiceCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RequisitionMetadataServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRequisitionMetadata(
    request: CreateRequisitionMetadataRequest
  ): RequisitionMetadata {
    val parentKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("requisition_metadata.parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val requisitionMetadataKey =
      if (request.requisitionMetadata.name.isNotEmpty()) {
        RequisitionMetadataKey.fromName(request.requisitionMetadata.name)
          ?: throw InvalidFieldValueException("requisition_metadata.name")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } else {
        null
      }

    val cmmsRequisitionKey =
      CanonicalRequisitionKey.fromName(request.requisitionMetadata.cmmsRequisition)
        ?: throw InvalidFieldValueException("requisition_metadata.cmms_requisition")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (cmmsRequisitionKey.dataProviderId != parentKey.dataProviderId) {
      throw DataProviderMismatchException(
          parentKey.dataProviderId,
          cmmsRequisitionKey.dataProviderId,
        )
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    // Validate report format.
    ReportKey.fromName(request.requisitionMetadata.report)
      ?: throw InvalidFieldValueException(
          "requisition_metadata.report: ${request.requisitionMetadata.report}"
        )
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalRequest = internalCreateRequisitionMetadataRequest {
      requisitionMetadata =
        request.requisitionMetadata.toInternal(parentKey, requisitionMetadataKey)
      requestId = request.requestId
    }

    val internalRequisitionMetadata =
      try {
        internalClient.createRequisitionMetadata(internalRequest)
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS ->
            RequisitionMetadataAlreadyExistsException(e)
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.ETAG_MISMATCH,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalRequisitionMetadata.toRequisitionMetadata()
  }

  override suspend fun getRequisitionMetadata(
    request: GetRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: throw InvalidFieldValueException("requisition_metadata.name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalRequest = internalGetRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
    }
    val internalResponse =
      try {
        internalClient.getRequisitionMetadata(internalRequest)
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND ->
            RequisitionMetadataNotFoundException(key.dataProviderId, key.requisitionMetadataId)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.ETAG_MISMATCH,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toRequisitionMetadata()
  }

  override suspend fun listRequisitionMetadata(
    request: ListRequisitionMetadataRequest
  ): ListRequisitionMetadataResponse {
    // Validate parent.
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    // Validate page size.
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val internalPageToken: InternalListRequisitionMetadataPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListRequisitionMetadataPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalFilter: InternalListRequisitionMetadataFilter =
      internalListRequisitionMetadataRequestFilter {
        if (request.hasFilter()) {
          state = request.filter.state.toInternalState()
          if (request.filter.groupId.isNotEmpty()) {
            groupId = request.filter.groupId
          }
          if (request.filter.report.isNotEmpty()) {
            report = request.filter.report
          }
        }
      }

    val internalResponse: InternalListRequisitionMetadataResponse =
      try {
        internalClient.listRequisitionMetadata(
          internalListRequisitionMetadataRequest {
            this.pageSize = pageSize
            dataProviderResourceId = dataProviderKey.dataProviderId
            filter = internalFilter
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.ETAG_MISMATCH,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return listRequisitionMetadataResponse {
      requisitionMetadata +=
        internalResponse.requisitionMetadataList.map { it.toRequisitionMetadata() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun lookupRequisitionMetadata(
    request: LookupRequisitionMetadataRequest
  ): RequisitionMetadata {
    val parentKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("requisition_metadata.parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalRequest = internalLookupRequisitionMetadataRequest {
      dataProviderResourceId = parentKey.dataProviderId
      when (request.lookupKeyCase) {
        LookupRequisitionMetadataRequest.LookupKeyCase.CMMS_REQUISITION ->
          cmmsRequisition = request.cmmsRequisition
        LookupRequisitionMetadataRequest.LookupKeyCase.LOOKUPKEY_NOT_SET ->
          throw InvalidFieldValueException("requisition_metadata.lookup_key")
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }
    val internalResponse =
      try {
        internalClient.lookupRequisitionMetadata(internalRequest)
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION ->
            RequisitionMetadataNotFoundByCmmsRequisitionException(
                parentKey.dataProviderId,
                request.cmmsRequisition,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.ETAG_MISMATCH,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toRequisitionMetadata()
  }

  override suspend fun fetchLatestCmmsCreateTime(
    request: FetchLatestCmmsCreateTimeRequest
  ): Timestamp {
    val parentKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("requisition_metadata.parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalRequest = internalFetchLatestCmmsCreateTimeRequest {
      dataProviderResourceId = parentKey.dataProviderId
    }
    return try {
      internalClient.fetchLatestCmmsCreateTime(internalRequest)
    } catch (e: StatusException) {
      throw when (InternalErrors.getReason(e)) {
        InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
        InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
        InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
        InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
        InternalErrors.Reason.ETAG_MISMATCH,
        InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
        InternalErrors.Reason.INVALID_FIELD_VALUE,
        InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
        InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
        InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
        null -> Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
  }

  override suspend fun queueRequisitionMetadata(
    request: QueueRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: throw InvalidFieldValueException("requisition_metadata.name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    WorkItemKey.fromName(request.workItem)
      ?: throw InvalidFieldValueException("requisition_metadata.work_item")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    if (request.etag.isEmpty()) {
      throw InvalidFieldValueException("requisition_metadata.etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val internalRequest = internalQueueRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
      etag = request.etag
      workItem = request.workItem
    }
    val internalResponse =
      try {
        internalClient.queueRequisitionMetadata(internalRequest)
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND ->
            RequisitionMetadataNotFoundException(key.dataProviderId, key.requisitionMetadataId)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.ETAG_MISMATCH ->
            EtagMismatchException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }
    return internalResponse.toRequisitionMetadata()
  }

  override suspend fun startProcessingRequisitionMetadata(
    request: StartProcessingRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: throw InvalidFieldValueException("requisition_metadata.name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    if (request.etag.isEmpty()) {
      throw InvalidFieldValueException("requisition_metadata.etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalRequest = internalStartProcessingRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
      etag = request.etag
    }
    val internalResponse =
      try {
        internalClient.startProcessingRequisitionMetadata(internalRequest)
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND ->
            RequisitionMetadataNotFoundException(key.dataProviderId, key.requisitionMetadataId)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.ETAG_MISMATCH ->
            EtagMismatchException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }
    return internalResponse.toRequisitionMetadata()
  }

  override suspend fun fulfillRequisitionMetadata(
    request: FulfillRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: throw InvalidFieldValueException("requisition_metadata.name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    if (request.etag.isEmpty()) {
      throw InvalidFieldValueException("requisition_metadata.etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalRequest = internalFulfillRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
      etag = request.etag
    }
    val internalResponse =
      try {
        internalClient.fulfillRequisitionMetadata(internalRequest)
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND ->
            RequisitionMetadataNotFoundException(key.dataProviderId, key.requisitionMetadataId)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.ETAG_MISMATCH ->
            EtagMismatchException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }
    return internalResponse.toRequisitionMetadata()
  }

  override suspend fun refuseRequisitionMetadata(
    request: RefuseRequisitionMetadataRequest
  ): RequisitionMetadata {
    val key =
      RequisitionMetadataKey.fromName(request.name)
        ?: throw InvalidFieldValueException("requisition_metadata.name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    if (request.etag.isEmpty()) {
      throw InvalidFieldValueException("requisition_metadata.etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.refusalMessage.isEmpty()) {
      throw InvalidFieldValueException("requisition_metadata.refusal_message")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalRequest = internalRefuseRequisitionMetadataRequest {
      dataProviderResourceId = key.dataProviderId
      requisitionMetadataResourceId = key.requisitionMetadataId
      etag = request.etag
      refusalMessage = request.refusalMessage
    }
    val internalResponse =
      try {
        internalClient.refuseRequisitionMetadata(internalRequest)
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND ->
            RequisitionMetadataNotFoundException(key.dataProviderId, key.requisitionMetadataId)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.ETAG_MISMATCH ->
            EtagMismatchException.fromInternal(e)
              .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }
    return internalResponse.toRequisitionMetadata()
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
    dataProviderKey: DataProviderKey,
    requisitionMetadataKey: RequisitionMetadataKey?,
  ): InternalRequisitionMetadata {
    val source = this
    return internalRequisitionMetadata {
      this.dataProviderResourceId = dataProviderKey.dataProviderId
      if (requisitionMetadataKey != null) {
        requisitionMetadataResourceId = requisitionMetadataKey.requisitionMetadataId
      }
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

  fun RequisitionMetadata.State.toInternalState(): InternalState {
    return when (this) {
      RequisitionMetadata.State.STORED -> InternalState.REQUISITION_METADATA_STATE_STORED
      RequisitionMetadata.State.QUEUED -> InternalState.REQUISITION_METADATA_STATE_QUEUED
      RequisitionMetadata.State.PROCESSING -> InternalState.REQUISITION_METADATA_STATE_PROCESSING
      RequisitionMetadata.State.FULFILLED -> InternalState.REQUISITION_METADATA_STATE_FULFILLED
      RequisitionMetadata.State.REFUSED -> InternalState.REQUISITION_METADATA_STATE_REFUSED
      RequisitionMetadata.State.UNRECOGNIZED,
      RequisitionMetadata.State.STATE_UNSPECIFIED ->
        InternalState.REQUISITION_METADATA_STATE_UNSPECIFIED
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}
