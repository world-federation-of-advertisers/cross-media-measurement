// Copyright 2026 The Cross-Media Measurement Authors
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
import io.grpc.StatusRuntimeException
import java.io.IOException
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.PoolAssignmentJobKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreatePoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.CreatePoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.MarkPoolAssignmentJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkPoolAssignmentJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.MarkPoolAssignmentJobSucceededResponseKt.lastShardResult
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobSucceededResponse
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsPageToken as InternalListPageToken
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsResponse as InternalListResponse
import org.wfanet.measurement.internal.edpaggregator.MarkPoolAssignmentJobSucceededResponse as InternalMarkSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJob as InternalPoolAssignmentJob
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub as InternalPoolAssignmentJobServiceStub
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentState
import org.wfanet.measurement.internal.edpaggregator.batchCreatePoolAssignmentJobsRequest as internalBatchCreateRequest
import org.wfanet.measurement.internal.edpaggregator.createPoolAssignmentJobRequest as internalCreateRequest
import org.wfanet.measurement.internal.edpaggregator.getPoolAssignmentJobRequest as internalGetRequest
import org.wfanet.measurement.internal.edpaggregator.listPoolAssignmentJobsRequest as internalListRequest
import org.wfanet.measurement.internal.edpaggregator.markPoolAssignmentJobFailedRequest as internalMarkFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markPoolAssignmentJobSucceededRequest as internalMarkSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.poolAssignmentJob as internalPoolAssignmentJob

class PoolAssignmentJobService(
  private val internalPoolAssignmentJobStub: InternalPoolAssignmentJobServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : PoolAssignmentJobServiceCoroutineImplBase(coroutineContext) {

  // TODO(world-federation-of-advertisers/cross-media-measurement#4078): Flip request_id to REQUIRED
  // + UUID4
  override suspend fun createPoolAssignmentJob(
    request: CreatePoolAssignmentJobRequest
  ): PoolAssignmentJob {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (!request.hasPoolAssignmentJob()) {
      throw RequiredFieldNotSetException("pool_assignment_job")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.poolAssignmentJob.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("pool_assignment_job.cmms_model_line")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.requestId.isNotEmpty()) {
      try {
        UUID.fromString(request.requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("request_id", e)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val internalResponse: InternalPoolAssignmentJob =
      try {
        internalPoolAssignmentJobStub.createPoolAssignmentJob(
          internalCreateRequest {
            poolAssignmentJob = internalPoolAssignmentJob {
              dataProviderResourceId = uploadKey.dataProviderId
              rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
              cmmsModelLine = request.poolAssignmentJob.cmmsModelLine
              shardIndex = request.poolAssignmentJob.shardIndex
            }
            requestId = request.requestId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  // TODO(world-federation-of-advertisers/cross-media-measurement#4078): Flip per-element request_id
  // to REQUIRED + UUID4
  override suspend fun batchCreatePoolAssignmentJobs(
    request: BatchCreatePoolAssignmentJobsRequest
  ): BatchCreatePoolAssignmentJobsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.requestsList.isEmpty()) {
      throw RequiredFieldNotSetException("requests")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_SIZE elements"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    request.requestsList.forEachIndexed { index, createRequest ->
      if (createRequest.parent.isNotEmpty() && createRequest.parent != request.parent) {
        throw InvalidFieldValueException("requests.$index.parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (createRequest.poolAssignmentJob.cmmsModelLine.isEmpty()) {
        throw RequiredFieldNotSetException("requests.$index.pool_assignment_job.cmms_model_line")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (createRequest.requestId.isNotEmpty()) {
        try {
          UUID.fromString(createRequest.requestId)
        } catch (e: IllegalArgumentException) {
          throw InvalidFieldValueException("requests.$index.request_id", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    }

    val internalResponse =
      try {
        internalPoolAssignmentJobStub.batchCreatePoolAssignmentJobs(
          internalBatchCreateRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
            requests +=
              request.requestsList.map { createRequest ->
                internalCreateRequest {
                  poolAssignmentJob = internalPoolAssignmentJob {
                    dataProviderResourceId = uploadKey.dataProviderId
                    rawImpressionUploadResourceId = uploadKey.rawImpressionUploadId
                    cmmsModelLine = createRequest.poolAssignmentJob.cmmsModelLine
                    shardIndex = createRequest.poolAssignmentJob.shardIndex
                  }
                  requestId = createRequest.requestId
                }
              }
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return batchCreatePoolAssignmentJobsResponse {
      poolAssignmentJobs += internalResponse.poolAssignmentJobsList.map { it.toPublic() }
    }
  }

  override suspend fun getPoolAssignmentJob(
    request: GetPoolAssignmentJobRequest
  ): PoolAssignmentJob {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val jobKey =
      PoolAssignmentJobKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalPoolAssignmentJob =
      try {
        internalPoolAssignmentJobStub.getPoolAssignmentJob(
          internalGetRequest {
            dataProviderResourceId = jobKey.dataProviderId
            rawImpressionUploadResourceId = jobKey.rawImpressionUploadId
            poolAssignmentJobResourceId = jobKey.poolAssignmentJobId
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  override suspend fun listPoolAssignmentJobs(
    request: ListPoolAssignmentJobsRequest
  ): ListPoolAssignmentJobsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val uploadKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (
      request.hasFilter() &&
        request.filter.stateInList.any {
          it == PoolAssignmentJob.State.STATE_UNSPECIFIED ||
            it == PoolAssignmentJob.State.UNRECOGNIZED
        }
    ) {
      throw InvalidFieldValueException("filter.state_in") {
          "$it must not contain STATE_UNSPECIFIED or an unrecognized value"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val internalPageToken: InternalListPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalResponse: InternalListResponse =
      try {
        internalPoolAssignmentJobStub.listPoolAssignmentJobs(
          internalListRequest {
            dataProviderResourceId = uploadKey.dataProviderId
            rawImpressionUploadResourceId =
              if (uploadKey.rawImpressionUploadId == WILDCARD_ID) ""
              else uploadKey.rawImpressionUploadId
            this.pageSize = pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
            if (request.hasFilter()) {
              filter =
                org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsRequestKt
                  .filter {
                    if (request.filter.stateInList.isNotEmpty()) {
                      stateIn += request.filter.stateInList.map { it.toInternal() }
                    }
                    if (request.filter.hasCreateTimeIn()) {
                      createTimeIn = request.filter.createTimeIn
                    }
                    if (request.filter.cmmsModelLine.isNotEmpty()) {
                      cmmsModelLine = request.filter.cmmsModelLine
                    }
                  }
            }
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return listPoolAssignmentJobsResponse {
      poolAssignmentJobs += internalResponse.poolAssignmentJobsList.map { it.toPublic() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  // TODO(world-federation-of-advertisers/cross-media-measurement#4078): Flip request_id to REQUIRED
  // + UUID4
  override suspend fun markPoolAssignmentJobSucceeded(
    request: MarkPoolAssignmentJobSucceededRequest
  ): MarkPoolAssignmentJobSucceededResponse {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val jobKey =
      PoolAssignmentJobKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!request.hasEncryptedDek()) {
      throw RequiredFieldNotSetException("encrypted_dek")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.requestId.isNotEmpty()) {
      try {
        UUID.fromString(request.requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("request_id", e)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val internalResponse: InternalMarkSucceededResponse =
      try {
        internalPoolAssignmentJobStub.markPoolAssignmentJobSucceeded(
          internalMarkSucceededRequest {
            dataProviderResourceId = jobKey.dataProviderId
            rawImpressionUploadResourceId = jobKey.rawImpressionUploadId
            poolAssignmentJobResourceId = jobKey.poolAssignmentJobId
            etag = request.etag
            requestId = request.requestId
            encryptedDek = request.encryptedDek.toInternal()
            poolOffsets += request.poolOffsetsList
            if (request.hasMaxEventDate()) {
              maxEventDate = request.maxEventDate
            }
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return markPoolAssignmentJobSucceededResponse {
      poolAssignmentJob = internalResponse.poolAssignmentJob.toPublic()
      if (internalResponse.hasLastShardResult()) {
        this.lastShardResult = lastShardResult {
          poolOffsets += internalResponse.lastShardResult.poolOffsetsList
          if (internalResponse.lastShardResult.hasMaxEventDate()) {
            maxEventDate = internalResponse.lastShardResult.maxEventDate
          }
        }
      }
    }
  }

  // TODO(world-federation-of-advertisers/cross-media-measurement#4078): Validate request_id (UUID4)
  // and forward it to the internal request, mirroring markPoolAssignmentJobSucceeded.
  override suspend fun markPoolAssignmentJobFailed(
    request: MarkPoolAssignmentJobFailedRequest
  ): PoolAssignmentJob {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val jobKey =
      PoolAssignmentJobKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    if (request.etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalResponse: InternalPoolAssignmentJob =
      try {
        internalPoolAssignmentJobStub.markPoolAssignmentJobFailed(
          internalMarkFailedRequest {
            dataProviderResourceId = jobKey.dataProviderId
            rawImpressionUploadResourceId = jobKey.rawImpressionUploadId
            poolAssignmentJobResourceId = jobKey.poolAssignmentJobId
            etag = request.etag
            errorMessage = request.errorMessage
          }
        )
      } catch (e: StatusException) {
        throw handleInternalError(e)
      }

    return internalResponse.toPublic()
  }

  companion object {
    private const val WILDCARD_ID = "-"
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
    private const val MAX_BATCH_SIZE = 50
  }
}

private fun handleInternalError(e: StatusException): StatusRuntimeException {
  return when (InternalErrors.getReason(e)) {
    InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
    InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
    InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
    InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
    InternalErrors.Reason.RAW_IMPRESSION_METADATA_NOT_FOUND,
    InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_NOT_FOUND,
    InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_STATE_INVALID,
    InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_NOT_FOUND,
    InternalErrors.Reason.RAW_IMPRESSION_METADATA_BATCH_FILE_ALREADY_EXISTS,
    InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
    InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
    InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
    InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
    InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
    InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
    InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
    InternalErrors.Reason.INVALID_FIELD_VALUE,
    InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND,
    InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_ALREADY_EXISTS,
    InternalErrors.Reason.VID_LABELING_JOB_NOT_FOUND,
    InternalErrors.Reason.VID_LABELING_JOB_STATE_INVALID,
    InternalErrors.Reason.VID_LABELING_JOB_ALREADY_EXISTS,
    InternalErrors.Reason.RANKER_JOB_NOT_FOUND,
    InternalErrors.Reason.RANKER_JOB_ALREADY_EXISTS,
    InternalErrors.Reason.RANKER_JOB_STATE_INVALID,
    InternalErrors.Reason.RANK_INDEX_BLOB_NOT_FOUND,
    InternalErrors.Reason.RANK_INDEX_BLOB_ALREADY_EXISTS,
    InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_NOT_FOUND,
    InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_INVALID,
    InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_STATE_INVALID,
    InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_CONCURRENT,
    InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_ALREADY_EXISTS,
    null -> Status.INTERNAL.withCause(e).asRuntimeException()
    InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND,
    InternalErrors.Reason.POOL_ASSIGNMENT_JOB_NOT_FOUND ->
      Status.NOT_FOUND.withCause(e).asRuntimeException()
    InternalErrors.Reason.POOL_ASSIGNMENT_JOB_STATE_INVALID ->
      Status.FAILED_PRECONDITION.withCause(e).asRuntimeException()
    InternalErrors.Reason.POOL_ASSIGNMENT_JOB_ALREADY_EXISTS ->
      Status.ALREADY_EXISTS.withCause(e).asRuntimeException()
    InternalErrors.Reason.ETAG_MISMATCH -> Status.ABORTED.withCause(e).asRuntimeException()
  }
}

/** Converts an internal [InternalPoolAssignmentJob] to a public one. */
internal fun InternalPoolAssignmentJob.toPublic(): PoolAssignmentJob {
  val source = this
  return poolAssignmentJob {
    name =
      PoolAssignmentJobKey(
          source.dataProviderResourceId,
          source.rawImpressionUploadResourceId,
          source.poolAssignmentJobResourceId,
        )
        .toName()
    state = source.state.toPublic()
    cmmsModelLine = source.cmmsModelLine
    shardIndex = source.shardIndex
    createTime = source.createTime
    updateTime = source.updateTime
    if (source.errorMessage.isNotEmpty()) {
      errorMessage = source.errorMessage
    }
    if (source.hasEncryptedDek()) {
      encryptedDek = source.encryptedDek.toPublic()
    }
    if (source.etag.isNotEmpty()) {
      etag = source.etag
    }
  }
}

/** Converts an internal [PoolAssignmentState] to a public [PoolAssignmentJob.State]. */
internal fun PoolAssignmentState.toPublic(): PoolAssignmentJob.State {
  return when (this) {
    PoolAssignmentState.POOL_ASSIGNMENT_STATE_CREATED -> PoolAssignmentJob.State.CREATED
    PoolAssignmentState.POOL_ASSIGNMENT_STATE_SUCCEEDED -> PoolAssignmentJob.State.SUCCEEDED
    PoolAssignmentState.POOL_ASSIGNMENT_STATE_FAILED -> PoolAssignmentJob.State.FAILED
    PoolAssignmentState.UNRECOGNIZED,
    PoolAssignmentState.POOL_ASSIGNMENT_STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}

/** Converts a public [PoolAssignmentJob.State] to an internal [PoolAssignmentState]. */
internal fun PoolAssignmentJob.State.toInternal(): PoolAssignmentState {
  return when (this) {
    PoolAssignmentJob.State.CREATED -> PoolAssignmentState.POOL_ASSIGNMENT_STATE_CREATED
    PoolAssignmentJob.State.SUCCEEDED -> PoolAssignmentState.POOL_ASSIGNMENT_STATE_SUCCEEDED
    PoolAssignmentJob.State.FAILED -> PoolAssignmentState.POOL_ASSIGNMENT_STATE_FAILED
    PoolAssignmentJob.State.UNRECOGNIZED,
    PoolAssignmentJob.State.STATE_UNSPECIFIED -> error("Unrecognized state")
  }
}
