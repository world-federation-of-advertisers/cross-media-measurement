// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RawImpressionUploadResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findLatestUploadByDoneBlobUri
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadByMarkRegistrationCompleteRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRawImpressionUpload
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionUploadExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionUploadHasModelLines
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionUploads
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadRegistrationComplete
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadState
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.CreateRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.GetRawImpressionUploadRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsResponse
import org.wfanet.measurement.internal.edpaggregator.MarkRawImpressionUploadRegistrationCompleteRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUpload
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadsPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadsResponse
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload

class SpannerRawImpressionUploadService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RawImpressionUploadServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRawImpressionUpload(
    request: CreateRawImpressionUploadRequest
  ): RawImpressionUpload {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUpload.doneBlobUri.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload.done_blob_uri")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUpload.doneBlobGeneration == 0L) {
      throw RequiredFieldNotSetException("raw_impression_upload.done_blob_generation")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUpload.doneBlobGeneration < 0L) {
      throw InvalidFieldValueException("raw_impression_upload.done_blob_generation")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (
      request.rawImpressionUpload.hasDoneBlobCreateTime() &&
        !Timestamps.isValid(request.rawImpressionUpload.doneBlobCreateTime)
    ) {
      throw InvalidFieldValueException("raw_impression_upload.done_blob_create_time")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val requestId: String = request.requestId
    if (requestId.isEmpty()) {
      throw RequiredFieldNotSetException("request_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    try {
      UUID.fromString(requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("request_id", e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createRawImpressionUpload"))

    val result: RawImpressionUpload =
      try {
        transactionRunner.run { txn ->
          val existing: RawImpressionUploadResult? =
            txn.findUploadByCreateRequestId(request.dataProviderResourceId, requestId)
          if (existing != null) {
            if (
              existing.rawImpressionUpload.doneBlobUri != request.rawImpressionUpload.doneBlobUri ||
                existing.rawImpressionUpload.doneBlobGeneration !=
                  request.rawImpressionUpload.doneBlobGeneration ||
                existing.rawImpressionUpload.hasDoneBlobCreateTime() !=
                  request.rawImpressionUpload.hasDoneBlobCreateTime() ||
                (existing.rawImpressionUpload.hasDoneBlobCreateTime() &&
                  existing.rawImpressionUpload.doneBlobCreateTime !=
                    request.rawImpressionUpload.doneBlobCreateTime)
            ) {
              throw RawImpressionUploadAlreadyExistsException(
                  request.dataProviderResourceId,
                  requestId,
                  existing.rawImpressionUpload.doneBlobUri,
                  request.rawImpressionUpload.doneBlobUri,
                  existing.rawImpressionUpload.doneBlobGeneration,
                  request.rawImpressionUpload.doneBlobGeneration,
                )
                .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
            }
            return@run existing.rawImpressionUpload
          }

          val previous =
            txn.findLatestUploadByDoneBlobUri(
              request.dataProviderResourceId,
              request.rawImpressionUpload.doneBlobUri,
            )
          if (
            previous != null &&
              (!request.rawImpressionUpload.hasDoneBlobCreateTime() ||
                (previous.rawImpressionUpload.hasDoneBlobCreateTime() &&
                  Timestamps.compare(
                    previous.rawImpressionUpload.doneBlobCreateTime,
                    request.rawImpressionUpload.doneBlobCreateTime,
                  ) >= 0))
          ) {
            throw RawImpressionUploadAlreadyExistsException(
                request.dataProviderResourceId,
                requestId,
                previous.rawImpressionUpload.doneBlobUri,
                request.rawImpressionUpload.doneBlobUri,
                previous.rawImpressionUpload.doneBlobGeneration,
                request.rawImpressionUpload.doneBlobGeneration,
              )
              .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
          }
          if (
            previous?.rawImpressionUpload?.state ==
              RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED &&
              !previous.rawImpressionUpload.registrationComplete
          ) {
            // The previous dispatcher invocation did not finish registration. Supersede it in the
            // same transaction that claims this newer generation. Any concurrent attempt to add
            // model lines to the previous upload will conflict and then observe FAILED.
            txn.updateRawImpressionUploadState(
              request.dataProviderResourceId,
              previous.rawImpressionUploadId,
              RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED,
            )
          }
          val replacesResourceId = previous?.rawImpressionUpload?.rawImpressionUploadResourceId

          val rawImpressionUploadId: Long =
            idGenerator.generateNewId { id ->
              txn.rawImpressionUploadExists(request.dataProviderResourceId, id)
            }

          val resolvedResourceId: String = "rawImpressionUpload-${UUID.randomUUID()}"

          txn.insertRawImpressionUpload(
            rawImpressionUploadId,
            request.dataProviderResourceId,
            resolvedResourceId,
            request.rawImpressionUpload.doneBlobUri,
            requestId,
            RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED,
            request.rawImpressionUpload.doneBlobGeneration,
            request.rawImpressionUpload.doneBlobCreateTime.takeIf {
              request.rawImpressionUpload.hasDoneBlobCreateTime()
            },
            replacesResourceId,
          )

          rawImpressionUpload {
            dataProviderResourceId = request.dataProviderResourceId
            rawImpressionUploadResourceId = resolvedResourceId
            doneBlobUri = request.rawImpressionUpload.doneBlobUri
            doneBlobGeneration = request.rawImpressionUpload.doneBlobGeneration
            if (request.rawImpressionUpload.hasDoneBlobCreateTime()) {
              doneBlobCreateTime = request.rawImpressionUpload.doneBlobCreateTime
            }
            if (replacesResourceId != null) {
              replacesRawImpressionUploadResourceId = replacesResourceId
            }
            state = RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED
            registrationComplete = false
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw RawImpressionUploadAlreadyExistsException(
              request.dataProviderResourceId,
              requestId,
              request.rawImpressionUpload.doneBlobUri,
              request.rawImpressionUpload.doneBlobUri,
              request.rawImpressionUpload.doneBlobGeneration,
              request.rawImpressionUpload.doneBlobGeneration,
              e,
            )
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    if (result.hasCreateTime()) {
      return result
    }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return result.copy {
      createTime = commitTimestamp
      updateTime = commitTimestamp
      etag = ETags.computeETag(commitTimestamp.toInstant())
    }
  }

  override suspend fun markRawImpressionUploadRegistrationComplete(
    request: MarkRawImpressionUploadRegistrationCompleteRequest
  ): RawImpressionUpload {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestId.isEmpty()) {
      throw RequiredFieldNotSetException("request_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    try {
      UUID.fromString(request.requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("request_id", e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(
        Options.tag("action=markRawImpressionUploadRegistrationComplete")
      )
    val (result, changed) =
      try {
        transactionRunner.run { txn ->
          val replay =
            txn.findUploadByMarkRegistrationCompleteRequestId(
              request.dataProviderResourceId,
              request.requestId,
            )
          if (replay != null) {
            if (
              replay.rawImpressionUpload.rawImpressionUploadResourceId !=
                request.rawImpressionUploadResourceId
            ) {
              throw Status.ALREADY_EXISTS.withDescription(
                  "request_id was already used for another RawImpressionUpload"
                )
                .asRuntimeException()
            }
            return@run replay.rawImpressionUpload to false
          }

          val existing =
            txn.getRawImpressionUploadByResourceId(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
            )
          if (request.etag.isEmpty()) {
            throw RequiredFieldNotSetException("etag")
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
          }
          if (request.etag != existing.rawImpressionUpload.etag) {
            throw EtagMismatchException(request.etag, existing.rawImpressionUpload.etag)
              .asStatusRuntimeException(Status.Code.ABORTED)
          }
          if (existing.rawImpressionUpload.registrationComplete) {
            throw Status.FAILED_PRECONDITION.withDescription(
                "RawImpressionUpload registration is already complete"
              )
              .asRuntimeException()
          }
          if (
            existing.rawImpressionUpload.state ==
              RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED
          ) {
            throw Status.FAILED_PRECONDITION.withDescription(
                "Cannot complete registration for a failed RawImpressionUpload"
              )
              .asRuntimeException()
          }
          val completedState =
            if (
              existing.rawImpressionUpload.state !=
                RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED ||
                txn.rawImpressionUploadHasModelLines(
                  request.dataProviderResourceId,
                  existing.rawImpressionUploadId,
                )
            ) {
              null
            } else {
              RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED
            }
          txn.updateRawImpressionUploadRegistrationComplete(
            request.dataProviderResourceId,
            existing.rawImpressionUploadId,
            request.requestId,
            completedState,
          )
          existing.rawImpressionUpload.copy {
            registrationComplete = true
            if (completedState != null) {
              state = completedState
            }
          } to true
        }
      } catch (e: RawImpressionUploadNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw Status.ALREADY_EXISTS.withCause(e).asRuntimeException()
        }
        throw e
      }
    return if (changed) {
      val commitTimestamp = transactionRunner.getCommitTimestamp().toProto()
      result.copy {
        updateTime = commitTimestamp
        etag = ETags.computeETag(commitTimestamp.toInstant())
      }
    } else {
      result
    }
  }

  override suspend fun getRawImpressionUpload(
    request: GetRawImpressionUploadRequest
  ): RawImpressionUpload {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      databaseClient.singleUse().use { txn ->
        txn
          .getRawImpressionUploadByResourceId(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
          )
          .rawImpressionUpload
      }
    } catch (e: RawImpressionUploadNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun listRawImpressionUploads(
    request: ListRawImpressionUploadsRequest
  ): ListRawImpressionUploadsResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName ->
          "$fieldName must be non-negative"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize: Int =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }

    val after: ListRawImpressionUploadsPageToken.After? =
      if (request.hasPageToken()) request.pageToken.after else null

    databaseClient.singleUse().use { txn ->
      val uploadFlow: Flow<RawImpressionUpload> =
        txn
          .readRawImpressionUploads(
            request.dataProviderResourceId,
            request.filter,
            pageSize + 1,
            after,
          )
          .map { it.rawImpressionUpload }
      return listRawImpressionUploadsResponse {
        uploadFlow.collectIndexed { index, upload ->
          if (index == pageSize) {
            nextPageToken = listRawImpressionUploadsPageToken {
              this.after =
                ListRawImpressionUploadsPageTokenKt.after {
                  createTime =
                    this@listRawImpressionUploadsResponse.rawImpressionUploads.last().createTime
                  rawImpressionUploadResourceId =
                    this@listRawImpressionUploadsResponse.rawImpressionUploads
                      .last()
                      .rawImpressionUploadResourceId
                }
            }
          } else {
            rawImpressionUploads += upload
          }
        }
      }
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
