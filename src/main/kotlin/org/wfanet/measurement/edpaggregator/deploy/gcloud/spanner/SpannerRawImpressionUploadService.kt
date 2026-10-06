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
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import java.security.MessageDigest
import java.time.Clock
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RawImpressionUploadResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.deleteVidLabelingEvictionFence
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findLatestUploadByDoneBlobUri
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findUploadByMarkRegistrationCompleteRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findVidLabelingEvictionFenceMutation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getVidLabelingEvictionFence
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.hasActiveDataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.hasActiveRawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.hasIncompleteRawImpressionUploadRegistration
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRawImpressionUpload
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertVidLabelingEvictionFence
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertVidLabelingEvictionFenceMutation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionUploadExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionUploadHasEvictionOperation
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rawImpressionUploadHasModelLines
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readProcessingDeferredRawImpressionUploadIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRawImpressionUploads
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadProcessingDeferred
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadRegistrationComplete
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadState
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateVidLabelingEvictionFenceState
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.AcquireRawImpressionUploadEvictionFenceRequest
import org.wfanet.measurement.internal.edpaggregator.AcquireRawImpressionUploadEvictionFenceResponse
import org.wfanet.measurement.internal.edpaggregator.AdvanceRawImpressionUploadEvictionFenceRequest
import org.wfanet.measurement.internal.edpaggregator.AdvanceRawImpressionUploadEvictionFenceResponse
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
import org.wfanet.measurement.internal.edpaggregator.ReleaseRawImpressionUploadEvictionFenceRequest
import org.wfanet.measurement.internal.edpaggregator.ReleaseRawImpressionUploadEvictionFenceResponse
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
import org.wfanet.measurement.internal.edpaggregator.acquireRawImpressionUploadEvictionFenceResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadsPageToken
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadsResponse
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUpload

class SpannerRawImpressionUploadService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
  private val clock: Clock = Clock.systemUTC(),
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
    if (request.evictionOperationId.isNotEmpty()) {
      try {
        UUID.fromString(request.evictionOperationId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("eviction_operation_id", e)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
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
                    request.rawImpressionUpload.doneBlobCreateTime) ||
                existing.rawImpressionUpload.evictionOperationId != request.evictionOperationId
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
          val activeEvictionFence = txn.getVidLabelingEvictionFence(request.dataProviderResourceId)
          val processingDeferred =
            if (activeEvictionFence == null) {
              if (request.evictionOperationId.isNotEmpty()) {
                throw Status.FAILED_PRECONDITION.withDescription(
                    "VID-labeling eviction ${request.evictionOperationId} is not active for " +
                      "DataProvider ${request.dataProviderResourceId}"
                  )
                  .asRuntimeException()
              }
              false
            } else if (request.evictionOperationId.isEmpty()) {
              true
            } else {
              if (request.evictionOperationId != activeEvictionFence.evictionOperationId) {
                throw Status.FAILED_PRECONDITION.withDescription(
                    "VID-labeling eviction ${activeEvictionFence.evictionOperationId} owns the fence for " +
                      "DataProvider ${request.dataProviderResourceId}"
                  )
                  .asRuntimeException()
              }
              if (
                activeEvictionFence.state !=
                  VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
              ) {
                throw Status.FAILED_PRECONDITION.withDescription(
                    "VID-labeling eviction ${request.evictionOperationId} is not ready for replay"
                  )
                  .asRuntimeException()
              }
              if (
                previous == null ||
                  (previous.rawImpressionUpload.evictionOperationId !=
                    request.evictionOperationId &&
                    !txn.rawImpressionUploadHasEvictionOperation(
                      request.dataProviderResourceId,
                      previous.rawImpressionUploadId,
                      request.evictionOperationId,
                    ))
              ) {
                throw Status.FAILED_PRECONDITION.withDescription(
                    "The preceding upload is not part of VID-labeling eviction " +
                      request.evictionOperationId
                  )
                  .asRuntimeException()
              }
              false
            }

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
            request.evictionOperationId.takeIf { it.isNotEmpty() },
            processingDeferred,
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
            evictionOperationId = request.evictionOperationId
            this.processingDeferred = processingDeferred
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

  override suspend fun acquireRawImpressionUploadEvictionFence(
    request: AcquireRawImpressionUploadEvictionFenceRequest
  ): AcquireRawImpressionUploadEvictionFenceResponse {
    validateEvictionFenceRequest(request.dataProviderResourceId, request.evictionOperationId)
    validateUuid(request.requestId, "request_id")
    val requestedState = request.initialFenceState()
    val requestFingerprint = request.fingerprint()
    val requestedEtag = requestFingerprint.toEtag()
    val result =
      databaseClient
        .readWriteTransaction(Options.tag("action=acquireRawImpressionUploadEvictionFence"))
        .run { txn ->
          txn
            .findVidLabelingEvictionFenceMutation(request.dataProviderResourceId, request.requestId)
            ?.let { replay ->
              requireMatchingFingerprint(replay.requestFingerprint, requestFingerprint)
              val liveFence =
                requireFenceOwnership(
                  txn.getVidLabelingEvictionFence(request.dataProviderResourceId),
                  request.dataProviderResourceId,
                  request.evictionOperationId,
                )
              if (
                requestedState ==
                  VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING &&
                  liveFence.state != requestedState
              ) {
                throw Status.FAILED_PRECONDITION.withDescription(
                    "Advance the VID-labeling eviction fence before eviction"
                  )
                  .asRuntimeException()
              }
              return@run FenceMutationResult(replay.newlyAcquired, liveFence.etag)
            }
          val currentFence = txn.getVidLabelingEvictionFence(request.dataProviderResourceId)
          if (currentFence?.evictionOperationId == request.evictionOperationId) {
            if (
              requestedState ==
                VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING &&
                currentFence.state != requestedState
            ) {
              throw Status.FAILED_PRECONDITION.withDescription(
                  "Advance the VID-labeling eviction fence before eviction"
                )
                .asRuntimeException()
            }
            txn.insertVidLabelingEvictionFenceMutation(
              request.dataProviderResourceId,
              request.requestId,
              requestFingerprint,
              currentFence.etag,
            )
            return@run FenceMutationResult(false, currentFence.etag)
          }
          if (currentFence != null) {
            throw Status.FAILED_PRECONDITION.withDescription(
                "VID-labeling eviction ${currentFence.evictionOperationId} is already in progress for " +
                  "DataProvider ${request.dataProviderResourceId}"
              )
              .asRuntimeException()
          }
          if (
            requestedState ==
              VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
          ) {
            requireDrained(txn, request.dataProviderResourceId)
          }
          txn.insertVidLabelingEvictionFence(
            request.dataProviderResourceId,
            request.evictionOperationId,
            requestedEtag,
            requestedState,
          )
          txn.insertVidLabelingEvictionFenceMutation(
            request.dataProviderResourceId,
            request.requestId,
            requestFingerprint,
            requestedEtag,
            newlyAcquired = true,
          )
          FenceMutationResult(true, requestedEtag)
        }
    return acquireRawImpressionUploadEvictionFenceResponse {
      newlyAcquired = result.newlyAcquired
      etag = result.etag
    }
  }

  override suspend fun advanceRawImpressionUploadEvictionFence(
    request: AdvanceRawImpressionUploadEvictionFenceRequest
  ): AdvanceRawImpressionUploadEvictionFenceResponse {
    validateEvictionFenceRequest(request.dataProviderResourceId, request.evictionOperationId)
    requireField(request.etag, "etag")
    validateUuid(request.requestId, "request_id")
    if (
      request.state != VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_DRAINING &&
        request.state != VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
    ) {
      throw InvalidFieldValueException("state")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val requestFingerprint = request.fingerprint()
    val requestedEtag = requestFingerprint.toEtag()
    val resultEtag =
      databaseClient
        .readWriteTransaction(Options.tag("action=advanceRawImpressionUploadEvictionFence"))
        .run { txn ->
          txn
            .findVidLabelingEvictionFenceMutation(request.dataProviderResourceId, request.requestId)
            ?.let { replay ->
              requireMatchingFingerprint(replay.requestFingerprint, requestFingerprint)
              val liveFence =
                requireFenceOwnership(
                  txn.getVidLabelingEvictionFence(request.dataProviderResourceId),
                  request.dataProviderResourceId,
                  request.evictionOperationId,
                )
              if (liveFence.state.number < request.state.number) {
                throw Status.FAILED_PRECONDITION.withDescription(
                    "VID-labeling eviction fence has not reached ${request.state}"
                  )
                  .asRuntimeException()
              }
              return@run liveFence.etag
            }
          val current =
            txn.getVidLabelingEvictionFence(request.dataProviderResourceId)
              ?: throw Status.FAILED_PRECONDITION.withDescription(
                  "No VID-labeling eviction fence exists for DataProvider " +
                    request.dataProviderResourceId
                )
                .asRuntimeException()
          if (current.evictionOperationId != request.evictionOperationId) {
            throw Status.FAILED_PRECONDITION.withDescription(
                "VID-labeling eviction ${current.evictionOperationId} owns the fence for " +
                  "DataProvider ${request.dataProviderResourceId}"
              )
              .asRuntimeException()
          }
          checkFenceEtag(request.etag, current.etag)
          if (current.state == request.state) {
            txn.insertVidLabelingEvictionFenceMutation(
              request.dataProviderResourceId,
              request.requestId,
              requestFingerprint,
              current.etag,
            )
            return@run current.etag
          }
          val validTransition =
            current.state ==
              VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING &&
              request.state ==
                VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_DRAINING ||
              current.state ==
                VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_DRAINING &&
                request.state ==
                  VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
          if (!validTransition) {
            throw Status.FAILED_PRECONDITION.withDescription(
                "Cannot advance VID-labeling eviction fence from ${current.state} to ${request.state}"
              )
              .asRuntimeException()
          }
          if (
            request.state ==
              VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
          ) {
            requireDrained(txn, request.dataProviderResourceId)
          }
          txn.updateVidLabelingEvictionFenceState(
            request.dataProviderResourceId,
            request.state,
            requestedEtag,
          )
          txn.insertVidLabelingEvictionFenceMutation(
            request.dataProviderResourceId,
            request.requestId,
            requestFingerprint,
            requestedEtag,
          )
          requestedEtag
        }
    return org.wfanet.measurement.internal.edpaggregator
      .advanceRawImpressionUploadEvictionFenceResponse { etag = resultEtag }
  }

  override suspend fun releaseRawImpressionUploadEvictionFence(
    request: ReleaseRawImpressionUploadEvictionFenceRequest
  ): ReleaseRawImpressionUploadEvictionFenceResponse {
    validateEvictionFenceRequest(request.dataProviderResourceId, request.evictionOperationId)
    requireField(request.etag, "etag")
    validateUuid(request.requestId, "request_id")
    val requestFingerprint = request.fingerprint()
    databaseClient
      .readWriteTransaction(Options.tag("action=releaseRawImpressionUploadEvictionFence"))
      .run { txn ->
        txn
          .findVidLabelingEvictionFenceMutation(request.dataProviderResourceId, request.requestId)
          ?.let { replay ->
            requireMatchingFingerprint(replay.requestFingerprint, requestFingerprint)
            return@run
          }
        val current = txn.getVidLabelingEvictionFence(request.dataProviderResourceId)
        if (current == null) {
          txn.insertVidLabelingEvictionFenceMutation(
            request.dataProviderResourceId,
            request.requestId,
            requestFingerprint,
            "",
          )
          return@run
        }
        if (current.evictionOperationId != request.evictionOperationId) {
          throw Status.FAILED_PRECONDITION.withDescription(
              "VID-labeling eviction ${current.evictionOperationId} owns the fence for DataProvider " +
                request.dataProviderResourceId
            )
            .asRuntimeException()
        }
        checkFenceEtag(request.etag, current.etag)
        txn.readProcessingDeferredRawImpressionUploadIds(request.dataProviderResourceId).collect {
          rawImpressionUploadId ->
          txn.updateRawImpressionUploadProcessingDeferred(
            request.dataProviderResourceId,
            rawImpressionUploadId,
            false,
          )
        }
        txn.deleteVidLabelingEvictionFence(request.dataProviderResourceId)
        txn.insertVidLabelingEvictionFenceMutation(
          request.dataProviderResourceId,
          request.requestId,
          requestFingerprint,
          "",
        )
      }
    return ReleaseRawImpressionUploadEvictionFenceResponse.getDefaultInstance()
  }

  private fun validateEvictionFenceRequest(
    dataProviderResourceId: String,
    evictionOperationId: String,
  ) {
    requireField(dataProviderResourceId, "data_provider_resource_id")
    validateUuid(evictionOperationId, "eviction_operation_id")
  }

  private fun requireField(value: String, field: String) {
    if (value.isEmpty()) {
      throw RequiredFieldNotSetException(field)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private fun validateUuid(value: String, field: String) {
    requireField(value, field)
    try {
      val uuid = UUID.fromString(value)
      if (
        uuid.version() != 4 ||
          uuid.variant() != 2 ||
          !uuid.toString().equals(value, ignoreCase = true)
      ) {
        throw IllegalArgumentException("$field must be a UUID4")
      }
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException(field, e)
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private fun checkFenceEtag(requestEtag: String, currentEtag: String) {
    try {
      EtagMismatchException.check(requestEtag, currentEtag)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  private fun requireMatchingFingerprint(existing: ByteString, requested: ByteString) {
    if (existing != requested) {
      throw Status.ALREADY_EXISTS.withDescription("request_id has already been used")
        .asRuntimeException()
    }
  }

  private fun requireFenceOwnership(
    fence: org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.VidLabelingEvictionFence?,
    dataProviderResourceId: String,
    evictionOperationId: String,
  ): org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.VidLabelingEvictionFence {
    if (fence == null) {
      throw Status.FAILED_PRECONDITION.withDescription(
          "No VID-labeling eviction fence exists for DataProvider $dataProviderResourceId"
        )
        .asRuntimeException()
    }
    if (fence.evictionOperationId != evictionOperationId) {
      throw Status.FAILED_PRECONDITION.withDescription(
          "VID-labeling eviction ${fence.evictionOperationId} owns the fence for DataProvider " +
            dataProviderResourceId
        )
        .asRuntimeException()
    }
    return fence
  }

  private fun AcquireRawImpressionUploadEvictionFenceRequest.fingerprint(): ByteString =
    fingerprint(this)

  private fun AdvanceRawImpressionUploadEvictionFenceRequest.fingerprint(): ByteString =
    fingerprint(this)

  private fun ReleaseRawImpressionUploadEvictionFenceRequest.fingerprint(): ByteString =
    fingerprint(this)

  private fun fingerprint(message: com.google.protobuf.Message): ByteString =
    MessageDigest.getInstance("SHA-256")
      .apply {
        update(message.descriptorForType.fullName.toByteArray(Charsets.UTF_8))
        update(byteArrayOf(0))
      }
      .digest(message.toByteArray())
      .toByteString()

  private fun ByteString.toEtag(): String =
    toByteArray().joinToString(separator = "") { byte -> "%02x".format(byte) }

  private data class FenceMutationResult(val newlyAcquired: Boolean, val etag: String)

  private fun AcquireRawImpressionUploadEvictionFenceRequest.initialFenceState():
    VidLabelingEvictionFenceState =
    when (state) {
      VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING ->
        VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
      VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING ->
        VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
      VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_UNSPECIFIED,
      VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_DRAINING,
      VidLabelingEvictionFenceState.UNRECOGNIZED ->
        throw InvalidFieldValueException("state")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

  private suspend fun requireDrained(
    txn: AsyncDatabaseClient.TransactionContext,
    dataProviderResourceId: String,
  ) {
    if (
      txn.hasIncompleteRawImpressionUploadRegistration(dataProviderResourceId) ||
        txn.hasActiveRawImpressionUploadModelLine(dataProviderResourceId) ||
        txn.hasActiveDataAvailabilitySyncLease(dataProviderResourceId, clock.instant())
    ) {
      throw Status.FAILED_PRECONDITION.withDescription(
          "The VID-labeling pipeline is not idle or synchronization leases have not drained for " +
            "DataProvider $dataProviderResourceId"
        )
        .asRuntimeException()
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
