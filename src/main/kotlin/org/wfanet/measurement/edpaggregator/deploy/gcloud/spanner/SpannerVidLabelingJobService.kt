/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import com.google.protobuf.Timestamp
import io.grpc.Status
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.VidLabelingJobResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.countOtherNonSucceededVidLabelingJobsForModelLine
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findVidLabelingJobByRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findVidLabelingJobsByRequestIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadIdForVidLabeling
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getVidLabelingJobByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertVidLabelingJob
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readVidLabelingJobs
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateVidLabelingJobState
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.vidLabelingJobExists
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.internal.VidLabelingJobAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.VidLabelingJobNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.VidLabelingJobStateInvalidException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.BatchCreateVidLabelingJobsRequest
import org.wfanet.measurement.internal.edpaggregator.BatchCreateVidLabelingJobsResponse
import org.wfanet.measurement.internal.edpaggregator.CreateVidLabelingJobRequest
import org.wfanet.measurement.internal.edpaggregator.GetVidLabelingJobRequest
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsRequest
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsResponse
import org.wfanet.measurement.internal.edpaggregator.MarkVidLabelingJobFailedRequest
import org.wfanet.measurement.internal.edpaggregator.MarkVidLabelingJobSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.MarkVidLabelingJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.MarkVidLabelingJobSucceededResponseKt
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJob
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.VidLabelingState as State
import org.wfanet.measurement.internal.edpaggregator.batchCreateVidLabelingJobsResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listVidLabelingJobsPageToken
import org.wfanet.measurement.internal.edpaggregator.listVidLabelingJobsResponse
import org.wfanet.measurement.internal.edpaggregator.markVidLabelingJobSucceededResponse

/**
 * Cloud Spanner implementation of the internal [VidLabelingJob] service.
 *
 * Persists VidLabelingJob rows interleaved under their parent RawImpressionUpload and enforces the
 * job state machine, request-ID idempotency, and etag optimistic concurrency.
 */
class SpannerVidLabelingJobService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : VidLabelingJobServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createVidLabelingJob(request: CreateVidLabelingJobRequest): VidLabelingJob {
    try {
      validateCreateRequest(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderResourceId = request.dataProviderResourceId
    val rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
    val job = request.vidLabelingJob

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createVidLabelingJob"))
    val createdJob: VidLabelingJob =
      try {
        transactionRunner.run { txn ->
          val existing =
            txn.findVidLabelingJobByRequestId(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              request.requestId,
            )
          if (existing != null) {
            return@run existing.vidLabelingJob
          }

          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForVidLabeling(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
            )
              ?: throw RawImpressionUploadNotFoundException(
                  dataProviderResourceId,
                  rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          val vidLabelingJobId =
            idGenerator.generateNewId { id ->
              txn.vidLabelingJobExists(dataProviderResourceId, rawImpressionUploadId, id)
            }

          val resourceId = "$VID_LABELING_JOB_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"
          val newEtag = UUID.randomUUID().toString()

          txn.insertVidLabelingJob(
            rawImpressionUploadId = rawImpressionUploadId,
            vidLabelingJobId = vidLabelingJobId,
            vidLabelingJobResourceId = resourceId,
            dataProviderResourceId = dataProviderResourceId,
            cmmsModelLines = job.cmmsModelLinesList,
            rawImpressionUploadFiles = job.rawImpressionUploadFilesList,
            createRequestId = request.requestId,
            etag = newEtag,
          )

          job.copy {
            this.dataProviderResourceId = dataProviderResourceId
            this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
            vidLabelingJobResourceId = resourceId
            state = State.VID_LABELING_STATE_CREATED
            etag = newEtag
            clearCreateTime()
            clearUpdateTime()
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw VidLabelingJobAlreadyExistsException(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              e,
            )
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    return if (createdJob.hasCreateTime()) {
      createdJob
    } else {
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
      createdJob.copy {
        createTime = commitTimestamp
        updateTime = commitTimestamp
      }
    }
  }

  override suspend fun batchCreateVidLabelingJobs(
    request: BatchCreateVidLabelingJobsRequest
  ): BatchCreateVidLabelingJobsResponse {
    if (request.requestsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_SIZE elements"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.isEmpty()) {
      return BatchCreateVidLabelingJobsResponse.getDefaultInstance()
    }

    val dataProviderResourceId = request.dataProviderResourceId
    val rawImpressionUploadResourceId = request.rawImpressionUploadResourceId

    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val requestIdSet = mutableSetOf<String>()

    request.requestsList.forEachIndexed { index, subRequest ->
      if (!subRequest.hasVidLabelingJob()) {
        throw RequiredFieldNotSetException("requests[$index].vid_labeling_job")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.vidLabelingJob.cmmsModelLinesList.isEmpty()) {
        throw RequiredFieldNotSetException("requests[$index].vid_labeling_job.cmms_model_lines")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.vidLabelingJob.rawImpressionUploadFilesList.isEmpty()) {
        throw RequiredFieldNotSetException(
            "requests[$index].vid_labeling_job.raw_impression_upload_files"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val requestId = subRequest.requestId
      if (requestId.isEmpty()) {
        throw RequiredFieldNotSetException("requests[$index].request_id")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      try {
        UUID.fromString(requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("requests[$index].request_id", e)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!requestIdSet.add(requestId)) {
        throw InvalidFieldValueException("requests[$index].request_id") {
            "Duplicate request_id $requestId in batch"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchCreateVidLabelingJobs"))

    val results: List<VidLabelingJob> =
      try {
        transactionRunner.run { txn ->
          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForVidLabeling(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
            )
              ?: throw RawImpressionUploadNotFoundException(
                  dataProviderResourceId,
                  rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          val existingByRequestId: Map<String, VidLabelingJobResult> =
            txn.findVidLabelingJobsByRequestIds(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              request.requestsList.map { it.requestId },
            )

          request.requestsList.map { subRequest ->
            val existing = existingByRequestId[subRequest.requestId]
            if (existing != null) {
              existing.vidLabelingJob
            } else {
              val vidLabelingJobId =
                idGenerator.generateNewId { id ->
                  txn.vidLabelingJobExists(dataProviderResourceId, rawImpressionUploadId, id)
                }

              val resourceId = "$VID_LABELING_JOB_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"
              val newEtag = UUID.randomUUID().toString()

              txn.insertVidLabelingJob(
                rawImpressionUploadId = rawImpressionUploadId,
                vidLabelingJobId = vidLabelingJobId,
                vidLabelingJobResourceId = resourceId,
                dataProviderResourceId = dataProviderResourceId,
                cmmsModelLines = subRequest.vidLabelingJob.cmmsModelLinesList,
                rawImpressionUploadFiles = subRequest.vidLabelingJob.rawImpressionUploadFilesList,
                createRequestId = subRequest.requestId,
                etag = newEtag,
              )

              subRequest.vidLabelingJob.copy {
                this.dataProviderResourceId = dataProviderResourceId
                this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
                vidLabelingJobResourceId = resourceId
                state = State.VID_LABELING_STATE_CREATED
                etag = newEtag
                clearCreateTime()
                clearUpdateTime()
              }
            }
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw VidLabelingJobAlreadyExistsException(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              e,
            )
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchCreateVidLabelingJobsResponse {
      vidLabelingJobs +=
        results.map { result ->
          if (result.hasCreateTime()) {
            result
          } else {
            result.copy {
              createTime = commitTimestamp
              updateTime = commitTimestamp
            }
          }
        }
    }
  }

  override suspend fun getVidLabelingJob(request: GetVidLabelingJobRequest): VidLabelingJob {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.vidLabelingJobResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("vid_labeling_job_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return databaseClient.singleUse().use { txn ->
      txn
        .getVidLabelingJobByResourceId(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId,
          request.vidLabelingJobResourceId,
        )
        ?.vidLabelingJob
        ?: throw VidLabelingJobNotFoundException(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.vidLabelingJobResourceId,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun listVidLabelingJobs(
    request: ListVidLabelingJobsRequest
  ): ListVidLabelingJobsResponse {
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

    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val after = if (request.hasPageToken()) request.pageToken.after else null

    databaseClient.singleUse().use { txn ->
      val rows: Flow<VidLabelingJobResult> =
        txn.readVidLabelingJobs(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId.ifEmpty { null },
          filter = if (request.hasFilter()) request.filter else null,
          limit = pageSize + 1,
          after = after,
        )

      return listVidLabelingJobsResponse {
        rows
          .map { it.vidLabelingJob }
          .collectIndexed { index, item ->
            if (index == pageSize) {
              val lastIncluded = vidLabelingJobs.last()
              nextPageToken = listVidLabelingJobsPageToken {
                this.after =
                  ListVidLabelingJobsPageTokenKt.after {
                    createTime = lastIncluded.createTime
                    vidLabelingJobResourceId = lastIncluded.vidLabelingJobResourceId
                  }
              }
            } else {
              vidLabelingJobs += item
            }
          }
      }
    }
  }

  override suspend fun markVidLabelingJobSucceeded(
    request: MarkVidLabelingJobSucceededRequest
  ): MarkVidLabelingJobSucceededResponse {
    validateMarkRequest(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.vidLabelingJobResourceId,
      request.etag,
      request.requestId,
    )

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=markVidLabelingJobSucceeded"))

    data class TransactionResult(
      val updatedJob: VidLabelingJob,
      val completedModelLines: List<String>,
      val isReplay: Boolean = false,
    )

    val txnResult: TransactionResult =
      transactionRunner.run { txn ->
        val result =
          txn.getVidLabelingJobByResourceId(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.vidLabelingJobResourceId,
          )
            ?: throw VidLabelingJobNotFoundException(
                request.dataProviderResourceId,
                request.rawImpressionUploadResourceId,
                request.vidLabelingJobResourceId,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)

        val currentJob = result.vidLabelingJob

        // Idempotency: if already succeeded by this same request_id, return the original response,
        // recomputing the completed model lines so the replay matches the first response (AIP-155).
        if (
          currentJob.state == State.VID_LABELING_STATE_SUCCEEDED &&
            result.markRequestId == request.requestId
        ) {
          return@run TransactionResult(
            currentJob,
            completedModelLines =
              currentJob.cmmsModelLinesList.filter { modelLine ->
                txn.countOtherNonSucceededVidLabelingJobsForModelLine(
                  request.dataProviderResourceId,
                  result.rawImpressionUploadId,
                  modelLine,
                  result.vidLabelingJobId,
                ) == 0L
              },
            isReplay = true,
          )
        }

        checkMarkPrecondition(
          dataProviderResourceId = request.dataProviderResourceId,
          rawImpressionUploadResourceId = request.rawImpressionUploadResourceId,
          requestEtag = request.etag,
          currentJob = currentJob,
        )

        val newEtag = UUID.randomUUID().toString()

        txn.updateVidLabelingJobState(
          dataProviderResourceId = request.dataProviderResourceId,
          rawImpressionUploadId = result.rawImpressionUploadId,
          vidLabelingJobId = result.vidLabelingJobId,
          state = State.VID_LABELING_STATE_SUCCEEDED,
          etag = newEtag,
        ) {
          set("MarkRequestId").to(request.requestId)
          // Clear any error message from a prior FAILED attempt; it is only set while FAILED.
          set("ErrorMessage").to(null as String?)
        }

        // A model line is complete when none of its sibling jobs (covering that model line) remain
        // non-SUCCEEDED. The buffered update above is not visible in this read, so the current job
        // is excluded by ID rather than relying on its persisted state.
        val completedModelLines: List<String> =
          currentJob.cmmsModelLinesList.filter { modelLine ->
            txn.countOtherNonSucceededVidLabelingJobsForModelLine(
              request.dataProviderResourceId,
              result.rawImpressionUploadId,
              modelLine,
              result.vidLabelingJobId,
            ) == 0L
          }

        TransactionResult(
          updatedJob =
            currentJob.copy {
              state = State.VID_LABELING_STATE_SUCCEEDED
              etag = newEtag
              clearErrorMessage()
              clearUpdateTime()
            },
          completedModelLines = completedModelLines,
        )
      }

    return markVidLabelingJobSucceededResponse {
      vidLabelingJob =
        if (txnResult.isReplay) {
          txnResult.updatedJob
        } else {
          val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
          txnResult.updatedJob.copy { updateTime = commitTimestamp }
        }

      if (txnResult.completedModelLines.isNotEmpty()) {
        lastVidLabelingJobResult =
          MarkVidLabelingJobSucceededResponseKt.lastVidLabelingJobResult {
            completedModelLines += txnResult.completedModelLines
          }
      }
    }
  }

  override suspend fun markVidLabelingJobFailed(
    request: MarkVidLabelingJobFailedRequest
  ): VidLabelingJob {
    validateMarkRequest(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.vidLabelingJobResourceId,
      request.etag,
      request.requestId,
    )

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=markVidLabelingJobFailed"))

    data class TransactionResult(val updatedJob: VidLabelingJob, val isReplay: Boolean = false)

    val txnResult: TransactionResult =
      transactionRunner.run { txn ->
        val result =
          txn.getVidLabelingJobByResourceId(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.vidLabelingJobResourceId,
          )
            ?: throw VidLabelingJobNotFoundException(
                request.dataProviderResourceId,
                request.rawImpressionUploadResourceId,
                request.vidLabelingJobResourceId,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)

        val currentJob = result.vidLabelingJob

        // Idempotency: if already failed by this same request_id, return as-is.
        if (
          currentJob.state == State.VID_LABELING_STATE_FAILED &&
            result.markRequestId == request.requestId
        ) {
          return@run TransactionResult(currentJob, isReplay = true)
        }

        checkMarkPrecondition(
          dataProviderResourceId = request.dataProviderResourceId,
          rawImpressionUploadResourceId = request.rawImpressionUploadResourceId,
          requestEtag = request.etag,
          currentJob = currentJob,
        )

        val newEtag = UUID.randomUUID().toString()

        txn.updateVidLabelingJobState(
          dataProviderResourceId = request.dataProviderResourceId,
          rawImpressionUploadId = result.rawImpressionUploadId,
          vidLabelingJobId = result.vidLabelingJobId,
          state = State.VID_LABELING_STATE_FAILED,
          etag = newEtag,
        ) {
          set("ErrorMessage").to(request.errorMessage)
          set("MarkRequestId").to(request.requestId)
        }

        TransactionResult(
          currentJob.copy {
            state = State.VID_LABELING_STATE_FAILED
            errorMessage = request.errorMessage
            etag = newEtag
            clearUpdateTime()
          }
        )
      }

    return if (txnResult.isReplay) {
      txnResult.updatedJob
    } else {
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
      txnResult.updatedJob.copy { updateTime = commitTimestamp }
    }
  }

  /**
   * Validates parent identifiers, the job resource ID, the etag, and the request ID common to both
   * Mark requests.
   */
  private fun validateMarkRequest(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    vidLabelingJobResourceId: String,
    etag: String,
    requestId: String,
  ) {
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (vidLabelingJobResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("vid_labeling_job_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
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
  }

  /**
   * Verifies that [currentJob] is in a state that can transition to a terminal state and that the
   * request etag matches.
   */
  private fun checkMarkPrecondition(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    requestEtag: String,
    currentJob: VidLabelingJob,
  ) {
    val validPreviousStates =
      setOf(State.VID_LABELING_STATE_CREATED, State.VID_LABELING_STATE_FAILED)
    if (currentJob.state !in validPreviousStates) {
      throw VidLabelingJobStateInvalidException(
          dataProviderResourceId,
          rawImpressionUploadResourceId,
          currentJob.vidLabelingJobResourceId,
          currentJob.state,
          validPreviousStates,
        )
        .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    try {
      EtagMismatchException.check(requestEtag, currentJob.etag)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  private fun validateCreateRequest(request: CreateVidLabelingJobRequest) {
    if (!request.hasVidLabelingJob()) {
      throw RequiredFieldNotSetException("vid_labeling_job")
    }
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
    }
    if (request.vidLabelingJob.cmmsModelLinesList.isEmpty()) {
      throw RequiredFieldNotSetException("vid_labeling_job.cmms_model_lines")
    }
    if (request.vidLabelingJob.rawImpressionUploadFilesList.isEmpty()) {
      throw RequiredFieldNotSetException("vid_labeling_job.raw_impression_upload_files")
    }
    if (request.requestId.isEmpty()) {
      throw RequiredFieldNotSetException("request_id")
    }
    try {
      UUID.fromString(request.requestId)
    } catch (e: IllegalArgumentException) {
      throw InvalidFieldValueException("request_id", e)
    }
  }

  companion object {
    private const val VID_LABELING_JOB_RESOURCE_ID_PREFIX = "vidLabelingJob"
    private const val MAX_PAGE_SIZE = 100
    private const val MAX_BATCH_SIZE = 50
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
