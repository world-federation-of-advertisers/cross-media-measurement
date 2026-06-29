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
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.RankerJobResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRankerJobByRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRankerJobsByRequestIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRankerJobByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadIdForRanker
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRankerJob
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.rankerJobExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRankerJobs
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readRankerJobsForModelLine
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRankerJobState
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RankerJobAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.RankerJobNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RankerJobStateInvalidException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRankerJobsRequest
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRankerJobsResponse
import org.wfanet.measurement.internal.edpaggregator.CreateRankerJobRequest
import org.wfanet.measurement.internal.edpaggregator.GetRankerJobRequest
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsRequest
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsResponse
import org.wfanet.measurement.internal.edpaggregator.MarkRankerJobFailedRequest
import org.wfanet.measurement.internal.edpaggregator.MarkRankerJobSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.MarkRankerJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.RankerJob
import org.wfanet.measurement.internal.edpaggregator.RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RankerState as State
import org.wfanet.measurement.internal.edpaggregator.batchCreateRankerJobsResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listRankerJobsPageToken
import org.wfanet.measurement.internal.edpaggregator.listRankerJobsResponse
import org.wfanet.measurement.internal.edpaggregator.markRankerJobSucceededResponse

/**
 * Cloud Spanner implementation of the internal [RankerJob] service.
 *
 * Persists RankerJob rows interleaved under their parent RawImpressionUpload and enforces the job
 * state machine, request-ID idempotency, and etag optimistic concurrency. The "last ranker job out"
 * of a (upload, model line) is detected and reported via `is_last_job` so the caller can transition
 * the parent state from RANKING to LABELING (Phase-2).
 */
class SpannerRankerJobService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : RankerJobServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createRankerJob(request: CreateRankerJobRequest): RankerJob {
    try {
      validateCreateRequest(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderResourceId = request.dataProviderResourceId
    val rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
    val job = request.rankerJob

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createRankerJob"))
    val createdJob: RankerJob =
      try {
        transactionRunner.run { txn ->
          if (request.requestId.isNotEmpty()) {
            val existing =
              txn.findRankerJobByRequestId(
                dataProviderResourceId,
                rawImpressionUploadResourceId,
                request.requestId,
              )
            if (existing != null) {
              return@run existing.rankerJob
            }
          }

          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForRanker(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
            )
              ?: throw RawImpressionUploadNotFoundException(
                  dataProviderResourceId,
                  rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          val rankerJobId =
            idGenerator.generateNewId { id ->
              txn.rankerJobExists(dataProviderResourceId, rawImpressionUploadId, id)
            }

          val resourceId = "$RANKER_JOB_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"
          val newEtag = UUID.randomUUID().toString()

          txn.insertRankerJob(
            rawImpressionUploadId = rawImpressionUploadId,
            rankerJobId = rankerJobId,
            rankerJobResourceId = resourceId,
            dataProviderResourceId = dataProviderResourceId,
            cmmsModelLine = job.cmmsModelLine,
            poolOffsets = job.poolOffsetsList,
            createRequestId = request.requestId,
            etag = newEtag,
          )

          job.copy {
            this.dataProviderResourceId = dataProviderResourceId
            this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
            rankerJobResourceId = resourceId
            state = State.RANKER_STATE_CREATED
            etag = newEtag
            clearCreateTime()
            clearUpdateTime()
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw RankerJobAlreadyExistsException(
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

  override suspend fun batchCreateRankerJobs(
    request: BatchCreateRankerJobsRequest
  ): BatchCreateRankerJobsResponse {
    if (request.requestsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_SIZE elements"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.isEmpty()) {
      return BatchCreateRankerJobsResponse.getDefaultInstance()
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
      if (!subRequest.hasRankerJob()) {
        throw RequiredFieldNotSetException("requests[$index].ranker_job")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.rankerJob.cmmsModelLine.isEmpty()) {
        throw RequiredFieldNotSetException("requests[$index].ranker_job.cmms_model_line")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.rankerJob.poolOffsetsList.isEmpty()) {
        throw RequiredFieldNotSetException("requests[$index].ranker_job.pool_offsets")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      val requestId = subRequest.requestId
      if (requestId.isNotEmpty()) {
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
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchCreateRankerJobs"))

    val results: List<RankerJob> =
      try {
        transactionRunner.run { txn ->
          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForRanker(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
            )
              ?: throw RawImpressionUploadNotFoundException(
                  dataProviderResourceId,
                  rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          val existingByRequestId: Map<String, RankerJobResult> =
            txn.findRankerJobsByRequestIds(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              request.requestsList.mapNotNull { it.requestId.ifEmpty { null } },
            )

          request.requestsList.map { subRequest ->
            val existing = existingByRequestId[subRequest.requestId]
            if (subRequest.requestId.isNotEmpty() && existing != null) {
              existing.rankerJob
            } else {
              val rankerJobId =
                idGenerator.generateNewId { id ->
                  txn.rankerJobExists(dataProviderResourceId, rawImpressionUploadId, id)
                }

              val resourceId = "$RANKER_JOB_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"
              val newEtag = UUID.randomUUID().toString()

              txn.insertRankerJob(
                rawImpressionUploadId = rawImpressionUploadId,
                rankerJobId = rankerJobId,
                rankerJobResourceId = resourceId,
                dataProviderResourceId = dataProviderResourceId,
                cmmsModelLine = subRequest.rankerJob.cmmsModelLine,
                poolOffsets = subRequest.rankerJob.poolOffsetsList,
                createRequestId = subRequest.requestId,
                etag = newEtag,
              )

              subRequest.rankerJob.copy {
                this.dataProviderResourceId = dataProviderResourceId
                this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
                rankerJobResourceId = resourceId
                state = State.RANKER_STATE_CREATED
                etag = newEtag
                clearCreateTime()
                clearUpdateTime()
              }
            }
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw RankerJobAlreadyExistsException(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              e,
            )
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchCreateRankerJobsResponse {
      rankerJobs +=
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

  override suspend fun getRankerJob(request: GetRankerJobRequest): RankerJob {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rankerJobResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("ranker_job_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return databaseClient.singleUse().use { txn ->
      txn
        .getRankerJobByResourceId(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId,
          request.rankerJobResourceId,
        )
        ?.rankerJob
        ?: throw RankerJobNotFoundException(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.rankerJobResourceId,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun listRankerJobs(request: ListRankerJobsRequest): ListRankerJobsResponse {
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
      val rows: Flow<RankerJobResult> =
        txn.readRankerJobs(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId.ifEmpty { null },
          filter = if (request.hasFilter()) request.filter else null,
          limit = pageSize + 1,
          after = after,
        )

      return listRankerJobsResponse {
        rows.collectIndexed { index, result ->
          if (index == pageSize) {
            val lastIncluded = rankerJobs.last()
            nextPageToken = listRankerJobsPageToken {
              this.after =
                ListRankerJobsPageTokenKt.after {
                  createTime = lastIncluded.createTime
                  rankerJobResourceId = lastIncluded.rankerJobResourceId
                }
            }
          } else {
            rankerJobs += result.rankerJob
          }
        }
      }
    }
  }

  override suspend fun markRankerJobSucceeded(
    request: MarkRankerJobSucceededRequest
  ): MarkRankerJobSucceededResponse {
    validateMarkRequest(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.rankerJobResourceId,
      request.etag,
      request.requestId,
      requireRequestId = true,
    )

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=markRankerJobSucceeded"))

    data class TransactionResult(
      val updatedJob: RankerJob,
      val isLastJob: Boolean,
      val isReplay: Boolean = false,
    )

    val txnResult: TransactionResult =
      transactionRunner.run { txn ->
        val result =
          txn.getRankerJobByResourceId(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.rankerJobResourceId,
          )
            ?: throw RankerJobNotFoundException(
                request.dataProviderResourceId,
                request.rawImpressionUploadResourceId,
                request.rankerJobResourceId,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)

        val currentJob = result.rankerJob

        // Idempotency: if already succeeded by this same request_id, do not transition again.
        // is_last_job is recomputed from the current committed state of the sibling jobs, so it
        // reflects whether the (upload, model line) is now complete rather than the exact value
        // returned by the original call: a replay issued after later siblings have also succeeded
        // can observe is_last_job=true where the original returned false. The caller MUST therefore
        // treat the resulting Phase-2 (RANKING -> LABELING) transition as idempotent, since both
        // the original "last job out" call and any later replay can report completion.
        if (
          currentJob.state == State.RANKER_STATE_SUCCEEDED &&
            request.requestId.isNotEmpty() &&
            result.markRequestId == request.requestId
        ) {
          return@run TransactionResult(
            currentJob,
            isLastJob =
              computeIsLastJob(
                txn.readRankerJobsForModelLine(
                  request.dataProviderResourceId,
                  result.rawImpressionUploadId,
                  currentJob.cmmsModelLine,
                ),
                result.rankerJobId,
              ),
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

        txn.updateRankerJobState(
          dataProviderResourceId = request.dataProviderResourceId,
          rawImpressionUploadId = result.rawImpressionUploadId,
          rankerJobId = result.rankerJobId,
          state = State.RANKER_STATE_SUCCEEDED,
          etag = newEtag,
        ) {
          set("MarkRequestId").to(request.requestId)
        }

        // Read all jobs for this (upload, model line). The buffered update above is not visible in
        // this read, so the current job still shows its previous state; it is treated as SUCCEEDED
        // via its ID below.
        val isLastJob =
          computeIsLastJob(
            txn.readRankerJobsForModelLine(
              request.dataProviderResourceId,
              result.rawImpressionUploadId,
              currentJob.cmmsModelLine,
            ),
            result.rankerJobId,
          )

        TransactionResult(
          updatedJob =
            currentJob.copy {
              state = State.RANKER_STATE_SUCCEEDED
              etag = newEtag
              clearUpdateTime()
            },
          isLastJob = isLastJob,
        )
      }

    return markRankerJobSucceededResponse {
      rankerJob =
        if (txnResult.isReplay) {
          txnResult.updatedJob
        } else {
          val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
          txnResult.updatedJob.copy { updateTime = commitTimestamp }
        }
      isLastJob = txnResult.isLastJob
    }
  }

  override suspend fun markRankerJobFailed(request: MarkRankerJobFailedRequest): RankerJob {
    validateMarkRequest(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.rankerJobResourceId,
      request.etag,
      requestId = "",
      requireRequestId = false,
    )

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=markRankerJobFailed"))

    val updatedJob: RankerJob =
      transactionRunner.run { txn ->
        val result =
          txn.getRankerJobByResourceId(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.rankerJobResourceId,
          )
            ?: throw RankerJobNotFoundException(
                request.dataProviderResourceId,
                request.rawImpressionUploadResourceId,
                request.rankerJobResourceId,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)

        val currentJob = result.rankerJob

        checkMarkPrecondition(
          dataProviderResourceId = request.dataProviderResourceId,
          rawImpressionUploadResourceId = request.rawImpressionUploadResourceId,
          requestEtag = request.etag,
          currentJob = currentJob,
        )

        val newEtag = UUID.randomUUID().toString()

        txn.updateRankerJobState(
          dataProviderResourceId = request.dataProviderResourceId,
          rawImpressionUploadId = result.rawImpressionUploadId,
          rankerJobId = result.rankerJobId,
          state = State.RANKER_STATE_FAILED,
          etag = newEtag,
        ) {
          set("ErrorMessage").to(request.errorMessage)
        }

        currentJob.copy {
          state = State.RANKER_STATE_FAILED
          errorMessage = request.errorMessage
          etag = newEtag
          clearUpdateTime()
        }
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return updatedJob.copy { updateTime = commitTimestamp }
  }

  /**
   * Validates parent identifiers, the job resource ID, the etag, and the request ID common to the
   * Mark requests. When [requireRequestId] is true, [requestId] must be set; it may be empty only
   * for Mark RPCs that do not support idempotency.
   */
  private fun validateMarkRequest(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    rankerJobResourceId: String,
    etag: String,
    requestId: String,
    requireRequestId: Boolean,
  ) {
    if (dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (rankerJobResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("ranker_job_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requireRequestId && requestId.isEmpty()) {
      throw RequiredFieldNotSetException("request_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (requestId.isNotEmpty()) {
      try {
        UUID.fromString(requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("request_id", e)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
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
    currentJob: RankerJob,
  ) {
    if (currentJob.state !in VALID_MARK_PREVIOUS_STATES) {
      throw RankerJobStateInvalidException(
          dataProviderResourceId,
          rawImpressionUploadResourceId,
          currentJob.rankerJobResourceId,
          currentJob.state,
          VALID_MARK_PREVIOUS_STATES,
        )
        .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    try {
      EtagMismatchException.check(requestEtag, currentJob.etag)
    } catch (e: EtagMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
  }

  private fun validateCreateRequest(request: CreateRankerJobRequest) {
    if (request.requestId.isNotEmpty()) {
      try {
        UUID.fromString(request.requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("request_id", e)
      }
    }

    if (!request.hasRankerJob()) {
      throw RequiredFieldNotSetException("ranker_job")
    }
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
    }
    if (request.rankerJob.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("ranker_job.cmms_model_line")
    }
    if (request.rankerJob.poolOffsetsList.isEmpty()) {
      throw RequiredFieldNotSetException("ranker_job.pool_offsets")
    }
  }

  companion object {
    private const val RANKER_JOB_RESOURCE_ID_PREFIX = "rankerJob"
    private const val MAX_PAGE_SIZE = 100
    private const val MAX_BATCH_SIZE = 50
    private const val DEFAULT_PAGE_SIZE = 50

    private val VALID_MARK_PREVIOUS_STATES =
      setOf(State.RANKER_STATE_CREATED, State.RANKER_STATE_FAILED)

    /**
     * Returns whether marking [currentJobId] as SUCCEEDED makes it the last ranker job to complete
     * for its (upload, model line). [jobsForModelLine] are the sibling jobs as read before the
     * buffered transition is visible, so [currentJobId] is treated as already SUCCEEDED.
     */
    private fun computeIsLastJob(
      jobsForModelLine: List<RankerJobResult>,
      currentJobId: Long,
    ): Boolean {
      return jobsForModelLine.all {
        it.rankerJob.state == State.RANKER_STATE_SUCCEEDED || it.rankerJobId == currentJobId
      }
    }
  }
}
