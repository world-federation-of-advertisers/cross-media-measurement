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
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.PoolAssignmentJobResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findPoolAssignmentJobByRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findPoolAssignmentJobsByRequestIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getPoolAssignmentJobByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadIdForPoolAssignment
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertPoolAssignmentJob
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.poolAssignmentJobExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readPoolAssignmentJobs
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updatePoolAssignmentJobState
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.BatchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.internal.edpaggregator.BatchCreatePoolAssignmentJobsResponse
import org.wfanet.measurement.internal.edpaggregator.CreatePoolAssignmentJobRequest
import org.wfanet.measurement.internal.edpaggregator.GetPoolAssignmentJobRequest
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsRequest
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsResponse
import org.wfanet.measurement.internal.edpaggregator.MarkPoolAssignmentJobFailedRequest
import org.wfanet.measurement.internal.edpaggregator.MarkPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.MarkPoolAssignmentJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.MarkPoolAssignmentJobSucceededResponseKt
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJob
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentState as State
import org.wfanet.measurement.internal.edpaggregator.batchCreatePoolAssignmentJobsResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.listPoolAssignmentJobsPageToken
import org.wfanet.measurement.internal.edpaggregator.listPoolAssignmentJobsResponse
import org.wfanet.measurement.internal.edpaggregator.markPoolAssignmentJobSucceededResponse

class SpannerPoolAssignmentJobService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : PoolAssignmentJobServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createPoolAssignmentJob(
    request: CreatePoolAssignmentJobRequest
  ): PoolAssignmentJob {
    try {
      validateCreateRequest(request)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: InvalidFieldValueException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val job = request.poolAssignmentJob

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createPoolAssignmentJob"))
    val createdJob: PoolAssignmentJob =
      try {
        transactionRunner.run { txn ->
          if (request.requestId.isNotEmpty()) {
            val existing =
              txn.findPoolAssignmentJobByRequestId(
                job.dataProviderResourceId,
                job.rawImpressionUploadResourceId,
                request.requestId,
              )
            if (existing != null) {
              return@run existing.poolAssignmentJob
            }
          }

          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForPoolAssignment(
              job.dataProviderResourceId,
              job.rawImpressionUploadResourceId,
            )
              ?: throw Status.NOT_FOUND
                .withDescription(
                  "RawImpressionUpload not found for DataProvider ${job.dataProviderResourceId}" +
                    " and upload ${job.rawImpressionUploadResourceId}"
                )
                .asRuntimeException()

          val poolAssignmentJobId =
            idGenerator.generateNewId { id ->
              txn.poolAssignmentJobExists(
                job.dataProviderResourceId,
                rawImpressionUploadId,
                id,
              )
            }

          val resourceId = "$POOL_ASSIGNMENT_JOB_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"

          txn.insertPoolAssignmentJob(
            rawImpressionUploadId = rawImpressionUploadId,
            poolAssignmentJobId = poolAssignmentJobId,
            poolAssignmentJobResourceId = resourceId,
            dataProviderResourceId = job.dataProviderResourceId,
            cmmsModelLine = job.cmmsModelLine,
            shardIndex = job.shardIndex.toLong(),
            createRequestId = request.requestId,
            etag = "", // Placeholder; real etag computed from commit timestamp.
          )

          job.copy {
            poolAssignmentJobResourceId = resourceId
            state = State.POOL_ASSIGNMENT_STATE_CREATED
            clearCreateTime()
            clearUpdateTime()
            clearEtag()
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw Status.ALREADY_EXISTS
            .withDescription("PoolAssignmentJob already exists")
            .withCause(e)
            .asRuntimeException()
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
        etag = ETags.computeETag(commitTimestamp.toInstant())
      }
    }
  }

  override suspend fun batchCreatePoolAssignmentJobs(
    request: BatchCreatePoolAssignmentJobsRequest
  ): BatchCreatePoolAssignmentJobsResponse {
    if (request.requestsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("requests") { "$it must contain at most $MAX_BATCH_SIZE elements" }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.requestsList.isEmpty()) {
      return BatchCreatePoolAssignmentJobsResponse.getDefaultInstance()
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
    val seenModelLineShards = mutableSetOf<Pair<String, Int>>()

    request.requestsList.forEachIndexed { index, subRequest ->
      if (!subRequest.hasPoolAssignmentJob()) {
        throw RequiredFieldNotSetException("requests[$index].pool_assignment_job")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (subRequest.poolAssignmentJob.cmmsModelLine.isEmpty()) {
        throw RequiredFieldNotSetException(
          "requests[$index].pool_assignment_job.cmms_model_line"
        ).asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (
        !seenModelLineShards.add(
          subRequest.poolAssignmentJob.cmmsModelLine to subRequest.poolAssignmentJob.shardIndex
        )
      ) {
        throw InvalidFieldValueException(
          "requests[$index].pool_assignment_job.shard_index"
        ) { "Duplicate (cmms_model_line, shard_index) in batch" }
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
          }.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=batchCreatePoolAssignmentJobs"))

    val results: List<PoolAssignmentJob> =
      try {
        transactionRunner.run { txn ->
          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForPoolAssignment(dataProviderResourceId, rawImpressionUploadResourceId)
              ?: throw Status.NOT_FOUND
                .withDescription(
                  "RawImpressionUpload not found for DataProvider $dataProviderResourceId" +
                    " and upload $rawImpressionUploadResourceId"
                )
                .asRuntimeException()

          val existingByRequestId: Map<String, PoolAssignmentJobResult> =
            txn.findPoolAssignmentJobsByRequestIds(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              request.requestsList.mapNotNull { it.requestId.ifEmpty { null } },
            )

          request.requestsList.map { subRequest ->
            val existing = existingByRequestId[subRequest.requestId]
            if (existing != null) {
              existing.poolAssignmentJob
            } else {
              val poolAssignmentJobId =
                idGenerator.generateNewId { id ->
                  txn.poolAssignmentJobExists(
                    dataProviderResourceId,
                    rawImpressionUploadId,
                    id,
                  )
                }

              val resourceId = "$POOL_ASSIGNMENT_JOB_RESOURCE_ID_PREFIX-${UUID.randomUUID()}"

              txn.insertPoolAssignmentJob(
                rawImpressionUploadId = rawImpressionUploadId,
                poolAssignmentJobId = poolAssignmentJobId,
                poolAssignmentJobResourceId = resourceId,
                dataProviderResourceId = dataProviderResourceId,
                cmmsModelLine = subRequest.poolAssignmentJob.cmmsModelLine,
                shardIndex = subRequest.poolAssignmentJob.shardIndex.toLong(),
                createRequestId = subRequest.requestId,
                etag = "", // Placeholder; real etag computed from commit timestamp.
              )

              subRequest.poolAssignmentJob.copy {
                this.dataProviderResourceId = dataProviderResourceId
                this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
                poolAssignmentJobResourceId = resourceId
                state = State.POOL_ASSIGNMENT_STATE_CREATED
                clearCreateTime()
                clearUpdateTime()
                clearEtag()
              }
            }
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw Status.ALREADY_EXISTS
            .withDescription("PoolAssignmentJob already exists")
            .withCause(e)
            .asRuntimeException()
        }
        throw e
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    val computedEtag = ETags.computeETag(commitTimestamp.toInstant())
    return batchCreatePoolAssignmentJobsResponse {
      poolAssignmentJobs +=
        results.map { result ->
          if (result.hasCreateTime()) {
            result
          } else {
            result.copy {
              createTime = commitTimestamp
              updateTime = commitTimestamp
              etag = computedEtag
            }
          }
        }
    }
  }

  override suspend fun getPoolAssignmentJob(
    request: GetPoolAssignmentJobRequest
  ): PoolAssignmentJob {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.poolAssignmentJobResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("pool_assignment_job_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return databaseClient.singleUse().use { txn ->
      txn
        .getPoolAssignmentJobByResourceId(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId,
          request.poolAssignmentJobResourceId,
        )
        ?.poolAssignmentJob
        ?: throw Status.NOT_FOUND
          .withDescription(
            "PoolAssignmentJob ${request.poolAssignmentJobResourceId} not found"
          )
          .asRuntimeException()
    }
  }

  override suspend fun listPoolAssignmentJobs(
    request: ListPoolAssignmentJobsRequest
  ): ListPoolAssignmentJobsResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName ->
        "$fieldName must be non-negative"
      }.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE
      else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val after = if (request.hasPageToken()) request.pageToken.after else null

    databaseClient.singleUse().use { txn ->
      val rows: Flow<PoolAssignmentJobResult> =
        txn.readPoolAssignmentJobs(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId.ifEmpty { null },
          filter = if (request.hasFilter()) request.filter else null,
          limit = pageSize + 1,
          after = after,
        )

      return listPoolAssignmentJobsResponse {
        var count = 0
        rows.map { it.poolAssignmentJob }.collectIndexed { index, item ->
          if (index == pageSize) {
            val lastIncluded = poolAssignmentJobs.last()
            nextPageToken = listPoolAssignmentJobsPageToken {
              this.after =
                ListPoolAssignmentJobsPageTokenKt.after {
                  createTime = lastIncluded.createTime
                  poolAssignmentJobResourceId = lastIncluded.poolAssignmentJobResourceId
                }
            }
          } else {
            poolAssignmentJobs += item
          }
        }
      }
    }
  }

  override suspend fun markPoolAssignmentJobSucceeded(
    request: MarkPoolAssignmentJobSucceededRequest
  ): MarkPoolAssignmentJobSucceededResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.poolAssignmentJobResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("pool_assignment_job_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=markPoolAssignmentJobSucceeded"))

    data class TransactionResult(
      val updatedJob: PoolAssignmentJob,
      val isLastShard: Boolean,
      val poolOffsets: List<Long>,
      val isReplay: Boolean = false,
    )

    val txnResult: TransactionResult =
      transactionRunner.run { txn ->
        val result =
          txn.getPoolAssignmentJobByResourceId(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.poolAssignmentJobResourceId,
          )
            ?: throw Status.NOT_FOUND
              .withDescription(
                "PoolAssignmentJob ${request.poolAssignmentJobResourceId} not found"
              )
              .asRuntimeException()

        val currentJob = result.poolAssignmentJob

        // Idempotency: if already succeeded by this same request_id, return as-is.
        if (
          currentJob.state == State.POOL_ASSIGNMENT_STATE_SUCCEEDED &&
            request.requestId.isNotEmpty() &&
            result.markRequestId == request.requestId
        ) {
          return@run TransactionResult(
            currentJob,
            isLastShard = false,
            poolOffsets = emptyList(),
            isReplay = true,
          )
        }

        val validPreviousStates =
          setOf(
            State.POOL_ASSIGNMENT_STATE_CREATED,
            State.POOL_ASSIGNMENT_STATE_FAILED,
          )
        if (currentJob.state !in validPreviousStates) {
          throw Status.FAILED_PRECONDITION
            .withDescription(
              "PoolAssignmentJob is in state ${currentJob.state}," +
                " expected one of $validPreviousStates"
            )
            .asRuntimeException()
        }

        try {
          EtagMismatchException.check(request.etag, currentJob.etag)
        } catch (e: EtagMismatchException) {
          throw e.asStatusRuntimeException(Status.Code.ABORTED)
        }

        txn.updatePoolAssignmentJobState(
          dataProviderResourceId = request.dataProviderResourceId,
          rawImpressionUploadId = result.rawImpressionUploadId,
          poolAssignmentJobId = result.poolAssignmentJobId,
          state = State.POOL_ASSIGNMENT_STATE_SUCCEEDED,
          etag = "", // Placeholder; real etag computed from commit timestamp.
        ) {
          set("MarkRequestId").to(request.requestId)
        }

        // Check if all jobs for this (upload, model line) are now SUCCEEDED.
        val allJobsForModelLine: List<PoolAssignmentJobResult> = buildList {
          txn
            .readPoolAssignmentJobs(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              filter =
                ListPoolAssignmentJobsRequest.Filter.newBuilder()
                  .setCmmsModelLine(currentJob.cmmsModelLine)
                  .build(),
              limit = Int.MAX_VALUE,
            )
            .collect { add(it) }
        }

        // This job is still CREATED/FAILED in the read (buffered mutation not visible),
        // so check: all *other* jobs are SUCCEEDED, and we're transitioning this one.
        val isLastShard =
          allJobsForModelLine.all { jobResult ->
            jobResult.poolAssignmentJob.state == State.POOL_ASSIGNMENT_STATE_SUCCEEDED ||
              jobResult.poolAssignmentJobId == result.poolAssignmentJobId
          }

        TransactionResult(
          updatedJob =
            currentJob.copy {
              state = State.POOL_ASSIGNMENT_STATE_SUCCEEDED
              clearUpdateTime()
              clearEtag()
            },
          isLastShard = isLastShard,
          poolOffsets = emptyList(), // Pool offsets would be populated from actual shard data.
        )
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    val computedEtag = ETags.computeETag(commitTimestamp.toInstant())

    return markPoolAssignmentJobSucceededResponse {
      poolAssignmentJob =
        if (txnResult.isReplay) {
          txnResult.updatedJob
        } else if (txnResult.updatedJob.hasCreateTime()) {
          txnResult.updatedJob.copy {
            updateTime = commitTimestamp
            etag = computedEtag
          }
        } else {
          txnResult.updatedJob.copy {
            createTime = commitTimestamp
            updateTime = commitTimestamp
            etag = computedEtag
          }
        }

      if (txnResult.isLastShard) {
        lastShardResult =
          MarkPoolAssignmentJobSucceededResponseKt.lastShardResult {
            poolOffsets += txnResult.poolOffsets
          }
      }
    }
  }

  override suspend fun markPoolAssignmentJobFailed(
    request: MarkPoolAssignmentJobFailedRequest
  ): PoolAssignmentJob {
    if (request.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("data_provider_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("raw_impression_upload_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.poolAssignmentJobResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("pool_assignment_job_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.etag.isEmpty()) {
      throw RequiredFieldNotSetException("etag")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=markPoolAssignmentJobFailed"))

    val updatedJob: PoolAssignmentJob =
      transactionRunner.run { txn ->
        val result =
          txn.getPoolAssignmentJobByResourceId(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.poolAssignmentJobResourceId,
          )
            ?: throw Status.NOT_FOUND
              .withDescription(
                "PoolAssignmentJob ${request.poolAssignmentJobResourceId} not found"
              )
              .asRuntimeException()

        val currentJob = result.poolAssignmentJob

        val validPreviousStates =
          setOf(
            State.POOL_ASSIGNMENT_STATE_CREATED,
            State.POOL_ASSIGNMENT_STATE_FAILED,
          )
        if (currentJob.state !in validPreviousStates) {
          throw Status.FAILED_PRECONDITION
            .withDescription(
              "PoolAssignmentJob is in state ${currentJob.state}," +
                " expected one of $validPreviousStates"
            )
            .asRuntimeException()
        }

        try {
          EtagMismatchException.check(request.etag, currentJob.etag)
        } catch (e: EtagMismatchException) {
          throw e.asStatusRuntimeException(Status.Code.ABORTED)
        }

        txn.updatePoolAssignmentJobState(
          dataProviderResourceId = request.dataProviderResourceId,
          rawImpressionUploadId = result.rawImpressionUploadId,
          poolAssignmentJobId = result.poolAssignmentJobId,
          state = State.POOL_ASSIGNMENT_STATE_FAILED,
          etag = "", // Placeholder; real etag computed from commit timestamp.
        ) {
          set("ErrorMessage").to(request.errorMessage)
        }

        currentJob.copy {
          state = State.POOL_ASSIGNMENT_STATE_FAILED
          errorMessage = request.errorMessage
          clearUpdateTime()
          clearEtag()
        }
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    val computedEtag = ETags.computeETag(commitTimestamp.toInstant())

    return updatedJob.copy {
      updateTime = commitTimestamp
      etag = computedEtag
    }
  }

  /**
   * Validates a [CreatePoolAssignmentJobRequest].
   *
   * @throws RequiredFieldNotSetException if required fields are missing
   * @throws InvalidFieldValueException if field values are invalid
   */
  private fun validateCreateRequest(request: CreatePoolAssignmentJobRequest) {
    if (request.requestId.isNotEmpty()) {
      try {
        UUID.fromString(request.requestId)
      } catch (e: IllegalArgumentException) {
        throw InvalidFieldValueException("request_id", e)
      }
    }

    if (!request.hasPoolAssignmentJob()) {
      throw RequiredFieldNotSetException("pool_assignment_job")
    }
    if (request.poolAssignmentJob.dataProviderResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("pool_assignment_job.data_provider_resource_id")
    }
    if (request.poolAssignmentJob.rawImpressionUploadResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("pool_assignment_job.raw_impression_upload_resource_id")
    }
    if (request.poolAssignmentJob.cmmsModelLine.isEmpty()) {
      throw RequiredFieldNotSetException("pool_assignment_job.cmms_model_line")
    }
  }

  companion object {
    private const val POOL_ASSIGNMENT_JOB_RESOURCE_ID_PREFIX = "paj"
    private const val MAX_PAGE_SIZE = 100
    private const val MAX_BATCH_SIZE = 50
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
