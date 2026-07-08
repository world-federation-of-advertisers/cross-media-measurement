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
import com.google.type.Date
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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.countNonSucceededPoolAssignmentJobs
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.countPoolAssignmentJobSiblingsSucceededAfter
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findPoolAssignmentJobByRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findPoolAssignmentJobsByRequestIds
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getPoolAssignmentJobByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadIdForPoolAssignment
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadModelLineMergeState
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertPoolAssignmentJob
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.poolAssignmentJobExists
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readPoolAssignmentJobs
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updatePoolAssignmentJobState
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateRawImpressionUploadModelLineMergedOutputs
import org.wfanet.measurement.edpaggregator.service.internal.EtagMismatchException
import org.wfanet.measurement.edpaggregator.service.internal.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.internal.PoolAssignmentJobAlreadyExistsException
import org.wfanet.measurement.edpaggregator.service.internal.PoolAssignmentJobNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.PoolAssignmentJobStateInvalidException
import org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException
import org.wfanet.measurement.edpaggregator.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
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
              if (
                existing.poolAssignmentJob.cmmsModelLine != job.cmmsModelLine ||
                  existing.poolAssignmentJob.shardIndex != job.shardIndex
              ) {
                throw PoolAssignmentJobAlreadyExistsException()
                  .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
              }
              return@run existing.poolAssignmentJob
            }
          }

          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForPoolAssignment(
              job.dataProviderResourceId,
              job.rawImpressionUploadResourceId,
            )
              ?: throw RawImpressionUploadNotFoundException(
                  job.dataProviderResourceId,
                  job.rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          val poolAssignmentJobId =
            idGenerator.generateNewId { id ->
              txn.poolAssignmentJobExists(job.dataProviderResourceId, rawImpressionUploadId, id)
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
          )

          job.copy {
            poolAssignmentJobResourceId = resourceId
            state = State.POOL_ASSIGNMENT_STATE_CREATED
            clearCreateTime()
            clearUpdateTime()
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw PoolAssignmentJobAlreadyExistsException(e)
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
        etag = ETags.computeETag(commitTimestamp.toInstant())
      }
    }
  }

  override suspend fun batchCreatePoolAssignmentJobs(
    request: BatchCreatePoolAssignmentJobsRequest
  ): BatchCreatePoolAssignmentJobsResponse {
    if (request.requestsList.size > MAX_BATCH_SIZE) {
      throw InvalidFieldValueException("requests") {
          "$it must contain at most $MAX_BATCH_SIZE elements"
        }
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
        throw RequiredFieldNotSetException("requests[$index].pool_assignment_job.cmms_model_line")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

      if (
        !seenModelLineShards.add(
          subRequest.poolAssignmentJob.cmmsModelLine to subRequest.poolAssignmentJob.shardIndex
        )
      ) {
        throw InvalidFieldValueException("requests[$index].pool_assignment_job.shard_index") {
            "Duplicate (cmms_model_line, shard_index) in batch"
          }
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
      databaseClient.readWriteTransaction(Options.tag("action=batchCreatePoolAssignmentJobs"))

    val results: List<PoolAssignmentJob> =
      try {
        transactionRunner.run { txn ->
          val rawImpressionUploadId =
            txn.getRawImpressionUploadIdForPoolAssignment(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
            )
              ?: throw RawImpressionUploadNotFoundException(
                  dataProviderResourceId,
                  rawImpressionUploadResourceId,
                )
                .asStatusRuntimeException(Status.Code.NOT_FOUND)

          val existingByRequestId: Map<String, PoolAssignmentJobResult> =
            txn.findPoolAssignmentJobsByRequestIds(
              dataProviderResourceId,
              rawImpressionUploadResourceId,
              request.requestsList.mapNotNull { it.requestId.ifEmpty { null } },
            )

          request.requestsList.map { subRequest ->
            val existing = existingByRequestId[subRequest.requestId]
            if (existing != null) {
              if (
                existing.poolAssignmentJob.cmmsModelLine !=
                  subRequest.poolAssignmentJob.cmmsModelLine ||
                  existing.poolAssignmentJob.shardIndex != subRequest.poolAssignmentJob.shardIndex
              ) {
                throw PoolAssignmentJobAlreadyExistsException()
                  .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
              }
              existing.poolAssignmentJob
            } else {
              val poolAssignmentJobId =
                idGenerator.generateNewId { id ->
                  txn.poolAssignmentJobExists(dataProviderResourceId, rawImpressionUploadId, id)
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
              )

              subRequest.poolAssignmentJob.copy {
                this.dataProviderResourceId = dataProviderResourceId
                this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
                poolAssignmentJobResourceId = resourceId
                state = State.POOL_ASSIGNMENT_STATE_CREATED
                clearCreateTime()
                clearUpdateTime()
              }
            }
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw PoolAssignmentJobAlreadyExistsException(e)
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        }
        throw e
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()
    return batchCreatePoolAssignmentJobsResponse {
      poolAssignmentJobs +=
        results.map { result ->
          if (result.hasCreateTime()) {
            result
          } else {
            result.copy {
              createTime = commitTimestamp
              updateTime = commitTimestamp
              etag = ETags.computeETag(commitTimestamp.toInstant())
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
        ?: throw PoolAssignmentJobNotFoundException(
            request.dataProviderResourceId,
            request.rawImpressionUploadResourceId,
            request.poolAssignmentJobResourceId,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
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
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

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
        rows
          .map { it.poolAssignmentJob }
          .collectIndexed { index, item ->
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

  /**
   * Marks a [PoolAssignmentJob] SUCCEEDED.
   *
   * Idempotency contract gap: while `request_id` remains OPTIONAL, a caller that omits it gets
   * non-idempotent behavior — on retry the replay short-circuit is skipped, the state is already
   * SUCCEEDED (not in the valid previous states), and the call returns `FAILED_PRECONDITION`.
   * Callers MUST pass a deterministic non-empty `request_id` until it is made REQUIRED. See
   * [#4078](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/4078).
   */
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
      val maxEventDate: Date? = null,
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
            ?: throw PoolAssignmentJobNotFoundException(
                request.dataProviderResourceId,
                request.rawImpressionUploadResourceId,
                request.poolAssignmentJobResourceId,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)

        val currentJob = result.poolAssignmentJob

        // Idempotency: a replay must reproduce the ORIGINAL response (AIP-155), so re-derive the
        // original last-shard status rather than re-deciding from current state: this shard was
        // last
        // iff all siblings are now SUCCEEDED AND none reached SUCCEEDED after it.
        if (
          currentJob.state == State.POOL_ASSIGNMENT_STATE_SUCCEEDED &&
            request.requestId.isNotEmpty() &&
            result.markRequestId == request.requestId
        ) {
          val wasLastShard =
            txn.countNonSucceededPoolAssignmentJobs(
              request.dataProviderResourceId,
              result.rawImpressionUploadId,
              currentJob.cmmsModelLine,
            ) == 0L &&
              txn.countPoolAssignmentJobSiblingsSucceededAfter(
                request.dataProviderResourceId,
                result.rawImpressionUploadId,
                currentJob.cmmsModelLine,
                result.poolAssignmentJobId,
                currentJob.updateTime,
              ) == 0L
          // On replay the merged outputs are already persisted (committed by the original
          // mark), so re-read them from the parent model line.
          val mergeState =
            if (wasLastShard) {
              txn.getRawImpressionUploadModelLineMergeState(
                request.dataProviderResourceId,
                result.rawImpressionUploadId,
                currentJob.cmmsModelLine,
              )
            } else {
              null
            }
          return@run TransactionResult(
            currentJob,
            isLastShard = wasLastShard,
            poolOffsets = mergeState?.poolOffsets ?: emptyList(),
            maxEventDate = mergeState?.maxEventDate,
            isReplay = true,
          )
        }

        val validPreviousStates =
          setOf(State.POOL_ASSIGNMENT_STATE_CREATED, State.POOL_ASSIGNMENT_STATE_FAILED)
        if (currentJob.state !in validPreviousStates) {
          throw PoolAssignmentJobStateInvalidException(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              request.poolAssignmentJobResourceId,
              currentJob.state,
              validPreviousStates,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
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
        ) {
          // Leave MarkSucceededRequestId NULL when no request_id is supplied; the NULL_FILTERED
          // unique index would otherwise reject a second empty-string value in the group.
          if (request.requestId.isNotEmpty()) {
            set("MarkSucceededRequestId").to(request.requestId)
          }
          // Persist the per-shard DEK that encrypts this shard's SubpoolFingerprints blobs.
          if (request.hasEncryptedDek()) {
            set("EncryptedDek").toProtoBytes(request.encryptedDek)
          }
          // Clear any stale failure detail on the FAILED -> SUCCEEDED transition.
          set("ErrorMessage").to(null as String?)
        }

        // Merge this shard's pool offsets + max event date into the parent model line's
        // accumulated outputs: read current values, merge in memory (the buffered write below
        // is not visible to reads in this txn), then persist the union/max.
        val mergeState =
          txn.getRawImpressionUploadModelLineMergeState(
            request.dataProviderResourceId,
            result.rawImpressionUploadId,
            currentJob.cmmsModelLine,
          )
            ?: throw IllegalStateException(
              "RawImpressionUploadModelLine not found for upload " +
                "${result.rawImpressionUploadId} model line ${currentJob.cmmsModelLine}"
            )
        val mergedOffsets = (mergeState.poolOffsets + request.poolOffsetsList).distinct().sorted()
        val mergedMaxEventDate =
          maxProtoDate(
            mergeState.maxEventDate,
            if (request.hasMaxEventDate()) request.maxEventDate else null,
          )
        // The buffered SUCCEEDED mutation is not visible to reads in this transaction, so
        // this job still counts as non-succeeded; it is the last shard iff it is the only
        // remaining non-succeeded row for the (upload, model line).
        val isLastShard =
          txn.countNonSucceededPoolAssignmentJobs(
            request.dataProviderResourceId,
            result.rawImpressionUploadId,
            currentJob.cmmsModelLine,
          ) == 1L

        txn.updateRawImpressionUploadModelLineMergedOutputs(
          request.dataProviderResourceId,
          result.rawImpressionUploadId,
          mergeState.rawImpressionUploadModelLineId,
          mergedOffsets,
          mergedMaxEventDate,
          // On last-shard-out, promote this (final) shard's DEK onto the parent as the merged DEK
          // so a retrying SubpoolAssigner can decrypt the merged per-subpool blobs.
          mergedDek = if (isLastShard && request.hasEncryptedDek()) request.encryptedDek else null,
        )

        TransactionResult(
          updatedJob =
            currentJob.copy {
              state = State.POOL_ASSIGNMENT_STATE_SUCCEEDED
              if (request.hasEncryptedDek()) {
                encryptedDek = request.encryptedDek
              }
              clearErrorMessage()
              clearUpdateTime()
            },
          isLastShard = isLastShard,
          poolOffsets = if (isLastShard) mergedOffsets else emptyList(),
          maxEventDate = if (isLastShard) mergedMaxEventDate else null,
        )
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

    return markPoolAssignmentJobSucceededResponse {
      poolAssignmentJob =
        if (txnResult.isReplay) {
          txnResult.updatedJob
        } else {
          txnResult.updatedJob.copy {
            updateTime = commitTimestamp
            etag = ETags.computeETag(commitTimestamp.toInstant())
          }
        }

      if (txnResult.isLastShard) {
        val hasOffsets = txnResult.poolOffsets.isNotEmpty()
        val hasMaxEventDate = txnResult.maxEventDate != null
        check(hasOffsets == hasMaxEventDate) {
          "LastShardResult invariant violated: pool_offsets present=$hasOffsets but " +
            "max_event_date present=$hasMaxEventDate"
        }
        // Omit LastShardResult when no shard wrote impressions.
        if (hasOffsets) {
          lastShardResult =
            MarkPoolAssignmentJobSucceededResponseKt.lastShardResult {
              poolOffsets += txnResult.poolOffsets
              maxEventDate = txnResult.maxEventDate!!
            }
        }
      }
    }
  }

  // TODO(world-federation-of-advertisers/cross-media-measurement#4078): Add AIP-155 request_id
  // idempotency (replay check + MarkRequestId stamp), mirroring markPoolAssignmentJobSucceeded.
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
            ?: throw PoolAssignmentJobNotFoundException(
                request.dataProviderResourceId,
                request.rawImpressionUploadResourceId,
                request.poolAssignmentJobResourceId,
              )
              .asStatusRuntimeException(Status.Code.NOT_FOUND)

        val currentJob = result.poolAssignmentJob

        val validPreviousStates =
          setOf(State.POOL_ASSIGNMENT_STATE_CREATED, State.POOL_ASSIGNMENT_STATE_FAILED)
        if (currentJob.state !in validPreviousStates) {
          throw PoolAssignmentJobStateInvalidException(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              request.poolAssignmentJobResourceId,
              currentJob.state,
              validPreviousStates,
            )
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
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
        ) {
          set("ErrorMessage").to(request.errorMessage)
        }

        currentJob.copy {
          state = State.POOL_ASSIGNMENT_STATE_FAILED
          errorMessage = request.errorMessage
          clearUpdateTime()
        }
      }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

    return updatedJob.copy {
      updateTime = commitTimestamp
      etag = ETags.computeETag(commitTimestamp.toInstant())
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

/** Returns the later of two [Date]s by (year, month, day); treats null as "no date". */
private fun maxProtoDate(a: Date?, b: Date?): Date? =
  when {
    a == null -> b
    b == null -> a
    else -> if (compareProtoDate(a, b) >= 0) a else b
  }

private fun compareProtoDate(a: Date, b: Date): Int {
  if (a.year != b.year) return a.year - b.year
  if (a.month != b.month) return a.month - b.month
  return a.day - b.day
}
