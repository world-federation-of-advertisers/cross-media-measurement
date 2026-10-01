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
import java.time.DateTimeException
import java.time.LocalDate
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findDataAvailabilitySyncTaskByCreateRequestId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findDataAvailabilitySyncTaskByDoneObject
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getDataAvailabilitySyncTaskByResourceId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadId
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getRawImpressionUploadModelLineStateByCmmsModelLine
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.hasDataAvailabilitySyncTaskPublicationSlot
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertDataAvailabilitySyncTask
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.readDataAvailabilitySyncTasks
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.releaseDataAvailabilitySyncTaskPublicationSlot
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.updateDataAvailabilitySyncTask
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.edpaggregator.CreateDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTask
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskFailureCategory
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskServiceGrpcKt.DataAvailabilitySyncTaskServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState
import org.wfanet.measurement.internal.edpaggregator.GetDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.internal.edpaggregator.ListDataAvailabilitySyncTasksPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListDataAvailabilitySyncTasksRequest
import org.wfanet.measurement.internal.edpaggregator.ListDataAvailabilitySyncTasksResponse
import org.wfanet.measurement.internal.edpaggregator.MarkDataAvailabilitySyncTaskFailedRequest
import org.wfanet.measurement.internal.edpaggregator.MarkDataAvailabilitySyncTaskRunningRequest
import org.wfanet.measurement.internal.edpaggregator.MarkDataAvailabilitySyncTaskSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.dataAvailabilitySyncTask
import org.wfanet.measurement.internal.edpaggregator.listDataAvailabilitySyncTasksPageToken
import org.wfanet.measurement.internal.edpaggregator.listDataAvailabilitySyncTasksResponse

/** Cloud Spanner service for durable data availability synchronization tasks. */
class SpannerDataAvailabilitySyncTaskService(
  private val databaseClient: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : DataAvailabilitySyncTaskServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createDataAvailabilitySyncTask(
    request: CreateDataAvailabilitySyncTaskRequest
  ): DataAvailabilitySyncTask {
    validateCreateRequest(request)
    val input = request.dataAvailabilitySyncTask
    val canonicalUri = BlobUris.canonicalGcsUri(input.doneBlobUri)
    val pathHash = VidLabelingTraceAttributes.gcsObjectPathHash(canonicalUri)
    val expectedResourceId =
      RequestIds.forDataAvailabilitySyncTask(pathHash, input.doneBlobGeneration)
    if (request.dataAvailabilitySyncTaskResourceId != expectedResourceId) {
      invalidArgument("data_availability_sync_task_resource_id must match the done object")
    }

    val task = dataAvailabilitySyncTask {
      dataProviderResourceId = request.dataProviderResourceId
      rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
      dataAvailabilitySyncTaskResourceId = request.dataAvailabilitySyncTaskResourceId
      state = DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING
      doneBlobUri = canonicalUri
      doneBlobPathHash = pathHash
      doneBlobGeneration = input.doneBlobGeneration
      cmmsModelLine = input.cmmsModelLine
      eventDate = input.eventDate
      traceparent = input.traceparent
      tracestate = input.tracestate
      attemptCount = 0
      failureCategory =
        DataAvailabilitySyncTaskFailureCategory
          .DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_UNSPECIFIED
    }

    val runner =
      databaseClient.readWriteTransaction(Options.tag("action=createDataAvailabilitySyncTask"))
    val result =
      try {
        runner.run { transaction ->
          transaction
            .findDataAvailabilitySyncTaskByCreateRequestId(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
              request.requestId,
            )
            ?.let { existing ->
              if (!existing.task.sameImmutableFields(task)) {
                alreadyExists("request_id was already used for a different task")
              }
              return@run existing.task
            }

          transaction
            .findDataAvailabilitySyncTaskByDoneObject(
              request.dataProviderResourceId,
              pathHash,
              input.doneBlobGeneration,
            )
            ?.let { existing ->
              if (!existing.task.sameImmutableFields(task)) {
                alreadyExists("the done object is already assigned to a different task")
              }
              return@run existing.task
            }

          val rawImpressionUploadId =
            transaction.getRawImpressionUploadId(
              request.dataProviderResourceId,
              request.rawImpressionUploadResourceId,
            )
          if (
            transaction.getRawImpressionUploadModelLineStateByCmmsModelLine(
              request.dataProviderResourceId,
              rawImpressionUploadId,
              task.cmmsModelLine,
            ) == null
          ) {
            throw Status.FAILED_PRECONDITION.withDescription(
                "the parent upload does not contain the requested model line"
              )
              .asRuntimeException()
          }
          transaction.insertDataAvailabilitySyncTask(rawImpressionUploadId, task, request.requestId)
          task
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw Status.ALREADY_EXISTS.withDescription("DataAvailabilitySyncTask already exists")
            .withCause(e)
            .asRuntimeException()
        }
        throw e
      } catch (
        e:
          org.wfanet.measurement.edpaggregator.service.internal.RawImpressionUploadNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    if (result.hasCreateTime()) return result
    val commitTime: Timestamp = runner.getCommitTimestamp().toProto()
    return result.copy {
      createTime = commitTime
      updateTime = commitTime
      etag = ETags.computeETag(commitTime.toInstant())
    }
  }

  override suspend fun getDataAvailabilitySyncTask(
    request: GetDataAvailabilitySyncTaskRequest
  ): DataAvailabilitySyncTask {
    if (
      request.dataProviderResourceId.isEmpty() ||
        request.rawImpressionUploadResourceId.isEmpty() ||
        request.dataAvailabilitySyncTaskResourceId.isEmpty()
    ) {
      invalidArgument("all resource IDs are required")
    }
    return databaseClient
      .singleUse()
      .getDataAvailabilitySyncTaskByResourceId(
        request.dataProviderResourceId,
        request.rawImpressionUploadResourceId,
        request.dataAvailabilitySyncTaskResourceId,
      )
      ?.task
      ?: throw Status.NOT_FOUND.withDescription("DataAvailabilitySyncTask not found")
        .asRuntimeException()
  }

  override suspend fun listDataAvailabilitySyncTasks(
    request: ListDataAvailabilitySyncTasksRequest
  ): ListDataAvailabilitySyncTasksResponse {
    if (request.dataProviderResourceId.isEmpty()) {
      invalidArgument("data_provider_resource_id is required")
    }
    if (request.pageSize < 0) invalidArgument("page_size must not be negative")
    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
    if (request.hasPageToken()) {
      if (
        request.pageToken.dataProviderResourceId != request.dataProviderResourceId ||
          request.pageToken.rawImpressionUploadResourceId !=
            request.rawImpressionUploadResourceId ||
          request.pageToken.filter != request.filter
      ) {
        invalidArgument("page_token does not match the request")
      }
      val after = request.pageToken.after
      if (!after.hasCreateTime() || after.dataAvailabilitySyncTaskResourceId.isEmpty()) {
        invalidArgument("page_token is invalid")
      }
    }
    val results =
      databaseClient
        .singleUse()
        .readDataAvailabilitySyncTasks(
          request.dataProviderResourceId,
          request.rawImpressionUploadResourceId.ifEmpty { null },
          if (request.hasFilter()) request.filter else null,
          pageSize + 1,
          if (request.hasPageToken()) request.pageToken.after else null,
        )
        .toList()
    return listDataAvailabilitySyncTasksResponse {
      dataAvailabilitySyncTasks += results.take(pageSize).map { it.task }
      if (results.size > pageSize) {
        val last = results[pageSize - 1].task
        nextPageToken = listDataAvailabilitySyncTasksPageToken {
          dataProviderResourceId = request.dataProviderResourceId
          rawImpressionUploadResourceId = request.rawImpressionUploadResourceId
          if (request.hasFilter()) filter = request.filter
          after =
            ListDataAvailabilitySyncTasksPageTokenKt.after {
              createTime = last.createTime
              dataAvailabilitySyncTaskResourceId = last.dataAvailabilitySyncTaskResourceId
            }
        }
      }
    }
  }

  override suspend fun markDataAvailabilitySyncTaskRunning(
    request: MarkDataAvailabilitySyncTaskRunningRequest
  ): DataAvailabilitySyncTask =
    transition(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.dataAvailabilitySyncTaskResourceId,
      request.etag,
      request.requestId,
      "MarkRunningRequestId",
      DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING,
      setOf(
        DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING,
        DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED,
      ),
      incrementAttempt = true,
      failureCategory =
        DataAvailabilitySyncTaskFailureCategory
          .DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_UNSPECIFIED,
    )

  override suspend fun markDataAvailabilitySyncTaskSucceeded(
    request: MarkDataAvailabilitySyncTaskSucceededRequest
  ): DataAvailabilitySyncTask =
    transition(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.dataAvailabilitySyncTaskResourceId,
      request.etag,
      request.requestId,
      "MarkSucceededRequestId",
      DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_SUCCEEDED,
      setOf(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING),
    )

  override suspend fun markDataAvailabilitySyncTaskFailed(
    request: MarkDataAvailabilitySyncTaskFailedRequest
  ): DataAvailabilitySyncTask {
    if (
      request.failureCategory ==
        DataAvailabilitySyncTaskFailureCategory
          .DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_UNSPECIFIED ||
        request.failureCategory == DataAvailabilitySyncTaskFailureCategory.UNRECOGNIZED
    ) {
      invalidArgument("failure_category is required")
    }
    return transition(
      request.dataProviderResourceId,
      request.rawImpressionUploadResourceId,
      request.dataAvailabilitySyncTaskResourceId,
      request.etag,
      request.requestId,
      "MarkFailedRequestId",
      DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED,
      setOf(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING),
      failureCategory = request.failureCategory,
    )
  }

  private suspend fun transition(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    taskResourceId: String,
    etag: String,
    requestId: String,
    requestIdColumn: String,
    nextState: DataAvailabilitySyncTaskState,
    allowedStates: Set<DataAvailabilitySyncTaskState>,
    incrementAttempt: Boolean = false,
    failureCategory: DataAvailabilitySyncTaskFailureCategory =
      DataAvailabilitySyncTaskFailureCategory
        .DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_UNSPECIFIED,
  ): DataAvailabilitySyncTask {
    if (
      dataProviderResourceId.isEmpty() ||
        rawImpressionUploadResourceId.isEmpty() ||
        taskResourceId.isEmpty() ||
        etag.isEmpty()
    ) {
      invalidArgument("name and etag are required")
    }
    validateRequestId(requestId)
    val runner =
      databaseClient.readWriteTransaction(Options.tag("action=transitionDataAvailabilitySyncTask"))
    val updated =
      runner.run { transaction ->
        val current =
          transaction.getDataAvailabilitySyncTaskByResourceId(
            dataProviderResourceId,
            rawImpressionUploadResourceId,
            taskResourceId,
          )
            ?: throw Status.NOT_FOUND.withDescription("DataAvailabilitySyncTask not found")
              .asRuntimeException()
        val previousRequestId =
          when (requestIdColumn) {
            "MarkRunningRequestId" -> current.markRunningRequestId
            "MarkSucceededRequestId" -> current.markSucceededRequestId
            "MarkFailedRequestId" -> current.markFailedRequestId
            else -> error("unsupported request ID column")
          }
        if (previousRequestId == requestId) return@run current.task
        if (current.task.etag != etag) {
          throw Status.ABORTED.withDescription("etag does not match").asRuntimeException()
        }
        if (current.task.state !in allowedStates) {
          throw Status.FAILED_PRECONDITION.withDescription(
              "task cannot transition from ${current.task.state} to $nextState"
            )
            .asRuntimeException()
        }
        if (
          nextState == DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING &&
            !transaction.hasDataAvailabilitySyncTaskPublicationSlot(
              dataProviderResourceId,
              current.rawImpressionUploadId,
              taskResourceId,
            )
        ) {
          throw Status.FAILED_PRECONDITION.withDescription("task publication slot is not held")
            .asRuntimeException()
        }
        transaction.updateDataAvailabilitySyncTask(
          current,
          nextState,
          attemptCount =
            if (incrementAttempt) current.task.attemptCount + 1 else current.task.attemptCount,
          failureCategory = failureCategory,
          requestIdColumn = requestIdColumn,
          requestId = requestId,
        )
        if (
          nextState == DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_SUCCEEDED ||
            nextState == DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED
        ) {
          transaction.releaseDataAvailabilitySyncTaskPublicationSlot(
            dataProviderResourceId,
            current.rawImpressionUploadId,
            taskResourceId,
          )
        }
        current.task.copy {
          state = nextState
          attemptCount =
            if (incrementAttempt) current.task.attemptCount + 1 else current.task.attemptCount
          this.failureCategory = failureCategory
          clearUpdateTime()
          clearEtag()
        }
      }
    if (updated.hasUpdateTime()) return updated
    val commitTime = runner.getCommitTimestamp().toProto()
    return updated.copy {
      updateTime = commitTime
      this.etag = ETags.computeETag(commitTime.toInstant())
    }
  }

  private fun validateRequestId(requestId: String) {
    if (requestId.isEmpty()) invalidArgument("request_id is required")
    try {
      if (UUID.fromString(requestId).version() != 4) invalidArgument("request_id must be a UUID4")
    } catch (e: IllegalArgumentException) {
      throw Status.INVALID_ARGUMENT.withDescription("request_id must be a UUID4")
        .withCause(e)
        .asRuntimeException()
    }
  }

  private fun validateCreateRequest(request: CreateDataAvailabilitySyncTaskRequest) {
    if (
      request.dataProviderResourceId.isEmpty() ||
        request.rawImpressionUploadResourceId.isEmpty() ||
        request.dataAvailabilitySyncTaskResourceId.isEmpty() ||
        !request.hasDataAvailabilitySyncTask()
    ) {
      invalidArgument("all create fields are required")
    }
    val task = request.dataAvailabilitySyncTask
    if (
      task.doneBlobUri.isEmpty() ||
        task.doneBlobGeneration <= 0 ||
        task.cmmsModelLine.isEmpty() ||
        !task.hasEventDate()
    ) {
      invalidArgument("done object, model line, and event date are required")
    }
    if (request.requestId.isEmpty()) {
      invalidArgument("request_id is required")
    }
    try {
      val canonicalDoneBlobUri = BlobUris.canonicalGcsUri(task.doneBlobUri)
      if (!canonicalDoneBlobUri.endsWith("/done")) {
        invalidArgument("done_blob_uri must identify a done object")
      }
      if (UUID.fromString(request.requestId).version() != 4) {
        invalidArgument("request_id must be a UUID4")
      }
      if (
        request.requestId !=
          RequestIds.forDataAvailabilitySyncTask(
            VidLabelingTraceAttributes.gcsObjectPathHash(canonicalDoneBlobUri),
            task.doneBlobGeneration,
          )
      ) {
        invalidArgument("request_id must match the done object")
      }
      LocalDate.of(task.eventDate.year, task.eventDate.month, task.eventDate.day)
    } catch (e: IllegalArgumentException) {
      throw Status.INVALID_ARGUMENT.withDescription(e.message).withCause(e).asRuntimeException()
    } catch (e: DateTimeException) {
      throw Status.INVALID_ARGUMENT.withDescription("event_date is invalid")
        .withCause(e)
        .asRuntimeException()
    }
    if (task.traceparent.length > 55 || task.tracestate.length > 512) {
      invalidArgument("trace context exceeds the supported size")
    }
    if (task.traceparent.isNotEmpty() && !TRACEPARENT_REGEX.matches(task.traceparent)) {
      invalidArgument("traceparent is invalid")
    }
    if (task.tracestate.isNotEmpty() && task.traceparent.isEmpty()) {
      invalidArgument("traceparent is required when tracestate is set")
    }
  }

  private fun DataAvailabilitySyncTask.sameImmutableFields(
    other: DataAvailabilitySyncTask
  ): Boolean =
    dataProviderResourceId == other.dataProviderResourceId &&
      rawImpressionUploadResourceId == other.rawImpressionUploadResourceId &&
      dataAvailabilitySyncTaskResourceId == other.dataAvailabilitySyncTaskResourceId &&
      doneBlobUri == other.doneBlobUri &&
      doneBlobGeneration == other.doneBlobGeneration &&
      cmmsModelLine == other.cmmsModelLine &&
      eventDate == other.eventDate

  private fun invalidArgument(description: String): Nothing {
    throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException()
  }

  private fun alreadyExists(description: String): Nothing {
    throw Status.ALREADY_EXISTS.withDescription(description).asRuntimeException()
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
    private val TRACEPARENT_REGEX =
      Regex("^(?!ff)[0-9a-f]{2}-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}$")
  }
}
