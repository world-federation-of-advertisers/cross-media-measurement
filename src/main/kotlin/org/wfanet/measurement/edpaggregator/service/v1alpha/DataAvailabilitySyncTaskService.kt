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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import java.time.DateTimeException
import java.time.LocalDate
import java.util.UUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.edpaggregator.BlobUris
import org.wfanet.measurement.edpaggregator.service.DataAvailabilitySyncTaskKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.v1alpha.CreateDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncTask
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncTaskServiceGrpcKt.DataAvailabilitySyncTaskServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.GetDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListDataAvailabilitySyncTasksRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListDataAvailabilitySyncTasksResponse
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncTask
import org.wfanet.measurement.edpaggregator.v1alpha.listDataAvailabilitySyncTasksResponse
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTask as InternalTask
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskFailureCategory as InternalFailureCategory
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskServiceGrpcKt.DataAvailabilitySyncTaskServiceCoroutineStub as InternalTaskServiceStub
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState as InternalState
import org.wfanet.measurement.internal.edpaggregator.ListDataAvailabilitySyncTasksPageToken as InternalPageToken
import org.wfanet.measurement.internal.edpaggregator.ListDataAvailabilitySyncTasksRequestKt as InternalListRequestKt
import org.wfanet.measurement.internal.edpaggregator.createDataAvailabilitySyncTaskRequest as internalCreateRequest
import org.wfanet.measurement.internal.edpaggregator.dataAvailabilitySyncTask as internalTask
import org.wfanet.measurement.internal.edpaggregator.getDataAvailabilitySyncTaskRequest as internalGetRequest
import org.wfanet.measurement.internal.edpaggregator.listDataAvailabilitySyncTasksRequest as internalListRequest

/** Public API service for durable data availability synchronization tasks. */
class DataAvailabilitySyncTaskService(
  private val internalStub: InternalTaskServiceStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : DataAvailabilitySyncTaskServiceCoroutineImplBase(coroutineContext) {

  override suspend fun createDataAvailabilitySyncTask(
    request: CreateDataAvailabilitySyncTaskRequest
  ): DataAvailabilitySyncTask {
    val parentKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: invalidArgument("parent must be a RawImpressionUpload resource name")
    if (parentKey.rawImpressionUploadId == WILDCARD_ID) {
      invalidArgument("parent must identify one RawImpressionUpload")
    }
    if (!request.hasDataAvailabilitySyncTask()) {
      invalidArgument("data_availability_sync_task is required")
    }
    val task = request.dataAvailabilitySyncTask
    validateTask(task)
    if (request.dataAvailabilitySyncTaskId.isEmpty()) {
      invalidArgument("data_availability_sync_task_id is required")
    }
    val expectedId =
      RequestIds.forDataAvailabilitySyncTask(
        VidLabelingTraceAttributes.gcsObjectPathHash(BlobUris.canonicalGcsUri(task.doneBlobUri)),
        task.doneBlobGeneration,
      )
    if (request.dataAvailabilitySyncTaskId != expectedId) {
      invalidArgument("data_availability_sync_task_id must match the done object")
    }
    if (request.requestId.isNotEmpty()) {
      validateUuid(request.requestId, "request_id")
      if (request.requestId != expectedId) {
        invalidArgument("request_id must match the done object")
      }
    }

    val internalResponse = callInternal {
      internalStub.createDataAvailabilitySyncTask(
        internalCreateRequest {
          dataProviderResourceId = parentKey.dataProviderId
          rawImpressionUploadResourceId = parentKey.rawImpressionUploadId
          dataAvailabilitySyncTaskResourceId = request.dataAvailabilitySyncTaskId
          dataAvailabilitySyncTask = internalTask {
            doneBlobUri = task.doneBlobUri
            doneBlobGeneration = task.doneBlobGeneration
            cmmsModelLine = task.cmmsModelLine
            eventDate = task.eventDate
            traceparent = task.traceparent
            tracestate = task.tracestate
          }
          requestId = request.requestId
        }
      )
    }
    return internalResponse.toPublic()
  }

  override suspend fun getDataAvailabilitySyncTask(
    request: GetDataAvailabilitySyncTaskRequest
  ): DataAvailabilitySyncTask {
    val key =
      DataAvailabilitySyncTaskKey.fromName(request.name)
        ?: invalidArgument("name must be a DataAvailabilitySyncTask resource name")
    if (key.rawImpressionUploadId == WILDCARD_ID) {
      invalidArgument("name must identify one RawImpressionUpload")
    }
    return callInternal {
        internalStub.getDataAvailabilitySyncTask(
          internalGetRequest {
            dataProviderResourceId = key.dataProviderId
            rawImpressionUploadResourceId = key.rawImpressionUploadId
            dataAvailabilitySyncTaskResourceId = key.dataAvailabilitySyncTaskId
          }
        )
      }
      .toPublic()
  }

  override suspend fun listDataAvailabilitySyncTasks(
    request: ListDataAvailabilitySyncTasksRequest
  ): ListDataAvailabilitySyncTasksResponse {
    val parentKey =
      RawImpressionUploadKey.fromName(request.parent)
        ?: invalidArgument("parent must be a RawImpressionUpload resource name")
    if (request.pageSize < 0) invalidArgument("page_size must not be negative")
    val pageSize =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
    val pageToken =
      if (request.pageToken.isEmpty()) null
      else
        try {
          InternalPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw Status.INVALID_ARGUMENT.withDescription("page_token is invalid")
            .withCause(e)
            .asRuntimeException()
        }
    val internalResponse = callInternal {
      internalStub.listDataAvailabilitySyncTasks(
        internalListRequest {
          dataProviderResourceId = parentKey.dataProviderId
          rawImpressionUploadResourceId =
            if (parentKey.rawImpressionUploadId == WILDCARD_ID) ""
            else parentKey.rawImpressionUploadId
          this.pageSize = pageSize
          if (pageToken != null) this.pageToken = pageToken
          if (request.hasFilter()) {
            filter =
              InternalListRequestKt.filter {
                if (request.filter.state == DataAvailabilitySyncTask.State.UNRECOGNIZED) {
                  invalidArgument("filter.state is invalid")
                }
                state = request.filter.state.toInternal()
                if (request.filter.hasCreateTimeInterval()) {
                  createTimeInterval = request.filter.createTimeInterval
                }
                cmmsModelLine = request.filter.cmmsModelLine
              }
          }
        }
      )
    }
    return listDataAvailabilitySyncTasksResponse {
      dataAvailabilitySyncTasks +=
        internalResponse.dataAvailabilitySyncTasksList.map { it.toPublic() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  private fun validateTask(task: DataAvailabilitySyncTask) {
    if (
      task.doneBlobUri.isEmpty() ||
        task.doneBlobGeneration <= 0 ||
        task.cmmsModelLine.isEmpty() ||
        !task.hasEventDate()
    ) {
      invalidArgument("done object, model line, and event date are required")
    }
    if (ModelLineKey.fromName(task.cmmsModelLine) == null) {
      invalidArgument("cmms_model_line must be a ModelLine resource name")
    }
    try {
      BlobUris.canonicalGcsUri(task.doneBlobUri)
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

  private fun validateUuid(value: String, field: String) {
    if (value.isEmpty()) invalidArgument("$field is required")
    try {
      if (UUID.fromString(value).version() != 4) invalidArgument("$field must be a UUID4")
    } catch (e: IllegalArgumentException) {
      throw Status.INVALID_ARGUMENT.withDescription("$field must be a UUID4")
        .withCause(e)
        .asRuntimeException()
    }
  }

  private suspend fun <T> callInternal(block: suspend () -> T): T =
    try {
      block()
    } catch (e: StatusException) {
      throw Status.fromThrowable(e).withCause(e).asRuntimeException()
    }

  private fun invalidArgument(description: String): Nothing {
    throw Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException()
  }

  companion object {
    private const val WILDCARD_ID = "-"
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
    private val TRACEPARENT_REGEX =
      Regex("^(?!ff)[0-9a-f]{2}-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}$")
  }
}

internal fun InternalTask.toPublic(): DataAvailabilitySyncTask {
  val source = this
  return dataAvailabilitySyncTask {
    name =
      DataAvailabilitySyncTaskKey(
          source.dataProviderResourceId,
          source.rawImpressionUploadResourceId,
          source.dataAvailabilitySyncTaskResourceId,
        )
        .toName()
    state = source.state.toPublic()
    doneBlobUri = source.doneBlobUri
    doneBlobPathHash = source.doneBlobPathHash
    doneBlobGeneration = source.doneBlobGeneration
    cmmsModelLine = source.cmmsModelLine
    eventDate = source.eventDate
    traceparent = source.traceparent
    tracestate = source.tracestate
    attemptCount = source.attemptCount
    failureCategory = source.failureCategory.toPublic()
    createTime = source.createTime
    updateTime = source.updateTime
    etag = source.etag
  }
}

private fun InternalState.toPublic(): DataAvailabilitySyncTask.State =
  when (this) {
    InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING ->
      DataAvailabilitySyncTask.State.PENDING
    InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING ->
      DataAvailabilitySyncTask.State.RUNNING
    InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_SUCCEEDED ->
      DataAvailabilitySyncTask.State.SUCCEEDED
    InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED -> DataAvailabilitySyncTask.State.FAILED
    InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_UNSPECIFIED,
    InternalState.UNRECOGNIZED -> DataAvailabilitySyncTask.State.STATE_UNSPECIFIED
  }

private fun DataAvailabilitySyncTask.State.toInternal(): InternalState =
  when (this) {
    DataAvailabilitySyncTask.State.PENDING ->
      InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING
    DataAvailabilitySyncTask.State.RUNNING ->
      InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_RUNNING
    DataAvailabilitySyncTask.State.SUCCEEDED ->
      InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_SUCCEEDED
    DataAvailabilitySyncTask.State.FAILED -> InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_FAILED
    DataAvailabilitySyncTask.State.STATE_UNSPECIFIED ->
      InternalState.DATA_AVAILABILITY_SYNC_TASK_STATE_UNSPECIFIED
    DataAvailabilitySyncTask.State.UNRECOGNIZED -> error("unrecognized state")
  }

private fun InternalFailureCategory.toPublic(): DataAvailabilitySyncTask.FailureCategory =
  when (this) {
    InternalFailureCategory.DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_PUBLICATION ->
      DataAvailabilitySyncTask.FailureCategory.PUBLICATION
    InternalFailureCategory.DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_SYNCHRONIZATION ->
      DataAvailabilitySyncTask.FailureCategory.SYNCHRONIZATION
    InternalFailureCategory.DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_METADATA_PERSISTENCE ->
      DataAvailabilitySyncTask.FailureCategory.METADATA_PERSISTENCE
    InternalFailureCategory.DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_GAP_POLICY ->
      DataAvailabilitySyncTask.FailureCategory.GAP_POLICY
    InternalFailureCategory.DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_KINGDOM_PUBLICATION ->
      DataAvailabilitySyncTask.FailureCategory.KINGDOM_PUBLICATION
    InternalFailureCategory.DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_INTERNAL ->
      DataAvailabilitySyncTask.FailureCategory.INTERNAL
    InternalFailureCategory.DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_UNSPECIFIED,
    InternalFailureCategory.UNRECOGNIZED ->
      DataAvailabilitySyncTask.FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
  }
