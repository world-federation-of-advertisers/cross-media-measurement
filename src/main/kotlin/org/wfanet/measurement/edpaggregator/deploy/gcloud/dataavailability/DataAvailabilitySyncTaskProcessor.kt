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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import java.time.LocalDate
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncTaskAttempts
import org.wfanet.measurement.edpaggregator.service.DataAvailabilitySyncTaskKey
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncTask
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncTaskServiceGrpcKt.DataAvailabilitySyncTaskServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markDataAvailabilitySyncTaskFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markDataAvailabilitySyncTaskRunningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markDataAvailabilitySyncTaskSucceededRequest
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds

/** Processes durable data-availability task notifications idempotently. */
class DataAvailabilitySyncTaskProcessor(
  private val taskStub: DataAvailabilitySyncTaskServiceCoroutineStub,
  private val leaseRunner: DataAvailabilitySyncLeaseRunner,
  private val buildDataAvailabilitySync: () -> DataAvailabilitySync,
  private val verifyDoneObject: suspend (DataAvailabilitySyncTask) -> Unit,
) {
  suspend fun process(taskName: String) {
    val task =
      taskStub.getDataAvailabilitySyncTask(getDataAvailabilitySyncTaskRequest { name = taskName })
    val taskKey =
      requireNotNull(DataAvailabilitySyncTaskKey.fromName(task.name)) {
        "data_availability_sync_task must be a valid resource name"
      }
    if (task.state == DataAvailabilitySyncTask.State.SUCCEEDED) {
      logTaskLifecycle(task, taskKey, "already_succeeded")
      return
    }
    if (
      task.state == DataAvailabilitySyncTask.State.SUPERSEDED ||
        task.state == DataAvailabilitySyncTask.State.CANCELLED
    ) {
      logTaskLifecycle(task, taskKey, "terminal")
      return
    }
    if (task.state == DataAvailabilitySyncTask.State.RUNNING) {
      logTaskLifecycle(task, taskKey, "already_running")
      return
    }

    val traceContext = buildMap {
      if (task.traceparent.isNotEmpty()) put("traceparent", task.traceparent)
      if (task.tracestate.isNotEmpty()) put("tracestate", task.tracestate)
    }
    val parentContext = Tracing.withW3CTraceContext(traceContext) { Context.current() }
    withContext(parentContext.asContextElement()) {
      val attributes =
        Attributes.builder()
          .put(VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_NAME, task.name)
          .put(VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME, taskKey.parentKey.toName())
          .put(VidLabelingTraceAttributes.MODEL_LINE_NAME, task.cmmsModelLine)
          .put(VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH, task.doneBlobPathHash)
          .put(VidLabelingTraceAttributes.GCS_OBJECT_GENERATION, task.doneBlobGeneration)
          .put(VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_STATE, task.state.name)
          .put(
            VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_ATTEMPT_COUNT,
            task.attemptCount.toLong(),
          )
          .put(XmmTraceAttributes.LIFECYCLE_STAGE, "availability_task_process")
          .put(XmmTraceAttributes.OUTCOME, "started")
          .build()
      Tracing.traceSuspending("edpa.data_availability.sync.task", attributes) {
        val running =
          try {
            taskStub.markDataAvailabilitySyncTaskRunning(
              markDataAvailabilitySyncTaskRunningRequest {
                name = task.name
                etag = task.etag
                requestId =
                  RequestIds.forMarkDataAvailabilitySyncTaskRunning(
                    task.name,
                    task.attemptCount + 1,
                  )
              }
            )
          } catch (e: CancellationException) {
            throw e
          } catch (e: Exception) {
            logTaskLifecycle(task, taskKey, "start_failed", e)
            throw e
          }
        val dataProviderName = DataProviderKey(taskKey.dataProviderId).toName()
        val synchronizationAttemptId =
          DataAvailabilitySyncTaskAttempts.leaseId(running.name, running.attemptCount)
        val leaseName = "$dataProviderName/dataAvailabilitySyncLeases/$synchronizationAttemptId"
        recordTaskState(running, leaseName = leaseName)
        logTaskLifecycle(running, taskKey, "started", leaseName = leaseName)
        var failureCategory = DataAvailabilitySyncTask.FailureCategory.SYNCHRONIZATION
        try {
          val outcome =
            leaseRunner.run(dataProviderName, synchronizationAttemptId) { lease ->
              verifyDoneObject(running)
              buildDataAvailabilitySync()
                .sync(
                  running.doneBlobUri,
                  dataAvailabilitySyncLease = lease.name,
                  doneBlobGeneration = running.doneBlobGeneration,
                  expectedRawImpressionUpload = taskKey.parentKey.toName(),
                  expectedModelLine = running.cmmsModelLine,
                  expectedEventDate =
                    LocalDate.of(
                      running.eventDate.year,
                      running.eventDate.month,
                      running.eventDate.day,
                    ),
                  onStage = { stage -> failureCategory = stage.toFailureCategory() },
                  ensureLeaseActive = lease::invoke,
                )
            }
          if (outcome != DataAvailabilitySync.Outcome.PUBLISHED) {
            val terminalFailureCategory =
              if (outcome == DataAvailabilitySync.Outcome.BLOCKED_GAPS) {
                DataAvailabilitySyncTask.FailureCategory.GAP_POLICY
              } else {
                failureCategory
              }
            val failed = markTaskFailed(running, terminalFailureCategory)
            recordTaskState(failed, terminalFailureCategory, leaseName)
            logTaskLifecycle(
              failed,
              taskKey,
              "failed",
              failureCategory = terminalFailureCategory,
              leaseName = leaseName,
            )
            Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome.name.lowercase())
            return@traceSuspending
          }
          val succeeded =
            taskStub.markDataAvailabilitySyncTaskSucceeded(
              markDataAvailabilitySyncTaskSucceededRequest {
                name = running.name
                etag = running.etag
                requestId = RequestIds.forMarkDataAvailabilitySyncTaskSucceeded(running.name)
              }
            )
          recordTaskState(succeeded, leaseName = leaseName)
          logTaskLifecycle(succeeded, taskKey, "succeeded", leaseName = leaseName)
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome.name.lowercase())
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          try {
            val failed = markTaskFailed(running, failureCategory)
            recordTaskState(failed, failureCategory, leaseName)
            logTaskLifecycle(failed, taskKey, "failed", e, failureCategory, leaseName)
          } catch (markFailedException: Exception) {
            e.addSuppressed(markFailedException)
            logTaskLifecycle(
              running,
              taskKey,
              "failure_writeback_failed",
              markFailedException,
              failureCategory,
              leaseName,
            )
            throw e
          }
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "failed")
        }
      }
    }
  }

  private suspend fun markTaskFailed(
    task: DataAvailabilitySyncTask,
    failureCategory: DataAvailabilitySyncTask.FailureCategory,
  ): DataAvailabilitySyncTask =
    taskStub.markDataAvailabilitySyncTaskFailed(
      markDataAvailabilitySyncTaskFailedRequest {
        name = task.name
        this.failureCategory = failureCategory
        etag = task.etag
        requestId = RequestIds.forMarkDataAvailabilitySyncTaskFailed(task.name, task.attemptCount)
      }
    )

  private fun logTaskLifecycle(
    task: DataAvailabilitySyncTask,
    taskKey: DataAvailabilitySyncTaskKey,
    outcome: String,
    error: Throwable? = null,
    failureCategory: DataAvailabilitySyncTask.FailureCategory = task.failureCategory,
    leaseName: String? = null,
  ) {
    VidLabelingTraceLogging.log(
      logger,
      if (error == null) Level.INFO else Level.WARNING,
      "edpa.data_availability_sync_task.process",
      VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_NAME_STRING to task.name,
      VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_LEASE_NAME_STRING to leaseName,
      VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_STATE_STRING to task.state.name,
      VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_ATTEMPT_COUNT_STRING to
        task.attemptCount.toString(),
      VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY_STRING to
        failureCategory.name.takeUnless {
          it == DataAvailabilitySyncTask.FailureCategory.FAILURE_CATEGORY_UNSPECIFIED.name
        },
      VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME_STRING to taskKey.parentKey.toName(),
      VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to task.cmmsModelLine,
      VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH_STRING to task.doneBlobPathHash,
      VidLabelingTraceAttributes.GCS_OBJECT_GENERATION_STRING to task.doneBlobGeneration.toString(),
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "availability_task_process",
      XmmTraceAttributes.OUTCOME_STRING to outcome,
      XmmTraceAttributes.ERROR_TYPE_STRING to error?.let(XmmTraceAttributes::errorType),
      XmmTraceAttributes.ERROR_CODE_STRING to error?.let(XmmTraceAttributes::errorCode),
    )
  }

  private fun recordTaskState(
    task: DataAvailabilitySyncTask,
    failureCategory: DataAvailabilitySyncTask.FailureCategory = task.failureCategory,
    leaseName: String? = null,
  ) {
    Span.current()
      .setAttribute(VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_STATE, task.state.name)
      .setAttribute(
        VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_ATTEMPT_COUNT,
        task.attemptCount.toLong(),
      )
    if (leaseName != null) {
      Span.current()
        .setAttribute(VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_LEASE_NAME, leaseName)
    }
    if (failureCategory != DataAvailabilitySyncTask.FailureCategory.FAILURE_CATEGORY_UNSPECIFIED) {
      Span.current()
        .setAttribute(
          VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_TASK_FAILURE_CATEGORY,
          failureCategory.name,
        )
    }
  }

  private fun DataAvailabilitySync.Stage.toFailureCategory():
    DataAvailabilitySyncTask.FailureCategory =
    when (this) {
      DataAvailabilitySync.Stage.DISCOVERY ->
        DataAvailabilitySyncTask.FailureCategory.SYNCHRONIZATION
      DataAvailabilitySync.Stage.METADATA_PERSISTENCE ->
        DataAvailabilitySyncTask.FailureCategory.METADATA_PERSISTENCE
      DataAvailabilitySync.Stage.GAP_POLICY -> DataAvailabilitySyncTask.FailureCategory.GAP_POLICY
      DataAvailabilitySync.Stage.KINGDOM_PUBLICATION ->
        DataAvailabilitySyncTask.FailureCategory.KINGDOM_PUBLICATION
    }

  companion object {
    private val logger = Logger.getLogger(DataAvailabilitySyncTaskProcessor::class.java.name)
  }
}
