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

import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import java.time.LocalDate
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseContext
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.completeWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.renewWorkItemAttemptRequest

/** Validated parameters carried by a DataAvailabilitySync `WorkItem`. */
internal data class DataAvailabilitySyncWorkItem(
  val workItem: WorkItem,
  val workItemParams: WorkItem.WorkItemParams,
  val appParams: DataAvailabilitySyncParams,
  val dataPathParams: WorkItem.WorkItemParams.DataPathParams,
) {
  val workItemName: String
    get() = workItem.name

  val workItemGeneration: Long
    get() = workItem.generation.takeUnless { it == 0L } ?: 1L

  val doneBlobUri: String
    get() = dataPathParams.dataPath

  val doneBlobGeneration: Long
    get() = dataPathParams.generation

  val eventDate: LocalDate
    get() =
      LocalDate.of(appParams.eventDate.year, appParams.eventDate.month, appParams.eventDate.day)

  companion object {
    fun parse(workItem: WorkItem): DataAvailabilitySyncWorkItem {
      val workItemId = workItem.name.removePrefix("workItems/")
      require(
        workItem.name == "workItems/$workItemId" && ResourceIds.RFC_1034_REGEX.matches(workItemId)
      ) {
        "name must be a WorkItem resource name"
      }
      require(workItem.generation >= 0L) { "generation must not be negative" }
      require(workItem.workItemParams.`is`(WorkItem.WorkItemParams::class.java)) {
        "work_item_params must contain WorkItemParams"
      }
      val workItemParams =
        try {
          workItem.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
        } catch (e: InvalidProtocolBufferException) {
          throw IllegalArgumentException("work_item_params cannot be unpacked", e)
        }
      require(workItemParams.hasAppParams()) { "app_params is required" }
      require(workItemParams.appParams.`is`(DataAvailabilitySyncParams::class.java)) {
        "app_params must contain DataAvailabilitySyncParams"
      }
      val appParams =
        try {
          workItemParams.appParams.unpack(DataAvailabilitySyncParams::class.java)
        } catch (e: InvalidProtocolBufferException) {
          throw IllegalArgumentException("app_params cannot be unpacked", e)
        }
      requireNotNull(DataProviderKey.fromName(appParams.dataProvider)) {
        "data_provider must be a DataProvider resource name"
      }
      requireNotNull(RawImpressionUploadKey.fromName(appParams.rawImpressionUpload)) {
        "raw_impression_upload must be a RawImpressionUpload resource name"
      }
      requireNotNull(ModelLineKey.fromName(appParams.modelLine)) {
        "model_line must be a ModelLine resource name"
      }
      require(appParams.hasEventDate()) { "event_date is required" }
      LocalDate.of(appParams.eventDate.year, appParams.eventDate.month, appParams.eventDate.day)

      require(workItemParams.hasDataPathParams()) { "data_path_params is required" }
      val dataPathParams = workItemParams.dataPathParams
      require(dataPathParams.dataPath.isNotEmpty()) { "data_path is required" }
      require(dataPathParams.hasGeneration() && dataPathParams.generation > 0L) {
        "data path generation must be positive"
      }
      require(
        dataPathParams.eventType ==
          WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
      ) {
        "event_type must be FINALIZED"
      }
      return DataAvailabilitySyncWorkItem(workItem, workItemParams, appParams, dataPathParams)
    }
  }
}

/** Processes DataAvailabilitySync WorkItems using the standard attempt lifecycle. */
internal class DataAvailabilitySyncWorkItemProcessor(
  private val workItemsStub: WorkItemsCoroutineStub,
  private val workItemAttemptsStub: WorkItemAttemptsCoroutineStub,
  private val leaseRunner: DataAvailabilitySyncLeaseRunner,
  private val synchronize:
    suspend (
      DataAvailabilitySyncWorkItem,
      DataAvailabilitySyncLeaseContext,
      (DataAvailabilitySync.Stage) -> Unit,
    ) -> DataAvailabilitySync.Outcome,
  private val verifyDoneObject: suspend (DataAvailabilitySyncWorkItem) -> Unit,
  private val uuidGenerator: () -> String = { UUID.randomUUID().toString() },
  private val activeAttemptRetryDelay: suspend () -> Unit = { delay(30_000L) },
  private val attemptLeaseRenewalDelay: suspend () -> Unit = { delay(60_000L) },
  private val attemptUpdateRetryDelay: suspend (Int) -> Unit = { attempt ->
    delay(ATTEMPT_UPDATE_BACKOFF.durationForAttempt(attempt).toMillis())
  },
) {
  suspend fun process(input: DataAvailabilitySyncWorkItem) {
    val parentContext =
      Tracing.withW3CTraceContext(input.workItemParams.traceContextMap) { Context.current() }
    withContext(parentContext.asContextElement()) {
      val attributes =
        Attributes.builder()
          .put(XmmTraceAttributes.WORK_ITEM_NAME, input.workItemName)
          .put(XmmTraceAttributes.WORK_ITEM_GENERATION, input.workItemGeneration)
          .put(
            VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME,
            input.appParams.rawImpressionUpload,
          )
          .put(VidLabelingTraceAttributes.MODEL_LINE_NAME, input.appParams.modelLine)
          .put(
            VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH,
            VidLabelingTraceAttributes.gcsObjectPathHash(input.doneBlobUri),
          )
          .put(VidLabelingTraceAttributes.GCS_OBJECT_GENERATION, input.doneBlobGeneration)
          .put(XmmTraceAttributes.LIFECYCLE_STAGE, LIFECYCLE_STAGE)
          .put(XmmTraceAttributes.OUTCOME, "started")
          .build()
      Tracing.traceSuspending("edpa.data_availability.sync.work_item", attributes) {
        processInContext(input)
      }
    }
  }

  private suspend fun processInContext(input: DataAvailabilitySyncWorkItem) {
    val claimed = awaitWorkItemAttempt(input) ?: return
    val attempt = claimed.attempt
    Span.current().setAttribute(XmmTraceAttributes.WORK_ITEM_ATTEMPT_NAME, attempt.name)
    logLifecycle(input, attempt, outcome = "started")

    var stage = DataAvailabilitySync.Stage.DISCOVERY
    var leaseName: String? = null
    try {
      val outcome =
        runWithAttemptLeaseRenewal(attempt) {
          leaseRunner.run(input.appParams.dataProvider, claimed.synchronizationAttemptId) { lease ->
            leaseName = lease.name
            Span.current()
              .setAttribute(
                VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_LEASE_NAME,
                lease.name,
              )
            verifyDoneObject(input)
            synchronize(input, lease) { nextStage ->
              stage = nextStage
              Span.current()
                .setAttribute(VidLabelingTraceAttributes.PIPELINE_PHASE, nextStage.name.lowercase())
            }
          }
        }

      if (outcome == DataAvailabilitySync.Outcome.PUBLISHED) {
        completeWorkItemAttempt(attempt)
        Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "succeeded")
        logLifecycle(input, attempt, outcome = "succeeded", stage = stage, leaseName = leaseName)
        return
      }

      val failure = IllegalStateException("DataAvailabilitySync outcome was ${outcome.name}")
      failTerminal(input, attempt, failureMessage(stage, failure))
      Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome.name.lowercase())
      logLifecycle(
        input,
        attempt,
        outcome = outcome.name.lowercase(),
        error = failure,
        stage = stage,
        leaseName = leaseName,
      )
    } catch (e: CancellationException) {
      throw e
    } catch (e: IllegalArgumentException) {
      failTerminal(input, attempt, failureMessage(stage, e))
      Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "failed")
      logLifecycle(input, attempt, "failed", e, stage, leaseName)
    } catch (e: Exception) {
      try {
        failWorkItemAttempt(attempt, failureMessage(stage, e))
      } catch (failureWritebackError: Exception) {
        e.addSuppressed(failureWritebackError)
      }
      Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "retryable_failure")
      logLifecycle(input, attempt, "retryable_failure", e, stage, leaseName)
      throw e
    }
  }

  private suspend fun awaitWorkItemAttempt(input: DataAvailabilitySyncWorkItem): ClaimedAttempt? {
    while (true) {
      val synchronizationAttemptId = uuidGenerator()
      try {
        val attempt =
          workItemAttemptsStub.createWorkItemAttempt(
            createWorkItemAttemptRequest {
              parent = input.workItemName
              workItemAttemptId = "data-availability-$synchronizationAttemptId"
              expectedWorkItemGeneration = input.workItemGeneration
              supportsAttemptLease = true
            }
          )
        return ClaimedAttempt(attempt, synchronizationAttemptId)
      } catch (e: StatusException) {
        val reason = e.errorInfo?.reason
        val workItemState = e.errorInfo?.metadataMap?.get(WORK_ITEM_STATE_METADATA_KEY)
        if (
          reason == INVALID_WORK_ITEM_STATE_REASON && workItemState == WorkItem.State.RUNNING.name
        ) {
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "in_progress")
          activeAttemptRetryDelay()
          continue
        }
        if (
          reason == WORK_ITEM_GENERATION_MISMATCH_REASON ||
            reason == WORK_ITEM_NOT_FOUND_REASON ||
            reason == INVALID_WORK_ITEM_STATE_REASON && workItemState in TERMINAL_WORK_ITEM_STATES
        ) {
          val outcome =
            if (reason == WORK_ITEM_GENERATION_MISMATCH_REASON) {
              "stale_delivery"
            } else {
              "already_terminal"
            }
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome)
          logLifecycle(input, outcome = outcome)
          return null
        }
        throw e
      }
    }
  }

  private suspend fun runWithAttemptLeaseRenewal(
    attempt: WorkItemAttempt,
    block: suspend () -> DataAvailabilitySync.Outcome,
  ): DataAvailabilitySync.Outcome = coroutineScope {
    if (!attempt.hasLeaseExpirationTime()) {
      return@coroutineScope block()
    }
    val renewalJob = launch {
      while (isActive) {
        attemptLeaseRenewalDelay()
        renewWorkItemAttempt(attempt)
      }
    }
    try {
      block()
    } finally {
      renewalJob.cancelAndJoin()
    }
  }

  private suspend fun renewWorkItemAttempt(attempt: WorkItemAttempt) {
    retryAttemptUpdate {
      workItemAttemptsStub.renewWorkItemAttempt(renewWorkItemAttemptRequest { name = attempt.name })
    }
  }

  private suspend fun completeWorkItemAttempt(attempt: WorkItemAttempt) {
    try {
      retryAttemptUpdate {
        workItemAttemptsStub.completeWorkItemAttempt(
          completeWorkItemAttemptRequest { name = attempt.name }
        )
      }
    } catch (e: StatusException) {
      if (
        e.errorInfo?.reason == INVALID_WORK_ITEM_ATTEMPT_STATE_REASON &&
          e.errorInfo?.metadataMap?.get(WORK_ITEM_ATTEMPT_STATE_METADATA_KEY) ==
            WorkItemAttempt.State.SUCCEEDED.name
      ) {
        return
      }
      throw e
    }
  }

  private suspend fun failTerminal(
    input: DataAvailabilitySyncWorkItem,
    attempt: WorkItemAttempt,
    errorMessage: String,
  ) {
    failWorkItemAttempt(attempt, errorMessage)
    retryAttemptUpdate {
      workItemsStub.failWorkItem(
        failWorkItemRequest {
          name = input.workItemName
          expectedWorkItemGeneration = input.workItemGeneration
        }
      )
    }
  }

  private suspend fun failWorkItemAttempt(attempt: WorkItemAttempt, errorMessage: String) {
    retryAttemptUpdate {
      workItemAttemptsStub.failWorkItemAttempt(
        failWorkItemAttemptRequest {
          name = attempt.name
          this.errorMessage = errorMessage
        }
      )
    }
  }

  private suspend fun <T> retryAttemptUpdate(block: suspend () -> T): T {
    var attempt = 1
    while (true) {
      try {
        return block()
      } catch (e: StatusException) {
        if (e.status.code !in RETRYABLE_CODES || attempt >= MAX_UPDATE_ATTEMPTS) throw e
        attemptUpdateRetryDelay(attempt)
        attempt++
      }
    }
  }

  private fun failureMessage(stage: DataAvailabilitySync.Stage, error: Throwable): String =
    "${stage.name}:${error::class.java.name.substringAfterLast('.').take(200)}"

  private fun logLifecycle(
    input: DataAvailabilitySyncWorkItem,
    attempt: WorkItemAttempt? = null,
    outcome: String,
    error: Throwable? = null,
    stage: DataAvailabilitySync.Stage? = null,
    leaseName: String? = null,
  ) {
    VidLabelingTraceLogging.log(
      logger,
      if (error == null) Level.INFO else Level.WARNING,
      "edpa.data_availability_sync_work_item.process",
      XmmTraceAttributes.WORK_ITEM_NAME_STRING to input.workItemName,
      XmmTraceAttributes.WORK_ITEM_ATTEMPT_NAME_STRING to attempt?.name,
      XmmTraceAttributes.WORK_ITEM_GENERATION_STRING to input.workItemGeneration.toString(),
      VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_LEASE_NAME_STRING to leaseName,
      VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME_STRING to
        input.appParams.rawImpressionUpload,
      VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to input.appParams.modelLine,
      VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH_STRING to
        VidLabelingTraceAttributes.gcsObjectPathHash(input.doneBlobUri),
      VidLabelingTraceAttributes.GCS_OBJECT_GENERATION_STRING to
        input.doneBlobGeneration.toString(),
      VidLabelingTraceAttributes.PIPELINE_PHASE_STRING to stage?.name?.lowercase(),
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING to LIFECYCLE_STAGE,
      XmmTraceAttributes.OUTCOME_STRING to outcome,
      XmmTraceAttributes.ERROR_TYPE_STRING to error?.let(XmmTraceAttributes::errorType),
      XmmTraceAttributes.ERROR_CODE_STRING to error?.let(XmmTraceAttributes::errorCode),
    )
  }

  private data class ClaimedAttempt(
    val attempt: WorkItemAttempt,
    val synchronizationAttemptId: String,
  )

  companion object {
    private const val LIFECYCLE_STAGE = "availability_work_item_process"
    private const val MAX_UPDATE_ATTEMPTS = 3
    private const val INVALID_WORK_ITEM_STATE_REASON = "INVALID_WORK_ITEM_STATE"
    private const val INVALID_WORK_ITEM_ATTEMPT_STATE_REASON = "INVALID_WORK_ITEM_ATTEMPT_STATE"
    private const val WORK_ITEM_GENERATION_MISMATCH_REASON = "WORK_ITEM_GENERATION_MISMATCH"
    private const val WORK_ITEM_NOT_FOUND_REASON = "WORK_ITEM_NOT_FOUND"
    private const val WORK_ITEM_STATE_METADATA_KEY = "workItemState"
    private const val WORK_ITEM_ATTEMPT_STATE_METADATA_KEY = "workItemAttemptState"
    private val ATTEMPT_UPDATE_BACKOFF = ExponentialBackoff()
    private val RETRYABLE_CODES =
      setOf(
        Status.Code.ABORTED,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.RESOURCE_EXHAUSTED,
        Status.Code.UNAVAILABLE,
      )
    private val TERMINAL_WORK_ITEM_STATES =
      setOf(
        WorkItem.State.FAILED.name,
        WorkItem.State.SUCCEEDED.name,
        WorkItem.State.STATE_UNSPECIFIED.name,
        WorkItem.State.UNRECOGNIZED.name,
      )
    private val logger = Logger.getLogger(DataAvailabilitySyncWorkItemProcessor::class.java.name)
  }
}
