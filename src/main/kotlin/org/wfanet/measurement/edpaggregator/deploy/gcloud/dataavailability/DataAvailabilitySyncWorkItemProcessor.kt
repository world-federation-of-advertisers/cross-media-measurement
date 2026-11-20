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
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseContext
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.dataavailability.ObsoleteDataAvailabilityWorkItemException
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.ExternalDataAvailabilitySyncParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemInvocation
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemRunner

/** Validated parameters carried by a DataAvailabilitySync `WorkItem`. */
internal data class DataAvailabilitySyncWorkItem(
  val workItem: WorkItem,
  val workItemParams: WorkItem.WorkItemParams,
  val appParams: DataAvailabilitySyncParams,
  val dataPathParams: WorkItem.WorkItemParams.DataPathParams,
) {
  val isExternal: Boolean
    get() = workItemParams.appParams.`is`(ExternalDataAvailabilitySyncParams::class.java)

  val workItemName: String
    get() = workItem.name

  val workItemGeneration: Long
    get() = workItem.generation.takeUnless { it == 0L } ?: 1L

  val doneBlobUri: String
    get() = dataPathParams.dataPath

  val doneBlobGeneration: Long
    get() = dataPathParams.generation

  val doneBlobCreateTime: Instant
    get() = dataPathParams.createTime.toInstant()

  val eventDate: LocalDate
    get() =
      LocalDate.of(appParams.eventDate.year, appParams.eventDate.month, appParams.eventDate.day)

  val rawImpressionUploadModelLineName: String
    get() = appParams.rawImpressionUploadModelLine

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
      val isExternal = workItemParams.appParams.`is`(ExternalDataAvailabilitySyncParams::class.java)
      require(isExternal || workItemParams.appParams.`is`(DataAvailabilitySyncParams::class.java)) {
        "app_params must contain internal or external DataAvailabilitySyncParams"
      }
      val appParams =
        try {
          if (isExternal) {
            DataAvailabilitySyncParams.newBuilder()
              .setDataProvider(
                workItemParams.appParams
                  .unpack(ExternalDataAvailabilitySyncParams::class.java)
                  .dataProvider
              )
              .build()
          } else {
            workItemParams.appParams.unpack(DataAvailabilitySyncParams::class.java)
          }
        } catch (e: InvalidProtocolBufferException) {
          throw IllegalArgumentException("app_params cannot be unpacked", e)
        }
      requireNotNull(DataProviderKey.fromName(appParams.dataProvider)) {
        "data_provider must be a DataProvider resource name"
      }
      if (!isExternal) {
        requireNotNull(RawImpressionUploadKey.fromName(appParams.triggeringRawImpressionUpload)) {
          "raw_impression_upload must be a RawImpressionUpload resource name"
        }
        requireNotNull(ModelLineKey.fromName(appParams.modelLine)) {
          "model_line must be a ModelLine resource name"
        }
        requireNotNull(
          RawImpressionUploadModelLineKey.fromName(appParams.rawImpressionUploadModelLine)
        ) {
          "raw_impression_upload_model_line must be a RawImpressionUploadModelLine resource name"
        }
        require(appParams.hasEventDate()) { "event_date is required" }
        LocalDate.of(appParams.eventDate.year, appParams.eventDate.month, appParams.eventDate.day)
      }

      require(workItemParams.hasDataPathParams()) { "data_path_params is required" }
      val dataPathParams = workItemParams.dataPathParams
      require(dataPathParams.dataPath.isNotEmpty()) { "data_path is required" }
      require(dataPathParams.hasGeneration() && dataPathParams.generation > 0L) {
        "data path generation must be positive"
      }
      require(dataPathParams.hasCreateTime()) { "data path create_time is required" }
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
  private val markAvailabilitySynchronized: suspend (DataAvailabilitySyncWorkItem) -> Unit,
  private val uuidGenerator: () -> String = { UUID.randomUUID().toString() },
  private val activeAttemptRetryDelay: suspend () -> Unit = { delay(30_000L) },
  private val attemptLeaseRenewalDelay: suspend () -> Unit = { delay(60_000L) },
  private val attemptUpdateRetryDelay: suspend (Int) -> Unit = { attempt ->
    delay(ATTEMPT_UPDATE_BACKOFF.durationForAttempt(attempt).toMillis())
  },
) {
  private val runner =
    WorkItemRunner(
      workItemsStub,
      workItemAttemptsStub,
      attemptIdGenerator = { "data-availability-${uuidGenerator()}" },
      activeAttemptDelay = activeAttemptRetryDelay,
      renewalDelay = attemptLeaseRenewalDelay,
      updateRetryDelay = attemptUpdateRetryDelay,
      // Preserve compatibility with unleased attempts from older control planes.
      requireAttemptLease = false,
      terminalWorkItemStates =
        setOf(
          WorkItem.State.FAILED,
          WorkItem.State.SUCCEEDED,
          WorkItem.State.STATE_UNSPECIFIED,
          WorkItem.State.UNRECOGNIZED,
        ),
    )

  suspend fun process(input: DataAvailabilitySyncWorkItem) {
    createInvocation(input, close = {}).run()
  }

  fun createInvocation(
    input: DataAvailabilitySyncWorkItem,
    close: suspend () -> Unit,
  ): WorkItemInvocation {
    val handler =
      object : WorkItemRunner.Handler() {
        override val workItem = input.workItem.copy { generation = input.workItemGeneration }
        private var stage = DataAvailabilitySync.Stage.DISCOVERY
        private var leaseName: String? = null
        private var outcome: DataAvailabilitySync.Outcome? = null

        override fun onStarted(attempt: WorkItemRunner.Attempt) {
          Span.current()
            .setAttribute(XmmTraceAttributes.WORK_ITEM_ATTEMPT_NAME, attempt.resource.name)
          logLifecycle(input, attempt.resource, outcome = "started")
        }

        override fun onWaiting() {
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "in_progress")
        }

        override fun onSkipped(reason: WorkItemRunner.SkipReason) {
          val outcome =
            if (reason == WorkItemRunner.SkipReason.STALE_GENERATION) "stale_delivery"
            else "already_terminal"
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome)
          logLifecycle(input, outcome = outcome)
        }

        override suspend fun execute(attempt: WorkItemRunner.Attempt): WorkItemRunner.Action {
          val result =
            leaseRunner.run(
              input.appParams.dataProvider,
              attempt.id.removePrefix("data-availability-"),
            ) { lease ->
              leaseName = lease.name
              Span.current()
                .setAttribute(
                  VidLabelingTraceAttributes.DATA_AVAILABILITY_SYNC_LEASE_NAME,
                  lease.name,
                )
              verifyDoneObject(input)
              val synchronized =
                synchronize(input, lease) { nextStage ->
                  stage = nextStage
                  Span.current()
                    .setAttribute(
                      VidLabelingTraceAttributes.PIPELINE_PHASE,
                      nextStage.name.lowercase(),
                    )
                }
              if (synchronized.canCompleteTrigger) {
                lease()
                if (!input.isExternal) markAvailabilitySynchronized(input)
              }
              synchronized
            }
          outcome = result
          return if (result.canCompleteTrigger) WorkItemRunner.Action.Complete
          else WorkItemRunner.Action.Fail(failureMessage(stage, blockedFailure(result)))
        }

        override fun failureAction(error: Exception): WorkItemRunner.Action? =
          when (error) {
            is ObsoleteDataAvailabilityWorkItemException -> WorkItemRunner.Action.Complete
            is IllegalArgumentException -> WorkItemRunner.Action.Fail(errorMessage(error))
            else -> null
          }

        override fun errorMessage(error: Exception): String = failureMessage(stage, error)

        override fun onFinished(
          attempt: WorkItemRunner.Attempt,
          action: WorkItemRunner.Action,
          error: Exception?,
        ) {
          val traceOutcome =
            when {
              error is ObsoleteDataAvailabilityWorkItemException -> "superseded"
              error != null -> "failed"
              outcome == DataAvailabilitySync.Outcome.NO_WORK -> "no_work"
              outcome == DataAvailabilitySync.Outcome.PUBLISHED -> "succeeded"
              else -> checkNotNull(outcome).name.lowercase()
            }
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, traceOutcome)
          if (error is ObsoleteDataAvailabilityWorkItemException) {
            logLifecycle(input, attempt.resource, outcome = traceOutcome)
          } else {
            val failure =
              error ?: outcome?.takeUnless { it.canCompleteTrigger }?.let(::blockedFailure)
            logLifecycle(input, attempt.resource, traceOutcome, failure, stage, leaseName)
          }
        }

        override fun onRetryableFailure(attempt: WorkItemRunner.Attempt, error: Exception) {
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "retryable_failure")
          logLifecycle(input, attempt.resource, "retryable_failure", error, stage, leaseName)
        }
      }
    return WorkItemInvocation(
      runner,
      handler,
      executeInContext = { block ->
        val parentContext =
          Tracing.withW3CTraceContext(input.workItemParams.traceContextMap) { Context.current() }
        withContext(parentContext.asContextElement()) {
          val attributes =
            Attributes.builder()
              .put(XmmTraceAttributes.WORK_ITEM_NAME, input.workItemName)
              .put(XmmTraceAttributes.WORK_ITEM_GENERATION, input.workItemGeneration)
              .put(
                VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME,
                input.appParams.triggeringRawImpressionUpload,
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
          Tracing.traceSuspending("edpa.data_availability.sync.work_item", attributes) { block() }
        }
      },
      close = close,
    )
  }

  private fun blockedFailure(outcome: DataAvailabilitySync.Outcome): Exception =
    IllegalStateException("DataAvailabilitySync outcome was ${outcome.name}")

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
        input.appParams.triggeringRawImpressionUpload,
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

  companion object {
    private const val LIFECYCLE_STAGE = "availability_work_item_process"
    private val ATTEMPT_UPDATE_BACKOFF = ExponentialBackoff()
    private val logger = Logger.getLogger(DataAvailabilitySyncWorkItemProcessor::class.java.name)
  }
}
