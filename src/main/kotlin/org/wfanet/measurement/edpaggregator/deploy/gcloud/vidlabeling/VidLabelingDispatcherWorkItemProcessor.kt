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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.protobuf.InvalidProtocolBufferException
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.edpaggregator.VidLabelingDispatchWorkItems
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemInvocation
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemRunner

internal data class VidLabelingDispatchInput(
  val source: WorkItem.WorkItemParams.DataPathParams,
  val appParams: VidLabelingDispatchWorkItemParams,
) {
  companion object {
    fun parse(item: WorkItem): VidLabelingDispatchInput {
      require(item.queue == VidLabelingDispatchWorkItems.QUEUE) { "Unexpected dispatch queue" }
      try {
        require(item.workItemParams.`is`(WorkItem.WorkItemParams::class.java)) {
          "Missing WorkItemParams"
        }
        val frame = item.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
        require(frame.appParams.`is`(VidLabelingDispatchWorkItemParams::class.java)) {
          "Missing dispatch parameters"
        }
        val app =
          VidLabelingDispatchWorkItems.normalize(
            frame.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java)
          )
        val source =
          VidLabelingDispatchWorkItems.source(
            app,
            frame.dataPathParams.takeIf { frame.hasDataPathParams() },
          )
        return VidLabelingDispatchInput(source, app)
      } catch (e: InvalidProtocolBufferException) {
        throw IllegalArgumentException("Malformed dispatch parameters", e)
      }
    }
  }
}

/** Adapts VID dispatch outcomes to the shared attempt lifecycle. */
internal class VidLabelingDispatcherWorkItemProcessor(
  items: WorkItemsCoroutineStub,
  attempts: WorkItemAttemptsCoroutineStub,
  private val dispatch:
    suspend (VidLabelingDispatchInput, suspend () -> Unit) -> VidLabelingDispatcher.UploadOutcome,
  activeDelay: suspend () -> Unit = { delay(1_000L) },
  renewalDelay: suspend () -> Unit = { delay(60_000L) },
  updateRetryDelay: suspend (Int) -> Unit = { delay(it * 100L) },
) {
  private val runner =
    WorkItemRunner(
      items,
      attempts,
      attemptIdGenerator = { "vid-dispatch-${UUID.randomUUID()}" },
      activeAttemptDelay = activeDelay,
      renewalDelay = renewalDelay,
      updateRetryDelay = updateRetryDelay,
      requireAttemptLease = true,
      terminalWorkItemStates = setOf(WorkItem.State.SUCCEEDED, WorkItem.State.FAILED),
    )

  suspend fun process(item: WorkItem) {
    createInvocation(item).run()
  }

  fun createInvocation(item: WorkItem): WorkItemInvocation {
    val frame =
      runCatching {
          if (item.workItemParams.`is`(WorkItem.WorkItemParams::class.java))
            item.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
          else WorkItem.WorkItemParams.getDefaultInstance()
        }
        .getOrDefault(WorkItem.WorkItemParams.getDefaultInstance())
    val app =
      runCatching { frame.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java) }
        .getOrNull()
    val source =
      if (app?.hasRecovery() == true) app.recovery.source
      else frame.dataPathParams.takeIf { frame.hasDataPathParams() }
    val safeProvider =
      app?.dataProvider?.takeIf { Regex("^dataProviders/[A-Za-z0-9_-]+$").matches(it) }
    fun evidence(
      outcome: String,
      attempt: WorkItemRunner.Attempt? = null,
      error: Exception? = null,
    ) {
      Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome)
      attempt?.let {
        Span.current().setAttribute(XmmTraceAttributes.WORK_ITEM_ATTEMPT_NAME, it.resource.name)
      }
      VidLabelingTraceLogging.log(
        logger,
        if (error == null) Level.INFO else Level.WARNING,
        "edpa.vid_labeling.dispatch.work_item",
        XmmTraceAttributes.WORK_ITEM_NAME_STRING to item.name,
        XmmTraceAttributes.WORK_ITEM_GENERATION_STRING to item.generation.toString(),
        XmmTraceAttributes.WORK_ITEM_ATTEMPT_NAME_STRING to attempt?.resource?.name,
        VidLabelingTraceAttributes.DATA_PROVIDER_NAME_STRING to safeProvider,
        VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH_STRING to
          source?.dataPath?.takeIf { it.isNotEmpty() }?.let(VidLabelingTraceLogging::sha256),
        VidLabelingTraceAttributes.GCS_OBJECT_GENERATION_STRING to
          source?.generation?.takeIf { it > 0L }?.toString(),
        XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "dispatch",
        XmmTraceAttributes.OUTCOME_STRING to outcome,
        XmmTraceAttributes.ERROR_TYPE_STRING to error?.let(XmmTraceAttributes::errorType),
        XmmTraceAttributes.ERROR_CODE_STRING to error?.let(XmmTraceAttributes::errorCode),
      )
    }
    val handler =
      object : WorkItemRunner.Handler() {
        override val workItem = item
        private var outcome: VidLabelingDispatcher.UploadOutcome? = null

        override fun onStarted(attempt: WorkItemRunner.Attempt) = evidence("started", attempt)

        override fun onWaiting() = evidence("in_progress")

        override fun onSkipped(reason: WorkItemRunner.SkipReason) =
          evidence(
            when (reason) {
              WorkItemRunner.SkipReason.STALE_GENERATION -> "stale_delivery"
              WorkItemRunner.SkipReason.NOT_FOUND -> "not_found"
              WorkItemRunner.SkipReason.TERMINAL -> "already_terminal"
            }
          )

        override fun onFinished(
          attempt: WorkItemRunner.Attempt,
          action: WorkItemRunner.Action,
          error: Exception?,
        ) =
          evidence(
            if (action is WorkItemRunner.Action.Fail) "failed"
            else outcome?.telemetryValue ?: "succeeded",
            attempt,
            error,
          )

        override fun onRetryableFailure(attempt: WorkItemRunner.Attempt, error: Exception) =
          evidence("retryable_failure", attempt, error)

        override suspend fun execute(attempt: WorkItemRunner.Attempt): WorkItemRunner.Action {
          val input = VidLabelingDispatchInput.parse(item)
          attempt.ensureActive()
          val result = dispatch(input, attempt::ensureActive)
          outcome = result
          return if (result == VidLabelingDispatcher.UploadOutcome.INVALID_DONE_MARKER)
            WorkItemRunner.Action.Fail("Invalid done marker")
          else WorkItemRunner.Action.Complete
        }

        override fun failureAction(error: Exception): WorkItemRunner.Action? =
          if (
            generateSequence<Throwable>(error) { it.cause }.any { it is IllegalArgumentException }
          )
            WorkItemRunner.Action.Fail(errorMessage(error))
          else null

        override fun errorMessage(error: Exception): String = error::class.java.simpleName.take(200)
      }
    return WorkItemInvocation(
      runner,
      handler,
      executeInContext = { block ->
        val context = Tracing.withW3CTraceContext(frame.traceContextMap) { Context.current() }
        withContext(context.asContextElement()) {
          val attributes =
            Attributes.builder()
              .put(XmmTraceAttributes.WORK_ITEM_NAME, item.name)
              .put(XmmTraceAttributes.WORK_ITEM_GENERATION, item.generation)
              .put(XmmTraceAttributes.LIFECYCLE_STAGE, "dispatch")
              .put(XmmTraceAttributes.OUTCOME, "started")
          app?.let {
            safeProvider?.let { provider ->
              attributes.put(VidLabelingTraceAttributes.DATA_PROVIDER_NAME, provider)
            }
            if (source?.dataPath.orEmpty().isNotEmpty()) {
              attributes.put(
                VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH,
                VidLabelingTraceLogging.sha256(source?.dataPath.orEmpty()),
              )
              attributes.put(
                VidLabelingTraceAttributes.GCS_OBJECT_GENERATION,
                source?.generation ?: 0L,
              )
            }
          }
          Tracing.traceSuspending("edpa.vid_labeling.dispatch.work_item", attributes.build()) {
            block()
          }
        }
      },
      close = {},
    )
  }

  companion object {
    private val logger = Logger.getLogger(VidLabelingDispatcherWorkItemProcessor::class.java.name)
  }
}
