/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.teesdk

import com.google.protobuf.Any
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Parser
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.trace.Span
import java.time.Duration
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.system.exitProcess
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.telemetry.W3CTraceContext
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.common.telemetry.XmmTracing
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.completeWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.renewWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.service.Errors
import org.wfanet.measurement.securecomputation.service.WorkItemKey

private fun canonicalWorkItemName(name: String): String {
  if (name.isEmpty()) return name
  return WorkItemKey.fromName(name)?.toName() ?: WorkItemKey(name).toName()
}

/**
 * BaseTeeApplication is an abstract base class for TEE applications that automatically subscribes
 * to a specified queue and processes messages as they arrive.
 *
 * @param T The type of message that this application will process.
 * @param subscriptionId The name of the subscription to which this application subscribes.
 * @param queueSubscriber A client that manages connections and interactions with the queue.
 * @param parser [Parser] used to parse serialized queue messages into [T] instances.
 * @param controlPlaneThrottler optional process-scoped limiter for `WorkItems` and
 *   `WorkItemAttempts` RPCs.
 * @param attemptUpdateRetryDelay suspends before retrying a transient attempt-state update.
 * @param activeAttemptRetryDelay suspends before retrying ownership of a delivery whose WorkItem
 *   already has an active attempt.
 * @param attemptLeaseRenewalContext [CoroutineContext] the lease-renewal loop runs in. It must not
 *   share a thread with [runWork]: subclasses commonly run [runWork] inside a dispatcher-less
 *   `runBlocking`, where CPU-bound or non-suspending work would otherwise starve renewal until the
 *   lease expires. It must not contain a [Job], which would detach renewal from the work's
 *   structured cancellation.
 * @param maxAttemptRuntime optional upper bound on a WorkItemAttempt's runtime, measured from when
 *   the attempt is created. `null` (the default unless [MAX_ATTEMPT_RUNTIME_ENV] is set) disables
 *   the bound. When exceeded, a watchdog that runs independently of [runWork]'s thread stops lease
 *   renewal, cancels [runWork], reports the attempt as failed, and nacks the message so the
 *   WorkItem can be retried. If [runWork] has not stopped after [attemptStopGracePeriod] — e.g.
 *   non-cooperative work that ignores cancellation — the process is terminated with exit code
 *   [ATTEMPT_RUNTIME_EXCEEDED_EXIT_CODE] so the runtime environment restarts it.
 * @param attemptStopGracePeriod how long [runWork] may take to stop after [maxAttemptRuntime] is
 *   exceeded before the process is terminated.
 * @param attemptRuntimeWatchdogContext [CoroutineContext] the runtime watchdog runs in. Like
 *   [attemptLeaseRenewalContext], it must not share a thread with [runWork] or contain a [Job].
 * @param attemptRuntimeTimer suspends for the given duration; used for [maxAttemptRuntime] and
 *   [attemptStopGracePeriod].
 * @param terminateProcess terminates the process with the given exit code.
 */
abstract class BaseTeeApplication(
  private val subscriptionId: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<WorkItem>,
  private val workItemsStub: WorkItemsCoroutineStub,
  private val workItemAttemptsStub: WorkItemAttemptsCoroutineStub,
  private val controlPlaneThrottler: Throttler? = null,
  private val attemptUpdateRetryDelay: suspend (Int) -> Unit = { attempt ->
    delay(ATTEMPT_UPDATE_RETRY_BACKOFF.durationForAttempt(attempt).toMillis())
  },
  private val activeAttemptRetryDelay: suspend () -> Unit = {
    delay(ACTIVE_ATTEMPT_RETRY_DELAY.toMillis())
  },
  private val attemptLeaseRenewalInterval: Duration = DEFAULT_ATTEMPT_LEASE_RENEWAL_INTERVAL,
  private val workItemConsumptionEnabled: Boolean = workItemConsumptionEnabledFromEnvironment(),
  private val attemptLeaseRenewalContext: CoroutineContext = DEFAULT_ATTEMPT_LEASE_RENEWAL_CONTEXT,
  private val maxAttemptRuntime: Duration? = maxAttemptRuntimeFromEnvironment(),
  private val attemptStopGracePeriod: Duration = DEFAULT_ATTEMPT_STOP_GRACE_PERIOD,
  private val attemptRuntimeWatchdogContext: CoroutineContext =
    DEFAULT_ATTEMPT_RUNTIME_WATCHDOG_CONTEXT,
  private val attemptRuntimeTimer: suspend (Duration) -> Unit = { delay(it.toMillis()) },
  private val terminateProcess: (exitCode: Int) -> Unit = { exitProcess(it) },
) : AutoCloseable {

  init {
    require(attemptLeaseRenewalInterval > Duration.ZERO) {
      "attemptLeaseRenewalInterval must be positive"
    }
    require(attemptLeaseRenewalContext[Job] == null) {
      "attemptLeaseRenewalContext must not contain a Job"
    }
    require(maxAttemptRuntime == null || maxAttemptRuntime > Duration.ZERO) {
      "maxAttemptRuntime must be positive"
    }
    require(attemptStopGracePeriod > Duration.ZERO) { "attemptStopGracePeriod must be positive" }
    require(attemptRuntimeWatchdogContext[Job] == null) {
      "attemptRuntimeWatchdogContext must not contain a Job"
    }
  }

  /** Starts the TEE application by listening for messages on the specified queue. */
  suspend fun run() {
    if (!workItemConsumptionEnabled) {
      logger.info("WorkItem consumption is disabled; waiting without subscribing")
      awaitCancellation()
    }
    logger.info("Starting BaseTeeApplication for subscription: $subscriptionId")
    receiveAndProcessMessages()
  }

  /**
   * Begins listening for messages on the specified queue. Each message is processed as it arrives.
   * If an error occurs during the message flow, it is logged and handling continues.
   */
  private suspend fun receiveAndProcessMessages() {
    logger.info("Creating subscription channel for subscriptionId: $subscriptionId")
    val messageChannel: ReceiveChannel<QueueSubscriber.QueueMessage<WorkItem>> =
      queueSubscriber.subscribe(subscriptionId, parser)
    logger.info("Subscription channel created. Waiting for messages...")

    var messageCount = 0
    for (message: QueueSubscriber.QueueMessage<WorkItem> in messageChannel) {
      messageCount++
      logger.info("Received message #$messageCount with ackId: ${message.ackId}")
      processMessage(message)
    }
    logger.warning("Message channel closed after processing $messageCount messages")
  }

  /**
   * Processes each message received from the queue by attempting to parse and pass it to [runWork].
   * If parsing fails, the message is negatively acknowledged and discarded. If processing fails,
   * the message is negatively acknowledged and optionally requeued.
   *
   * @param queueMessage The raw message received from the queue of type [WorkItem].
   */
  private suspend fun processMessage(queueMessage: QueueSubscriber.QueueMessage<WorkItem>) {
    val body = queueMessage.body
    val workItemName = canonicalWorkItemName(body.name)
    val workItemGeneration = body.generation.takeUnless { it == 0L } ?: 1L
    val traceContext =
      if (body.workItemParams.`is`(WorkItem.WorkItemParams::class.java)) {
        runCatching {
            body.workItemParams.unpack(WorkItem.WorkItemParams::class.java).traceContextMap
          }
          .getOrDefault(emptyMap())
      } else {
        emptyMap()
      }
    W3CTraceContext.withExtractedContext(traceContext) {
      XmmTracing.traceSuspending(
        spanName = "secure_computation.work_item.process",
        attributes =
          io.opentelemetry.api.common.Attributes.builder()
            .put(XmmTraceAttributes.WORK_ITEM_NAME, workItemName)
            .put(XmmTraceAttributes.WORK_ITEM_GENERATION, workItemGeneration)
            .put(XmmTraceAttributes.LIFECYCLE_STAGE, "work_item_processing")
            .put(XmmTraceAttributes.OUTCOME, "started")
            .build(),
      ) {
        processMessageInContext(queueMessage, workItemName, workItemGeneration)
      }
    }
  }

  private suspend fun processMessageInContext(
    queueMessage: QueueSubscriber.QueueMessage<WorkItem>,
    workItemName: String,
    workItemGeneration: Long,
  ) {
    logger.info("Starting to process message with ackId: ${queueMessage.ackId}")
    val body: WorkItem = queueMessage.body

    if (body.name.isEmpty()) {
      val error = IllegalArgumentException("WorkItem name is empty")
      recordCurrentSpanError(error)
      logger.log(Level.SEVERE, error) { "Cannot proceed. Nacking message." }
      queueMessage.nack()
      return
    }
    logger.info("Processing WorkItem: ${body.name}")
    val workItemAttempt =
      awaitWorkItemAttempt(queueMessage, workItemName, workItemGeneration) ?: return
    Span.current().setAttribute(XmmTraceAttributes.WORK_ITEM_ATTEMPT_NAME, workItemAttempt.name)

    coroutineScope {
      val guard = AttemptGuard()
      val watchdog: Job? =
        startAttemptRuntimeWatchdog(this, queueMessage, workItemName, workItemAttempt, guard)
      try {
        processAttempt(queueMessage, workItemName, workItemAttempt, guard)
      } finally {
        // Once the watchdog owns the attempt, let it finish reporting and its stop check instead of
        // cancelling it mid-report.
        if (guard.isAbandoned) watchdog?.join() else watchdog?.cancelAndJoin()
      }
    }
  }

  /**
   * Runs the work for [workItemAttempt] and reports its outcome, unless the runtime watchdog has
   * abandoned the attempt, in which case the watchdog owns the outcome.
   */
  private suspend fun processAttempt(
    queueMessage: QueueSubscriber.QueueMessage<WorkItem>,
    workItemName: String,
    workItemAttempt: WorkItemAttempt,
    guard: AttemptGuard,
  ) {
    val body: WorkItem = queueMessage.body
    try {
      logger.info("Starting runWork for WorkItemAttempt: ${workItemAttempt.name}")
      if (!runWorkGuarded(workItemAttempt, body.workItemParams, guard) || !guard.finish()) {
        return
      }
      logger.info("Completed runWork for WorkItemAttempt: ${workItemAttempt.name}")
      val completionError =
        try {
          completeWorkItemAttempt(workItemAttempt)
          null
        } catch (e: CancellationException) {
          throw e
        } catch (e: Throwable) {
          e
        }
      if (completionError != null) {
        val statusException =
          when (completionError) {
            is StatusException -> completionError
            is ControlPlaneApiException -> completionError.cause as? StatusException
            else -> null
          }
        if (
          statusException?.status?.code == Status.Code.FAILED_PRECONDITION &&
            statusException.errorInfo?.reason ==
              Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE.name &&
            statusException.errorInfo
              ?.metadataMap
              ?.get(Errors.Metadata.WORK_ITEM_ATTEMPT_STATE.key) ==
              WorkItemAttempt.State.SUCCEEDED.name
        ) {
          logger.info("WorkItemAttempt already succeeded. Acking message ${queueMessage.ackId}")
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "succeeded")
          queueMessage.ack()
          return
        }
        recordCurrentSpanError(completionError)
        logger.log(Level.SEVERE, completionError) {
          "Failed to report work item as completed. Nacking message ${queueMessage.ackId}"
        }
        queueMessage.nack()
        return
      }
      logger.info("Successfully completed processing. Acking message ${queueMessage.ackId}")
      Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "succeeded")
      queueMessage.ack()
    } catch (e: InvalidProtocolBufferException) {
      if (!guard.finish()) return
      recordCurrentSpanError(e)
      logger.log(Level.SEVERE, e) { "Failed to parse protobuf message ${queueMessage.ackId}" }
      try {
        failWorkItem(workItemName, body.generation.takeUnless { it == 0L } ?: 1L)
        logger.info("Marked WorkItem as failed. Acking message ${queueMessage.ackId}")
        queueMessage.ack()
      } catch (error: CancellationException) {
        throw error
      } catch (error: Throwable) {
        recordFailureWriteback(
          spanName = "secure_computation.work_item.failure_writeback",
          lifecycleStage = "work_item_failure_writeback",
          workItemName = workItemName,
          workItemAttemptName = workItemAttempt.name,
          error = error,
        )
        logger.log(Level.SEVERE, error) {
          "Failed to report work item failure. Nacking message ${queueMessage.ackId}"
        }
        queueMessage.nack()
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      if (!guard.finish()) return
      recordCurrentSpanError(e)
      logger.log(Level.SEVERE, e) { "Error processing message ${queueMessage.ackId}" }
      try {
        failWorkItemAttempt(workItemAttempt, e)
        logger.info("WorkItemAttempt failure reported. Nacking message ${queueMessage.ackId}")
      } catch (error: CancellationException) {
        throw error
      } catch (error: Throwable) {
        recordFailureWriteback(
          spanName = "secure_computation.work_item_attempt.failure_writeback",
          lifecycleStage = "work_item_attempt_failure_writeback",
          workItemName = workItemName,
          workItemAttemptName = workItemAttempt.name,
          error = error,
        )
        logger.log(Level.SEVERE, error) { "Failed to report work item attempt failure" }
      }
      queueMessage.nack()
    } finally {
      logger.info("Finished processing message ${queueMessage.ackId}")
    }
  }

  private fun recordFailureWriteback(
    spanName: String,
    lifecycleStage: String,
    workItemName: String,
    workItemAttemptName: String,
    error: Throwable,
  ) {
    XmmTracing.recordFailure(
      spanName,
      io.opentelemetry.api.common.Attributes.builder()
        .put(XmmTraceAttributes.WORK_ITEM_NAME, workItemName)
        .put(XmmTraceAttributes.WORK_ITEM_ATTEMPT_NAME, workItemAttemptName)
        .put(XmmTraceAttributes.LIFECYCLE_STAGE, lifecycleStage)
        .build(),
      error,
    )
  }

  private fun recordCurrentSpanError(error: Throwable) {
    XmmTracing.recordFailure(Span.current(), error)
  }

  private suspend fun createWorkItemAttempt(
    parent: String,
    workItemAttemptId: String,
    expectedWorkItemGeneration: Long,
  ): WorkItemAttempt {
    try {
      return callControlPlane {
        workItemAttemptsStub.createWorkItemAttempt(
          createWorkItemAttemptRequest {
            this.parent = parent
            this.workItemAttemptId = workItemAttemptId
            this.expectedWorkItemGeneration = expectedWorkItemGeneration
            supportsAttemptLease = true
          }
        )
      }
    } catch (e: StatusException) {
      throw ControlPlaneApiException("Failed to create WorkItemAttempt for parent: $parent", e)
    }
  }

  private suspend fun awaitWorkItemAttempt(
    queueMessage: QueueSubscriber.QueueMessage<WorkItem>,
    workItemName: String,
    expectedWorkItemGeneration: Long,
  ): WorkItemAttempt? {
    while (true) {
      try {
        val workItemAttemptId = "work-item-attempt-" + UUID.randomUUID().toString()
        logger.info("Creating WorkItemAttempt: $workItemAttemptId for WorkItem: $workItemName")
        return createWorkItemAttempt(
          parent = workItemName,
          workItemAttemptId = workItemAttemptId,
          expectedWorkItemGeneration = expectedWorkItemGeneration,
        )
      } catch (e: ControlPlaneApiException) {
        val cause = e.cause
        if (cause is StatusException) {
          val reason = cause.errorInfo?.reason
          val workItemState = cause.errorInfo?.metadataMap?.get(Errors.Metadata.WORK_ITEM_STATE.key)
          if (
            reason == Errors.Reason.INVALID_WORK_ITEM_STATE.name &&
              workItemState == WorkItem.State.RUNNING.name
          ) {
            Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "in_progress")
            logger.info(
              "WorkItem $workItemName already has an active attempt; retaining delivery while " +
                "waiting for ownership"
            )
            activeAttemptRetryDelay()
            continue
          }
          val invalidTerminalState =
            reason == Errors.Reason.INVALID_WORK_ITEM_STATE.name &&
              workItemState in TERMINAL_OR_INVALID_WORK_ITEM_STATES
          if (
            invalidTerminalState ||
              reason == Errors.Reason.WORK_ITEM_GENERATION_MISMATCH.name ||
              reason == Errors.Reason.WORK_ITEM_NOT_FOUND.name
          ) {
            when {
              workItemState == WorkItem.State.SUCCEEDED.name ->
                Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "already_completed")
              reason == Errors.Reason.WORK_ITEM_GENERATION_MISMATCH.name ->
                Span.current().setAttribute(XmmTraceAttributes.OUTCOME, "stale_delivery")
              else -> recordCurrentSpanError(e)
            }
            logger.log(Level.WARNING, e) {
              "Non-retriable error. createWorkItemAttempt failure: reason=$reason"
            }
            queueMessage.ack()
            return null
          }
        }
        recordCurrentSpanError(e)
        logger.log(Level.WARNING, e) { "Error creating a WorkItemAttempt. Nacking message." }
        queueMessage.nack()
        return null
      }
    }
  }

  private suspend fun completeWorkItemAttempt(workItemAttempt: WorkItemAttempt) {
    try {
      retryAttemptUpdate("CompleteWorkItemAttempt", workItemAttempt.name) {
        callControlPlane {
          workItemAttemptsStub.completeWorkItemAttempt(
            completeWorkItemAttemptRequest { this.name = workItemAttempt.name }
          )
        }
      }
    } catch (e: StatusException) {
      throw ControlPlaneApiException(
        "Failed to set WorkItemAttempt ${workItemAttempt.name} as succeeded",
        e,
      )
    }
  }

  /**
   * Runs [runWorkWithLeaseRenewal] as a child job that the runtime watchdog can cancel.
   *
   * @return `true` if the work completed normally, or `false` if the watchdog abandoned the attempt
   *   (it then owns reporting the outcome). Exceptions thrown by the work propagate.
   */
  private suspend fun runWorkGuarded(
    workItemAttempt: WorkItemAttempt,
    message: Any,
    guard: AttemptGuard,
  ): Boolean = coroutineScope {
    val work = async { runWorkWithLeaseRenewal(workItemAttempt, message) }
    guard.attachWork(work)
    try {
      work.await()
      true
    } catch (e: CancellationException) {
      if (guard.isAbandoned) false else throw e
    }
  }

  /**
   * Starts the watchdog enforcing [maxAttemptRuntime] for [workItemAttempt], or returns `null` when
   * the bound is disabled.
   *
   * The watchdog runs in [attemptRuntimeWatchdogContext], independently of [runWork]'s thread. On
   * expiry it claims the attempt (so the normal path neither completes nor reports it), cancels the
   * work and therefore its lease renewal, reports the attempt as failed, and nacks [queueMessage].
   * If the work then fails to stop within [attemptStopGracePeriod], it terminates the process.
   */
  private fun startAttemptRuntimeWatchdog(
    scope: CoroutineScope,
    queueMessage: QueueSubscriber.QueueMessage<WorkItem>,
    workItemName: String,
    workItemAttempt: WorkItemAttempt,
    guard: AttemptGuard,
  ): Job? {
    val limit: Duration = maxAttemptRuntime ?: return null
    return scope.launch(attemptRuntimeWatchdogContext) {
      attemptRuntimeTimer(limit)
      if (!guard.abandon()) {
        return@launch
      }
      val error = AttemptRuntimeExceededException(workItemAttempt.name, limit)
      logger.log(Level.SEVERE, error) { "Stopping work and lease renewal for abandoned attempt" }
      try {
        failWorkItemAttempt(workItemAttempt, error)
        logger.info("WorkItemAttempt failure reported. Nacking message ${queueMessage.ackId}")
      } catch (e: CancellationException) {
        throw e
      } catch (e: Throwable) {
        recordFailureWriteback(
          spanName = "secure_computation.work_item_attempt.failure_writeback",
          lifecycleStage = "work_item_attempt_failure_writeback",
          workItemName = workItemName,
          workItemAttemptName = workItemAttempt.name,
          error = e,
        )
        logger.log(Level.SEVERE, e) { "Failed to report work item attempt failure" }
      }
      queueMessage.nack()
      if (!awaitWorkStopped(guard)) {
        logger.severe(
          "runWork for ${workItemAttempt.name} did not stop within $attemptStopGracePeriod of " +
            "exceeding the maximum attempt runtime; terminating the process"
        )
        terminateProcess(ATTEMPT_RUNTIME_EXCEEDED_EXIT_CODE)
      }
    }
  }

  /** Returns whether the cancelled work stops within [attemptStopGracePeriod]. */
  private suspend fun awaitWorkStopped(guard: AttemptGuard): Boolean {
    // Not yet attached: attachWork cancels the work as soon as it starts.
    val work: Job = guard.work ?: return true
    return coroutineScope {
      val stopped = async { work.join() }
      val expired = async { attemptRuntimeTimer(attemptStopGracePeriod) }
      select {
          stopped.onAwait { true }
          expired.onAwait { false }
        }
        .also {
          stopped.cancel()
          expired.cancel()
        }
    }
  }

  private suspend fun runWorkWithLeaseRenewal(workItemAttempt: WorkItemAttempt, message: Any) =
    coroutineScope {
      if (!workItemAttempt.hasLeaseExpirationTime()) {
        runWork(message)
        return@coroutineScope
      }
      // Renew on a separate context so a runWork that blocks or busy-waits its thread cannot delay
      // renewal past lease expiry.
      val renewalJob =
        launch(attemptLeaseRenewalContext) {
          while (isActive) {
            delay(attemptLeaseRenewalInterval.toMillis())
            renewWorkItemAttempt(workItemAttempt)
          }
        }
      try {
        runWork(message)
      } finally {
        renewalJob.cancelAndJoin()
      }
    }

  private suspend fun renewWorkItemAttempt(workItemAttempt: WorkItemAttempt) {
    try {
      retryAttemptUpdate("RenewWorkItemAttempt", workItemAttempt.name) {
        callControlPlane {
          workItemAttemptsStub.renewWorkItemAttempt(
            renewWorkItemAttemptRequest { name = workItemAttempt.name }
          )
        }
      }
    } catch (e: StatusException) {
      throw ControlPlaneApiException("Failed to renew WorkItemAttempt ${workItemAttempt.name}", e)
    }
  }

  private suspend fun failWorkItemAttempt(workItemAttempt: WorkItemAttempt, e: Exception) {
    try {
      retryAttemptUpdate("FailWorkItemAttempt", workItemAttempt.name) {
        callControlPlane {
          workItemAttemptsStub.failWorkItemAttempt(
            failWorkItemAttemptRequest {
              this.name = workItemAttempt.name
              this.errorMessage = e.toString()
            }
          )
        }
      }
    } catch (e: StatusException) {
      throw ControlPlaneApiException(
        "Failed to set WorkItemAttempt ${workItemAttempt.name} as failed",
        e,
      )
    }
  }

  private suspend fun failWorkItem(workItemName: String, expectedWorkItemGeneration: Long) {
    try {
      callControlPlane {
        workItemsStub.failWorkItem(
          failWorkItemRequest {
            name = workItemName
            this.expectedWorkItemGeneration = expectedWorkItemGeneration
          }
        )
      }
    } catch (e: StatusException) {
      throw ControlPlaneApiException("Failed to set WorkItem $workItemName as failed", e)
    }
  }

  abstract suspend fun runWork(message: Any)

  private suspend fun <T> callControlPlane(block: suspend () -> T): T {
    return controlPlaneThrottler?.onReady(block) ?: block()
  }

  private suspend fun retryAttemptUpdate(
    operation: String,
    workItemAttemptName: String,
    block: suspend () -> Unit,
  ) {
    var attempt = 1
    while (true) {
      try {
        block()
        return
      } catch (e: StatusException) {
        if (
          e.status.code !in RETRYABLE_ATTEMPT_UPDATE_CODES || attempt >= ATTEMPT_UPDATE_MAX_ATTEMPTS
        ) {
          throw e
        }
        logger.log(
          Level.WARNING,
          "$operation failed transiently for $workItemAttemptName on attempt $attempt of " +
            "$ATTEMPT_UPDATE_MAX_ATTEMPTS; retrying",
          e,
        )
        attemptUpdateRetryDelay(attempt)
        attempt++
      }
    }
  }

  override fun close() {
    logger.info("Closing BaseTeeApplication and QueueSubscriber for subscription: $subscriptionId")
    queueSubscriber.close()
    logger.info("BaseTeeApplication closed")
  }

  companion object {
    protected val logger = Logger.getLogger(this::class.java.name)

    private const val ATTEMPT_UPDATE_MAX_ATTEMPTS = 3
    private const val WORK_ITEM_CONSUMPTION_ENABLED_ENV = "WORK_ITEM_CONSUMPTION_ENABLED"
    val DEFAULT_ATTEMPT_LEASE_RENEWAL_INTERVAL: Duration = Duration.ofMinutes(1)
    /**
     * Single-threaded view of [Dispatchers.IO] for lease renewal: renewals stay serialized while
     * running independently of whatever thread [runWork] occupies.
     */
    private val DEFAULT_ATTEMPT_LEASE_RENEWAL_CONTEXT: CoroutineContext =
      Dispatchers.IO.limitedParallelism(1)

    /**
     * Environment variable setting [maxAttemptRuntime] in whole seconds. Unset or blank disables
     * the bound.
     */
    const val MAX_ATTEMPT_RUNTIME_ENV = "WORK_ITEM_ATTEMPT_MAX_RUNTIME_SECONDS"

    /** Largest supported [MAX_ATTEMPT_RUNTIME_ENV] value: the timer works in milliseconds. */
    const val MAX_SUPPORTED_ATTEMPT_RUNTIME_SECONDS: Long = Long.MAX_VALUE / 1000

    /**
     * Process exit code when [runWork] does not stop after the maximum attempt runtime is exceeded
     * (`EX_TEMPFAIL`): the attempt has already been failed and its message nacked for retry.
     */
    const val ATTEMPT_RUNTIME_EXCEEDED_EXIT_CODE = 75

    private val DEFAULT_ATTEMPT_STOP_GRACE_PERIOD: Duration = Duration.ofMinutes(1)

    /** Separate single-threaded view of [Dispatchers.IO] for the attempt runtime watchdog. */
    private val DEFAULT_ATTEMPT_RUNTIME_WATCHDOG_CONTEXT: CoroutineContext =
      Dispatchers.IO.limitedParallelism(1)

    private val ACTIVE_ATTEMPT_RETRY_DELAY: Duration = Duration.ofSeconds(30)
    private val ATTEMPT_UPDATE_RETRY_BACKOFF = ExponentialBackoff()
    private val RETRYABLE_ATTEMPT_UPDATE_CODES =
      setOf(
        Status.Code.ABORTED,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.RESOURCE_EXHAUSTED,
        Status.Code.UNAVAILABLE,
      )

    private val TERMINAL_OR_INVALID_WORK_ITEM_STATES =
      setOf(
        WorkItem.State.FAILED.name,
        WorkItem.State.SUCCEEDED.name,
        WorkItem.State.STATE_UNSPECIFIED.name,
        WorkItem.State.UNRECOGNIZED.name,
      )

    private fun workItemConsumptionEnabledFromEnvironment(): Boolean {
      val value = System.getenv(WORK_ITEM_CONSUMPTION_ENABLED_ENV) ?: return true
      return requireNotNull(value.toBooleanStrictOrNull()) {
        "$WORK_ITEM_CONSUMPTION_ENABLED_ENV must be either true or false"
      }
    }

    private fun maxAttemptRuntimeFromEnvironment(): Duration? =
      parseMaxAttemptRuntime(System.getenv(MAX_ATTEMPT_RUNTIME_ENV))

    /**
     * Parses a [MAX_ATTEMPT_RUNTIME_ENV] value: `null` or blank disables the bound; otherwise it
     * must be a positive whole number of seconds up to [MAX_SUPPORTED_ATTEMPT_RUNTIME_SECONDS].
     *
     * @throws IllegalArgumentException for any other value. The value itself is not echoed.
     */
    fun parseMaxAttemptRuntime(value: String?): Duration? {
      if (value.isNullOrBlank()) {
        return null
      }
      val seconds: Long? = value.trim().toLongOrNull()
      require(seconds != null && seconds > 0 && seconds <= MAX_SUPPORTED_ATTEMPT_RUNTIME_SECONDS) {
        "$MAX_ATTEMPT_RUNTIME_ENV must be a positive whole number of seconds no greater than " +
          "$MAX_SUPPORTED_ATTEMPT_RUNTIME_SECONDS"
      }
      return Duration.ofSeconds(seconds)
    }
  }
}

/**
 * Thrown, and reported as the attempt failure, when a WorkItemAttempt exceeds its maximum runtime.
 */
class AttemptRuntimeExceededException(workItemAttemptName: String, maxAttemptRuntime: Duration) :
  Exception(
    "WorkItemAttempt $workItemAttemptName exceeded its maximum runtime of $maxAttemptRuntime"
  )

/**
 * Ownership of one WorkItemAttempt's outcome, shared by the normal processing path and the runtime
 * watchdog. Exactly one of [finish] or [abandon] wins; the loser must not ack, nack, complete, or
 * fail the attempt.
 */
private class AttemptGuard {
  private enum class State {
    RUNNING,
    FINISHED,
    ABANDONED,
  }

  private var state = State.RUNNING
  private var workJob: Job? = null

  val work: Job?
    @Synchronized get() = workJob

  val isAbandoned: Boolean
    @Synchronized get() = state == State.ABANDONED

  /** Records the work [job]; cancels it immediately if the attempt was already abandoned. */
  @Synchronized
  fun attachWork(job: Job) {
    workJob = job
    if (state == State.ABANDONED) {
      job.cancel()
    }
  }

  /** Claims the outcome for the normal path. Returns `false` if the watchdog already claimed it. */
  @Synchronized
  fun finish(): Boolean {
    if (state == State.RUNNING) {
      state = State.FINISHED
    }
    return state == State.FINISHED
  }

  /**
   * Claims the outcome for the watchdog and cancels the work (and so its lease renewal). Returns
   * `false` if the normal path already claimed it.
   */
  @Synchronized
  fun abandon(): Boolean {
    if (state != State.RUNNING) {
      return false
    }
    state = State.ABANDONED
    workJob?.cancel()
    return true
  }
}
