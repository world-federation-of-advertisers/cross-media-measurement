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
import io.opentelemetry.api.trace.StatusCode
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.delay
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.telemetry.ReportTracing
import org.wfanet.measurement.common.telemetry.W3CTraceContext
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
import org.wfanet.measurement.securecomputation.service.Errors
import org.wfanet.measurement.securecomputation.service.WorkItemKey

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
) : AutoCloseable {

  /** Starts the TEE application by listening for messages on the specified queue. */
  suspend fun run() {
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
      ReportTracing.traceSuspending(
        spanName = "secure_computation.work_item.process",
        attributes =
          io.opentelemetry.api.common.Attributes.builder()
            .put(ReportTraceAttributes.WORK_ITEM_NAME, body.name)
            .put(ReportTraceAttributes.LIFECYCLE_STAGE, "work_item_processing")
            .put(ReportTraceAttributes.OUTCOME, "started")
            .build(),
      ) {
        processMessageInContext(queueMessage)
      }
    }
  }

  private suspend fun processMessageInContext(
    queueMessage: QueueSubscriber.QueueMessage<WorkItem>
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
    val workItemName = WorkItemKey(body.name).toName()
    val workItemAttempt: WorkItemAttempt =
      try {
        val workItemAttemptId = "work-item-attempt-" + UUID.randomUUID().toString()
        logger.info("Creating WorkItemAttempt: $workItemAttemptId for WorkItem: $workItemName")
        createWorkItemAttempt(
          parent = workItemName,
          workItemAttemptId = workItemAttemptId,
          expectedWorkItemGeneration = body.generation.takeUnless { it == 0L } ?: 1L,
        )
      } catch (e: ControlPlaneApiException) {
        val cause = e.cause
        if (cause is StatusException) {
          val reason = cause.errorInfo?.reason
          val workItemState = cause.errorInfo?.metadataMap?.get(Errors.Metadata.WORK_ITEM_STATE.key)
          val invalidTerminalState =
            reason == Errors.Reason.INVALID_WORK_ITEM_STATE.name &&
              workItemState in TERMINAL_OR_INVALID_WORK_ITEM_STATES
          val activeAttempt =
            reason == Errors.Reason.INVALID_WORK_ITEM_STATE.name &&
              workItemState == WorkItem.State.RUNNING.name
          if (
            invalidTerminalState ||
              activeAttempt ||
              reason == Errors.Reason.WORK_ITEM_GENERATION_MISMATCH.name ||
              reason == Errors.Reason.WORK_ITEM_NOT_FOUND.name
          ) {
            logger.log(Level.WARNING, e) {
              "Non-retriable error. createWorkItemAttempt failure: reason=$reason"
            }
            recordCurrentSpanError(e)
            queueMessage.ack()
            return
          }
        }
        recordCurrentSpanError(e)
        logger.log(Level.WARNING, e) { "Error creating a WorkItemAttempt. Nacking message." }
        queueMessage.nack()
        return
      }

    try {
      logger.info("Starting runWork for WorkItemAttempt: ${workItemAttempt.name}")
      runWork(queueMessage.body.workItemParams)
      logger.info("Completed runWork for WorkItemAttempt: ${workItemAttempt.name}")
      val completionError =
        runCatching { completeWorkItemAttempt(workItemAttempt) }.exceptionOrNull()
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
          Span.current().setAttribute(ReportTraceAttributes.OUTCOME, "succeeded")
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
      Span.current().setAttribute(ReportTraceAttributes.OUTCOME, "succeeded")
      queueMessage.ack()
    } catch (e: InvalidProtocolBufferException) {
      recordCurrentSpanError(e)
      logger.log(Level.SEVERE, e) { "Failed to parse protobuf message ${queueMessage.ackId}" }
      try {
        failWorkItem(workItemName, body.generation.takeUnless { it == 0L } ?: 1L)
        logger.info("Marked WorkItem as failed. Acking message ${queueMessage.ackId}")
        queueMessage.ack()
      } catch (error: Throwable) {
        logger.log(Level.SEVERE, error) {
          "Failed to report work item failure. Nacking message ${queueMessage.ackId}"
        }
        queueMessage.nack()
      }
    } catch (e: Exception) {
      recordCurrentSpanError(e)
      logger.log(Level.SEVERE, e) { "Error processing message ${queueMessage.ackId}" }
      runCatching { failWorkItemAttempt(workItemAttempt, e) }
        .onFailure { error ->
          logger.log(Level.SEVERE, error) { "Failed to report work item attempt failure" }
        }
      logger.info("Nacking message ${queueMessage.ackId} after error")
      queueMessage.nack()
    } finally {
      logger.info("Finished processing message ${queueMessage.ackId}")
    }
  }

  private fun recordCurrentSpanError(error: Throwable) {
    Span.current()
      .setStatus(StatusCode.ERROR, error.message ?: error::class.java.name)
      .setAttribute(ReportTraceAttributes.OUTCOME, "failed")
      .setAttribute(ReportTraceAttributes.ERROR_TYPE, ReportTraceAttributes.errorType(error))
      .recordException(error)
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
          }
        )
      }
    } catch (e: StatusException) {
      throw ControlPlaneApiException("Failed to create WorkItemAttempt for parent: $parent", e)
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
  }
}
