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
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.channels.ReceiveChannel
import org.wfanet.measurement.common.grpc.errorInfo
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
 */
abstract class BaseTeeApplication(
  private val subscriptionId: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<WorkItem>,
  private val workItemsStub: WorkItemsCoroutineStub,
  private val workItemAttemptsStub: WorkItemAttemptsCoroutineStub,
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
    logger.info("Starting to process message with ackId: ${queueMessage.ackId}")
    val body: WorkItem = queueMessage.body

    if (body.name.isEmpty()) {
      logger.log(Level.SEVERE, "WorkItem name is empty. Cannot proceed. Nacking message.")
      queueMessage.nack()
      return
    }
    logger.info("Processing WorkItem: ${body.name}")
    val workItemName = WorkItemKey(body.name).toName()
    val workItemAttempt: WorkItemAttempt =
      try {
        val workItemAttemptId = "work-item-attempt-" + UUID.randomUUID().toString()
        logger.info("Creating WorkItemAttempt: $workItemAttemptId for WorkItem: $workItemName")
        createWorkItemAttempt(parent = workItemName, workItemAttemptId = workItemAttemptId)
      } catch (e: ControlPlaneApiException) {
        // If createWorkItemAttempt failed because the WorkItem is not found or in an invalid state,
        // ack the message and stop processing.
        val cause = e.cause
        if (cause is StatusException) {
          val reason = cause.errorInfo?.reason
          if (
            reason == Errors.Reason.INVALID_WORK_ITEM_STATE.name ||
              reason == Errors.Reason.WORK_ITEM_NOT_FOUND.name
          ) {
            logger.log(Level.WARNING, e) {
              "Non-retriable error. createWorkItemAttempt failure: reason=$reason"
            }
            queueMessage.ack()
            return
          }
        }
        logger.log(Level.WARNING, e) { "Error creating a WorkItemAttempt" }
        return
      }

    try {
      logger.info("Starting runWork for WorkItemAttempt: ${workItemAttempt.name}")
      runWork(queueMessage.body.workItemParams)
      logger.info("Completed runWork for WorkItemAttempt: ${workItemAttempt.name}")
      runCatching { completeWorkItemAttempt(workItemAttempt) }
        .onFailure { error ->
          when (error) {
            is StatusException -> {
              if (
                error.status.code == Status.Code.FAILED_PRECONDITION &&
                  error.errorInfo?.reason == Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE.name &&
                  error.errorInfo?.metadataMap?.get(Errors.Metadata.WORK_ITEM_ATTEMPT_STATE.key) ==
                    WorkItemAttempt.State.SUCCEEDED.name
              ) {
                logger.info(
                  "WorkItemAttempt already succeeded. Acking message ${queueMessage.ackId}"
                )
                queueMessage.ack()
                return@processMessage
              } else {
                logger.log(Level.SEVERE, error) {
                  "Failed to report work item as completed. Nacking message ${queueMessage.ackId}"
                }
                queueMessage.nack()
                return@processMessage
              }
            }
          }
        }
      logger.info("Successfully completed processing. Acking message ${queueMessage.ackId}")
      queueMessage.ack()
    } catch (e: InvalidProtocolBufferException) {
      logger.log(Level.SEVERE, e) { "Failed to parse protobuf message ${queueMessage.ackId}" }
      try {
        failWorkItem(workItemName)
        logger.info("Marked WorkItem as failed. Acking message ${queueMessage.ackId}")
        queueMessage.ack()
      } catch (error: Throwable) {
        logger.log(Level.SEVERE, error) {
          "Failed to report work item failure. Nacking message ${queueMessage.ackId}"
        }
        queueMessage.nack()
      }
    } catch (e: Exception) {
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

  private suspend fun createWorkItemAttempt(
    parent: String,
    workItemAttemptId: String,
  ): WorkItemAttempt {
    try {
      return workItemAttemptsStub.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          this.parent = parent
          this.workItemAttemptId = workItemAttemptId
        }
      )
    } catch (e: StatusException) {
      throw ControlPlaneApiException("Failed to create WorkItemAttempt for parent: $parent", e)
    }
  }

  private suspend fun completeWorkItemAttempt(workItemAttempt: WorkItemAttempt) {
    try {
      workItemAttemptsStub.completeWorkItemAttempt(
        completeWorkItemAttemptRequest { this.name = workItemAttempt.name }
      )
    } catch (e: StatusException) {
      throw ControlPlaneApiException(
        "Failed to set WorkItemAttempt ${workItemAttempt.name} as succeeded",
        e,
      )
    }
  }

  private suspend fun failWorkItemAttempt(workItemAttempt: WorkItemAttempt, e: Exception) {
    try {
      workItemAttemptsStub.failWorkItemAttempt(
        failWorkItemAttemptRequest {
          this.name = workItemAttempt.name
          this.errorMessage = e.message.toString()
        }
      )
    } catch (e: StatusException) {
      throw ControlPlaneApiException(
        "Failed to set WorkItemAttempt ${workItemAttempt.name} as failed",
        e,
      )
    }
  }

  private suspend fun failWorkItem(workItemName: String) {
    try {
      workItemsStub.failWorkItem(failWorkItemRequest { this.name = workItemName })
    } catch (e: StatusException) {
      throw ControlPlaneApiException("Failed to set WorkItem $workItemName as failed", e)
    }
  }

  abstract suspend fun runWork(message: Any)

  override fun close() {
    logger.info("Closing BaseTeeApplication and QueueSubscriber for subscription: $subscriptionId")
    queueSubscriber.close()
    logger.info("BaseTeeApplication closed")
  }

  companion object {
    protected val logger = Logger.getLogger(this::class.java.name)
  }
}
