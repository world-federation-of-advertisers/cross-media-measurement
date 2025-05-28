/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter

import com.google.protobuf.Parser
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.channels.ReceiveChannel
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest
import org.wfanet.measurement.securecomputation.service.Errors
import org.wfanet.measurement.securecomputation.service.WorkItemKey
import org.wfanet.measurement.securecomputation.service.WorkItemNotFoundException

/**
 * Service that listens to a dead letter queue and marks failed work items as FAILED in the database.
 *
 * This service subscribes to a Google PubSub dead letter queue where messages are sent after a TEE
 * application fails to process them after multiple attempts. It processes each message by extracting
 * the work item ID and calling the WorkItems API to mark the item as failed.
 *
 * @param subscriptionId The subscription ID for the dead letter queue.
 * @param queueSubscriber A client that manages connections and interactions with the queue.
 * @param parser Parser used to parse serialized queue messages into WorkItem instances.
 * @param workItemsStub Client for the WorkItems API.
 */
class DeadLetterQueueListener(
  private val subscriptionId: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<WorkItem>,
  private val workItemsStub: WorkItemsCoroutineStub
) : AutoCloseable {

  /** Starts the listener by subscribing to the dead letter queue. */
  suspend fun run() {
    logger.info("Starting DeadLetterQueueListener for subscription: $subscriptionId")
    receiveAndProcessMessages()
  }

  /**
   * Begins listening for messages on the dead letter queue. Each message is processed as it arrives.
   * If an error occurs during processing, it is logged and handling continues.
   */
  private suspend fun receiveAndProcessMessages() {
    val messageChannel: ReceiveChannel<QueueSubscriber.QueueMessage<WorkItem>> =
      queueSubscriber.subscribe(subscriptionId, parser)

    logger.info("Successfully subscribed to dead letter queue: $subscriptionId")

    for (message: QueueSubscriber.QueueMessage<WorkItem> in messageChannel) {
      try {
        processMessage(message)
      } catch (e: Exception) {
        logger.log(Level.SEVERE, "Unexpected error processing dead letter queue message", e)
        // Continue processing other messages even if one fails
      }
    }
  }

  /**
   * Processes each message received from the dead letter queue by extracting the work item ID
   * and calling the WorkItems API to mark it as failed.
   *
   * @param queueMessage The message received from the dead letter queue.
   */
  private suspend fun processMessage(queueMessage: QueueSubscriber.QueueMessage<WorkItem>) {
    val workItem = queueMessage.body

    if (workItem.name.isEmpty()) {
      logger.warning("Received message with empty WorkItem name. Acknowledging and skipping.")
      queueMessage.ack()
      return
    }

    logger.info("Processing dead letter message for work item: ${workItem.name}")

    try {
      markWorkItemAsFailed(workItem.name)
      logger.info("Successfully marked work item as failed: $workItem.name")
      queueMessage.ack()
    } catch (e: Exception) {
      when (e) {
        is WorkItemNotFoundException -> {
          logger.warning("Work item not found: ${workItem.name}. Acknowledging message.")
          queueMessage.ack()
        }
        is StatusRuntimeException -> {
          if (isAlreadyFailedError(e)) {
            // If the work item is already failed, just acknowledge the message
            logger.info("Work item ${workItem.name} is already in FAILED state. Acknowledging message.")
            queueMessage.ack()
          } else {
            // For other API errors, log and nack to retry
            logger.log(Level.SEVERE, "Error calling WorkItems API", e)
            queueMessage.nack()
          }
        }
        else -> {
          // For unexpected errors, log and nack to retry
          logger.log(Level.SEVERE, "Unexpected error processing message", e)
          queueMessage.nack()
        }
      }
    }
  }

  /**
   * Calls the WorkItems API to mark a work item as failed.
   *
   * @param workItemId The ID of the work item to mark as failed.
   * @throws StatusRuntimeException If the API call fails.
   * @throws WorkItemNotFoundException If the work item does not exist.
   */
  private suspend fun markWorkItemAsFailed(workItemId: String) {
    val resourceName = WorkItemKey(workItemId).toName()
    
    try {
      workItemsStub.failWorkItem(
        failWorkItemRequest { name = resourceName }
      )
    } catch (e: StatusRuntimeException) {
      // Translate specific error cases
      when {
        e.status.code == Status.Code.NOT_FOUND -> {
          throw WorkItemNotFoundException(resourceName, e)
        }
        else -> throw e
      }
    }
  }

  /**
   * Checks if a StatusRuntimeException indicates that the work item is already in a FAILED state.
   */
  private fun isAlreadyFailedError(e: StatusRuntimeException): Boolean {
    // Check if this is a failed precondition error due to the item already being in FAILED state
    return e.status.code == Status.Code.FAILED_PRECONDITION &&
        e.errorInfo?.reason == Errors.Reason.INVALID_WORK_ITEM_STATE.name &&
        e.errorInfo?.metadataMap?.get(Errors.Metadata.WORK_ITEM_STATE.key) ==
        WorkItem.State.FAILED.name
  }

  override fun close() {
    queueSubscriber.close()
  }

  companion object {
    private val logger = Logger.getLogger(DeadLetterQueueListener::class.java.name)
  }
}
