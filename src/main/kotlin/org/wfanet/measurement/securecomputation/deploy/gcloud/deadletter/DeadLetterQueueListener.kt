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
import io.grpc.StatusException
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.ReceiveChannel
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem as InternalWorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.processWorkItemDeadLetterRequest
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.service.internal.Errors

/**
 * Consumes a dead-letter subscription and delegates generic WorkItem recovery to the WorkItems
 * service.
 *
 * Workload-specific state remains owned by the worker or its workload service. This listener does
 * not inspect `app_params` or call EDP-Aggregator APIs.
 */
class DeadLetterQueueListener(
  private val subscriptionId: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<WorkItem>,
  private val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
) : AutoCloseable {

  suspend fun run() {
    logger.info("Starting DeadLetterQueueListener for subscription: $subscriptionId")
    receiveAndProcessMessages()
  }

  private suspend fun receiveAndProcessMessages() {
    val messageChannel: ReceiveChannel<QueueSubscriber.QueueMessage<WorkItem>> =
      queueSubscriber.subscribe(subscriptionId, parser)
    logger.info("Successfully subscribed to dead letter queue: $subscriptionId")

    for (message in messageChannel) {
      try {
        processMessage(message)
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        logger.log(Level.SEVERE, "Unexpected error processing dead letter queue message", e)
      }
    }
  }

  private suspend fun processMessage(queueMessage: QueueSubscriber.QueueMessage<WorkItem>) {
    val workItem = queueMessage.body
    if (workItem.name.isEmpty()) {
      logger.warning("Received message with empty WorkItem name. Acknowledging and skipping.")
      queueMessage.ack()
      return
    }

    try {
      val processedWorkItem =
        workItemsStub.processWorkItemDeadLetter(
          processWorkItemDeadLetterRequest {
            workItemResourceId = workItem.name
            expectedWorkItemGeneration = workItem.generation.takeUnless { it == 0L } ?: 1L
          }
        )
      check(processedWorkItem.state == InternalWorkItem.State.FAILED) {
        "Unexpected state ${processedWorkItem.state} after processing ${workItem.name}"
      }
      logger.info("Marked WorkItem ${workItem.name} FAILED after retries were exhausted")
      queueMessage.ack()
    } catch (e: StatusException) {
      when {
        isWorkItemNotFound(e) -> {
          logger.warning("WorkItem not found: ${workItem.name}. Acknowledging message.")
          queueMessage.ack()
        }
        isTerminalWorkItemError(e) || isStaleGenerationError(e) -> {
          logger.info(
            "WorkItem ${workItem.name} is already terminal or the delivery is stale. " +
              "Acknowledging message."
          )
          queueMessage.ack()
        }
        else -> {
          logger.log(Level.SEVERE, "Error calling WorkItems API", e)
          queueMessage.nack()
        }
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Unexpected error processing message", e)
      queueMessage.nack()
    }
  }

  override fun close() {
    queueSubscriber.close()
  }

  companion object {
    private val logger = Logger.getLogger(DeadLetterQueueListener::class.java.name)

    fun isWorkItemNotFound(e: StatusException): Boolean {
      val errorInfo = e.errorInfo
      return e.status.code == Status.Code.NOT_FOUND &&
        errorInfo?.domain == Errors.DOMAIN &&
        errorInfo.reason == Errors.Reason.WORK_ITEM_NOT_FOUND.name
    }

    fun isTerminalWorkItemError(e: StatusException): Boolean {
      val state = e.errorInfo?.metadataMap?.get(Errors.Metadata.WORK_ITEM_STATE.key)
      return e.status.code == Status.Code.FAILED_PRECONDITION &&
        e.errorInfo?.domain == Errors.DOMAIN &&
        e.errorInfo?.reason == Errors.Reason.INVALID_WORK_ITEM_STATE.name &&
        (state == WorkItem.State.FAILED.name || state == WorkItem.State.SUCCEEDED.name)
    }

    fun isStaleGenerationError(e: StatusException): Boolean {
      return e.status.code == Status.Code.FAILED_PRECONDITION &&
        e.errorInfo?.domain == Errors.DOMAIN &&
        e.errorInfo?.reason == Errors.Reason.WORK_ITEM_GENERATION_MISMATCH.name
    }
  }
}
