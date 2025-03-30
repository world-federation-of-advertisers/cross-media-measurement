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

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Parser
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import com.google.protobuf.Any
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.channels.ReceiveChannel
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.completeWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest

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
  private val workItemAttemptsStub: WorkItemAttemptsCoroutineStub
) : AutoCloseable {

  /** Starts the TEE application by listening for messages on the specified queue. */
  suspend fun run() {
    receiveAndProcessMessages()
  }

  /**
   * Begins listening for messages on the specified queue. Each message is processed as it arrives.
   * If an error occurs during the message flow, it is logged and handling continues.
   */
  private suspend fun receiveAndProcessMessages() {
    val messageChannel: ReceiveChannel<QueueSubscriber.QueueMessage<WorkItem>> =
      queueSubscriber.subscribe(subscriptionId, parser)
    for (message: QueueSubscriber.QueueMessage<WorkItem> in messageChannel) {
      processMessage(message)
    }
  }

  /**
   * Processes each message received from the queue by attempting to parse and pass it to [runWork].
   * If parsing fails, the message is negatively acknowledged and discarded. If processing fails,
   * the message is negatively acknowledged and optionally requeued.
   *
   * @param queueMessage The raw message received from the queue of type [WorkItem].
   */
  private suspend fun processMessage(queueMessage: QueueSubscriber.QueueMessage<WorkItem>) {
    val body: WorkItem = queueMessage.body

    if (body.name.isEmpty()) {
      logger.log(Level.SEVERE, "WorkItem name is empty. Cannot proceed.")
      queueMessage.nack()
      return
    }
    val workItemName = body.name
    val workItemAttempt: WorkItemAttempt = try {
      val workItemAttemptId = UUID.randomUUID().toString()
      createWorkItemAttempt(workItemName, workItemAttemptId)
    } catch (e: ControlPlaneApiException) {
      return
    } catch (e: Exception) {
      logger.severe("Unknown error creating a WorkItemAttempt: $e")
      return
    }
    try {
      runWork(queueMessage.body.workItemParams)
      queueMessage.ack()
      runCatching { completeWorkItemAttempt(workItemAttempt) }
        .onFailure { error -> logger.log(Level.SEVERE, error) { "Failed to report work item attempt completed successfully" } }
    } catch (e: InvalidProtocolBufferException) {
      logger.log(Level.SEVERE, e) { "Failed to parse protobuf message" }
      queueMessage.nack()
      runCatching { failWorkItem(workItemName) }
        .onFailure { error -> logger.log(Level.SEVERE, error) { "Failed to report work item failure" } }
    } catch (e: Exception) {
      logger.log(Level.SEVERE, e) { "Error processing message" }
      queueMessage.nack()
      runCatching { failWorkItemAttempt(workItemAttempt, e) }
        .onFailure { error -> logger.log(Level.SEVERE, error) { "Failed to report work item attempt failure" } }
    }
  }

  private suspend fun createWorkItemAttempt(parent: String, workItemAttemptId: String) : WorkItemAttempt {
    try {
      return workItemAttemptsStub.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          this.parent = parent
          this.workItemAttemptId = workItemAttemptId
        }
      )
    } catch (e: StatusRuntimeException) {
      val errorMessage = "Error creating WorkItemAttempt"
      logger.severe("$errorMessage: $e")
      throw ControlPlaneApiException("$errorMessage: ${e.message}")
    }
  }

  private suspend fun completeWorkItemAttempt(workItemAttempt: WorkItemAttempt) {
    try {
      workItemAttemptsStub.completeWorkItemAttempt(
        completeWorkItemAttemptRequest {
          this.name = workItemAttempt.name
        }
      )
    } catch (e: StatusRuntimeException) {
      val errorMessage = "Error setting WorkItemAttempt as succeeded"
      logger.severe("$errorMessage: $e")
      throw ControlPlaneApiException("$errorMessage: ${e.message}")
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
    } catch (e: StatusRuntimeException) {
      val errorMessage = "Error setting WorkItemAttempt as failed"
      logger.severe("$errorMessage: $e")
      throw ControlPlaneApiException("$errorMessage: ${e.message}")
    }
  }

  private suspend fun failWorkItem(workItemName: String) {
    try {
      workItemsStub.failWorkItem(
        failWorkItemRequest {
          this.name = workItemName
        }
      )
    } catch (e: StatusRuntimeException) {
      val errorMessage = "Error setting WorkItem as failed"
      logger.severe("$errorMessage: $e")
      throw ControlPlaneApiException("$errorMessage: ${e.message}")
    }
  }

  abstract suspend fun runWork(message: Any)

  override fun close() {
    queueSubscriber.close()
  }

  companion object {
    protected val logger = Logger.getLogger(this::class.java.name)
  }
}
