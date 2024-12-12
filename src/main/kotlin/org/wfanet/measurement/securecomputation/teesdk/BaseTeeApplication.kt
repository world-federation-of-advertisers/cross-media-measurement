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
import com.google.protobuf.Message
import com.google.protobuf.Parser
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.consumeAsFlow
import org.wfanet.measurement.queue.QueueSubscriber

/**
 * BaseTeeApplication is an abstract base class for TEE applications that automatically subscribes
 * to a specified queue and processes messages as they arrive.
 *
 * @param T The type of message that this application will process.
 * @param subscriptionId The name of the subscription to which this application subscribes.
 * @param queueSubscriber A client that manages connections and interactions with the queue.
 * @param parser [Parser] used to parse serialized queue messages into [T] instances.
 */
abstract class BaseTeeApplication<T : Message>(
  private val subscriptionId: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<T>,
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
    val messageChannel: ReceiveChannel<QueueSubscriber.QueueMessage<T>> =
      queueSubscriber.subscribe(subscriptionId, parser)
    for (message: QueueSubscriber.QueueMessage<T> in messageChannel) {
      processMessage(message)
    }
  }

  /**
   * Processes each message received from the queue by attempting to parse and pass it to [runWork].
   * If parsing fails, the message is negatively acknowledged and discarded. If processing fails,
   * the message is negatively acknowledged and optionally requeued.
   *
   * @param queueMessage The raw message received from the queue.
   */
  private suspend fun processMessage(queueMessage: QueueSubscriber.QueueMessage<T>) {
    try {
      runWork(queueMessage.body)
      queueMessage.ack()
    } catch (e: InvalidProtocolBufferException) {
      logger.log(Level.SEVERE, e) { "Failed to parse protobuf message" }
      queueMessage.nack()
    } catch (e: Exception) {
      logger.log(Level.SEVERE, e) { "Error processing message" }
      queueMessage.nack()
    }
  }

  abstract suspend fun runWork(message: T)

  override fun close() {
    queueSubscriber.close()
  }

  companion object {
    protected val logger = Logger.getLogger(this::class.java.name)
  }
}
