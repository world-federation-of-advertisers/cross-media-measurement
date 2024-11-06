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
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.wfanet.measurement.common.rabbitmq.RabbitMqClient

abstract class BaseTeeApplication<T : Message>(
  private val queueName: String,
  private val rabbitMqClient: RabbitMqClient,
  private val parser: (ByteArray) -> T,
) : AutoCloseable {

  private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
  private val job: Job

  init {
    job = scope.launch {
      try {
        startListening()
      } catch (e: Exception) {
        logger.severe("Error starting to listen to RabbitMQ: ${e.message}")
        throw e
      }
    }
  }

  private suspend fun startListening() {
    while (scope.isActive) {
      try {
        val messageChannel: ReceiveChannel<RabbitMqClient.QueueMessage<ByteArray>> =
          rabbitMqClient.subscribe(queueName)

        messageChannel.consumeAsFlow()
          .catch { e ->
            logger.severe("Error in message flow: ${e.message}")
          }
          .collect { queueMessage ->
            processMessage(queueMessage)
          }
      } catch (e: Exception) {
        logger.severe("Connection error in startListening: ${e.message}")
      }
    }
  }

  private suspend fun processMessage(queueMessage: RabbitMqClient.QueueMessage<ByteArray>) {
    try {
      val message = parser(queueMessage.body)
      runWork(message)
      queueMessage.ack()
    } catch (e: InvalidProtocolBufferException) {
      logger.severe("Failed to parse protobuf message: ${e.message}")
      queueMessage.nack(requeue = false)
    } catch (e: Exception) {
      logger.severe("Error processing message: ${e.message}")
      queueMessage.nack(requeue = true)
    }
  }

  abstract fun runWork(message: T)

  override fun close() {
    try {
      job.cancel()
      scope.cancel()
      rabbitMqClient.close()
    } catch (e: Exception) {
      logger.severe("Error during close: ${e.message}")
    }
  }

  companion object {
    protected val logger = Logger.getLogger(this::class.java.name)
  }

}
