// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.securecomputation.controlplane.client

import org.wfanet.measurement.common.commandLineMain
import java.io.File
import java.io.IOException
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import picocli.CommandLine

/**
 * A handler class responsible for managing RabbitMQ connections and subscriptions.
 *
 * This class provides methods to:
 * - Subscribe to a RabbitMQ queue and process incoming messages by launching an external process
 *   to run the TEE application.
 * - Unsubscribe from a RabbitMQ queue by safely closing the channel and connection.
 */
@CommandLine.Command(
  name = "RabbitMQClient",
  description = ["Server daemon for RabbitMQClient service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class RabbitMQClient : Runnable {

  @CommandLine.Mixin lateinit var rabbitMQClientFlags: Flags
  private lateinit var connection: Connection
  private lateinit var channel: Channel

  init {
    try {

      Runtime.getRuntime().addShutdownHook(Thread {
        unsubscribe()
      })

    } catch (e: IOException) {
      println("Error reading secret file: \${e.message}")
      throw e
    } catch (e: Exception) {
      println("An error occurred: \${e.message}")
      throw e
    }
  }

  data class QueueMessage(val body: ByteArray, val ack: () -> Unit, val nack: () -> Unit)

  override fun run() = runBlocking {
    try {
      val queueChannel = subscribe()
      processQueueMessages(queueChannel)
    } catch (e: IOException) {
      println("Error during RabbitMQ client operation: ${e.message}")
      throw e
    } catch (e: Exception) {
      println("An error occurred: ${e.message}")
      throw e
    }
  }

  /**
   * Subscribes to a RabbitMQ queue and processes incoming messages by dispatching the message body
   * to an external Kotlin application running in a separate process. After processing the message,
   * it sends an acknowledgment or negative acknowledgment back to RabbitMQ based on the result.
   *
   * @param rabbitHost The hostname of the RabbitMQ server.
   * @param rabbitPort The port of the RabbitMQ server.
   * @param rabbitUsername The username for authenticating with RabbitMQ.
   * @param rabbitPassword The password for authenticating with RabbitMQ.
   * @param rabbitQueueName The name of the RabbitMQ queue to subscribe to.
   *
   * The function:
   * - Connects to the RabbitMQ server using the provided credentials and queue details.
   * - Consumes messages from the queue and processes them in a separate process.
   * - Writes the message body to the external process's input stream.
   * - Waits for the external process to complete.
   * - Acknowledges the message if the process completes successfully, or negatively acknowledges it if the process fails.
   */
  @OptIn(ExperimentalCoroutinesApi::class)
  suspend fun subscribe(): ReceiveChannel<QueueMessage> = coroutineScope {
    val factory = ConnectionFactory().apply {
      host = rabbitMQClientFlags.rabbitHost
      port = rabbitMQClientFlags.rabbitPort
      username = rabbitMQClientFlags.rabbitUsername
      password = rabbitMQClientFlags.rabbitPassword
    }
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    produce {
      channel.basicConsume(rabbitMQClientFlags.rabbitQueueName, false, object : com.rabbitmq.client.DefaultConsumer(channel) {
        override fun handleDelivery(
          consumerTag: String,
          envelope: com.rabbitmq.client.Envelope,
          properties: com.rabbitmq.client.AMQP.BasicProperties,
          body: ByteArray
        ) {
          val ack = { channel.basicAck(envelope.deliveryTag, false) }
          val nack = { channel.basicNack(envelope.deliveryTag, false, true) }
          val result = trySend(QueueMessage(body, ack, nack))
          if (result.isFailure) {
            println("Failed to send message to the channel.")
          }
        }
      })
    }
  }

  suspend fun processQueueMessages(queue: ReceiveChannel<QueueMessage>) {
    for (message in queue) {
      val process = ProcessBuilder("java", "-jar", "/app/tee_app.jar").start()
      process.outputStream.use { outputStream ->
        outputStream.write(message.body)
        outputStream.flush()
      }
      process.inputStream.bufferedReader().use { it.readText() }
      process.errorStream.bufferedReader().use { it.readText() }
      val exitCode = process.waitFor()

      if (exitCode == 0) {
        message.ack()
      } else {
        message.nack()
      }
    }
  }

  /**
   * Unsubscribes from a RabbitMQ queue by closing the associated channel and connection.
   * Safely closes the channel and connection if they are initialized, and handles any exceptions
   * that occur during the unsubscription process.
   *
   * This function ensures:
   * - The channel is closed if it has been initialized.
   * - The connection is closed if it has been initialized.
   * - Any errors during the unsubscription are caught, logged, and rethrown.
   *
   * @throws Exception If an error occurs while closing the channel or connection.
   */
  fun unsubscribe() {
    try {
      if (::channel.isInitialized) {
        channel.close()
      }
      if (::connection.isInitialized) {
        connection.close()
      }
    } catch (e: Exception) {
      println("An error occurred during unsubscription: \${e.message}")
      throw e
    }
  }

  class Flags {
    @CommandLine.Option(
      names = ["--rabbitmq-host"],
      description = ["Host name of the RabbitMQ server."],
      required = true
    )
    lateinit var rabbitHost: String
      private set

    @CommandLine.Option(
      names = ["--rabbitmq-port"],
      description = ["Port of the RabbitMQ server."],
      required = true
    )
    var rabbitPort: Int = 5672
      private set

    @CommandLine.Option(
      names = ["--rabbitmq-username"],
      description = ["Username to authenticate to the RabbitMQ server."],
      required = true
    )
    lateinit var rabbitUsername: String
      private set

    @CommandLine.Option(
      names = ["--rabbitmq-password"],
      description = ["Password to authenticate to the RabbitMQ server."],
      required = true
    )
    lateinit var rabbitPassword: String
      private set

    @CommandLine.Option(
    names = ["--rabbitmq-queue-name"],
    description = ["The queue name to subscribe."],
    required = true
    )
    lateinit var rabbitQueueName: String
      private set
  }

}

fun main(args: Array<String>) = commandLineMain(RabbitMQClient(), args)

