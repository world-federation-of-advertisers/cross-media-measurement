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

package org.wfanet.measurement.securecomputation.controlplane.client.v1alpha

import com.rabbitmq.client.ConnectionFactory
import java.io.File
import java.io.IOException
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection

/**
 * A handler class responsible for managing RabbitMQ connections and subscriptions.
 *
 * This class provides methods to:
 * - Subscribe to a RabbitMQ queue and process incoming messages by launching an external process
 *   to run the TEE application.
 * - Unsubscribe from a RabbitMQ queue by safely closing the channel and connection.
 */
class RabbitMQHandler {

  private lateinit var connection: Connection
  private lateinit var channel: Channel

  init {
    try {

      Runtime.getRuntime().addShutdownHook(Thread {
        unsubscribeFromQueue()
      })

    } catch (e: IOException) {
      println("Error reading secret file: \${e.message}")
      throw e
    } catch (e: Exception) {
      println("An error occurred: \${e.message}")
      throw e
    }
  }

  /**
   * Subscribes to a RabbitMQ queue and processes incoming messages by dispatching the message body
   * to an external Kotlin application running in a separate process. After processing the message,
   * it sends an acknowledgment or negative acknowledgment back to RabbitMQ based on the result.
   *
   * @param rabbit_host The hostname of the RabbitMQ server.
   * @param rabbit_port The port of the RabbitMQ server.
   * @param rabbit_username The username for authenticating with RabbitMQ.
   * @param rabbit_password The password for authenticating with RabbitMQ.
   * @param rabbit_queue_name The name of the RabbitMQ queue to subscribe to.
   *
   * The function:
   * - Connects to the RabbitMQ server using the provided credentials and queue details.
   * - Consumes messages from the queue and processes them in a separate process.
   * - Writes the message body to the external process's input stream.
   * - Waits for the external process to complete.
   * - Acknowledges the message if the process completes successfully, or negatively acknowledges it if the process fails.
   */
  fun subscribeToQueue(rabbit_host: String, rabbit_port: Int, rabbit_username: String, rabbit_password: String, rabbit_queue_name: String) {
    val factory = ConnectionFactory()
    factory.host = rabbit_host
    factory.port = rabbit_port
    factory.username = rabbit_username
    factory.password = rabbit_password
    connection = factory.newConnection()
    channel = connection.createChannel()
    channel.basicConsume(rabbit_queue_name, false, { _: String, delivery: Delivery ->
      val body = delivery.body
      val process = ProcessBuilder("java", "-jar", "/app/tee_app.jar").start()

      process.outputStream.use { outputStream ->
        outputStream.write(body)
        outputStream.flush()
      }

      process.inputStream.bufferedReader().use { it.readText() }
      process.errorStream.bufferedReader().use { it.readText() }
      val exitCode = process.waitFor()
      if (exitCode == 0) {
        channel.basicAck(delivery.envelope.deliveryTag, false)
      } else {
        channel.basicNack(delivery.envelope.deliveryTag, false, true)
      }
    }, { _ -> })
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
  fun unsubscribeFromQueue() {
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
}

fun readSecretFromEnv(variableName: String): String {
  return System.getenv(variableName) ?: throw IllegalArgumentException("Environment variable $variableName not found")
}

fun main() {
  val rabbitMQHandler = RabbitMQHandler()
  val rabbitHost = readSecretFromEnv("RABBIT_HOST")
  val rabbitPort = (readSecretFromEnv("RABBIT_PORT")).toInt()
  val rabbitUsername = readSecretFromEnv("RABBIT_USERNAME")
  val rabbitPassword = readSecretFromEnv("RABBIT_PASSWORD")
  val rabbitQueueName = readSecretFromEnv("RABBIT_QUEUE_NAME")
  rabbitMQHandler.subscribeToQueue(rabbitHost, rabbitPort, rabbitUsername, rabbitPassword, rabbitQueueName)
}
