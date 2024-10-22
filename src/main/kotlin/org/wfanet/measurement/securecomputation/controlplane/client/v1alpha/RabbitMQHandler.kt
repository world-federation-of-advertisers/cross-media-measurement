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
