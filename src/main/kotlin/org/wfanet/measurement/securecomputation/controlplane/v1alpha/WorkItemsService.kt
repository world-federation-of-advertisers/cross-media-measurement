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

package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase


class WorkItemsService(
  private val rabbitMqHost: String,
  private val rabbitMqPort: Int,
  private val rabbitMqUsername: String,
  private val rabbitMqPassword: String
) : WorkItemsCoroutineImplBase(), AutoCloseable {

  private val connectionFactory = ConnectionFactory().apply {
    host = rabbitMqHost
    port = rabbitMqPort
    username = rabbitMqUsername
    password = rabbitMqPassword
  }

  private lateinit var connection: Connection
  private lateinit var channel: Channel

  init {
    setupRabbitMqConnection()
  }

  private fun setupRabbitMqConnection() {
    try {
      connection = connectionFactory.newConnection()
      channel = connection.createChannel()
    } catch (e: Exception) {
      throw StatusException(
        Status.UNAVAILABLE
          .withDescription("Failed to connect to RabbitMQ: ${e.message}")
      )
    }
  }

  override suspend fun createWorkItem(request: CreateWorkItemRequest): WorkItem {

    val workItem = request.workItem
    val queueName = workItem.queue
    val workItemParams = workItem.workItemParams

    grpcRequireNotNull(queueName) {
      "Queue name is unspecified"
    }

    try {
      channel.queueDeclarePassive(queueName)
    } catch (e: Exception) {
      throw StatusException(
        Status.PERMISSION_DENIED
          .withDescription("Queue '$queueName' does not exist")
      )
    }

    try {
      // Makes the message persistent.
      val props = AMQP.BasicProperties.Builder()
        .deliveryMode(2)
        .build()

      channel.basicPublish(
        "",
        queueName,
        props,
        workItemParams.toByteArray()
      )
    } catch (e: Exception) {
      throw StatusException(
        Status.INTERNAL
          .withDescription("Failed to enqueue work item: ${e.message}")
      )
    }

    return workItem
  }

  override fun close() {
    channel.close()
    connection.close()
  }

}
