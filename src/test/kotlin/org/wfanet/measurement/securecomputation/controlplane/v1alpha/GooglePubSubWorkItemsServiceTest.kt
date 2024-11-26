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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.StringValue
import com.rabbitmq.client.AMQP.Queue.DeclareOk
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert.assertThrows
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class GooglePubSubWorkItemsServiceTest {

  private lateinit var connectionFactory: ConnectionFactory
  private lateinit var monitorChannel: Channel
  private lateinit var monitorConnection: Connection
  private val testQueue = "test-queue"
  private lateinit var workItemsService: GooglePubSubWorkItemsService

  @Before
  fun setup() {
    connectionFactory =
      ConnectionFactory().apply {
        host = "localhost"
        port = 5672
        username = "guest"
        password = "guest"
      }

    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        channel.queueDeclare(testQueue, true, false, false, null)
      }
    }

    monitorConnection = connectionFactory.newConnection()
    monitorChannel = monitorConnection.createChannel()
    workItemsService =
      GooglePubSubWorkItemsService(
        rabbitMqHost = "localhost",
        rabbitMqPort = 5672,
        rabbitMqUsername = "guest",
        rabbitMqPassword = "guest",
      )
  }

  @After
  fun cleanup() {
    try {
      monitorChannel.close()
      monitorConnection.close()
    } catch (e: Exception) {
      println("Failed to close monitor channel: ${e.message}")
    }

    connectionFactory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        try {
          channel.queueDelete(testQueue)
        } catch (e: Exception) {
          println("Failed to delete queue: ${e.message}")
        }
      }
    }

    workItemsService.close()
  }

  private fun getQueueInfo(): DeclareOk {
    if (!monitorChannel.isOpen) {
      monitorChannel = monitorConnection.createChannel()
    }
    return monitorChannel.queueDeclarePassive(testQueue)
  }

  @Test
  fun `test successful work item creation`() = runBlocking {
    val workItemParams = Any.pack(StringValue.of("test-params"))
    val request =
      CreateWorkItemRequest.newBuilder()
        .setWorkItemId("test-work-item-1")
        .setWorkItem(
          WorkItem.newBuilder()
            .setName("workItems/test-work-item-1")
            .setQueue(testQueue)
            .setWorkItemParams(workItemParams)
            .build()
        )
        .build()

    assertThat(getQueueInfo().messageCount).isEqualTo(0)
    val response = workItemsService.createWorkItem(request)
    assertThat(response.name).isEqualTo("workItems/test-work-item-1")
    assertThat(response.queue).isEqualTo(testQueue)
    assertThat(response.workItemParams).isEqualTo(workItemParams)
    delay(100)
    assertThat(getQueueInfo().messageCount).isEqualTo(1)
  }

  @Test
  fun `test non-existent queue throws PERMISSION_DENIED`() = runBlocking {
    val workItemParams = Any.pack(StringValue.of("test-params"))
    val request =
      CreateWorkItemRequest.newBuilder()
        .setWorkItemId("test-work-item-2")
        .setWorkItem(
          WorkItem.newBuilder()
            .setName("workItems/test-work-item-2")
            .setQueue("non-existent-queue")
            .setWorkItemParams(workItemParams)
            .build()
        )
        .build()

    val exception =
      assertThrows(StatusException::class.java) {
        runBlocking { workItemsService.createWorkItem(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).contains("Queue 'non-existent-queue' does not exist")

    val secondRequest = createTestRequest("test-work-item-4")
    runBlocking { workItemsService.createWorkItem(secondRequest) }
    delay(100)
    assertThat(getQueueInfo().messageCount).isEqualTo(1)
  }

  @Test
  fun `test missing queue name throws INVALID_ARGUMENT`() = runBlocking {
    val workItemParams = Any.pack(StringValue.of("test-params"))
    val request =
      CreateWorkItemRequest.newBuilder()
        .setWorkItemId("test-work-item-3")
        .setWorkItem(
          WorkItem.newBuilder()
            .setName("workItems/test-work-item-3")
            .setWorkItemParams(workItemParams)
            .build()
        )
        .build()

    val exception =
      assertThrows(StatusException::class.java) {
        runBlocking { workItemsService.createWorkItem(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).contains("Queue '' does not exist")
  }

  @Test
  fun `test check messages persistence after connection failure`() = runBlocking {
    assertThat(getQueueInfo().messageCount).isEqualTo(0)

    val request = createTestRequest("test-work-item-4")
    workItemsService.createWorkItem(request)
    delay(100)
    assertThat(getQueueInfo().messageCount).isEqualTo(1)
    workItemsService.close()

    workItemsService =
      GooglePubSubWorkItemsService(
        rabbitMqHost = "localhost",
        rabbitMqPort = 5672,
        rabbitMqUsername = "guest",
        rabbitMqPassword = "guest",
      )

    val request2 = createTestRequest("test-work-item-5")
    workItemsService.createWorkItem(request2)
    delay(100)
    assertThat(getQueueInfo().messageCount).isEqualTo(2)
  }

  @Test
  fun `test sending multiple messages in sequence`() = runBlocking {
    val numMessages = 1000
    assertThat(getQueueInfo().messageCount).isEqualTo(0)

    repeat(numMessages) { index ->
      val request = createTestRequest("test-work-item-multiple-$index")
      val response = workItemsService.createWorkItem(request)
      assertThat(response.name).isEqualTo("workItems/test-work-item-multiple-$index")
    }

    withTimeout(5000) {
      while (getQueueInfo().messageCount < numMessages) {
        delay(100)
      }
    }
    assertThat(getQueueInfo().messageCount).isEqualTo(numMessages)
  }

  private fun createTestRequest(workItemId: String): CreateWorkItemRequest {
    val workItemParams = Any.pack(StringValue.of("test-params"))
    return CreateWorkItemRequest.newBuilder()
      .setWorkItemId(workItemId)
      .setWorkItem(
        WorkItem.newBuilder()
          .setName("workItems/$workItemId")
          .setQueue(testQueue)
          .setWorkItemParams(workItemParams)
          .build()
      )
      .build()
  }
}
