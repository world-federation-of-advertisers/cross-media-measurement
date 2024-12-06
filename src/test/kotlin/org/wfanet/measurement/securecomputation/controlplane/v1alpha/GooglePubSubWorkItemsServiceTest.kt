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
import org.junit.Assert.assertThrows
import com.google.protobuf.Any
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.util.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import kotlinx.coroutines.CompletableDeferred
import org.junit.After
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.junit.Rule
import org.threeten.bp.Duration
import org.wfa.measurement.queue.testing.TestWork
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient

@RunWith(JUnit4::class)
class GooglePubSubWorkItemsServiceTest {

  @Rule
  @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

  private val projectId = "test-project-id"
  private val topicId = "test-topid-id"
  private val workItemId = "test-work-item-1"
  private val subscriptionId = "test-subscription-id"
  private lateinit var workItemsService: GooglePubSubWorkItemsService
  private lateinit var googlePubSubClient: GooglePubSubEmulatorClient

  @Before
  fun createServices() {
    googlePubSubClient = GooglePubSubEmulatorClient(
      host = pubSubEmulatorProvider.host,
      port = pubSubEmulatorProvider.port,
    )
    workItemsService = GooglePubSubWorkItemsService(projectId, googlePubSubClient)
  }

  @Test
  fun `test successful work item creation`() = runBlocking {

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val workItemParams = createWorkItemParams()
    val request = createTestRequest(workItemId, workItemParams, topicId)

    val response = workItemsService.createWorkItem(request)
    assertThat(response.name).isEqualTo("workItems/$workItemId")
    assertThat(response.workItemParams).isEqualTo(workItemParams)

    val deferred = CompletableDeferred<String>()

    withTimeout(1000) {
      val subscriber = googlePubSubClient.buildSubscriber(
        projectId = projectId,
        subscriptionId = "test-subscription-id",
        ackExtensionPeriod = Duration.ofHours(6),
      ) { message, consumer ->
        try {

          val anyMessage = Any.parseFrom(message.data.toByteArray())
          val testWork = anyMessage.unpack(TestWork::class.java)
          deferred.complete(testWork.userName)
          consumer.ack()

        } catch (e: Exception) {
          consumer.nack()
        }
      }

      subscriber.startAsync().awaitRunning()
      val result = deferred.await()
      assertThat(result).isEqualTo("test-user-name")
      googlePubSubClient.deleteTopic(projectId, topicId)
    }
  }

  @Test
  fun `test non-existent queue throws NOT_FOUND`() = runBlocking {
    val request = createTestRequest(workItemId, createWorkItemParams(), topicId)

    val exception =
      assertThrows(StatusRuntimeException::class.java) {
        runBlocking { workItemsService.createWorkItem(request) }
      }
    assertThat(exception.message).contains("Topic test-topid-id does not exist")

  }

  @Test
  fun `test sending multiple messages in sequence`() {
    runBlocking {

      googlePubSubClient.createTopic(projectId, topicId)
      googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

      val numMessages = 1000
      val receivedMessages = Collections.synchronizedList(mutableListOf<String>())
      val allMessagesReceived = CompletableDeferred<Boolean>()
      val subscriber = googlePubSubClient.buildSubscriber(
        projectId = projectId,
        subscriptionId = subscriptionId,
        ackExtensionPeriod = Duration.ofHours(6),
      ) { message, consumer ->
        try {
          val anyMessage = Any.parseFrom(message.data.toByteArray())
          val testWork = anyMessage.unpack(TestWork::class.java)

          receivedMessages.add(testWork.userName)
          consumer.ack()
          if (receivedMessages.size == numMessages) {
            allMessagesReceived.complete(true)
          }
        } catch (e: Exception) {
          consumer.nack()
        }
      }

      subscriber.startAsync().awaitRunning()

      repeat(numMessages) { index ->
        val request = createTestRequest("test-work-item-multiple-$index", createWorkItemParams(), topicId)
        val response = workItemsService.createWorkItem(request)
        assertThat(response.name).isEqualTo("workItems/test-work-item-multiple-$index")
      }

      withTimeout(10_000) {
        allMessagesReceived.await()
      }

      subscriber.stopAsync().awaitTerminated()
      assertThat(receivedMessages.size).isEqualTo(numMessages)
      googlePubSubClient.deleteTopic(projectId, topicId)
    }
  }

  @Test
  fun `test sending messages to multiple subscribers`() {
    runBlocking {
      val firstTopic = "$topicId-1"
      val secondTopic = "$topicId-2"
      val firstSubscription = "$subscriptionId-1"
      val secondSubscription = "$subscriptionId-2"
      val topics = listOf(firstTopic, secondTopic)
      val subscriptions = listOf(firstSubscription, secondSubscription)

      topics.forEach { topic ->
        googlePubSubClient.createTopic(projectId, topic)
      }
      topics.zip(subscriptions).forEach { (topic, sub) ->
        googlePubSubClient.createSubscription(projectId, sub, topic)
      }

      val messagesPerTopic = 5
      val receivedMessages = Collections.synchronizedMap(mutableMapOf<String, MutableList<String>>())
      val allMessagesReceived = CompletableDeferred<Boolean>()

      receivedMessages[firstSubscription] = Collections.synchronizedList(mutableListOf())
      receivedMessages[secondSubscription] = Collections.synchronizedList(mutableListOf())

      val subscribers = subscriptions.map { sub ->
        googlePubSubClient.buildSubscriber(
          projectId = projectId,
          subscriptionId = sub,
          ackExtensionPeriod = Duration.ofHours(6)
        ) { message, consumer ->
          try {
            val anyMessage = Any.parseFrom(message.data.toByteArray())
            val testWork = anyMessage.unpack(TestWork::class.java)

            receivedMessages.get(sub)!!.add(testWork.userName)

            val totalSize = receivedMessages.values.sumOf { it.size }
            consumer.ack()
            if (totalSize == messagesPerTopic * topics.size) {
              allMessagesReceived.complete(true)
            }
          } catch (e: Exception) {
            consumer.nack()
          }
        }
      }

      subscribers.forEach { it.startAsync().awaitRunning() }

      listOf(firstTopic, secondTopic).forEach { topic ->
        repeat(messagesPerTopic) { index ->
          val request = createTestRequest("test-work-item-multiple-$index", createWorkItemParams(), topic)
          val response = workItemsService.createWorkItem(request)
          assertThat(response.name).isEqualTo("workItems/test-work-item-multiple-$index")
        }
      }

      withTimeout(10_000) {
        allMessagesReceived.await()
      }

      subscribers.forEach { it.stopAsync().awaitTerminated() }

      receivedMessages.values.forEach { messages ->
        assertThat(messages.size).isEqualTo(messagesPerTopic)
      }

      topics.forEach { topic ->
        googlePubSubClient.deleteTopic(projectId, topic)
      }

    }
  }

  private fun createWorkItemParams(): Any {
    return Any.pack(TestWork.newBuilder().setUserName("test-user-name").setUserAge("25").setUserCountry("US").build())
  }
  private fun createTestRequest(workItemId: String, workItemParams: Any, topicId: String): CreateWorkItemRequest {
    val workItem = WorkItem.newBuilder()
      .setName("workItems/$workItemId")
      .setQueue(topicId)
      .setWorkItemParams(workItemParams)
      .build()
    return CreateWorkItemRequest.newBuilder()
      .setWorkItemId(workItemId)
      .setWorkItem(workItem)
      .build()
  }
}

