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
import io.grpc.StatusException
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
import org.wfa.measurement.queue.TestWork
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
  fun setup() {
    googlePubSubClient = GooglePubSubEmulatorClient(
      host = pubSubEmulatorProvider.host,
      port = pubSubEmulatorProvider.port,
    )
    workItemsService = GooglePubSubWorkItemsService(projectId, googlePubSubClient)
    runBlocking {
      if (googlePubSubClient.topicExists(projectId, topicId)) {
        googlePubSubClient.deleteTopic(projectId, topicId)
      }
    }
  }

  @After
  fun clear() {
    runBlocking {
      if (googlePubSubClient.topicExists(projectId, topicId)) {
        googlePubSubClient.deleteTopic(projectId, topicId)
      }
    }
  }


  @Test
  fun `test successful work item creation`() = runBlocking {

    googlePubSubClient.createTopic(projectId, topicId)
    googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

    val workItemParams = createWorkItemParams()
    val request = createTestRequest(workItemId, workItemParams)

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
    }
  }

  @Test
  fun `test non-existent queue throws NOT_FOUND`() = runBlocking {
    val request = createTestRequest(workItemId, createWorkItemParams())

    val exception =
      assertThrows(StatusRuntimeException::class.java) {
        runBlocking { workItemsService.createWorkItem(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)

  }

  @Test
  fun `test sending multiple messages in sequence`() {
    runBlocking {

      googlePubSubClient.createTopic(projectId, topicId)
      googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)

      val numMessages = 1000
      val receivedMessages = Collections.synchronizedSet(mutableSetOf<String>())
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
        val request = createTestRequest("test-work-item-multiple-$index", createWorkItemParams())
        val response = workItemsService.createWorkItem(request)
        assertThat(response.name).isEqualTo("workItems/test-work-item-multiple-$index")
      }

      subscriber.stopAsync().awaitTerminated()
    }
  }

  private fun createWorkItemParams(): Any {
    return Any.pack(TestWork.newBuilder().setUserName("test-user-name").setUserAge("25").setUserCountry("US").build())
  }
  private fun createTestRequest(workItemId: String, workItemParams: Any): CreateWorkItemRequest {
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

