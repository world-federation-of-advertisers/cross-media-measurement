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

package org.wfanet.measurement.securecomputation.teesdk

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Parser
import com.google.pubsub.v1.PubsubMessage
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfa.measurement.queue.TestWork
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.queue.QueueClient

class BaseTeeApplicationImpl(
  queueName: String,
  queueClient: QueueClient,
  parser: Parser<TestWork>,
) :
  BaseTeeApplication<TestWork>(queueName = queueName, queueClient = queueClient, parser = parser) {
  val processedMessages: MutableList<TestWork> = mutableListOf()
  val messageProcessed = CompletableDeferred<Unit>()

  override suspend fun runWork(message: TestWork) {
    processedMessages.add(message)
    messageProcessed.complete(Unit)
  }
}

class BaseTeeApplicationTest {

  @Test
  fun `test processing protobuf message`() = runBlocking {
    val projectId = "test-project"
    val subscriptionId = "test-subscription"
    val topicId = "test-topic"

    val emulatorClient = GooglePubSubEmulatorClient()
    emulatorClient.startEmulator()

    val topicName = emulatorClient.createTopic(projectId, topicId)
    emulatorClient.createSubscription(projectId, subscriptionId, topicName)
    val subscriberStub = emulatorClient.createSubscriberStub()
    val pubSubClient = GooglePubSubClient(projectId = projectId, subscriberStub = subscriberStub)
    val app =
      BaseTeeApplicationImpl(
        queueName = subscriptionId,
        queueClient = pubSubClient,
        parser = TestWork.parser(),
      )
    val job = launch { app.run() }

    val publisher = emulatorClient.createPublisher(projectId, topicId)
    val message = "UserName1"
    val testWork = createTestWork(message)
    val pubsubMessage: PubsubMessage =
      PubsubMessage.newBuilder().setData(testWork.toByteString()).build()
    publisher.publish(pubsubMessage)

    app.messageProcessed.await()
    assertThat(app.processedMessages.contains(testWork)).isTrue()

    job.cancelAndJoin()
  }

  private fun createTestWork(message: String): TestWork {
    return TestWork.newBuilder().setUserName(message).setUserAge("25").setUserCountry("US").build()
  }
}
