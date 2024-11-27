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
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.wfa.measurement.queue.TestWork
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.queue.QueueSubscriber
import org.junit.Test
import org.junit.After
import org.junit.Before
import org.junit.Rule

class BaseTeeApplicationImpl(
  queueName: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<TestWork>,
) :
  BaseTeeApplication<TestWork>(
    queueName = queueName,
    queueSubscriber = queueSubscriber,
    parser = parser,
  ) {
  val processedMessages: MutableList<TestWork> = mutableListOf()
  val messageProcessed = CompletableDeferred<Unit>()

  override suspend fun runWork(message: TestWork) {
    processedMessages.add(message)
    messageProcessed.complete(Unit)
  }
}

class BaseTeeApplicationTest {

  @Rule
  @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

  private val projectId = "test-project"
  private val subscriptionId = "test-subscription"
  private val topicId = "test-topic"

  private lateinit var emulatorClient: GooglePubSubEmulatorClient

  @Before
  fun setup() {
    runBlocking {
      emulatorClient = GooglePubSubEmulatorClient(
        host = pubSubEmulatorProvider.host,
        port = pubSubEmulatorProvider.port
      )
      emulatorClient.createTopic(projectId, topicId)
      emulatorClient.createSubscription(projectId, subscriptionId, topicId)
    }
  }

  @After
  fun tearDown() {
    runBlocking {
      emulatorClient.deleteTopic(projectId, topicId)
      emulatorClient.deleteSubscription(projectId, subscriptionId)
    }
  }

  @Test
  fun `test processing protobuf message`() = runBlocking {
    val pubSubClient = Subscriber(projectId = projectId, googlePubSubClient = emulatorClient)
    val app =
      BaseTeeApplicationImpl(
        queueName = subscriptionId,
        queueSubscriber = pubSubClient,
        parser = TestWork.parser(),
      )
    val job = launch { app.run() }

    val message = "UserName1"
    val testWork = createTestWork(message)

    emulatorClient.publishMessage(projectId, topicId, testWork)

    app.messageProcessed.await()
    assertThat(app.processedMessages.contains(testWork)).isTrue()

    job.cancelAndJoin()
  }

  private fun createTestWork(message: String): TestWork {
    return TestWork.newBuilder().setUserName(message).setUserAge("25").setUserCountry("US").build()
  }
}
