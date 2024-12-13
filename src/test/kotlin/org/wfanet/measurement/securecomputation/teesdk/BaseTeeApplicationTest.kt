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
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.wfa.measurement.queue.testing.TestWork
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.queue.QueueSubscriber

class BaseTeeApplicationImpl(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<TestWork>,
) :
  BaseTeeApplication<TestWork>(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
  ) {
  val messageProcessed = CompletableDeferred<TestWork>()

  override suspend fun runWork(message: TestWork) {
    messageProcessed.complete(message)
  }
}

class BaseTeeApplicationTest {

  private lateinit var emulatorClient: GooglePubSubEmulatorClient

  @Before
  fun setupPubSubResources() {
    runBlocking {
      emulatorClient =
        GooglePubSubEmulatorClient(
          host = pubSubEmulatorProvider.host,
          port = pubSubEmulatorProvider.port,
        )
      emulatorClient.createTopic(PROJECT_ID, TOPIC_ID)
      emulatorClient.createSubscription(PROJECT_ID, SUBSCRIPTION_ID, TOPIC_ID)
    }
  }

  @After
  fun cleanPubSubResources() {
    runBlocking {
      emulatorClient.deleteTopic(PROJECT_ID, TOPIC_ID)
      emulatorClient.deleteSubscription(PROJECT_ID, SUBSCRIPTION_ID)
    }
  }

  @Test
  fun `test processing protobuf message`() = runBlocking {
    val pubSubClient = Subscriber(projectId = PROJECT_ID, googlePubSubClient = emulatorClient)
    val publisher = Publisher<TestWork>(PROJECT_ID, emulatorClient)
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = pubSubClient,
        parser = TestWork.parser(),
      )
    val job = launch { app.run() }

    val message = "UserName1"
    val testWork = createTestWork(message)

    publisher.publishMessage(TOPIC_ID, testWork)

    val processedMessage = app.messageProcessed.await()
    assertThat(processedMessage).isEqualTo(testWork)

    job.cancelAndJoin()
  }

  private fun createTestWork(message: String): TestWork {
    return TestWork.newBuilder().setUserName(message).setUserAge("25").setUserCountry("US").build()
  }

  companion object {

    private const val PROJECT_ID = "test-project"
    private const val SUBSCRIPTION_ID = "test-subscription"
    private const val TOPIC_ID = "test-topic"

    @get:ClassRule @JvmStatic val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()
  }
}
