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
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.rabbitmq.QueueClient
import org.wfanet.measurement.common.rabbitmq.testing.InMemoryQueueClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.testing.TestWork
import com.google.protobuf.Parser
import kotlinx.coroutines.launch

class BaseTeeApplicationImpl(
  queueClient: QueueClient,
  parser: Parser<TestWork>,
) :
  BaseTeeApplication<TestWork>(
    queueName = "test-queue",
    queueClient = queueClient,
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

  @Test
  fun `test processing protobuf message`() {

    runBlocking {
      val inMemoryQueueClient = InMemoryQueueClient(Dispatchers.IO)
      val app =
        BaseTeeApplicationImpl(
          queueClient = inMemoryQueueClient,
          parser = TestWork.parser(),
        )

      launch {
        app.run()
      }

      val testWork =
        TestWork.newBuilder()
          .setName("testWorks/123")
          .setUserName("Alice")
          .setUserAge("30")
          .setUserCountry("US")
          .build()

      launch {
        app.messageProcessed.await()
        assertThat(app.processedMessages.contains(testWork)).isTrue()
        inMemoryQueueClient.close()
        app.close()
      }

      launch {
        inMemoryQueueClient.sendMessage(testWork.toByteArray())
      }

    }

  }
}
