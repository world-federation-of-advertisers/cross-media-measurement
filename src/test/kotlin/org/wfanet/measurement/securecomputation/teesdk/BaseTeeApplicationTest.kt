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
import com.google.protobuf.Any
import com.google.protobuf.Parser
import com.google.rpc.ErrorInfo
import io.grpc.StatusException
import io.grpc.protobuf.StatusProto
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.stub
import org.mockito.kotlin.whenever
import org.wfa.measurement.queue.testing.TestWork
import org.wfa.measurement.queue.testing.testWork
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt
import org.wfanet.measurement.securecomputation.service.Errors

class BaseTeeApplicationImpl(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsCoroutineStub,
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient,
  ) {
  val messageProcessed = CompletableDeferred<TestWork>()

  override suspend fun runWork(message: Any) {
    val testWork = message.unpack(TestWork::class.java)
    messageProcessed.complete(testWork)
  }
}

class BaseTeeApplicationTest {

  private lateinit var emulatorClient: GooglePubSubEmulatorClient

  private val workItemsServiceMock = mockService<WorkItemsCoroutineImplBase>()
  private val workItemAttemptsServiceMock = mockService<WorkItemAttemptsCoroutineImplBase>()

  @get:Rule
  val grpcTestServer = GrpcTestServerRule {
    addService(workItemsServiceMock)
    addService(workItemAttemptsServiceMock)
  }

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
    val pubSubClient =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = emulatorClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        ackDeadlineExtensionIntervalSeconds = 60,
        ackDeadlineExtensionSeconds = 600,
        blockingContext = Dispatchers.IO,
      )
    val publisher = Publisher<WorkItem>(projectId = PROJECT_ID, googlePubSubClient = emulatorClient)
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServer.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServer.channel)

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val testWorkItem = workItem { name = "workItems/workItem" }
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { completeWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { failWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemsServiceMock.stub { onBlocking { failWorkItem(any()) } doReturn testWorkItem }

    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = pubSubClient,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
      )
    val job = launch { app.run() }

    val testWork = createTestWork()
    val workItem = createWorkItem(testWork)

    publisher.publishMessage(TOPIC_ID, workItem)

    val processedMessage = app.messageProcessed.await()
    assertThat(processedMessage).isEqualTo(testWork)

    job.cancelAndJoin()
  }

  @Test
  fun `acks message when createWorkItemAttempt returns non-retriable error`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()

    runBlocking {
      whenever(
          workItemAttemptsStub.createWorkItemAttempt(
            any<CreateWorkItemAttemptRequest>(),
            any<io.grpc.Metadata>(),
          )
        )
        .thenAnswer { throw makeCreateAttemptInvalidStateException() }
    }

    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
      )

    val job = launch { app.run() }

    val testWork = createTestWork()
    val workItem = createWorkItem(testWork)
    val consumer = TestMessageConsumer()
    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(body = workItem, consumer = consumer, ackId = "some-ack-id")
    )

    consumer.disposition.await()

    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)

    assertThat(app.messageProcessed.isCompleted).isFalse()
    job.cancelAndJoin()
  }

  private class FakeQueueSubscriber : QueueSubscriber {
    private val ch = Channel<QueueSubscriber.QueueMessage<*>>(capacity = Channel.UNLIMITED)

    @Suppress("UNCHECKED_CAST")
    override fun <T : com.google.protobuf.Message> subscribe(
      subscriptionId: String,
      parser: com.google.protobuf.Parser<T>,
    ): kotlinx.coroutines.channels.ReceiveChannel<QueueSubscriber.QueueMessage<T>> {
      return ch as Channel<QueueSubscriber.QueueMessage<T>>
    }

    suspend fun <T : com.google.protobuf.Message> send(msg: QueueSubscriber.QueueMessage<T>) {
      ch.send(msg)
    }

    override fun close() {
      ch.close()
    }
  }

  private enum class Disposition {
    ACK,
    NACK,
  }

  private class TestMessageConsumer : MessageConsumer {
    @Volatile var ackCount = 0
    @Volatile var nackCount = 0

    private val _disposition = CompletableDeferred<Disposition>()
    val disposition: Deferred<Disposition>
      get() = _disposition

    override fun ack() {
      ackCount++
      _disposition.complete(Disposition.ACK)
    }

    override fun nack() {
      nackCount++
      _disposition.complete(Disposition.NACK)
    }
  }

  private fun makeCreateAttemptInvalidStateException(
    workItemName: String = "workItems/workItem",
    workItemState: String = "COMPLETED",
  ): StatusException {
    val errorInfo =
      ErrorInfo.newBuilder()
        .setReason(Errors.Reason.INVALID_WORK_ITEM_STATE.name)
        .putMetadata("work_item", workItemName)
        .putMetadata("work_item_state", workItemState)
        .build()

    val status =
      com.google.rpc.Status.newBuilder()
        .setCode(
          io.grpc.Status.Code.FAILED_PRECONDITION.value()
        ) // what you'd expect for invalid state
        .setMessage("WorkItem $workItemName is in an invalid state for this operation")
        .addDetails(Any.pack(errorInfo))
        .build()

    return StatusProto.toStatusException(status)
  }

  private fun createTestWork(): TestWork {
    return testWork {
      userName = "UserName"
      userAge = "25"
      userCountry = "US"
    }
  }

  private fun createWorkItem(testWork: TestWork): WorkItem {

    val packedWorkItemParams = Any.pack(testWork)
    return workItem {
      name = "workItems/workItem"
      workItemParams = packedWorkItemParams
    }
  }

  companion object {

    private const val PROJECT_ID = "test-project"
    private const val SUBSCRIPTION_ID = "test-subscription"
    private const val TOPIC_ID = "test-topic"

    @get:ClassRule @JvmStatic val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()
  }
}
