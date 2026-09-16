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
import com.google.protobuf.Empty
import com.google.protobuf.Parser
import com.google.protobuf.timestamp
import com.google.rpc.ErrorInfo
import io.grpc.StatusException
import io.grpc.protobuf.StatusProto
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import java.time.Duration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfa.measurement.queue.testing.TestWork
import org.wfa.measurement.queue.testing.testWork
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
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
  controlPlaneThrottler: Throttler? = null,
  private val failure: Exception? = null,
  workItemConsumptionEnabled: Boolean = true,
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient,
    controlPlaneThrottler = controlPlaneThrottler,
    attemptUpdateRetryDelay = {},
    workItemConsumptionEnabled = workItemConsumptionEnabled,
  ) {
  val messageProcessed = CompletableDeferred<TestWork>()

  override suspend fun runWork(message: Any) {
    val testWork = message.unpack(TestWork::class.java)
    failure?.let { throw it }
    messageProcessed.complete(testWork)
  }
}

class BaseTeeApplicationTest {

  private lateinit var emulatorClient: GooglePubSubEmulatorClient
  private lateinit var openTelemetry: OpenTelemetrySdk
  private lateinit var spanExporter: InMemorySpanExporter

  private val workItemsServiceMock = mockService<WorkItemsCoroutineImplBase>()
  private val workItemAttemptsServiceMock = mockService<WorkItemAttemptsCoroutineImplBase>()

  @get:Rule
  val grpcTestServer = GrpcTestServerRule {
    addService(workItemsServiceMock)
    addService(workItemAttemptsServiceMock)
  }

  @Before
  fun setupPubSubResources() {
    GlobalOpenTelemetry.resetForTest()
    Instrumentation.resetForTest()
    spanExporter = InMemorySpanExporter.create()
    openTelemetry =
      OpenTelemetrySdk.builder()
        .setTracerProvider(
          SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()
        )
        .buildAndRegisterGlobal()
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
    openTelemetry.close()
  }

  private fun assertFailedProcessingSpan(
    workItemName: String,
    expectedErrorType: String,
    expectedErrorCode: String?,
  ) {
    val span =
      spanExporter.finishedSpanItems.single { it.name == "secure_computation.work_item.process" }
    assertThat(span.attributes.get(ReportTraceAttributes.LIFECYCLE_STAGE))
      .isEqualTo("work_item_processing")
    assertThat(span.attributes.get(ReportTraceAttributes.WORK_ITEM_NAME)).isEqualTo(workItemName)
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("failed")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_TYPE)).isEqualTo(expectedErrorType)
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_CODE)).isEqualTo(expectedErrorCode)
  }

  private fun assertFailureWritebackSpan(
    spanName: String,
    lifecycleStage: String,
    workItemName: String,
    workItemAttemptName: String,
    expectedErrorType: String,
    expectedErrorCode: String?,
  ) {
    val span = spanExporter.finishedSpanItems.single { it.name == spanName }
    assertThat(span.attributes.get(ReportTraceAttributes.LIFECYCLE_STAGE)).isEqualTo(lifecycleStage)
    assertThat(span.attributes.get(ReportTraceAttributes.WORK_ITEM_NAME)).isEqualTo(workItemName)
    assertThat(span.attributes.get(ReportTraceAttributes.WORK_ITEM_ATTEMPT_NAME))
      .isEqualTo(workItemAttemptName)
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("failed")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_TYPE)).isEqualTo(expectedErrorType)
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_CODE)).isEqualTo(expectedErrorCode)
  }

  @Test
  fun `nacks empty WorkItem name and traces validation failure`() = runBlocking {
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        WorkItemsCoroutineStub(grpcTestServer.channel),
        WorkItemAttemptsCoroutineStub(grpcTestServer.channel),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = workItem {},
        consumer = consumer,
        ackId = "empty-name-ack-id",
      )
    )
    consumer.disposition.await()
    job.cancelAndJoin()

    assertThat(consumer.ackCount).isEqualTo(0)
    assertThat(consumer.nackCount).isEqualTo(1)
    assertFailedProcessingSpan(
      workItemName = "",
      expectedErrorType = "IllegalArgumentException",
      expectedErrorCode = null,
    )
  }

  @Test
  fun `acks WorkItem not found and traces terminal failure`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw makeWorkItemNotFoundException() }
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
    val consumer = TestMessageConsumer()
    val workItem = createWorkItem(createTestWork())

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(body = workItem, consumer = consumer, ackId = "not-found-ack-id")
    )
    consumer.disposition.await()
    job.cancelAndJoin()

    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    assertFailedProcessingSpan(
      workItemName = workItem.name,
      expectedErrorType = "ControlPlaneApiException",
      expectedErrorCode = "grpc.NOT_FOUND",
    )
  }

  @Test
  fun `acks malformed WorkItem parameters after tracing parse failure`() = runBlocking {
    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemsServiceMock.stub {
      onBlocking { failWorkItem(any()) } doReturn workItem { name = "workItems/workItem" }
    }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        WorkItemsCoroutineStub(grpcTestServer.channel),
        WorkItemAttemptsCoroutineStub(grpcTestServer.channel),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()
    val workItem = workItem {
      name = "workItem"
      generation = 1L
      workItemParams = Any.pack(Empty.getDefaultInstance())
    }
    val canonicalWorkItemName = "workItems/workItem"

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = workItem,
        consumer = consumer,
        ackId = "malformed-params-ack-id",
      )
    )
    consumer.disposition.await()
    job.cancelAndJoin()

    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    assertFailedProcessingSpan(
      workItemName = canonicalWorkItemName,
      expectedErrorType = "InvalidProtocolBufferException",
      expectedErrorCode = null,
    )
  }

  @Test
  fun `nacks malformed WorkItem parameters when failure writeback fails`() = runBlocking {
    val workItemAttemptName = "workItems/workItem/workItemAttempts/workItemAttempt"
    val testWorkItemAttempt = workItemAttempt { name = workItemAttemptName }
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemsServiceMock.stub {
      onBlocking { failWorkItem(any()) } doThrow io.grpc.Status.UNAVAILABLE.asRuntimeException()
    }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        WorkItemsCoroutineStub(grpcTestServer.channel),
        WorkItemAttemptsCoroutineStub(grpcTestServer.channel),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()
    val workItem = workItem {
      name = "workItem"
      generation = 1L
      workItemParams = Any.pack(Empty.getDefaultInstance())
    }
    val canonicalWorkItemName = "workItems/workItem"

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = workItem,
        consumer = consumer,
        ackId = "malformed-params-writeback-failure-ack-id",
      )
    )
    consumer.disposition.await()
    job.cancelAndJoin()

    assertThat(consumer.ackCount).isEqualTo(0)
    assertThat(consumer.nackCount).isEqualTo(1)
    assertFailedProcessingSpan(
      workItemName = canonicalWorkItemName,
      expectedErrorType = "InvalidProtocolBufferException",
      expectedErrorCode = null,
    )
    assertFailureWritebackSpan(
      spanName = "secure_computation.work_item.failure_writeback",
      lifecycleStage = "work_item_failure_writeback",
      workItemName = canonicalWorkItemName,
      workItemAttemptName = workItemAttemptName,
      expectedErrorType = "ControlPlaneApiException",
      expectedErrorCode = "grpc.UNAVAILABLE",
    )
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
    val controlPlaneThrottler = RecordingThrottler()

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
        controlPlaneThrottler,
      )
    val job = launch { app.run() }

    val testWork = createTestWork()
    val workItem = createWorkItem(testWork, generation = 7L)

    publisher.publishMessage(TOPIC_ID, workItem)

    val processedMessage = app.messageProcessed.await()
    assertThat(processedMessage).isEqualTo(testWork)
    withTimeout(5_000) {
      while (controlPlaneThrottler.onReadyCalls < 2) {
        delay(10)
      }
    }
    assertThat(controlPlaneThrottler.onReadyCalls).isEqualTo(2)
    val createRequestCaptor = argumentCaptor<CreateWorkItemAttemptRequest>()
    verifyBlocking(workItemAttemptsServiceMock, times(1)) {
      createWorkItemAttempt(createRequestCaptor.capture())
    }
    assertThat(createRequestCaptor.firstValue.expectedWorkItemGeneration).isEqualTo(7L)
    assertThat(createRequestCaptor.firstValue.supportsAttemptLease).isTrue()
    job.cancelAndJoin()
    val processingSpan =
      spanExporter.finishedSpanItems.single { it.name == "secure_computation.work_item.process" }
    assertThat(processingSpan.attributes.get(ReportTraceAttributes.WORK_ITEM_ATTEMPT_NAME))
      .isEqualTo(testWorkItemAttempt.name)
  }

  @Test
  fun `processes legacy queue message as generation one`() = runBlocking {
    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { completeWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        WorkItemsCoroutineStub(grpcTestServer.channel),
        WorkItemAttemptsCoroutineStub(grpcTestServer.channel),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork(), generation = 0L),
        consumer = consumer,
        ackId = "legacy-ack-id",
      )
    )
    consumer.disposition.await()

    val requestCaptor = argumentCaptor<CreateWorkItemAttemptRequest>()
    verifyBlocking(workItemAttemptsServiceMock, times(1)) {
      createWorkItemAttempt(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.expectedWorkItemGeneration).isEqualTo(1L)
    assertThat(app.messageProcessed.isCompleted).isTrue()
    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    job.cancelAndJoin()
  }

  @Test
  fun `does not renew attempt returned without lease by older API`() = runBlocking {
    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { renewWorkItemAttempt(any()) } doThrow
        io.grpc.Status.UNIMPLEMENTED.asRuntimeException()
      onBlocking { completeWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      object :
        BaseTeeApplication(
          subscriptionId = SUBSCRIPTION_ID,
          queueSubscriber = fakeSubscriber,
          parser = WorkItem.parser(),
          workItemsStub = WorkItemsCoroutineStub(grpcTestServer.channel),
          workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServer.channel),
          attemptUpdateRetryDelay = {},
          attemptLeaseRenewalInterval = Duration.ofNanos(1),
        ) {
        override suspend fun runWork(message: Any) {
          repeat(10) { kotlinx.coroutines.yield() }
        }
      }
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "old-api-ack-id",
      )
    )
    consumer.disposition.await()

    verifyBlocking(workItemAttemptsServiceMock, times(0)) { renewWorkItemAttempt(any()) }
    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    job.cancelAndJoin()
  }

  @Test
  fun `renews active attempt lease while work is running`() = runBlocking {
    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
      leaseExpirationTime = timestamp { seconds = 1L }
    }
    val leaseRenewed = CompletableDeferred<Unit>()
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { renewWorkItemAttempt(any()) }
        .thenAnswer {
          leaseRenewed.complete(Unit)
          testWorkItemAttempt
        }
      onBlocking { completeWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    val fakeSubscriber = FakeQueueSubscriber()
    val workStarted = CompletableDeferred<Unit>()
    val releaseWork = CompletableDeferred<Unit>()
    val app =
      object :
        BaseTeeApplication(
          subscriptionId = SUBSCRIPTION_ID,
          queueSubscriber = fakeSubscriber,
          parser = WorkItem.parser(),
          workItemsStub = WorkItemsCoroutineStub(grpcTestServer.channel),
          workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServer.channel),
          attemptUpdateRetryDelay = {},
          attemptLeaseRenewalInterval = Duration.ofMillis(1),
        ) {
        override suspend fun runWork(message: Any) {
          workStarted.complete(Unit)
          releaseWork.await()
        }
      }
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "heartbeat-ack-id",
      )
    )
    workStarted.await()
    withTimeout(5_000) { leaseRenewed.await() }
    releaseWork.complete(Unit)
    consumer.disposition.await()

    verifyBlocking(workItemAttemptsServiceMock, atLeastOnce()) { renewWorkItemAttempt(any()) }
    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    job.cancelAndJoin()
  }

  @Test
  fun `acks message when WorkItem is already terminal`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()

    runBlocking {
      whenever(
          workItemAttemptsStub.createWorkItemAttempt(
            any<CreateWorkItemAttemptRequest>(),
            any<io.grpc.Metadata>(),
          )
        )
        .thenAnswer { throw makeCreateAttemptInvalidStateException(WorkItem.State.SUCCEEDED) }
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
    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("already_completed")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_TYPE)).isNull()
  }

  @Test
  fun `acks redelivery when WorkItem has an active attempt`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw makeCreateAttemptInvalidStateException(WorkItem.State.RUNNING) }

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
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "redelivery-ack-id",
      )
    )
    consumer.disposition.await()

    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    assertThat(app.messageProcessed.isCompleted).isFalse()
    job.cancelAndJoin()
    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("in_progress")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_TYPE)).isNull()
  }

  @Test
  fun `acks active-attempt redelivery after completion RPC failure`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
      .thenAnswer { throw makeCreateAttemptInvalidStateException(WorkItem.State.RUNNING) }
    whenever(
        workItemAttemptsStub.completeWorkItemAttempt(
          any<CompleteWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw StatusException(io.grpc.Status.UNAVAILABLE) }
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
    val workItem = createWorkItem(createTestWork())
    val firstDelivery = TestMessageConsumer()
    val redelivery = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = workItem,
        consumer = firstDelivery,
        ackId = "first-ack-id",
      )
    )
    firstDelivery.disposition.await()
    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = workItem,
        consumer = redelivery,
        ackId = "redelivery-ack-id",
      )
    )
    redelivery.disposition.await()

    assertThat(firstDelivery.ackCount).isEqualTo(0)
    assertThat(firstDelivery.nackCount).isEqualTo(1)
    assertThat(redelivery.ackCount).isEqualTo(1)
    assertThat(redelivery.nackCount).isEqualTo(0)
    verifyBlocking(workItemAttemptsStub, times(3)) {
      completeWorkItemAttempt(any(), any<io.grpc.Metadata>())
    }
    job.cancelAndJoin()
  }

  @Test
  fun `retries transient completion RPC failure`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
    whenever(
        workItemAttemptsStub.completeWorkItemAttempt(
          any<CompleteWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw StatusException(io.grpc.Status.UNAVAILABLE) }
      .thenReturn(workItemAttempt)
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
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()

    verifyBlocking(workItemAttemptsStub, times(2)) {
      completeWorkItemAttempt(any(), any<io.grpc.Metadata>())
    }
    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    job.cancelAndJoin()
  }

  @Test
  fun `acks when retry after lost completion response reports already succeeded`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
    whenever(
        workItemAttemptsStub.completeWorkItemAttempt(
          any<CompleteWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw StatusException(io.grpc.Status.UNAVAILABLE) }
      .thenAnswer { throw makeAttemptAlreadySucceededException(workItemAttempt.name) }
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
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()

    verifyBlocking(workItemAttemptsStub, times(2)) {
      completeWorkItemAttempt(any(), any<io.grpc.Metadata>())
    }
    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    job.cancelAndJoin()
  }

  @Test
  fun `does not retry non-transient completion RPC failure`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
    whenever(
        workItemAttemptsStub.completeWorkItemAttempt(
          any<CompleteWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw StatusException(io.grpc.Status.PERMISSION_DENIED) }
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
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()

    verifyBlocking(workItemAttemptsStub, times(1)) {
      completeWorkItemAttempt(any(), any<io.grpc.Metadata>())
    }
    assertThat(consumer.ackCount).isEqualTo(0)
    assertThat(consumer.nackCount).isEqualTo(1)
    job.cancelAndJoin()
    assertFailedProcessingSpan(
      workItemName = "workItems/workItem",
      expectedErrorType = "ControlPlaneApiException",
      expectedErrorCode = "grpc.PERMISSION_DENIED",
    )
  }

  @Test
  fun `acks message when completion reports attempt already succeeded`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
    whenever(
        workItemAttemptsStub.completeWorkItemAttempt(
          any<CompleteWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw makeAttemptAlreadySucceededException(workItemAttempt.name) }
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
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()

    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    job.cancelAndJoin()
  }

  @Test
  fun `acks active-attempt redelivery after failure RPC failure`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
      .thenAnswer { throw makeCreateAttemptInvalidStateException(WorkItem.State.RUNNING) }
    whenever(
        workItemAttemptsStub.failWorkItemAttempt(
          any<FailWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw StatusException(io.grpc.Status.UNAVAILABLE) }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        failure = IllegalStateException("worker failed"),
      )
    val job = launch { app.run() }
    val workItem = createWorkItem(createTestWork())
    val firstDelivery = TestMessageConsumer()
    val redelivery = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = workItem,
        consumer = firstDelivery,
        ackId = "first-ack-id",
      )
    )
    firstDelivery.disposition.await()
    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = workItem,
        consumer = redelivery,
        ackId = "redelivery-ack-id",
      )
    )
    redelivery.disposition.await()

    assertThat(firstDelivery.ackCount).isEqualTo(0)
    assertThat(firstDelivery.nackCount).isEqualTo(1)
    assertThat(redelivery.ackCount).isEqualTo(1)
    assertThat(redelivery.nackCount).isEqualTo(0)
    verifyBlocking(workItemAttemptsStub, times(3)) {
      failWorkItemAttempt(any(), any<io.grpc.Metadata>())
    }
    job.cancelAndJoin()
  }

  @Test
  fun `retries transient failure RPC failure`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
    whenever(
        workItemAttemptsStub.failWorkItemAttempt(
          any<FailWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw StatusException(io.grpc.Status.UNAVAILABLE) }
      .thenReturn(workItemAttempt)
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        failure = IllegalStateException("worker failed"),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()

    verifyBlocking(workItemAttemptsStub, times(2)) {
      failWorkItemAttempt(any(), any<io.grpc.Metadata>())
    }
    assertThat(consumer.ackCount).isEqualTo(0)
    assertThat(consumer.nackCount).isEqualTo(1)
    job.cancelAndJoin()
  }

  @Test
  fun `worker failure records failure writeback error separately`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttemptName = "workItems/workItem/workItemAttempts/workItemAttempt"
    val testWorkItemAttempt = workItemAttempt { name = workItemAttemptName }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(testWorkItemAttempt)
    whenever(
        workItemAttemptsStub.failWorkItemAttempt(
          any<FailWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw StatusException(io.grpc.Status.UNAVAILABLE) }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        failure = IllegalStateException("worker failed"),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()
    val workItem = createWorkItem(createTestWork())

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = workItem,
        consumer = consumer,
        ackId = "failure-writeback-ack-id",
      )
    )
    consumer.disposition.await()
    job.cancelAndJoin()

    assertThat(consumer.ackCount).isEqualTo(0)
    assertThat(consumer.nackCount).isEqualTo(1)
    assertFailedProcessingSpan(
      workItemName = workItem.name,
      expectedErrorType = "IllegalStateException",
      expectedErrorCode = null,
    )
    assertFailureWritebackSpan(
      spanName = "secure_computation.work_item_attempt.failure_writeback",
      lifecycleStage = "work_item_attempt_failure_writeback",
      workItemName = workItem.name,
      workItemAttemptName = workItemAttemptName,
      expectedErrorType = "ControlPlaneApiException",
      expectedErrorCode = "grpc.UNAVAILABLE",
    )
  }

  @Test
  fun `nacks leased worker failure when failure cannot be reported`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
      leaseExpirationTime = timestamp { seconds = 300L }
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
    whenever(
        workItemAttemptsStub.failWorkItemAttempt(
          any<FailWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw StatusException(io.grpc.Status.UNAVAILABLE) }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        failure = IllegalStateException("worker failed"),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()

    verifyBlocking(workItemAttemptsStub, times(3)) {
      failWorkItemAttempt(any(), any<io.grpc.Metadata>())
    }
    assertThat(consumer.ackCount).isEqualTo(0)
    assertThat(consumer.nackCount).isEqualTo(1)
    job.cancelAndJoin()
  }

  @Test
  fun `acks leased worker failure when retry confirms attempt already failed`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
      leaseExpirationTime = timestamp { seconds = 300L }
    }
    val failedAttempt = workItemAttempt {
      name = workItemAttempt.name
      state = WorkItemAttempt.State.FAILED
      leaseExpirationTime = workItemAttempt.leaseExpirationTime
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
    whenever(
        workItemAttemptsStub.failWorkItemAttempt(
          any<FailWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw StatusException(io.grpc.Status.UNAVAILABLE) }
      .thenReturn(failedAttempt)
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        failure = IllegalStateException("worker failed"),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()

    verifyBlocking(workItemAttemptsStub, times(2)) {
      failWorkItemAttempt(any(), any<io.grpc.Metadata>())
    }
    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    job.cancelAndJoin()
  }

  @Test
  fun `leased worker failure is durably reported without stale nack`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    val workItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
      leaseExpirationTime = timestamp { seconds = 300L }
    }
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
      .thenAnswer { throw makeCreateAttemptInvalidStateException(WorkItem.State.RUNNING) }
    whenever(
        workItemAttemptsStub.failWorkItemAttempt(
          any<FailWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenReturn(workItemAttempt)
    val originalSubscriber = FakeQueueSubscriber()
    val duplicateSubscriber = FakeQueueSubscriber()
    val workerStarted = CompletableDeferred<Unit>()
    val releaseWorker = CompletableDeferred<Unit>()
    val originalApp =
      object :
        BaseTeeApplication(
          subscriptionId = SUBSCRIPTION_ID,
          queueSubscriber = originalSubscriber,
          parser = WorkItem.parser(),
          workItemsStub = workItemsStub,
          workItemAttemptsStub = workItemAttemptsStub,
        ) {
        override suspend fun runWork(message: Any) {
          message.unpack(TestWork::class.java)
          workerStarted.complete(Unit)
          releaseWorker.await()
          error("worker failed")
        }
      }
    val duplicateApp =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = duplicateSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
      )
    val originalJob = launch { originalApp.run() }
    val duplicateJob = launch { duplicateApp.run() }
    val workItem = createWorkItem(createTestWork())
    val broker = ReplacingAckBroker(workItem)
    val originalDelivery = broker.deliver("original-ack-id")

    originalSubscriber.send(originalDelivery.message)
    workerStarted.await()
    val duplicateDelivery = broker.deliver("duplicate-ack-id")
    duplicateSubscriber.send(duplicateDelivery.message)
    duplicateDelivery.disposition.await()
    releaseWorker.complete(Unit)
    originalDelivery.disposition.await()

    assertThat(broker.acknowledged).isTrue()
    assertThat(duplicateDelivery.ackCount).isEqualTo(1)
    assertThat(duplicateDelivery.nackCount).isEqualTo(0)
    assertThat(originalDelivery.ackCount).isEqualTo(0)
    assertThat(originalDelivery.ackCallCount).isEqualTo(1)
    assertThat(originalDelivery.nackCallCount).isEqualTo(0)
    assertThat(originalDelivery.nackCount).isEqualTo(0)
    verifyBlocking(workItemAttemptsStub, times(1)) {
      failWorkItemAttempt(any(), any<io.grpc.Metadata>())
    }
    originalJob.cancelAndJoin()
    duplicateJob.cancelAndJoin()
  }

  @Test
  fun `disabled worker waits without subscribing`() = runBlocking {
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsClient = mock(),
        workItemAttemptsClient = mock(),
        workItemConsumptionEnabled = false,
      )

    val job = launch(start = CoroutineStart.UNDISPATCHED) { app.run() }

    assertThat(fakeSubscriber.subscribeCount).isEqualTo(0)
    job.cancelAndJoin()
  }

  @Test
  fun `acks stale delivery when createWorkItemAttempt reports generation mismatch`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
    whenever(
        workItemAttemptsStub.createWorkItemAttempt(
          any<CreateWorkItemAttemptRequest>(),
          any<io.grpc.Metadata>(),
        )
      )
      .thenAnswer { throw makeCreateAttemptGenerationMismatchException() }

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
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork(), generation = 1L),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()

    assertThat(consumer.ackCount).isEqualTo(1)
    assertThat(consumer.nackCount).isEqualTo(0)
    assertThat(app.messageProcessed.isCompleted).isFalse()
    job.cancelAndJoin()
    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("stale_delivery")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_TYPE)).isNull()
  }

  @Test
  fun `nacks message when createWorkItemAttempt returns retriable error`() = runBlocking {
    val workItemsStub = mock<WorkItemsCoroutineStub>()
    val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()

    runBlocking {
      whenever(
          workItemAttemptsStub.createWorkItemAttempt(
            any<CreateWorkItemAttemptRequest>(),
            any<io.grpc.Metadata>(),
          )
        )
        .thenAnswer {
          throw StatusException(io.grpc.Status.UNAVAILABLE.withDescription("control plane down"))
        }
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

    assertThat(consumer.nackCount).isEqualTo(1)
    assertThat(consumer.ackCount).isEqualTo(0)

    assertThat(app.messageProcessed.isCompleted).isFalse()
    job.cancelAndJoin()
    assertFailedProcessingSpan(
      workItemName = workItem.name,
      expectedErrorType = "ControlPlaneApiException",
      expectedErrorCode = "grpc.UNAVAILABLE",
    )
  }

  @Test
  fun `failure report includes exception type when exception has no message`() = runBlocking {
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServer.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServer.channel)
    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { failWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        failure = IllegalStateException(),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()

    val requestCaptor = argumentCaptor<FailWorkItemAttemptRequest>()
    verifyBlocking(workItemAttemptsServiceMock, times(1)) {
      failWorkItemAttempt(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.errorMessage).isEqualTo("java.lang.IllegalStateException")
    assertThat(consumer.nackCount).isEqualTo(1)
    job.cancelAndJoin()
  }

  @Test
  fun `worker cancellation does not fail attempt or message`() = runBlocking {
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServer.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServer.channel)
    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        failure = CancellationException("worker stopping"),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    job.join()

    verifyBlocking(workItemAttemptsServiceMock, times(0)) { failWorkItemAttempt(any()) }
    assertThat(consumer.ackCount).isEqualTo(0)
    assertThat(consumer.nackCount).isEqualTo(0)
    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("started")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_TYPE)).isNull()
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_CODE)).isNull()
  }

  @Test
  fun `worker failure records wrapped grpc status code`() = runBlocking {
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServer.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServer.channel)
    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
      onBlocking { failWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    val fakeSubscriber = FakeQueueSubscriber()
    val app =
      BaseTeeApplicationImpl(
        subscriptionId = SUBSCRIPTION_ID,
        queueSubscriber = fakeSubscriber,
        parser = WorkItem.parser(),
        workItemsStub,
        workItemAttemptsStub,
        failure =
          IllegalStateException(
            "wrapped control-plane failure",
            StatusException(io.grpc.Status.PERMISSION_DENIED),
          ),
      )
    val job = launch { app.run() }
    val consumer = TestMessageConsumer()

    fakeSubscriber.send(
      QueueSubscriber.QueueMessage(
        body = createWorkItem(createTestWork()),
        consumer = consumer,
        ackId = "some-ack-id",
      )
    )
    consumer.disposition.await()
    job.cancelAndJoin()

    val span = spanExporter.finishedSpanItems.single()
    assertThat(span.attributes.get(ReportTraceAttributes.OUTCOME)).isEqualTo("failed")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_TYPE))
      .isEqualTo("IllegalStateException")
    assertThat(span.attributes.get(ReportTraceAttributes.ERROR_CODE))
      .isEqualTo("grpc.PERMISSION_DENIED")
  }

  private class FakeQueueSubscriber : QueueSubscriber {
    private val ch = Channel<QueueSubscriber.QueueMessage<*>>(capacity = Channel.UNLIMITED)
    var subscribeCount = 0

    @Suppress("UNCHECKED_CAST")
    override fun <T : com.google.protobuf.Message> subscribe(
      subscriptionId: String,
      parser: com.google.protobuf.Parser<T>,
    ): kotlinx.coroutines.channels.ReceiveChannel<QueueSubscriber.QueueMessage<T>> {
      subscribeCount++
      return ch as Channel<QueueSubscriber.QueueMessage<T>>
    }

    suspend fun <T : com.google.protobuf.Message> send(msg: QueueSubscriber.QueueMessage<T>) {
      ch.send(msg)
    }

    override fun close() {
      ch.close()
    }
  }

  private class RecordingThrottler : Throttler {
    var onReadyCalls = 0

    override suspend fun <T> onReady(block: suspend () -> T): T {
      onReadyCalls++
      return block()
    }
  }

  private enum class Disposition {
    ACK,
    NACK,
  }

  private class TestMessageConsumer(
    private val acceptAck: () -> Boolean = { true },
    private val acceptNack: () -> Boolean = { true },
  ) : MessageConsumer {
    @Volatile var ackCallCount = 0
    @Volatile var ackCount = 0
    @Volatile var nackCount = 0
    @Volatile var nackCallCount = 0

    private val _disposition = CompletableDeferred<Disposition>()
    val disposition: Deferred<Disposition>
      get() = _disposition

    override fun ack() {
      ackCallCount++
      if (acceptAck()) {
        ackCount++
      }
      _disposition.complete(Disposition.ACK)
    }

    override fun nack() {
      nackCallCount++
      if (acceptNack()) {
        nackCount++
      }
      _disposition.complete(Disposition.NACK)
    }
  }

  /** Models Pub/Sub replacing the acknowledgement ID for one logical delivery. */
  private class ReplacingAckBroker(private val workItem: WorkItem) {
    private var currentAckId: String? = null
    var acknowledged = false
      private set

    fun deliver(ackId: String): Delivery {
      currentAckId = ackId
      val consumer =
        TestMessageConsumer(
          acceptAck = {
            val current = currentAckId == ackId
            if (current) acknowledged = true
            current
          },
          acceptNack = { currentAckId == ackId },
        )
      return Delivery(
        QueueSubscriber.QueueMessage(body = workItem, consumer = consumer, ackId = ackId),
        consumer,
      )
    }

    data class Delivery(
      val message: QueueSubscriber.QueueMessage<WorkItem>,
      val consumer: TestMessageConsumer,
    ) {
      val disposition: Deferred<Disposition>
        get() = consumer.disposition

      val ackCount: Int
        get() = consumer.ackCount

      val ackCallCount: Int
        get() = consumer.ackCallCount

      val nackCount: Int
        get() = consumer.nackCount

      val nackCallCount: Int
        get() = consumer.nackCallCount
    }
  }

  private fun makeCreateAttemptInvalidStateException(
    workItemState: WorkItem.State
  ): StatusException {
    val workItemName = "workItems/workItem"
    val errorInfo =
      ErrorInfo.newBuilder()
        .setReason(Errors.Reason.INVALID_WORK_ITEM_STATE.name)
        .putMetadata(Errors.Metadata.WORK_ITEM.key, workItemName)
        .putMetadata(Errors.Metadata.WORK_ITEM_STATE.key, workItemState.name)
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

  private fun makeWorkItemNotFoundException(): StatusException {
    val workItemName = "workItems/workItem"
    val errorInfo =
      ErrorInfo.newBuilder()
        .setReason(Errors.Reason.WORK_ITEM_NOT_FOUND.name)
        .putMetadata(Errors.Metadata.WORK_ITEM.key, workItemName)
        .build()
    val status =
      com.google.rpc.Status.newBuilder()
        .setCode(io.grpc.Status.Code.NOT_FOUND.value())
        .setMessage("WorkItem not found")
        .addDetails(Any.pack(errorInfo))
        .build()
    return StatusProto.toStatusException(status)
  }

  private fun makeCreateAttemptGenerationMismatchException(): StatusException {
    val errorInfo =
      ErrorInfo.newBuilder()
        .setReason(Errors.Reason.WORK_ITEM_GENERATION_MISMATCH.name)
        .putMetadata("workItem", "workItems/workItem")
        .putMetadata("expectedWorkItemGeneration", "1")
        .putMetadata("actualWorkItemGeneration", "2")
        .build()
    val status =
      com.google.rpc.Status.newBuilder()
        .setCode(io.grpc.Status.Code.FAILED_PRECONDITION.value())
        .setMessage("WorkItem generation does not match")
        .addDetails(Any.pack(errorInfo))
        .build()

    return StatusProto.toStatusException(status)
  }

  private fun makeAttemptAlreadySucceededException(workItemAttemptName: String): StatusException {
    val errorInfo =
      ErrorInfo.newBuilder()
        .setReason(Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE.name)
        .putMetadata(Errors.Metadata.WORK_ITEM_ATTEMPT.key, workItemAttemptName)
        .putMetadata(
          Errors.Metadata.WORK_ITEM_ATTEMPT_STATE.key,
          WorkItemAttempt.State.SUCCEEDED.name,
        )
        .build()
    val status =
      com.google.rpc.Status.newBuilder()
        .setCode(io.grpc.Status.Code.FAILED_PRECONDITION.value())
        .setMessage("WorkItemAttempt is already succeeded")
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

  private fun createWorkItem(testWork: TestWork, generation: Long = 1L): WorkItem {

    val packedWorkItemParams = Any.pack(testWork)
    return workItem {
      name = "workItems/workItem"
      this.generation = generation
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
