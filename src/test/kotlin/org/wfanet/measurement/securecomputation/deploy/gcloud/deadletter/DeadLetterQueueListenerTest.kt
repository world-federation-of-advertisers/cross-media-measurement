/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter

import com.google.protobuf.Any
import com.google.protobuf.Parser
import com.google.rpc.ErrorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.protobuf.StatusProto
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.*
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.internal.securecomputation.controlplane.ProcessWorkItemDeadLetterRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem as InternalWorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem as internalWorkItem
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.service.Errors
import org.wfanet.measurement.securecomputation.service.internal.WorkItemGenerationMismatchException

class DeadLetterQueueListenerTest {
  private val staleGenerationWorkItemsService =
    object : WorkItemsGrpcKt.WorkItemsCoroutineImplBase() {
      override suspend fun processWorkItemDeadLetter(
        request: ProcessWorkItemDeadLetterRequest
      ): InternalWorkItem {
        throw WorkItemGenerationMismatchException("work-item", 1L, 2L)
          .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }
    }

  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(staleGenerationWorkItemsService) }

  @Test
  fun `run subscribes and terminates when channel closes`(): Unit = runBlocking {
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()
    val subscribed = CompletableDeferred<Unit>()
    val queueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(eq(SUBSCRIPTION_ID), any<Parser<WorkItem>>()) } doAnswer
          {
            subscribed.complete(Unit)
            messageChannel
          }
      }
    val listener = listener(queueSubscriber)
    val job = launch { listener.run() }

    withTimeout(1_000) { subscribed.await() }
    messageChannel.close()
    withTimeout(5_000) { job.join() }

    verify(queueSubscriber).subscribe(eq(SUBSCRIPTION_ID), any<Parser<WorkItem>>())
  }

  @Test
  fun `run propagates subscription errors`(): Unit = runBlocking {
    val expected = RuntimeException("Subscription error")
    val queueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(eq(SUBSCRIPTION_ID), eq(WorkItem.parser())) } doThrow expected
      }
    val caught = CompletableDeferred<Throwable>()

    val job = launch {
      try {
        listener(queueSubscriber).run()
        fail("Expected exception")
      } catch (e: Exception) {
        caught.complete(e)
      }
    }

    assertEquals(expected, withTimeout(5_000) { caught.await() })
    job.cancel()
  }

  @Test
  fun `close closes subscriber`() {
    val queueSubscriber = mock<QueueSubscriber>()

    listener(queueSubscriber).close()

    verify(queueSubscriber).close()
  }

  @Test
  fun `delivery sends name and generation and acknowledges failed WorkItem`(): Unit = runBlocking {
    val item = workItem {
      name = WORK_ITEM_NAME
      generation = 7L
    }
    val requestCaptor = argumentCaptor<ProcessWorkItemDeadLetterRequest>()
    val fixture = fixture(item, terminalWorkItemsStub())

    fixture.channel.send(fixture.message)

    verify(fixture.workItemsStub, timeout(5_000))
      .processWorkItemDeadLetter(requestCaptor.capture(), any())
    assertEquals(WORK_ITEM_NAME, requestCaptor.firstValue.workItemResourceId)
    assertEquals(7L, requestCaptor.firstValue.expectedWorkItemGeneration)
    verify(fixture.message, timeout(5_000)).ack()
    fixture.close()
  }

  @Test
  fun `republished queued WorkItem is acknowledged`(): Unit = runBlocking {
    val stub =
      mock<WorkItemsGrpcKt.WorkItemsCoroutineStub> {
        onBlocking { processWorkItemDeadLetter(any(), any()) } doReturn
          internalWorkItem {
            state = InternalWorkItem.State.QUEUED
            generation = 2L
          }
      }
    val fixture = fixture(workItem { name = WORK_ITEM_NAME }, stub)

    fixture.channel.send(fixture.message)

    verify(fixture.message, timeout(5_000)).ack()
    verify(fixture.message, never()).nack()
    fixture.close()
  }

  @Test
  fun `empty WorkItem name is acknowledged without RPC`(): Unit = runBlocking {
    val stub = mock<WorkItemsGrpcKt.WorkItemsCoroutineStub>()
    val fixture = fixture(workItem {}, stub)

    fixture.channel.send(fixture.message)

    verify(fixture.message, timeout(5_000)).ack()
    verify(stub, never()).processWorkItemDeadLetter(any(), any())
    fixture.close()
  }

  @Test
  fun `not found WorkItem is acknowledged`(): Unit = runBlocking {
    val stub = throwingStub(Status.NOT_FOUND.asException())
    val fixture = fixture(workItem { name = WORK_ITEM_NAME }, stub)

    fixture.channel.send(fixture.message)

    verify(fixture.message, timeout(5_000)).ack()
    verify(fixture.message, never()).nack()
    fixture.close()
  }

  @Test
  fun `active leased attempt is deferred`(): Unit = runBlocking {
    val errorInfo =
      com.google.rpc.errorInfo {
        reason = Errors.Reason.INVALID_WORK_ITEM_STATE.name
        domain = Errors.DOMAIN
        metadata.put(Errors.Metadata.WORK_ITEM_STATE.key, WorkItem.State.RUNNING.name)
      }
    val fixture =
      fixture(
        workItem { name = WORK_ITEM_NAME },
        throwingStub(statusException(Status.FAILED_PRECONDITION, errorInfo)),
      )

    fixture.channel.send(fixture.message)

    verify(fixture.message, timeout(5_000)).nack()
    verify(fixture.message, never()).ack()
    fixture.close()
  }

  @Test
  fun `stale generation is acknowledged`(): Unit = runBlocking {
    val fixture =
      fixture(
        workItem {
          name = WORK_ITEM_NAME
          generation = 1L
        },
        WorkItemsGrpcKt.WorkItemsCoroutineStub(grpcTestServer.channel),
      )

    fixture.channel.send(fixture.message)

    verify(fixture.message, timeout(5_000)).ack()
    verify(fixture.message, never()).nack()
    fixture.close()
  }

  @Test
  fun `terminal WorkItem error is acknowledged`(): Unit = runBlocking {
    val errorInfo =
      com.google.rpc.errorInfo {
        reason = Errors.Reason.INVALID_WORK_ITEM_STATE.name
        domain = Errors.DOMAIN
        metadata.put(Errors.Metadata.WORK_ITEM_STATE.key, WorkItem.State.FAILED.name)
      }
    val exception = statusException(Status.FAILED_PRECONDITION, errorInfo)
    val fixture = fixture(workItem { name = WORK_ITEM_NAME }, throwingStub(exception))

    fixture.channel.send(fixture.message)

    verify(fixture.message, timeout(5_000)).ack()
    assertTrue(DeadLetterQueueListener.isTerminalWorkItemError(exception))
    fixture.close()
  }

  @Test
  fun `transient status is nacked`(): Unit = runBlocking {
    val fixture =
      fixture(workItem { name = WORK_ITEM_NAME }, throwingStub(Status.UNAVAILABLE.asException()))

    fixture.channel.send(fixture.message)

    verify(fixture.message, timeout(5_000)).nack()
    verify(fixture.message, never()).ack()
    fixture.close()
  }

  @Test
  fun `listener continues after processing error`(): Unit = runBlocking {
    val errorItem = workItem { name = "error-item" }
    val successItem = workItem { name = "success-item" }
    val errorMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn errorItem }
    val successProcessed = CompletableDeferred<Unit>()
    val successMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> {
        on { body } doReturn successItem
        on { ack() } doAnswer
          {
            successProcessed.complete(Unit)
            Unit
          }
      }
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()
    val queueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(SUBSCRIPTION_ID, WorkItem.parser()) } doReturn messageChannel
      }
    val stub =
      mock<WorkItemsGrpcKt.WorkItemsCoroutineStub> {
        onBlocking { processWorkItemDeadLetter(any(), any()) } doAnswer
          {
            val request = it.getArgument<ProcessWorkItemDeadLetterRequest>(0)
            if (request.workItemResourceId == "error-item") {
              throw RuntimeException("processing error")
            }
            terminalWorkItem()
          }
      }
    val job = launch { listener(queueSubscriber, stub).run() }

    messageChannel.send(errorMessage)
    verify(errorMessage, timeout(5_000)).nack()
    messageChannel.send(successMessage)
    withTimeout(5_000) { successProcessed.await() }
    verify(successMessage).ack()

    messageChannel.close()
    job.cancel()
  }

  private fun fixture(
    item: WorkItem,
    workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  ): Fixture {
    val message = mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn item }
    val channel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()
    val queueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(SUBSCRIPTION_ID, WorkItem.parser()) } doReturn channel
      }
    val job =
      kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.Dispatchers.Default).launch {
        listener(queueSubscriber, workItemsStub).run()
      }
    return Fixture(message, channel, workItemsStub, job)
  }

  private data class Fixture(
    val message: QueueSubscriber.QueueMessage<WorkItem>,
    val channel: Channel<QueueSubscriber.QueueMessage<WorkItem>>,
    val workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
    val job: kotlinx.coroutines.Job,
  ) {
    fun close() {
      channel.close()
      job.cancel()
    }
  }

  private fun listener(
    queueSubscriber: QueueSubscriber,
    workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub = mock(),
  ) =
    DeadLetterQueueListener(
      subscriptionId = SUBSCRIPTION_ID,
      queueSubscriber = queueSubscriber,
      parser = WorkItem.parser(),
      workItemsStub = workItemsStub,
    )

  private fun terminalWorkItemsStub(): WorkItemsGrpcKt.WorkItemsCoroutineStub = mock {
    onBlocking { processWorkItemDeadLetter(any(), any()) } doReturn terminalWorkItem()
  }

  private fun terminalWorkItem(): InternalWorkItem = internalWorkItem {
    state = InternalWorkItem.State.FAILED
  }

  private fun throwingStub(exception: Exception): WorkItemsGrpcKt.WorkItemsCoroutineStub = mock {
    onBlocking { processWorkItemDeadLetter(any(), any()) } doAnswer { throw exception }
  }

  private fun statusException(status: Status, errorInfo: ErrorInfo): StatusException {
    val statusProto =
      com.google.rpc.Status.newBuilder()
        .setCode(status.code.value())
        .setMessage(status.description.orEmpty())
        .addDetails(Any.pack(errorInfo))
        .build()
    return StatusProto.toStatusException(statusProto)
  }

  companion object {
    private const val SUBSCRIPTION_ID = "test-subscription"
    private const val WORK_ITEM_NAME = "workItems/test-work-item"
  }
}
