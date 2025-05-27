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

import com.google.protobuf.Parser
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.junit.Test
import org.mockito.kotlin.*
import org.wfanet.measurement.internal.securecomputation.controlplane.FailWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.service.Errors

class DeadLetterQueueListenerTest {

  @Test
  fun `run method verifies subscription`() = runBlocking {
    // Create mockWorkItemsSers
    val mockParser = WorkItem.parser()
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()
    val subscribeCalled = CompletableDeferred<Unit>()

    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(eq(subscriptionId), any<Parser<WorkItem>>()) } doAnswer
          {
            subscribeCalled.complete(Unit)
            messageChannel
          }
      }
    val channel =
      io.grpc.testing
        .GrpcCleanupRule()
        .register(
          io.grpc.inprocess.InProcessChannelBuilder.forName("test").directExecutor().build()
        )

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = mockParser,
        workItemsStub = WorkItemsGrpcKt.WorkItemsCoroutineStub(channel),
      )

    // Start the listener in a separate coroutine that we'll cancel shortly
    val job = launch { listener.run() }

    // Wait for subscribe to be called
    withTimeout(1000) { subscribeCalled.await() }

    // Verify that the subscribe method was called with the correct parameters
    verify(mockQueueSubscriber, times(1)).subscribe(eq(subscriptionId), any<Parser<WorkItem>>())

    // Clean up
    messageChannel.close()
    job.cancel()
  }

  @Test
  fun `run method terminates cleanly when channel is closed`() = runBlocking {
    // Create mocks
    val mockParser = WorkItem.parser()
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()
    val mockQueueSubscriber =
      mock<QueueSubscriber> { on { subscribe(subscriptionId, mockParser) } doReturn messageChannel }
    val mockWorkItemsStub = mock<WorkItemsGrpcKt.WorkItemsCoroutineStub>()

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = mockParser,
        workItemsStub = mockWorkItemsStub,
      )

    // Set up a completion flag to check if the method completes
    val completed = CompletableDeferred<Unit>()

    // Start the listener in a separate coroutine
    val job = launch {
      try {
        listener.run()
        // If run() returns without exception, mark as completed
        completed.complete(Unit)
      } catch (e: Exception) {
        // If an exception is thrown, fail the test
        completed.completeExceptionally(e)
      }
    }

    // Close the channel to simulate normal termination
    messageChannel.close()

    // Wait for the run method to complete, with a timeout
    withTimeout(5000) { completed.await() }

    // If we got here, the run method completed without exception
    assertTrue(completed.isCompleted)

    // Clean up
    job.cancel()
  }

  @Test
  fun `run method propagates subscription errors`() = runBlocking {
    // Create a mock QueueSubscriber that throws an exception when subscribe is called
    val expectedError = RuntimeException("Subscription error")
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(eq(subscriptionId), eq(WorkItem.parser())) } doThrow expectedError
      }
    val mockWorkItemsStub = mock<WorkItemsGrpcKt.WorkItemsCoroutineStub>()

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Set up a completion flag to catch the exception
    val exceptionCaught = CompletableDeferred<Throwable>()

    // Start the listener in a separate coroutine
    val job = launch {
      try {
        listener.run()
        fail("Expected exception was not thrown")
      } catch (e: Exception) {
        // Catch the exception and complete the deferred
        exceptionCaught.complete(e)
      }
    }

    // Wait for the exception to be caught, with a timeout
    val thrownException = withTimeout(5000) { exceptionCaught.await() }

    // Verify that the exception is of the expected type
    assertEquals(expectedError, thrownException)

    // Clean up
    job.cancel()
  }

  @Test
  fun `close method calls queueSubscriber close`() {
    // Create a mock QueueSubscriber
    val mockQueueSubscriber = mock<QueueSubscriber>()
    val mockWorkItemsStub = mock<WorkItemsGrpcKt.WorkItemsCoroutineStub>()

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Call close on the listener
    listener.close()

    // Verify that close was called on the QueueSubscriber
    verify(mockQueueSubscriber, times(1)).close()
  }

  @Test
  fun `listener continues processing after error`() = runBlocking {
    // Create two mock work items - one that will cause an error and one that will succeed
    val errorWorkItem = workItem { name = "error-item" }
    val successWorkItem = workItem { name = "success-item" }

    // Create mock QueueMessages
    val errorQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn errorWorkItem }
    val successQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn successWorkItem }

    // Create a channel to simulate subscription
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(subscriptionId, WorkItem.parser()) } doReturn messageChannel
      }

    // Create a mock WorkItemsStub that throws an exception for error item and succeeds for success
    // item
    val mockWorkItemsStub =
      mock<WorkItemsGrpcKt.WorkItemsCoroutineStub> {
        onBlocking {
          failWorkItem(argThat<FailWorkItemRequest> { workItemResourceId == "error-item" }, any())
        } doThrow RuntimeException("Simulated processing error")
      }

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Set up signal to track processing of both messages
    val errorProcessed = CompletableDeferred<Unit>()
    val successProcessed = CompletableDeferred<Unit>()

    // Capture when messages are nacked or acked
    whenever(errorQueueMessage.nack()).thenAnswer {
      errorProcessed.complete(Unit)
      Unit
    }

    whenever(successQueueMessage.ack()).thenAnswer {
      successProcessed.complete(Unit)
      Unit
    }

    // Start the listener
    val job = launch { listener.run() }

    // Send the error message
    messageChannel.send(errorQueueMessage)

    // Wait for the error message to be processed
    withTimeout(5000) { errorProcessed.await() }

    // Verify the error message was nacked
    verify(errorQueueMessage, times(1)).nack()
    verify(errorQueueMessage, never()).ack()

    // Send the success message
    messageChannel.send(successQueueMessage)

    // Wait for the success message to be processed
    withTimeout(5000) { successProcessed.await() }

    // Verify the success message was acked
    verify(successQueueMessage, times(1)).ack()
    verify(successQueueMessage, never()).nack()

    // Clean up
    messageChannel.close()
    job.cancel()
  }

  @Test
  fun `test work item with full resource name is processed correctly`() = runBlocking {
    // Create a mock work item with full resource name
    val workItem = workItem { name = workItemId }

    // Create a mock QueueMessage
    val mockQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn workItem }

    // Set up a channel to simulate subscription
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()

    // Create a mock QueueSubscriber
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(subscriptionId, WorkItem.parser()) } doReturn messageChannel
      }

    // Create a mock WorkItemsStub
    val mockWorkItemsStub = mock<WorkItemsGrpcKt.WorkItemsCoroutineStub>()

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Capture the arguments to the failWorkItem call
    val requestCaptor = argumentCaptor<FailWorkItemRequest>()

    // Start the listener
    val job = launch { listener.run() }

    // Send a message to the channel
    messageChannel.send(mockQueueMessage)

    // Wait for the message to be processed and verify that the work item resource ID was passed
    // correctly
    verify(mockWorkItemsStub, timeout(5000)).failWorkItem(requestCaptor.capture(), any())
    assertEquals(workItemId, requestCaptor.firstValue.workItemResourceId)

    // Clean up
    messageChannel.close()
    job.cancel()
  }

  @Test
  fun `test work item with ID only is processed correctly`() = runBlocking {
    // Create a mock work item with just the ID (not a full resource name)
    val workItemIdOnly = workItemId // Using workItemId constant = "test-work-item"
    val workItem = workItem { name = workItemIdOnly }

    // Create a mock QueueMessage
    val mockQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn workItem }

    // Set up a channel to simulate subscription
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()

    // Create a mock QueueSubscriber
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(subscriptionId, WorkItem.parser()) } doReturn messageChannel
      }

    // Create a mock WorkItemsStub
    val mockWorkItemsStub = mock<WorkItemsGrpcKt.WorkItemsCoroutineStub>()

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Capture the arguments to the failWorkItem call
    val requestCaptor = argumentCaptor<FailWorkItemRequest>()

    // Start the listener
    val job = launch { listener.run() }

    // Send a message to the channel
    messageChannel.send(mockQueueMessage)

    // Wait for the message to be processed and verify that the work item resource ID was passed
    // correctly
    verify(mockWorkItemsStub, timeout(5000)).failWorkItem(requestCaptor.capture(), any())
    assertEquals(workItemIdOnly, requestCaptor.firstValue.workItemResourceId)

    // Clean up
    messageChannel.close()
    job.cancel()
  }

  @Test
  fun `test processing message marks work item as failed`() = runBlocking {
    // Create a mock work item
    val workItem = workItem { name = workItemId }

    // Create a mock QueueMessage
    val mockQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn workItem }

    // Set up a channel to simulate subscription
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()

    // Create a mock QueueSubscriber
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(subscriptionId, WorkItem.parser()) } doReturn messageChannel
      }

    // Create a mock WorkItemsStub
    val mockWorkItemsStub = mock<WorkItemsGrpcKt.WorkItemsCoroutineStub>()

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Capture the arguments to the failWorkItem call
    val requestCaptor = argumentCaptor<FailWorkItemRequest>()

    // Start the listener
    val job = launch { listener.run() }

    // Send a message to the channel
    messageChannel.send(mockQueueMessage)

    // Wait for the message to be processed and verify that the work item resource ID was passed
    // correctly
    verify(mockWorkItemsStub, timeout(5000)).failWorkItem(requestCaptor.capture(), any())

    // Verify the message is acknowledged
    verify(mockQueueMessage, timeout(5000)).ack()

    // Verify that the work item resource ID was passed correctly
    assertEquals(workItemId, requestCaptor.firstValue.workItemResourceId)

    // Close the channel and cancel the job
    messageChannel.close()
    job.cancel()
  }

  @Test
  fun `test message with empty work item name is acknowledged`() = runBlocking {
    // Create a mock work item with empty name
    val workItem = workItem {}

    // Create a mock QueueMessage
    val mockQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn workItem }

    // Set up a channel to simulate subscription
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()

    // Create a mock QueueSubscriber
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(subscriptionId, WorkItem.parser()) } doReturn messageChannel
      }

    // Create a mock WorkItemsStub
    val mockWorkItemsStub = mock<WorkItemsGrpcKt.WorkItemsCoroutineStub>()

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Start the listener
    val job = launch { listener.run() }

    // Send a message to the channel
    messageChannel.send(mockQueueMessage)

    // Verify the message is acknowledged
    verify(mockQueueMessage, timeout(5000)).ack()

    // Verify that the failWorkItem method was not called
    verify(mockWorkItemsStub, never()).failWorkItem(any(), any())

    // Close the channel and cancel the job
    messageChannel.close()
    job.cancel()
  }

  @Test
  fun `test work item not found error is acknowledged`() = runBlocking {
    // Create a mock work item
    val workItem = workItem { name = workItemId }

    // Create a mock QueueMessage
    val mockQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn workItem }

    // Set up a channel to simulate subscription
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()

    // Create a mock QueueSubscriber
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(subscriptionId, WorkItem.parser()) } doReturn messageChannel
      }

    // Create a mock WorkItemsStub that throws a NOT_FOUND StatusRuntimeException
    val statusException =
      StatusRuntimeException(Status.NOT_FOUND.withDescription("Work item not found"))

    val mockWorkItemsStub =
      mock<WorkItemsGrpcKt.WorkItemsCoroutineStub> {
        onBlocking { failWorkItem(any<FailWorkItemRequest>(), any()) } doThrow statusException
      }

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Start the listener
    val job = launch { listener.run() }

    // Send a message to the channel
    messageChannel.send(mockQueueMessage)

    // Verify the message is acknowledged
    verify(mockQueueMessage, timeout(5000)).ack()

    // Close the channel and cancel the job
    messageChannel.close()
    job.cancel()
  }

  @Test
  fun `test already failed work item error is acknowledged`() = runBlocking {
    // Create a mock work item
    val workItem = workItem { name = workItemId }

    // Create a mock QueueMessage
    val mockQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn workItem }

    // Set up a channel to simulate subscription
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()

    // Create a mock QueueSubscriber
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(subscriptionId, WorkItem.parser()) } doReturn messageChannel
      }

    // Create a Status with errorInfo indicating the work item is already in FAILED state
    val errorInfoProto =
      com.google.rpc.errorInfo {
        reason = Errors.Reason.INVALID_WORK_ITEM_STATE.name
        domain = Errors.DOMAIN
        metadata.put(Errors.Metadata.WORK_ITEM_STATE.key, WorkItem.State.FAILED.name)
      }

    val statusException =
      org.wfanet.measurement.common.grpc.Errors.buildStatusRuntimeException(
        Status.FAILED_PRECONDITION.withDescription("Work item already failed"),
        errorInfoProto,
      )

    // Create a mock WorkItemsStub that throws the status exception
    val mockWorkItemsStub =
      mock<WorkItemsGrpcKt.WorkItemsCoroutineStub> {
        onBlocking { failWorkItem(any<FailWorkItemRequest>(), any()) } doThrow statusException
      }

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Start the listener
    val job = launch { listener.run() }

    // Send a message to the channel
    messageChannel.send(mockQueueMessage)

    // Verify the message is acknowledged
    verify(mockQueueMessage, timeout(5000)).ack()

    // Close the channel and cancel the job
    messageChannel.close()
    job.cancel()
  }

  @Test
  fun `test other status error is not acknowledged`() = runBlocking {
    // Create a mock work item
    val workItem = workItem { name = workItemId }

    // Create a mock QueueMessage
    val mockQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn workItem }

    // Set up a channel to simulate subscription
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()

    // Create a mock QueueSubscriber
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(subscriptionId, WorkItem.parser()) } doReturn messageChannel
      }

    // Create a Status with general error
    val statusException = StatusRuntimeException(Status.INTERNAL.withDescription("Internal error"))

    // Create a mock WorkItemsStub that throws the status exception
    val mockWorkItemsStub =
      mock<WorkItemsGrpcKt.WorkItemsCoroutineStub> {
        onBlocking { failWorkItem(any<FailWorkItemRequest>(), any()) } doThrow statusException
      }

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Start the listener
    val job = launch { listener.run() }

    // Send a message to the channel
    messageChannel.send(mockQueueMessage)

    // Verify the message is not acknowledged
    verify(mockQueueMessage, timeout(5000)).nack()
    verify(mockQueueMessage, never()).ack()

    // Close the channel and cancel the job
    messageChannel.close()
    job.cancel()
  }

  @Test
  fun `test general exception is not acknowledged`() = runBlocking {
    // Create a mock work item
    val workItem = workItem { name = workItemId }

    // Create a mock QueueMessage
    val mockQueueMessage =
      mock<QueueSubscriber.QueueMessage<WorkItem>> { on { body } doReturn workItem }

    // Set up a channel to simulate subscription
    val messageChannel = Channel<QueueSubscriber.QueueMessage<WorkItem>>()

    // Create a mock QueueSubscriber
    val mockQueueSubscriber =
      mock<QueueSubscriber> {
        on { subscribe(subscriptionId, WorkItem.parser()) } doReturn messageChannel
      }

    // Create a mock WorkItemsStub that throws a general exception
    val mockWorkItemsStub =
      mock<WorkItemsGrpcKt.WorkItemsCoroutineStub> {
        onBlocking { failWorkItem(any<FailWorkItemRequest>(), any()) } doThrow
          RuntimeException("Unexpected error")
      }

    // Create the listener
    val listener =
      DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = mockQueueSubscriber,
        parser = WorkItem.parser(),
        workItemsStub = mockWorkItemsStub,
      )

    // Start the listener
    val job = launch { listener.run() }

    // Send a message to the channel
    messageChannel.send(mockQueueMessage)

    // Verify the message is not acknowledged
    verify(mockQueueMessage, timeout(5000)).nack()
    verify(mockQueueMessage, never()).ack()

    // Close the channel and cancel the job
    messageChannel.close()
    job.cancel()
  }

  companion object {
    private val subscriptionId = "test-subscription"
    private val workItemId = "test-work-item"
  }
}
