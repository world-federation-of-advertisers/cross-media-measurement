/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Message
import com.google.protobuf.Parser
import kotlin.test.assertFailsWith
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueuePublisher
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

@RunWith(JUnit4::class)
class DlqRedelivererTest {
  @Test
  fun `redeliver republishes each work item to its origin queue and acks it`() {
    val messages =
      listOf(
        workItem {
          name = "workItems/wi1"
          queue = "vid-rank-builder-queue"
        },
        workItem {
          name = "workItems/wi2"
          queue = "vid-labeler-queue"
        },
      )
    val subscriber = FakeQueueSubscriber(messages)
    val publisher = FakeQueuePublisher()

    val count = runBlocking {
      DlqRedeliverer(subscriber, publisher)
        .redeliver(DLQ_SUBSCRIPTION, maxMessages = 10, idleTimeoutMillis = IDLE_MILLIS)
    }

    assertThat(count).isEqualTo(2)
    assertThat(publisher.published)
      .containsExactly("vid-rank-builder-queue" to messages[0], "vid-labeler-queue" to messages[1])
    assertThat(subscriber.acked).containsExactly("workItems/wi1", "workItems/wi2")
  }

  @Test
  fun `redeliver stops after maxMessages`() {
    val messages =
      (1..3).map {
        workItem {
          name = "workItems/wi$it"
          queue = "vid-labeler-queue"
        }
      }
    val subscriber = FakeQueueSubscriber(messages)
    val publisher = FakeQueuePublisher()

    val count = runBlocking {
      DlqRedeliverer(subscriber, publisher)
        .redeliver(DLQ_SUBSCRIPTION, maxMessages = 2, idleTimeoutMillis = IDLE_MILLIS)
    }

    assertThat(count).isEqualTo(2)
    assertThat(publisher.published).hasSize(2)
    assertThat(subscriber.acked).hasSize(2)
  }

  @Test
  fun `redeliver with a queue filter only republishes matching messages`() {
    val messages =
      listOf(
        workItem {
          name = "workItems/wi1"
          queue = "vid-rank-builder-queue"
        },
        workItem {
          name = "workItems/wi2"
          queue = "vid-labeler-queue"
        },
        workItem {
          name = "workItems/wi3"
          queue = "vid-rank-builder-queue"
        },
      )
    val subscriber = FakeQueueSubscriber(messages)
    val publisher = FakeQueuePublisher()

    val count = runBlocking {
      DlqRedeliverer(subscriber, publisher)
        .redeliver(
          DLQ_SUBSCRIPTION,
          maxMessages = 10,
          idleTimeoutMillis = IDLE_MILLIS,
          queueFilter = "vid-rank-builder-queue",
        )
    }

    assertThat(count).isEqualTo(2)
    assertThat(publisher.published.map { it.second.name })
      .containsExactly("workItems/wi1", "workItems/wi3")
    assertThat(subscriber.acked).containsExactly("workItems/wi1", "workItems/wi3")
  }

  @Test
  fun `redeliver honors topicOverride`() {
    val messages =
      listOf(
        workItem {
          name = "workItems/wi1"
          queue = "vid-labeler-queue"
        }
      )
    val subscriber = FakeQueueSubscriber(messages)
    val publisher = FakeQueuePublisher()

    runBlocking {
      DlqRedeliverer(subscriber, publisher)
        .redeliver(
          DLQ_SUBSCRIPTION,
          maxMessages = 10,
          idleTimeoutMillis = IDLE_MILLIS,
          topicOverride = "override-queue",
        )
    }

    assertThat(publisher.published.single().first).isEqualTo("override-queue")
  }

  @Test
  fun `redeliver throws when a work item has no origin queue and no override`() {
    val subscriber = FakeQueueSubscriber(listOf(workItem { name = "workItems/wi1" }))
    val publisher = FakeQueuePublisher()

    assertFailsWith<IllegalArgumentException> {
      runBlocking {
        DlqRedeliverer(subscriber, publisher)
          .redeliver(DLQ_SUBSCRIPTION, maxMessages = 10, idleTimeoutMillis = IDLE_MILLIS)
      }
    }
  }

  private class FakeQueueSubscriber(private val messages: List<WorkItem>) : QueueSubscriber {
    val acked = mutableListOf<String>()

    override fun <T : Message> subscribe(
      subscriptionId: String,
      parser: Parser<T>,
    ): ReceiveChannel<QueueSubscriber.QueueMessage<T>> {
      val channel = Channel<QueueSubscriber.QueueMessage<T>>(Channel.UNLIMITED)
      for (message in messages) {
        val consumer =
          object : MessageConsumer {
            override fun ack() {
              acked.add(message.name)
            }

            override fun nack() {}
          }
        @Suppress("UNCHECKED_CAST")
        channel.trySend(QueueSubscriber.QueueMessage(message as T, message.name, consumer))
      }
      // Left open (not closed) so the redeliver loop ends via its idle timeout, mirroring the real
      // subscriber which pulls indefinitely.
      return channel
    }

    override fun close() {}
  }

  private class FakeQueuePublisher : QueuePublisher<WorkItem> {
    val published = mutableListOf<Pair<String, WorkItem>>()

    override suspend fun publishMessage(topicId: String, message: WorkItem) {
      published.add(topicId to message)
    }

    override fun close() {}
  }

  companion object {
    private const val DLQ_SUBSCRIPTION = "vid-labeler-queue-dlq-sub"
    private const val IDLE_MILLIS = 200L
  }
}
