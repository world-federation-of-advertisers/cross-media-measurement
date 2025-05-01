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

package org.wfanet.measurement.gcloud.pubsub

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.protobuf.ExtensionRegistryLite
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.trySendBlocking
import org.jetbrains.annotations.BlockingExecutor
import org.threeten.bp.Duration
import org.wfanet.measurement.queue.MessageConsumer
import org.wfanet.measurement.queue.QueueSubscriber
import com.google.pubsub.v1.PubsubMessage
import com.google.protobuf.TextFormat
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.io.Reader
import java.nio.charset.StandardCharsets
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.unpack

/**
 * A Google Pub/Sub client that subscribes to a Pub/Sub subscription and provides messages in a
 * coroutine-based channel.
 *
 * @param projectId The Google Cloud project ID.
 * @param googlePubSubClient The client interface for interacting with the Google Pub/Sub service.
 * @param context The coroutine context used for producing the channel.
 */
class SubscriberDelete(
  private val projectId: String,
  private val googlePubSubClient: GooglePubSubClient = DefaultGooglePubSubClient(),
  private val supportedMessages: List<Message>,
  private val supportedMessage: Message,
  blockingContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) : QueueSubscriber {

  private val scope = CoroutineScope(blockingContext)

  @OptIn(ExperimentalCoroutinesApi::class) // For `produce`.
  override fun <T : Message> subscribe(
    subscriptionId: String,
    parser: Parser<T>,
  ): ReceiveChannel<QueueSubscriber.QueueMessage<T>> =
    scope.produce(capacity = Channel.RENDEZVOUS) {
      val subscriber =
        googlePubSubClient.buildSubscriber(
          projectId = projectId,
          subscriptionId = subscriptionId,
          ackExtensionPeriod = Duration.ofHours(6),
        ) { message: PubsubMessage, consumer ->
          val typeRegistry =
            TypeRegistry.newBuilder()
              .add(supportedMessages.flatMap { it.getDescriptorForType().getFile().messageTypes })
              .build()
          val registry = ExtensionRegistryLite.newInstance()
          //val builder = supportedMessage.newBuilderForType()
          /*val parsedMessage = builder.apply {
            val inputStream = InputStreamReader(ByteArrayInputStream(message.data.toByteArray()), StandardCharsets.UTF_8)
            TextFormat.Parser.newBuilder().setTypeRegistry(typeRegistry).build().merge(inputStream, this)
          }.build() as T*/
          val data: ByteString = message.data
          //val parsedMessage = data.unpack(supportedMessage::class.java)
          val parsedMessage = parser.parseFrom(message.data.toByteArray())
          val queueMessage =
            QueueSubscriber.QueueMessage(
              body = parsedMessage,
              consumer = PubSubMessageConsumer(consumer),
            )

          val result = trySendBlocking(queueMessage)
          if (result.isClosed) {
            throw ClosedSendChannelException("Channel is closed. Cannot send message.")
          }
        }

      subscriber.startAsync().awaitRunning()
      logger.info("Subscriber started for subscription: $subscriptionId")

      awaitClose {
        subscriber.stopAsync()
        subscriber.awaitTerminated()
      }
    }

  override fun close() {
    scope.cancel()
  }

  private class PubSubMessageConsumer(private val consumer: AckReplyConsumer) : MessageConsumer {
    override fun ack() {
      consumer.ack()
    }

    override fun nack() {
      consumer.nack()
    }
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}
