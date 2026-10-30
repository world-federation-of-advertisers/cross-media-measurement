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

package org.wfanet.measurement.securecomputation.deploy.gcloud.publisher

import com.google.cloud.pubsub.v1.Publisher as GooglePublisher
import com.google.protobuf.Message
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.TopicName
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CancellationException
import org.wfanet.measurement.gcloud.common.await
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.PublishFailedException
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher

class GoogleWorkItemPublisher(
  private val projectId: String,
  googlePubSubClient: GooglePubSubClient,
  private val orderedPublisherFactory: (String, String) -> GooglePublisher = { projectId, topicId ->
    buildOrderedPublisher(projectId, topicId)
  },
) : WorkItemPublisher {

  private val publisher: Publisher<Message> =
    Publisher(projectId = projectId, googlePubSubClient = googlePubSubClient)
  private val orderedPublishers = ConcurrentHashMap<String, GooglePublisher>()

  override suspend fun publishMessage(queueName: String, message: Message) {
    publisher.publishMessage(queueName, message)
  }

  override suspend fun publishMessage(queueName: String, message: Message, orderingKey: String) {
    if (orderingKey.isEmpty()) {
      publishMessage(queueName, message)
      return
    }

    val orderedPublisher =
      orderedPublishers.computeIfAbsent(queueName) { orderedPublisherFactory(projectId, queueName) }
    val pubsubMessage =
      PubsubMessage.newBuilder().setData(message.toByteString()).setOrderingKey(orderingKey).build()
    try {
      orderedPublisher.publish(pubsubMessage).await()
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      throw PublishFailedException(queueName, e)
    }
  }

  companion object {
    private fun buildOrderedPublisher(projectId: String, topicId: String): GooglePublisher =
      GooglePublisher.newBuilder(TopicName.of(projectId, topicId))
        .setEnableMessageOrdering(true)
        .build()
  }
}
