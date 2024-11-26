/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import com.google.protobuf.ByteString
//import com.rabbitmq.client.Channel
//import com.rabbitmq.client.Connection
//import com.rabbitmq.client.ConnectionFactory
import io.grpc.Status
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import com.google.pubsub.v1.PubsubMessage

class GooglePubSubWorkItemsService(
  private val googlePubSubClient: GooglePubSubClient = DefaultGooglePubSubClient()
//  private val rabbitMqHost: String,
//  private val rabbitMqPort: Int,
//  private val rabbitMqUsername: String,
//  private val rabbitMqPassword: String,
//  private val publisherProvider: ((projectId: String, topicId: String) -> Publisher)? = null
) : WorkItemsCoroutineImplBase(), WorkItemsService {

  override suspend fun createWorkItem(
    request: CreateWorkItemRequest
  ): WorkItem {

    val workItem = request.workItem
    val projectId = workItem.queue.projectId
    val topicId = workItem.queue.name
    if (queueExists(projectId, topicId) == false) {
      throw Status.NOT_FOUND.withDescription("Google Pub/Sub topicId '$topicId' does not exist in project '$projectId'")
        .asRuntimeException()
    }

    val serializedWorkItem = workItem.toByteString()

    // Publish the WorkItem to Pub/Sub
    publishMessage(projectId, topicId, serializedWorkItem)

    return workItem
  }

  override fun publishMessage(projectId: String, queueName: String, messageContent: ByteString) {
    googlePubSubClient.publishMessage(projectId, queueName, messageContent)
  }

  override fun queueExists(projectId: String, queueName: String): Boolean {
    return googlePubSubClient.topicExists(projectId, queueName)
  }

}
