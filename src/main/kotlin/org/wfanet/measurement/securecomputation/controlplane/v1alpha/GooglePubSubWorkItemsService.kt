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
import com.google.cloud.pubsub.v1.Publisher
//import com.rabbitmq.client.Channel
//import com.rabbitmq.client.Connection
//import com.rabbitmq.client.ConnectionFactory
import io.grpc.Status
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import com.google.api.gax.rpc.NotFoundException
import org.wfanet.measurement.gcloud.pubsub.GooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient

class GooglePubSubWorkItemsService(
  private val googlePubSubClient: GooglePubSubClient = DefaultGooglePubSubClient()
//  private val rabbitMqHost: String,
//  private val rabbitMqPort: Int,
//  private val rabbitMqUsername: String,
//  private val rabbitMqPassword: String,
//  private val publisherProvider: ((projectId: String, topicId: String) -> Publisher)? = null
) : WorkItemsCoroutineImplBase(), WorkItemService {

//  private val connectionFactory =
//    ConnectionFactory().apply {
//      host = rabbitMqHost
//      port = rabbitMqPort
//      username = rabbitMqUsername
//      password = rabbitMqPassword
//    }

//  private lateinit var connection: Connection
//  private lateinit var channel: Channel

//  init {
//    setupRabbitMqConnection()
//  }

//  private fun setupRabbitMqConnection() {
//    try {
//      connection = connectionFactory.newConnection()
//      channel = connection.createChannel()
//    } catch (e: Exception) {
//      throw StatusException(
//        Status.UNAVAILABLE.withDescription("Failed to connect to RabbitMQ: ${e.message}")
//      )
//    }
//  }

  /**
   * Checks if the current RabbitMQ channel is open and creates a new one if it's closed.
   *
   * RabbitMQ may close channels in response to certain errors (e.g., accessing non-existent
   * queues). This method ensures we have a valid channel for subsequent operations by creating a
   * new one from the existing connection if needed.
   */
//  private fun recreateChannelIfNeeded() {
//    if (!channel.isOpen) {
//      channel = connection.createChannel()
//    }
//  }

  override suspend fun createWorkItem(
    request: CreateWorkItemRequest
  ): WorkItem {

    val workItem = request.workItem
    val projectId = workItem.queue.projectId
    val topicId = workItem.queue.name

//    try {
//      recreateChannelIfNeeded()
//      channel.queueDeclarePassive(queueName)
//    } catch (e: Exception) {
//      throw StatusException(
//        Status.PERMISSION_DENIED.withDescription("Queue '$queueName' does not exist")
//      )
//    }

//    try {
//      // Makes the message persistent.
//      val props = AMQP.BasicProperties.Builder().deliveryMode(2).build()
//
//      channel.basicPublish("", queueName, props, workItemParams.toByteArray())
//    } catch (e: Exception) {
//      throw StatusException(
//        Status.INTERNAL.withDescription("Failed to enqueue work item: ${e.message}")
//      )
//    }

    val serializedWorkItem = workItem.toByteString()

    // Publish the WorkItem to Pub/Sub
    publishToPubSub(projectId, topicId, serializedWorkItem)

//    when (queue.providerCase) {
//      Queue.ProviderCase.GOOGLE_PUBSUB -> {
//        handleGooglePubSub(queue.googlePubsub, workItem)
//      }
//      else -> throw IllegalArgumentException("Unsupported queue provider")
//    }

    return workItem
  }

//  private fun handleGooglePubSub(googlePubSubParam: Queue, workItem: WorkItem) {
//
//    val projectId = googlePubSubParam.projectId
//    val topicId = googlePubSubParam.name
//
//    // Validate subscription existence (you might need Google Cloud client libraries here)
////    if (!subscriptionExists(projectId, subscriptionId)) {
////      throw Status.NOT_FOUND.withDescription("Google Pub/Sub subscription '$subscriptionId' does not exist in project '$projectId'")
////        .asRuntimeException()
////    }
//
//    // Serialize the WorkItem
//    val serializedWorkItem = workItem.toByteString()
//
//    // Publish the WorkItem to Pub/Sub
//    publishToPubSub(projectId, topicId, serializedWorkItem)
//  }

  override fun publishMessage(projectId: String, queueName: String, messageContent: ByteString) {
    require(publisherProvider != null) { "Google pubsub publisherProvider must not be null" }

//    val publisher = Publisher.newBuilder(topicName).build()
    val publisher = publisherProvider.invoke(projectId, queueName)

    try {
      val pubsubMessage = PubsubMessage.newBuilder()
        .setData(messageContent)
        .build()
      publisher.publish(pubsubMessage).get()
    } catch (e: Exception) {
      throw Status.UNKNOWN.withDescription("ailed to publish message to Pub/Sub")
          .withCause(e)
          .asRuntimeException()
    } finally {
      publisher.shutdown()
    }
  }

  override fun queueExists(projectId: String, queueName: String, messageContent: ByteString): Boolean {

  }

//  private fun subscriptionExists(projectId: String, subscriptionId: String): Boolean {
//    val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)
//
//    return SubscriptionAdminClient.create().use { subscriptionAdminClient ->
//      try {
//        // Try to get the subscription details. If it doesn't exist, an exception will be thrown.
//        subscriptionAdminClient.getSubscription(subscriptionName)
//        true
//      } catch (e: Exception) {
//        when (e) {
//          is NotFoundException -> false // Subscription doesn't exist
//          else -> {
//            throw Status.UNKNOWN.withDescription("Failed to check subscription existence")
//              .withCause(e)
//              .asRuntimeException()
//          }
//        }
//      }
//    }
//  }

//  override fun close() {
//    connection.close()
//  }
}
