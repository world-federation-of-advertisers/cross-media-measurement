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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner

import com.google.protobuf.Parser
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import java.util.concurrent.TimeUnit
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter.DeadLetterQueueListener

/**
 * Wrapper that manages the lifecycle of a [DeadLetterQueueListener] along with its associated
 * in-process gRPC server and channel.
 *
 * This class implements [AutoCloseable] to ensure proper cleanup of resources. The typical usage
 * pattern is:
 * ```kotlin
 * deadLetterListenerWithServer.use { listener ->
 *   listener.run()
 * }
 * ```
 *
 * The in-process gRPC server allows the DLQ listener to communicate with the WorkItemsService
 * without external network calls.
 */
class DeadLetterQueueListenerWithServer(
  private val deadLetterQueueListener: DeadLetterQueueListener,
  private val server: Server,
  private val channel: ManagedChannel
) : AutoCloseable {

  /**
   * Runs the dead letter queue listener.
   *
   * This suspending function will process messages from the dead letter queue until cancelled
   * or an error occurs.
   */
  suspend fun run() {
    deadLetterQueueListener.run()
  }

  /**
   * Closes all resources in the proper order:
   * 1. The DLQ listener itself
   * 2. The gRPC channel
   * 3. The gRPC server
   *
   * Attempts graceful shutdown with a 5-second timeout before forcing shutdown.
   */
  override fun close() {
    deadLetterQueueListener.close()
    channel.shutdown()
    server.shutdown()
    try {
      if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
        channel.shutdownNow()
      }
      if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
        server.shutdownNow()
      }
    } catch (e: InterruptedException) {
      channel.shutdownNow()
      server.shutdownNow()
      Thread.currentThread().interrupt()
    }
  }
  
  companion object {
    /**
     * Creates a new [DeadLetterQueueListenerWithServer] instance with an in-process gRPC server.
     *
     * @param spannerWorkItemsService The WorkItemsService instance to use
     * @param subscriptionId The Google Cloud Pub/Sub subscription ID for the dead letter queue
     * @param queueSubscriber The queue subscriber for receiving messages
     * @param parser The protobuf parser for WorkItem messages
     * @return A configured DLQ listener with its associated server and channel
     */
    fun create(
      spannerWorkItemsService: SpannerWorkItemsService,
      subscriptionId: String,
      queueSubscriber: QueueSubscriber,
      parser: Parser<WorkItem> = WorkItem.parser()
    ): DeadLetterQueueListenerWithServer {
      // Set up an in-process gRPC server with the existing WorkItemsService instance
      val serviceName = InProcessServerBuilder.generateName()
      val spannerWorkItemsServer = InProcessServerBuilder.forName(serviceName)
        .directExecutor()
        .addService(spannerWorkItemsService)
        .build()
        .start()
      
      // Create the corresponding in-process channel
      val channel = InProcessChannelBuilder.forName(serviceName)
        .directExecutor()
        .build()
      
      // Create the DLQ listener with a gRPC stub for the WorkItemsService
      val deadLetterQueueListener = DeadLetterQueueListener(
        subscriptionId = subscriptionId,
        queueSubscriber = queueSubscriber,
        parser = parser,
        workItemsStub = WorkItemsGrpcKt.WorkItemsCoroutineStub(channel)
      )
      
      return DeadLetterQueueListenerWithServer(
        deadLetterQueueListener = deadLetterQueueListener,
        server = spannerWorkItemsServer,
        channel = channel
      )
    }
  }
}
