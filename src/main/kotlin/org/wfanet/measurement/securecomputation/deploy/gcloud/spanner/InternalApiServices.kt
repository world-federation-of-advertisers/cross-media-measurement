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
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.securecomputation.controlplane.FailWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter.DeadLetterQueueListener
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.Services
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher

class InternalApiServices(
  private val workItemPublisher: WorkItemPublisher,
  private val databaseClient: AsyncDatabaseClient,
  private val queueMapping: QueueMapping,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) {
  fun build(): Services {
    return Services(
      SpannerWorkItemsService(databaseClient, queueMapping, idGenerator, workItemPublisher),
      SpannerWorkItemAttemptsService(databaseClient, queueMapping, idGenerator),
    )
  }

  fun createDeadLetterQueueListener(
    subscriptionId: String,
    queueSubscriber: QueueSubscriber,
    parser: Parser<WorkItem> = WorkItem.parser()
  ): DeadLetterQueueListenerWithServer {
    val spannerWorkItemsService = SpannerWorkItemsService(databaseClient, queueMapping, idGenerator, workItemPublisher)
    val serviceName = InProcessServerBuilder.generateName()
    val spannerWorkItemsServer = InProcessServerBuilder.forName(serviceName)
      .directExecutor()
      .addService(spannerWorkItemsService)
      .build()
      .start()
    val channel = InProcessChannelBuilder.forName(serviceName)
      .directExecutor()
      .build()
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

  class DeadLetterQueueListenerWithServer(
    private val deadLetterQueueListener: DeadLetterQueueListener,
    private val server: Server,
    private val channel: ManagedChannel
  ) : AutoCloseable {
    suspend fun run() {
      deadLetterQueueListener.run()
    }

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
  }
}
