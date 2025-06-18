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

import io.grpc.BindableService
import java.io.File
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.securecomputation.QueuesConfig
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.publisher.GoogleWorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import picocli.CommandLine

/**
 * Internal API Server for the Secure Computation system.
 *
 * This server provides gRPC services for managing work items and optionally runs a Dead Letter
 * Queue (DLQ) listener in parallel. The DLQ listener monitors failed messages and marks the
 * corresponding work items as failed in the database.
 *
 * ## Lifecycle:
 * 1. Server initialization reads configuration and sets up dependencies
 * 2. Main gRPC server starts in an async coroutine
 * 3. If configured, DLQ listener starts in a separate async coroutine
 * 4. Both components run until shutdown is requested
 * 5. Graceful shutdown ensures both components clean up properly
 */
@CommandLine.Command(name = InternalApiServer.SERVER_NAME)
class InternalApiServer : Runnable {
  @CommandLine.Mixin private lateinit var serverFlags: CommonServer.Flags
  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags
  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Option(
      names = ["--queue-config"],
      description = ["Path to file containing a QueueConfig protobuf message in text format"],
      required = true,
  )
  private lateinit var queuesConfigFile: File

  @CommandLine.Option(
      names = ["--google-project-id"],
      description = ["Google Project ID that provides the PubSub"],
      required = true,
  )
  private lateinit var googleProjectId: String

  @CommandLine.Option(
      names = ["--dead-letter-subscription-id"],
      description = ["PubSub subscription ID for the dead letter queue"],
      required = false,
  )
  private var deadLetterSubscriptionId: String? = null

  override fun run() {
    val queuesConfig = parseTextProto(queuesConfigFile, QueuesConfig.getDefaultInstance())
    val queueMapping = QueueMapping(queuesConfig)

    runBlocking {
      spannerFlags.usingSpanner { spanner ->
        val databaseClient: AsyncDatabaseClient = spanner.databaseClient
        val googlePubSubClient = DefaultGooglePubSubClient()
        val workItemPublisher = GoogleWorkItemPublisher(googleProjectId, googlePubSubClient)
        val internalApiServices =
            InternalApiServices(workItemPublisher, databaseClient, queueMapping)
        val services = internalApiServices.build(serviceFlags.executor.asCoroutineDispatcher())
        val servicesList: List<BindableService> = services.toList()
        val server = CommonServer.fromFlags(serverFlags, SERVER_NAME, servicesList)

        val serverJob = async { server.start().blockUntilShutdown() }

        val deadLetterListenerJob: Deferred<Unit>? =
            deadLetterSubscriptionId?.let { subscriptionId ->
              val subscriber = Subscriber(googleProjectId, googlePubSubClient)
              // Get the SpannerWorkItemsService instance from the services
              val spannerWorkItemsService =
                  services.workItems as? SpannerWorkItemsService
                      ?: throw RuntimeException("Failed to get work items service")
              val deadLetterListener =
                  DeadLetterQueueListenerWithServer.create(
                      spannerWorkItemsService = spannerWorkItemsService,
                      subscriptionId = subscriptionId,
                      queueSubscriber = subscriber,
                      parser = WorkItem.parser())
              async { deadLetterListener.use { listener -> listener.run() } }
            }

        if (deadLetterListenerJob != null) {
          awaitAll(serverJob, deadLetterListenerJob)
        } else {
          serverJob.await()
        }
      }
    }
  }

  companion object {
    const val SERVER_NAME = "SecureComputationInternalApiServer"

    @JvmStatic fun main(args: Array<String>) = commandLineMain(InternalApiServer(), args)
  }
}
