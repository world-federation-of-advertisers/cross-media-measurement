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
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import java.io.File
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.selects.select
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.InProcessServersMethods
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.securecomputation.QueuesConfig
import org.wfanet.measurement.edpaggregator.VidLabelingRpcDurationConverter
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter.DeadLetterQueueListener
import org.wfanet.measurement.securecomputation.deploy.gcloud.publisher.GoogleWorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import picocli.CommandLine

/** Runs a blocking gRPC server alongside suspending background jobs. */
internal suspend fun runInternalApiServerJobs(
  blockingServer: () -> Unit,
  shutdownServer: () -> Unit,
  backgroundJobs: List<suspend () -> Unit>,
) = coroutineScope {
  val serverJob = async { runInterruptible(Dispatchers.IO) { blockingServer() } }
  val jobs = backgroundJobs.map { backgroundJob -> async { backgroundJob() } }
  try {
    select<Unit> {
      serverJob.onAwait {}
      jobs.forEach { job -> job.onAwait {} }
    }
  } finally {
    shutdownServer()
    serverJob.cancelAndJoin()
    jobs.forEach { job -> job.cancelAndJoin() }
  }
}

/**
 * Internal API Server for the Secure Computation system.
 *
 * This server provides gRPC services for managing work items and optionally runs one Dead Letter
 * Queue (DLQ) listener per configured dead-letter subscription in parallel. Each listener delegates
 * generic recovery or terminal failure to the WorkItems service.
 *
 * ## Lifecycle:
 * 1. Server initialization reads configuration and sets up dependencies
 * 2. Main gRPC server starts in an async coroutine
 * 3. The WorkItem publication runner starts in its own async coroutine
 * 4. The WorkItem attempt lease reaper starts in its own async coroutine
 * 5. If configured, one DLQ listener per dead-letter subscription starts in its own async coroutine
 * 6. All components run until shutdown is requested
 * 7. Graceful shutdown ensures all components clean up properly
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
    description =
      [
        "PubSub subscription ID for a dead letter queue. May be specified multiple times to run " +
          "one listener per dead-letter subscription (e.g. one per phase queue)."
      ],
    required = false,
    arity = "1",
  )
  private var deadLetterSubscriptionIds: List<String> = emptyList()

  @CommandLine.Option(
    names = ["--dead-letter-processing-enabled"],
    description = ["Whether configured dead-letter subscriptions may be consumed."],
    defaultValue = "true",
  )
  private var deadLetterProcessingEnabled: Boolean = true

  @CommandLine.Option(
    names = ["--work-item-publication-enabled"],
    description = ["Whether WorkItem publication and legacy reconciliation may run."],
    defaultValue = "true",
  )
  private var workItemPublicationEnabled: Boolean = true

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."],
  )
  private lateinit var channelShutdownTimeout: Duration

  @CommandLine.Option(
    names = ["--work-item-publication-poll-interval"],
    defaultValue = "1s",
    description = ["How often to poll for pending WorkItem publications."],
    converter = [VidLabelingRpcDurationConverter::class],
  )
  private lateinit var workItemPublicationPollInterval: Duration

  @CommandLine.Option(
    names = ["--work-item-publication-lease-duration"],
    defaultValue = "1m",
    description = ["How long a WorkItem publication is leased to one server replica."],
    converter = [VidLabelingRpcDurationConverter::class],
  )
  private lateinit var workItemPublicationLeaseDuration: Duration

  override fun run() {
    val queuesConfig = parseTextProto(queuesConfigFile, QueuesConfig.getDefaultInstance())
    val queueMapping = QueueMapping(queuesConfig)
    val activeDeadLetterSubscriptionIds =
      if (deadLetterProcessingEnabled) deadLetterSubscriptionIds else emptyList()

    runBlocking {
      spannerFlags.usingSpanner { spanner ->
        val databaseClient: AsyncDatabaseClient = spanner.databaseClient
        val googlePubSubClient = DefaultGooglePubSubClient()
        val workItemPublisher = GoogleWorkItemPublisher(googleProjectId, googlePubSubClient)

        val internalApiServices =
          InternalApiServices(
            workItemPublisher,
            databaseClient,
            queueMapping,
            workItemPublicationPollInterval = workItemPublicationPollInterval,
            workItemPublicationLeaseDuration = workItemPublicationLeaseDuration,
            workItemPublicationEnabled = workItemPublicationEnabled,
          )
        val services = internalApiServices.build(serviceFlags.executor.asCoroutineDispatcher())
        val servicesList: List<BindableService> = services.toList()
        val server = createMainServer(servicesList)
        val spannerWorkItemsService =
          services.workItems as? SpannerWorkItemsService
            ?: throw RuntimeException("Failed to get work items service")

        // A single in-process server + channel + WorkItems stub is shared by every DLQ listener:
        // they all route to the same SpannerWorkItemsService, so one loopback server suffices, and
        // it is shut down below instead of leaking one server per subscription.
        val (inProcessServer, inProcessChannel) = createInProcessServer(spannerWorkItemsService)
        val workItemsStub = WorkItemsGrpcKt.WorkItemsCoroutineStub(inProcessChannel)
        try {
          // Run one DLQ listener per dead-letter subscription (e.g. one per phase queue).
          val deadLetterListenerJobs: List<suspend () -> Unit> =
            activeDeadLetterSubscriptionIds.map { subscriptionId ->
              val subscriber =
                Subscriber(
                  projectId = googleProjectId,
                  googlePubSubClient = googlePubSubClient,
                  maxMessages = 10,
                  pullIntervalMillis = 100,
                  blockingContext = Dispatchers.IO,
                )
              val deadLetterListener =
                createDeadLetterQueueListener(
                  workItemsStub = workItemsStub,
                  subscriptionId = subscriptionId,
                  queueSubscriber = subscriber,
                )
              suspend {
                try {
                  deadLetterListener.run()
                } finally {
                  deadLetterListener.close()
                }
              }
            }

          runInternalApiServerJobs(
            blockingServer = { server.start().blockUntilShutdown() },
            shutdownServer = { server.shutdown() },
            backgroundJobs =
              listOf<suspend () -> Unit>(
                { internalApiServices.workItemPublicationRunner.run() },
                { internalApiServices.workItemAttemptLeaseReaper.run() },
              ) + deadLetterListenerJobs,
          )
        } finally {
          inProcessChannel.shutdown()
          inProcessServer.shutdown()
        }
      }
    }
  }

  private fun createInProcessServer(
    spannerWorkItemsService: SpannerWorkItemsService
  ): Pair<Server, ManagedChannel> {
    val serverName = InProcessServerBuilder.generateName()
    val server =
      InProcessServersMethods.startInProcessServerWithService(
        serverName = serverName,
        commonServerFlags = serverFlags,
        service = spannerWorkItemsService.bindService(),
      )
    val channel =
      InProcessChannelBuilder.forName(serverName)
        .directExecutor()
        .build()
        .withShutdownTimeout(channelShutdownTimeout)
    return Pair(server, channel)
  }

  private fun createDeadLetterQueueListener(
    workItemsStub: WorkItemsGrpcKt.WorkItemsCoroutineStub,
    subscriptionId: String,
    queueSubscriber: QueueSubscriber,
  ): DeadLetterQueueListener {
    return DeadLetterQueueListener(
      subscriptionId = subscriptionId,
      queueSubscriber = queueSubscriber,
      parser = WorkItem.parser(),
      workItemsStub = workItemsStub,
    )
  }

  private fun createMainServer(services: List<BindableService>): CommonServer {
    return CommonServer.fromFlags(serverFlags, SERVER_NAME, services)
  }

  companion object {
    const val SERVER_NAME = "SecureComputationInternalApiServer"

    @JvmStatic fun main(args: Array<String>) = commandLineMain(InternalApiServer(), args)
  }
}
