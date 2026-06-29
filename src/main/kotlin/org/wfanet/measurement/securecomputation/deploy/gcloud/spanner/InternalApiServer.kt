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
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import java.io.File
import java.time.Duration
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.InProcessServersMethods
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.securecomputation.QueuesConfig
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
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

/**
 * Internal API Server for the Secure Computation system.
 *
 * This server provides gRPC services for managing work items and optionally runs one Dead Letter
 * Queue (DLQ) listener per configured dead-letter subscription in parallel. Each DLQ listener
 * monitors failed messages, marks the corresponding work item as failed in the database, and (when
 * the EDP-Aggregator metadata-storage channel is configured) marks the EDPA resources the failed
 * WorkItem references FAILED.
 *
 * ## Lifecycle:
 * 1. Server initialization reads configuration and sets up dependencies
 * 2. Main gRPC server starts in an async coroutine
 * 3. If configured, one DLQ listener per dead-letter subscription starts in its own async coroutine
 * 4. All components run until shutdown is requested
 * 5. Graceful shutdown ensures all components clean up properly
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
    arity = "0..*",
  )
  private var deadLetterSubscriptionIds: List<String> = emptyList()

  @CommandLine.Option(
    names = ["--edpa-tls-cert-file"],
    description =
      [
        "Path to the EDP-Aggregator client TLS certificate (PEM) used for the metadata-storage " +
          "mTLS channel."
      ],
    required = true,
  )
  private lateinit var edpaTlsCertFile: File

  @CommandLine.Option(
    names = ["--edpa-tls-key-file"],
    description =
      [
        "Path to the EDP-Aggregator client TLS private key (PEM) used for the metadata-storage " +
          "mTLS channel."
      ],
    required = true,
  )
  private lateinit var edpaTlsKeyFile: File

  @CommandLine.Option(
    names = ["--metadata-storage-cert-collection-file"],
    description =
      [
        "Path to the trusted root certificate collection (PEM) for the EDP-Aggregator " +
          "metadata-storage public API."
      ],
    required = true,
  )
  private lateinit var metadataStorageCertCollectionFile: File

  @CommandLine.Option(
    names = ["--metadata-storage-public-api-target"],
    description = ["gRPC target of the EDP-Aggregator metadata-storage public API server."],
    required = true,
  )
  private lateinit var metadataStoragePublicApiTarget: String

  @CommandLine.Option(
    names = ["--metadata-storage-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the metadata-storage public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from " +
          "--metadata-storage-public-api-target.",
      ],
    required = false,
  )
  private var metadataStoragePublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."],
  )
  private lateinit var channelShutdownTimeout: Duration

  override fun run() {
    val queuesConfig = parseTextProto(queuesConfigFile, QueuesConfig.getDefaultInstance())
    val queueMapping = QueueMapping(queuesConfig)
    val edpaStubs: EdpaStubs = buildEdpaStubs()

    runBlocking {
      spannerFlags.usingSpanner { spanner ->
        val databaseClient: AsyncDatabaseClient = spanner.databaseClient
        val googlePubSubClient = DefaultGooglePubSubClient()
        val workItemPublisher = GoogleWorkItemPublisher(googleProjectId, googlePubSubClient)

        val internalApiServices =
          InternalApiServices(workItemPublisher, databaseClient, queueMapping)
        val services = internalApiServices.build(serviceFlags.executor.asCoroutineDispatcher())
        val servicesList: List<BindableService> = services.toList()
        val server = createMainServer(servicesList)
        val spannerWorkItemsService =
          services.workItems as? SpannerWorkItemsService
            ?: throw RuntimeException("Failed to get work items service")

        val serverJob = async { server.start().blockUntilShutdown() }

        // A single in-process server + channel + WorkItems stub is shared by every DLQ listener:
        // they all route to the same SpannerWorkItemsService, so one loopback server suffices, and
        // it is shut down below instead of leaking one server per subscription.
        val (inProcessServer, inProcessChannel) = createInProcessServer(spannerWorkItemsService)
        val workItemsStub = WorkItemsGrpcKt.WorkItemsCoroutineStub(inProcessChannel)
        try {
          // Run one DLQ listener per dead-letter subscription (e.g. one per phase queue), each in
          // its own coroutine.
          val deadLetterListenerJobs: List<Deferred<Unit>> =
            deadLetterSubscriptionIds.map { subscriptionId ->
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
                  edpaStubs = edpaStubs,
                )
              async {
                try {
                  deadLetterListener.run()
                } finally {
                  deadLetterListener.close()
                }
              }
            }

          awaitAll(serverJob, *deadLetterListenerJobs.toTypedArray())
        } finally {
          inProcessChannel.shutdown()
          inProcessServer.shutdown()
        }
      }
    }
  }

  /** The four EDP-Aggregator metadata-storage client stubs used for EDPA resource marking. */
  private class EdpaStubs(
    val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
    val rankerJobsStub: RankerJobServiceCoroutineStub,
    val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
    val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  )

  /**
   * Builds the [EdpaStubs] from a mutual-TLS channel to the EDP-Aggregator metadata-storage public
   * API. The TLS cert/key, trusted root collection, and API target are required flags, so the EDPA
   * resource-marking path is always enabled.
   */
  private fun buildEdpaStubs(): EdpaStubs {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = edpaTlsCertFile,
        privateKeyFile = edpaTlsKeyFile,
        trustedCertCollectionFile = metadataStorageCertCollectionFile,
      )
    val channel: Channel =
      buildMutualTlsChannel(
        metadataStoragePublicApiTarget,
        clientCerts,
        metadataStoragePublicApiCertHost,
      )
    return EdpaStubs(
      poolAssignmentJobsStub = PoolAssignmentJobServiceCoroutineStub(channel),
      rankerJobsStub = RankerJobServiceCoroutineStub(channel),
      vidLabelingJobsStub = VidLabelingJobServiceCoroutineStub(channel),
      rawImpressionUploadModelLinesStub = RawImpressionUploadModelLineServiceCoroutineStub(channel),
    )
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
    edpaStubs: EdpaStubs,
  ): DeadLetterQueueListener {
    return DeadLetterQueueListener(
      subscriptionId = subscriptionId,
      queueSubscriber = queueSubscriber,
      parser = WorkItem.parser(),
      workItemsStub = workItemsStub,
      poolAssignmentJobsStub = edpaStubs.poolAssignmentJobsStub,
      rankerJobsStub = edpaStubs.rankerJobsStub,
      vidLabelingJobsStub = edpaStubs.vidLabelingJobsStub,
      rawImpressionUploadModelLinesStub = edpaStubs.rawImpressionUploadModelLinesStub,
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
