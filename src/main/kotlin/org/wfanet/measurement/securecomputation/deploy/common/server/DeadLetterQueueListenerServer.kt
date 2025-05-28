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

package org.wfanet.measurement.securecomputation.deploy.common.server

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter.DeadLetterQueueListener
import picocli.CommandLine

private const val SERVICE_NAME = "DeadLetterQueueListener"

@CommandLine.Command(
  name = "DeadLetterQueueListenerServer",
  description = ["Server that listens to dead letter queue and marks work items as failed."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private class DeadLetterQueueListenerServer : Runnable {
  @CommandLine.Mixin
  private lateinit var flags: DeadLetterQueueListenerFlags
  
  @CommandLine.Mixin
  private lateinit var tlsFlags: TlsFlags

  override fun run() = runBlocking {
    logger.info("Starting $SERVICE_NAME...")
    createDeadLetterQueueListener().use { listener ->
      listener.run()
    }
    logger.info("$SERVICE_NAME shutdown complete")
  }

  private fun createDeadLetterQueueListener(): DeadLetterQueueListener {
    // Set up the gRPC channel to the WorkItems API
    val workItemsChannel = createChannel()
    val workItemsStub = WorkItemsCoroutineStub(workItemsChannel)
    
    // Set up the PubSub subscriber
    val googlePubSubClient = DefaultGooglePubSubClient()
    val subscriber = Subscriber(flags.projectId, googlePubSubClient)
    
    // Create the listener
    return DeadLetterQueueListener(
      subscriptionId = flags.deadLetterSubscriptionId,
      queueSubscriber = subscriber,
      parser = WorkItem.parser(),
      workItemsStub = workItemsStub
    )
  }

  private fun createChannel(): ManagedChannel {
    return if (tlsFlags.certFile != null) {
      // Set up mutual TLS authentication
      val clientCerts = SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile!!,
        privateKeyFile = tlsFlags.privateKeyFile!!,
        trustedCertCollectionFile = tlsFlags.certCollectionFile
      )
      buildMutualTlsChannel(
        flags.workItemsApiTarget,
        clientCerts,
        hostName = flags.workItemsApiCertHost
      )
    } else {
      // Use plaintext channel for testing or non-TLS environments
      ManagedChannelBuilder.forTarget(flags.workItemsApiTarget)
        .usePlaintext()
        .build()
    }
  }

  /** Command-line flags for [DeadLetterQueueListener]. */
  private class DeadLetterQueueListenerFlags {
    @CommandLine.Option(
      names = ["--dead-letter-subscription-id"],
      description = ["PubSub subscription ID for the dead letter queue"],
      required = true
    )
    lateinit var deadLetterSubscriptionId: String
      private set

    @CommandLine.Option(
      names = ["--project-id"],
      description = ["Google Cloud project ID"],
      required = true
    )
    lateinit var projectId: String
      private set

    @CommandLine.Option(
      names = ["--work-items-api-target"],
      description = ["gRPC target (authority) for the WorkItems API"],
      required = true
    )
    lateinit var workItemsApiTarget: String
      private set

    @CommandLine.Option(
      names = ["--work-items-api-cert-host"],
      description = ["Expected hostname (DNS-ID) in the WorkItems API server's TLS certificate. This overrides derivation of the TLS DNS-ID from --work-items-api-target."],
      required = false
    )
    var workItemsApiCertHost: String? = null
      private set
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

fun main(args: Array<String>) {
  commandLineMain(DeadLetterQueueListenerServer(), args)
}