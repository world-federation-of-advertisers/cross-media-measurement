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

package org.wfanet.measurement.securecomputation.deploy.gcloud.deadletter

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.logging.Logger
import kotlin.concurrent.thread
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.gcloud.pubsub.GoogleCloudPubSubClient
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
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

  private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

  override fun run() = runBlocking {
    val deadLetterListener = createDeadLetterQueueListener()
    
    // Register shutdown hook to close resources gracefully
    val mainThread = Thread.currentThread()
    Runtime.getRuntime().addShutdownHook(thread(start = false) {
      logger.info("Shutting down $SERVICE_NAME...")
      deadLetterListener.close()
      mainThread.join()
      logger.info("$SERVICE_NAME shutdown complete")
    })
    
    // Start listening for messages
    logger.info("Starting $SERVICE_NAME...")
    deadLetterListener.run()
  }

  private fun createDeadLetterQueueListener(): DeadLetterQueueListener {
    // Set up the gRPC channel to the WorkItems API
    val workItemsChannel = createChannel()
    val workItemsStub = WorkItemsCoroutineStub(workItemsChannel)
    
    // Set up the PubSub subscriber
    val googlePubSubClient = GoogleCloudPubSubClient()
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
    return if (tlsFlags.tlsCertFile != null && tlsFlags.tlsKeyFile != null) {
      // Set up mutual TLS authentication
      buildMutualTlsChannel(
        flags.workItemsApiTarget,
        tlsFlags.certCollectionFile,
        TinkPrivateKeyHandle.fromPemFile(tlsFlags.tlsKeyFile!!),
        tlsFlags.tlsCertFile!!
      ).withPrincipalName(SERVICE_NAME)
    } else {
      // Use plaintext channel for testing or non-TLS environments
      ManagedChannelBuilder.forTarget(flags.workItemsApiTarget)
        .usePlaintext()
        .build()
    }
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

fun main(args: Array<String>) {
  commandLineMain(DeadLetterQueueListenerServer(), args)
}