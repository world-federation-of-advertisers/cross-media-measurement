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
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.securecomputation.QueuesConfig
import org.wfanet.measurement.gcloud.pubsub.DefaultGooglePubSubClient
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.securecomputation.deploy.gcloud.publisher.GoogleWorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import picocli.CommandLine

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

  override fun run() {
    val queuesConfig = parseTextProto(queuesConfigFile, QueuesConfig.getDefaultInstance())
    val queueMapping = QueueMapping(queuesConfig)

    runBlocking {
      spannerFlags.usingSpanner { spanner ->
        val databaseClient: AsyncDatabaseClient = spanner.databaseClient
        val googlePubSubClient = DefaultGooglePubSubClient()
        val workItemPublisher = GoogleWorkItemPublisher(googleProjectId, googlePubSubClient)
        val services: List<BindableService> =
          InternalApiServices(workItemPublisher, databaseClient, queueMapping)
            .build(serviceFlags.executor.asCoroutineDispatcher())
            .toList()
        val server = CommonServer.fromFlags(serverFlags, SERVER_NAME, services)

        server.start().blockUntilShutdown()
      }
    }
  }

  companion object {
    const val SERVER_NAME = "SecureComputationInternalApiServer"

    @JvmStatic fun main(args: Array<String>) = commandLineMain(InternalApiServer(), args)
  }
}
