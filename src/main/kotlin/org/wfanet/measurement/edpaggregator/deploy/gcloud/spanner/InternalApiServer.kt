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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import io.grpc.ServerServiceDefinition
import java.io.File
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.RateLimitConfig
import org.wfanet.measurement.edpaggregator.deploy.common.server.EdpaApiRateLimiting
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import picocli.CommandLine

private const val SERVER_NAME = "EdpAggregatorInternalApiServer"

@CommandLine.Command(name = SERVER_NAME)
class InternalApiServer : Runnable {
  @CommandLine.Mixin private lateinit var serverFlags: CommonServer.Flags
  @CommandLine.Mixin private lateinit var serviceFlags: ServiceFlags
  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Option(
    names = ["--rate-limit-config-file"],
    description =
      [
        "File path to a RateLimitConfig protobuf message in text format.",
        "If unset, expensive List methods are bounded by a conservative default and all other " +
          "methods are unlimited.",
      ],
    required = false,
  )
  private var rateLimitConfigFile: File? = null

  private val rateLimitConfig: RateLimitConfig by lazy {
    val configFile = rateLimitConfigFile
    if (configFile == null) {
      EdpaApiRateLimiting.defaultConfig(EdpaApiRateLimiting.INTERNAL_API_EXPENSIVE_METHODS)
    } else {
      parseTextProto(configFile, RateLimitConfig.getDefaultInstance())
    }
  }

  override fun run() {
    runBlocking {
      spannerFlags.usingSpanner { spanner ->
        val databaseClient: AsyncDatabaseClient = spanner.databaseClient
        val rateLimitingInterceptor = EdpaApiRateLimiting.interceptor(rateLimitConfig)
        val services: List<ServerServiceDefinition> =
          InternalApiServices.build(databaseClient, serviceFlags.executor.asCoroutineDispatcher())
            .toList()
            .map { it.withInterceptor(rateLimitingInterceptor) }
        val server = CommonServer.fromFlags(serverFlags, SERVER_NAME, services)

        server.start().blockUntilShutdown()
      }
    }
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(InternalApiServer(), args)
  }
}
