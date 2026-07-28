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
import org.wfanet.measurement.common.grpc.RateLimiterProvider
import org.wfanet.measurement.common.grpc.RateLimitingServerInterceptor
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.RateLimitConfig
import org.wfanet.measurement.config.RateLimitConfigKt
import org.wfanet.measurement.config.rateLimitConfig
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
      DEFAULT_RATE_LIMIT_CONFIG
    } else {
      parseTextProto(configFile, RateLimitConfig.getDefaultInstance())
    }
  }

  override fun run() {
    runBlocking {
      spannerFlags.usingSpanner { spanner ->
        val databaseClient: AsyncDatabaseClient = spanner.databaseClient
        // Global per-method limiting (the principal extractor returns null): this is overload
        // protection, not per-client fairness.
        val rateLimitingInterceptor =
          RateLimitingServerInterceptor(
            RateLimiterProvider(rateLimitConfig) { null }::getRateLimiter
          )
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
    private const val LIST_IMPRESSION_METADATA_METHOD =
      "wfa.measurement.internal.edpaggregator.ImpressionMetadataService/ListImpressionMetadata"
    private const val LIST_REQUISITION_METADATA_METHOD =
      "wfa.measurement.internal.edpaggregator.RequisitionMetadataService/ListRequisitionMetadata"

    /**
     * Default rate-limit config: bounds the expensive internal `List*` methods (which can OOM the
     * server under load) with a conservative token bucket and leaves every other method unlimited.
     * Override per-environment with `--rate-limit-config-file`.
     */
    private val DEFAULT_RATE_LIMIT_CONFIG = rateLimitConfig {
      rateLimit =
        RateLimitConfigKt.rateLimit {
          // maximumRequestCount < 0 short-circuits to unlimited before any token bucket is built.
          defaultRateLimit = RateLimitConfigKt.methodRateLimit { maximumRequestCount = -1 }
          val boundedRateLimit =
            RateLimitConfigKt.methodRateLimit {
              maximumRequestCount = 200
              averageRequestRate = 100.0
            }
          perMethodRateLimit[LIST_IMPRESSION_METADATA_METHOD] = boundedRateLimit
          perMethodRateLimit[LIST_REQUISITION_METADATA_METHOD] = boundedRateLimit
        }
    }

    @JvmStatic fun main(args: Array<String>) = commandLineMain(InternalApiServer(), args)
  }
}
