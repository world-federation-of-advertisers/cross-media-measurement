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

package org.wfanet.measurement.loadtest.dataprovider.tools

import com.google.common.math.Quantiles
import com.google.common.math.Stats
import io.grpc.ManagedChannel
import io.grpc.StatusException
import io.grpc.serviceconfig.copy
import java.time.Duration
import kotlin.properties.Delegates
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.ProtobufServiceConfig
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.throttler.MaximumRateThrottler
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import picocli.CommandLine

/**
 * Tool for load testing by reading [org.wfanet.measurement.reporting.v2alpha.EventGroup] resources.
 */
@CommandLine.Command(name = "ReadEventGroups", subcommands = [CommandLine.HelpCommand::class])
class ReadEventGroups private constructor() : Runnable {
  @CommandLine.Option(names = ["--reporting-public-api-target"], required = true)
  private lateinit var reportingPublicApiTarget: String

  @CommandLine.Option(names = ["--reporting-public-api-cert-host"], required = false)
  private var reportingPublicApiCertHost: String? = null

  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags

  @set:CommandLine.Option(names = ["--max-qps"], required = false, defaultValue = "10")
  private var maxQps: Double by Delegates.notNull()

  @CommandLine.Option(names = ["--rpc-timeout"], required = false, defaultValue = "10s")
  private lateinit var rpcTimeout: Duration

  private lateinit var throttler: MaximumRateThrottler
  private lateinit var eventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub

  private fun init() {
    throttler = MaximumRateThrottler(maxQps)

    val clientCerts =
      SigningCerts.fromPemFiles(
        tlsFlags.certFile,
        tlsFlags.privateKeyFile,
        tlsFlags.certCollectionFile,
      )
    val serviceConfig =
      ProtobufServiceConfig.DEFAULT.copy {
        methodConfig[0] = methodConfig[0].copy { timeout = rpcTimeout.toProtoDuration() }
      }
    val apiChannel: ManagedChannel =
      buildMutualTlsChannel(
        reportingPublicApiTarget,
        clientCerts,
        hostName = reportingPublicApiCertHost,
        defaultServiceConfig = serviceConfig,
      )
    eventGroupsStub = EventGroupsGrpcKt.EventGroupsCoroutineStub(apiChannel)
  }

  override fun run() {
    CommandLine.usage(this, System.err)
    exitProcess(1)
  }

  @CommandLine.Command(name = "list", description = ["Calls ListEventGroups until interrupted"])
  private fun list(
    @CommandLine.Option(names = ["--parent"], required = true) parent: String,
    @CommandLine.Option(names = ["--media-type"]) mediaTypes: Set<MediaType>,
    @CommandLine.Option(names = ["--metadata-search-query"], defaultValue = "")
    metadataSearchQuery: String,
    @CommandLine.Option(names = ["--page-token"], required = false, defaultValue = "")
    pageToken: String,
    @CommandLine.Option(names = ["--legacy"], required = false, defaultValue = "false")
    legacy: Boolean,
  ) {
    init()

    runBlocking {
      val latencies = flow {
        while (coroutineContext.isActive) {
          val request = listEventGroupsRequest {
            this.parent = parent
            this.pageToken = pageToken
            if (legacy) {
              val terms = buildList {
                if (mediaTypes.isNotEmpty()) {
                  add(mediaTypes.joinToString(" || ") { "${it.number} in media_types" })
                }
                if (metadataSearchQuery.isNotEmpty()) {
                  add(
                    "$BRAND_NAME_FIELD.contains(\"$metadataSearchQuery\") || " +
                      "$CAMPAIGN_NAME_FIELD).contains(\"$metadataSearchQuery\")"
                  )
                }
              }
              filter = terms.joinToString(" && ") { "($it)" }
            } else {
              orderBy =
                ListEventGroupsRequestKt.orderBy {
                  field = ListEventGroupsRequest.OrderBy.Field.DATA_AVAILABILITY_START_TIME
                  descending = true
                }
              structuredFilter =
                ListEventGroupsRequestKt.filter {
                  mediaTypesIntersect += mediaTypes
                  this.metadataSearchQuery = metadataSearchQuery
                }
            }
          }

          throttler.acquire()
          val start = TimeSource.Monotonic.markNow()
          try {
            val response: ListEventGroupsResponse = eventGroupsStub.listEventGroups(request)
            System.err.println(
              "Received ${response.eventGroupsCount} EventGroups. " +
                "Next page token: ${response.nextPageToken}"
            )
            emit(start.elapsedNow())
          } catch (e: StatusException) {
            throw Exception("Error listing EventGroups", e)
          }
        }
      }

      val latenciesMillis = mutableListOf<Long>()
      Runtime.getRuntime()
        .addShutdownHook(
          object : Thread() {
            override fun run() {
              outputStats(latenciesMillis)
            }
          }
        )
      latencies.collect { latenciesMillis.add(it.inWholeMilliseconds) }
    }
  }

  private fun outputStats(latenciesMillis: Collection<Long>) {
    val stats = Stats.of(latenciesMillis)
    val mean: kotlin.time.Duration = stats.mean().milliseconds
    val standardDeviation: kotlin.time.Duration = stats.populationStandardDeviation().milliseconds
    val p50: kotlin.time.Duration =
      Quantiles.percentiles().index(50).compute(latenciesMillis).milliseconds
    val p95: kotlin.time.Duration =
      Quantiles.percentiles().index(95).compute(latenciesMillis).milliseconds

    println("Latency stats")
    println("Count: ${latenciesMillis.size}")
    println("Mean: $mean")
    println("Standard deviation: $standardDeviation")
    println("50th percentile: $p50")
    println("95th percentile: $p95")
  }

  companion object {
    private const val CAMPAIGN_METADATA_FIELD = "event_group_metadata.ad_metadata.campaign_metadata"
    private const val BRAND_NAME_FIELD = "$CAMPAIGN_METADATA_FIELD.brand_name"
    private const val CAMPAIGN_NAME_FIELD = "$CAMPAIGN_METADATA_FIELD.campaign_name"

    @JvmStatic fun main(args: Array<String>) = commandLineMain(ReadEventGroups(), args)
  }
}

