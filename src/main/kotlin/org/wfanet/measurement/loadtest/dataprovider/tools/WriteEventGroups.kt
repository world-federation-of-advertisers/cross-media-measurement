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

import com.google.type.interval
import io.grpc.ManagedChannel
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.serviceconfig.copy
import java.io.PrintStream
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.properties.Delegates
import kotlin.system.exitProcess
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.deleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.ProtobufServiceConfig
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.throttler.MaximumRateThrottler
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import picocli.CommandLine

/** Tool for load testing by writing [EventGroup] resources. */
@CommandLine.Command(name = "WriteEventGroups", subcommands = [CommandLine.HelpCommand::class])
class WriteEventGroups private constructor() : Runnable {
  @CommandLine.Option(names = ["--kingdom-public-api-target"], required = true)
  private lateinit var kingdomPublicApiTarget: String

  @CommandLine.Option(names = ["--kingdom-public-api-cert-host"], required = false)
  private var kingdomPublicApiCertHost: String? = null

  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags

  @CommandLine.Option(names = ["--measurement-consumer"], required = true)
  private lateinit var measurementConsumerName: String

  @set:CommandLine.Option(names = ["--max-qps"], required = false, defaultValue = "10")
  private var maxQps: Double by Delegates.notNull()

  @set:CommandLine.Option(names = ["--parallelism"], required = false, defaultValue = "10")
  private var parallelism: Int by Delegates.notNull()

  @CommandLine.Option(names = ["--rpc-timeout"], required = false, defaultValue = "10s")
  private lateinit var rpcTimeout: Duration

  @CommandLine.Spec private lateinit var commandSpec: CommandLine.Model.CommandSpec

  private lateinit var throttler: MaximumRateThrottler
  private lateinit var dispatcher: CoroutineDispatcher
  private lateinit var eventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub

  private fun init() {
    throttler = MaximumRateThrottler(maxQps)
    dispatcher = Dispatchers.IO.limitedParallelism(parallelism)

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
        kingdomPublicApiTarget,
        clientCerts,
        hostName = kingdomPublicApiCertHost,
        defaultServiceConfig = serviceConfig,
      )
    eventGroupsStub = EventGroupsGrpcKt.EventGroupsCoroutineStub(apiChannel)
  }

  override fun run() {
    CommandLine.usage(this, System.err)
    exitProcess(1)
  }

  @CommandLine.Command(name = "create")
  fun create(
    @CommandLine.Option(names = ["--parent"], required = true) parent: String,
    @CommandLine.Option(names = ["--count"], required = true) count: Int,
  ) {
    init()
    printUpdateOptions()

    runBlocking {
      repeat(count) { index ->
        val request = createEventGroupRequest {
          this.parent = parent
          eventGroup = eventGroup { fillEventGroup(index) }
        }

        launchThrottled {
          val response: EventGroup = eventGroupsStub.createEventGroup(request)
          println("--event-group=${response.name}")
        }
      }
    }
  }

  @CommandLine.Command(name = "update")
  fun update(
    @CommandLine.Option(names = ["--event-group"], required = true) eventGroupNames: List<String>
  ) {
    init()

    runBlocking {
      eventGroupNames.forEachIndexed { index, eventGroupName ->
        val request = updateEventGroupRequest {
          eventGroup = eventGroup {
            name = eventGroupName
            fillEventGroup(index)
          }
        }

        launchThrottled {
          try {
            eventGroupsStub.updateEventGroup(request)
          } catch (e: StatusException) {
            if (e.status.code == Status.Code.NOT_FOUND) {
              throw Exception(
                "EventGroup $eventGroupName with reference ID ${request.eventGroup.eventGroupReferenceId} not found",
                e,
              )
            }
            throw e
          }
          System.err.println("Updated $eventGroupName")
        }
      }
    }
  }

  @CommandLine.Command(
    name = "update-single",
    description = ["Updates the same event group repeatedly"],
  )
  fun updateSingle(
    @CommandLine.Option(names = ["--event-group"], required = true) eventGroupName: String,
    @CommandLine.Option(names = ["--count"], required = true) count: Int,
  ) {
    init()

    runBlocking {
      repeat(count) { index ->
        val request = updateEventGroupRequest {
          eventGroup = eventGroup {
            name = eventGroupName
            fillEventGroup(index)
          }
        }
        launchThrottled {
          System.err.println("Updating $eventGroupName in thread ${Thread.currentThread().name}")
          eventGroupsStub.updateEventGroup(request)
          System.err.println("Updated $eventGroupName")
        }
      }
    }
  }

  @CommandLine.Command(name = "delete")
  fun delete(
    @CommandLine.Option(names = ["--event-group"], required = true) eventGroupNames: List<String>
  ) {
    init()

    runBlocking {
      for (eventGroupName in eventGroupNames) {
        val request = deleteEventGroupRequest { name = eventGroupName }

        launchThrottled {
          eventGroupsStub.deleteEventGroup(request)
          System.err.println("Deleted $eventGroupName")
        }
      }
    }
  }

  private fun CoroutineScope.launchThrottled(block: suspend CoroutineScope.() -> Unit) =
    launch(dispatcher) { throttler.onReady { block() } }

  private fun PrintStream.printArgs(
    parseResult: CommandLine.ParseResult,
    excluded: List<String> = emptyList(),
    swappedCommands: Map<String, String> = emptyMap(),
  ) {
    for (option: CommandLine.Model.OptionSpec in parseResult.matchedOptionsSet()) {
      for (value: String in option.originalStringValues()) {
        val name = option.longestName()
        if (name in excluded) {
          continue
        }
        println("$name=$value")
      }
    }

    if (parseResult.hasSubcommand()) {
      val subcommand: CommandLine.ParseResult = parseResult.subcommand()
      val name = subcommand.commandSpec().name()

      println(swappedCommands.getOrDefault(name, name))
      printArgs(subcommand, excluded, swappedCommands)
    }
  }

  private fun printUpdateOptions() {
    System.out.printArgs(
      commandSpec.commandLine().parseResult,
      listOf("--count", "--parent"),
      mapOf("create" to "update"),
    )
  }

  private fun EventGroupKt.Dsl.fillEventGroup(index: Int) {
    val now = Instant.now()

    measurementConsumer = measurementConsumerName
    eventGroupReferenceId = "$REFERENCE_ID_PREFIX-$index"
    dataAvailabilityInterval = interval { startTime = now.minus(10, ChronoUnit.DAYS).toProtoTime() }
    eventGroupMetadata = eventGroupMetadata {
      adMetadata =
        EventGroupMetadataKt.adMetadata {
          campaignMetadata =
            EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
              campaignName = "Campaign$index"
              brandName = "Brand$index"
            }
        }
    }
  }

  companion object {
    private const val REFERENCE_ID_PREFIX = "load-test"

    @JvmStatic fun main(args: Array<String>) = commandLineMain(WriteEventGroups(), args)
  }
}
