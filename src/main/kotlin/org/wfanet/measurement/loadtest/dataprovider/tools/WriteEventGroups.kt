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

import com.google.type.date
import com.google.type.interval
import io.grpc.ManagedChannel
import io.grpc.StatusException
import io.grpc.serviceconfig.copy
import java.io.PrintStream
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import kotlin.properties.Delegates
import kotlin.system.exitProcess
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.deleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupActivity
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.updateEventGroupActivityRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.ProtobufServiceConfig
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.throttler.MaximumRateThrottler
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import picocli.CommandLine

/** Tool for load testing by writing [EventGroup] resources. */
@CommandLine.Command(name = "WriteEventGroups", subcommands = [CommandLine.HelpCommand::class])
class WriteEventGroups(
  private val injectedEventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub? = null,
  private val injectedEventGroupActivitiesStub:
    EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub? =
    null,
) : Runnable {
  @CommandLine.Option(names = ["--kingdom-public-api-target"], required = true)
  private lateinit var kingdomPublicApiTarget: String

  @CommandLine.Option(names = ["--kingdom-public-api-cert-host"], required = false)
  private var kingdomPublicApiCertHost: String? = null

  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags

  @CommandLine.Option(names = ["--measurement-consumer"], required = true)
  private lateinit var measurementConsumerName: String

  @set:CommandLine.Option(names = ["--max-qps"], required = false, defaultValue = "10")
  private var maxQps: Double by Delegates.notNull()

  @set:CommandLine.Option(
    names = ["--parallelism"],
    description = ["Maximum number of in-flight requests"],
    required = false,
    defaultValue = "10",
  )
  private var parallelism: Int by Delegates.notNull()

  @CommandLine.Option(names = ["--rpc-timeout"], required = false, defaultValue = "10s")
  private lateinit var rpcTimeout: Duration

  @CommandLine.Spec private lateinit var commandSpec: CommandLine.Model.CommandSpec

  private lateinit var throttler: MaximumRateThrottler
  private lateinit var semaphore: Semaphore
  private lateinit var dispatcher: CoroutineDispatcher
  private lateinit var eventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub
  private lateinit var eventGroupActivitiesStub:
    EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub

  private fun init() {
    commandSpec.commandLine().setUseSimplifiedAtFiles(true)
    throttler = MaximumRateThrottler(maxQps)
    dispatcher = Dispatchers.IO.limitedParallelism(parallelism)
    semaphore = Semaphore(parallelism)

    if (injectedEventGroupsStub != null && injectedEventGroupActivitiesStub != null) {
      eventGroupsStub =
        injectedEventGroupsStub.withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
      eventGroupActivitiesStub =
        injectedEventGroupActivitiesStub.withDeadlineAfter(
          rpcTimeout.toMillis(),
          TimeUnit.MILLISECONDS,
        )
      return
    }

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
    eventGroupActivitiesStub =
      EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub(apiChannel)
  }

  override fun run() {
    CommandLine.usage(this, System.err)
    exitProcess(1)
  }

  @CommandLine.Command(name = "create")
  private fun create(
    @CommandLine.Option(names = ["--parent"], required = true) parent: String,
    @CommandLine.Option(names = ["--count"], required = true) count: Int,
    @CommandLine.ArgGroup(multiplicity = "1..*", exclusive = false)
    campaignOptions: List<CampaignOptions>,
  ) {
    init()
    // Print the options for an update command so it can be passed in using @file syntax. This is
    // also useful to keep track of what was created so it can be easily deleted.
    printUpdateOptions()

    runBlocking {
      repeat(count) { index ->
        val request = createEventGroupRequest {
          this.parent = parent
          eventGroup = eventGroup {
            fillEventGroup(index, campaignOptions.flatMap { it.toMetadata() })
          }
        }

        semaphore.acquire()
        throttler.acquire()
        launch(dispatcher) {
          try {
            val response: EventGroup = eventGroupsStub.createEventGroup(request)
            println("--event-group=${response.name}")
          } finally {
            semaphore.release()
          }
        }
      }
    }
  }

  private inline fun looping(loop: Boolean, action: () -> Unit) {
    if (loop) {
      while (true) {
        action()
      }
    } else {
      action()
    }
  }

  @CommandLine.Command(name = "update")
  private fun update(
    @CommandLine.ArgGroup(multiplicity = "1..*", exclusive = false)
    campaignOptions: List<CampaignOptions>,
    @CommandLine.Option(names = ["--event-group"], required = true) eventGroupNames: List<String>,
    @CommandLine.Option(names = ["--loop"], required = false, defaultValue = "false") loop: Boolean,
  ) {
    init()

    looping(loop) {
      runBlocking {
        eventGroupNames.forEachIndexed { index, eventGroupName ->
          val request = updateEventGroupRequest {
            eventGroup = eventGroup {
              name = eventGroupName
              fillEventGroup(index, campaignOptions.flatMap { it.toMetadata() })
            }
          }
          val referenceId: String = request.eventGroup.eventGroupReferenceId

          semaphore.acquire()
          throttler.acquire()
          launch(dispatcher) {
            try {
              eventGroupsStub.updateEventGroup(request)
              System.err.println("Updated $eventGroupName with reference ID $referenceId")
            } catch (e: StatusException) {
              val message = "Error updating $eventGroupName with reference ID $referenceId"
              if (loop) {
                System.err.println(message)
                System.err.println(e)
              } else {
                throw Exception(message, e)
              }
            } finally {
              semaphore.release()
            }
          }
        }
      }
    }
  }

  @CommandLine.Command(
    name = "update-single",
    description = ["Updates the same event group repeatedly"],
  )
  private fun updateSingle(
    @CommandLine.Option(names = ["--event-group"], required = true) eventGroupName: String,
    @CommandLine.Option(names = ["--count"], required = true) count: Int,
    @CommandLine.ArgGroup(multiplicity = "1..*", exclusive = false)
    campaignOptions: List<CampaignOptions>,
  ) {
    init()

    runBlocking {
      repeat(count) { index ->
        val request = updateEventGroupRequest {
          eventGroup = eventGroup {
            name = eventGroupName
            fillEventGroup(index, campaignOptions.flatMap { it.toMetadata() })
          }
        }

        semaphore.acquire()
        throttler.acquire()
        launch(dispatcher) {
          try {
            System.err.println("Updating $eventGroupName in thread ${Thread.currentThread().name}")
            eventGroupsStub.updateEventGroup(request)
            System.err.println("Updated $eventGroupName")
          } finally {
            semaphore.release()
          }
        }
      }
    }
  }

  @CommandLine.Command(name = "delete")
  private fun delete(
    @CommandLine.Option(names = ["--event-group"], required = true) eventGroupNames: List<String>
  ) {
    init()

    runBlocking {
      for (eventGroupName in eventGroupNames) {
        val request = deleteEventGroupRequest { name = eventGroupName }

        semaphore.acquire()
        throttler.acquire()
        launch(dispatcher) {
          try {
            eventGroupsStub.deleteEventGroup(request)
            System.err.println("Deleted $eventGroupName")
          } finally {
            semaphore.release()
          }
        }
      }
    }
  }

  @CommandLine.Command(name = "create-activities")
  private fun createActivities(
    @CommandLine.Option(names = ["--event-group"], required = true) eventGroupNames: List<String>,
    @CommandLine.Option(names = ["--start-date"], required = true) startDate: String,
    @CommandLine.Option(names = ["--end-date"], required = true) endDate: String,
    @CommandLine.Option(names = ["--batch-size"], defaultValue = "50") batchSize: Int,
    @CommandLine.Option(names = ["--step-days"], defaultValue = "1") stepDays: Long,
  ) {
    init()
    val start = LocalDate.parse(startDate)
    val end = LocalDate.parse(endDate)

    runBlocking {
      for (eventGroupName in eventGroupNames) {
        val dates = mutableListOf<LocalDate>()
        var currentDate = start
        while (!currentDate.isAfter(end)) {
          dates.add(currentDate)
          currentDate = currentDate.plusDays(stepDays)
        }

        dates.chunked(batchSize).forEach { batchDates ->
          val request = batchUpdateEventGroupActivitiesRequest {
            parent = eventGroupName
            requests +=
              batchDates.map { date ->
                updateEventGroupActivityRequest {
                  eventGroupActivity = eventGroupActivity {
                    name = "$eventGroupName/eventGroupActivities/$date"
                    this.date = date {
                      year = date.year
                      month = date.monthValue
                      day = date.dayOfMonth
                    }
                  }
                  allowMissing = true
                }
              }
          }

          semaphore.acquire()
          throttler.acquire()
          launch(dispatcher) {
            try {
              eventGroupActivitiesStub.batchUpdateEventGroupActivities(request)
              println(
                "Created ${batchDates.size} activities for $eventGroupName from ${batchDates.first()} to ${batchDates.last()}"
              )
            } catch (e: Exception) {
              System.err.println(
                "Failed to create activities for $eventGroupName from ${batchDates.first()} to ${batchDates.last()}: $e"
              )
            } finally {
              semaphore.release()
            }
          }
        }
      }
    }
  }

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

  private fun EventGroupKt.Dsl.fillEventGroup(index: Int, allMetadata: List<EventGroupMetadata>) {
    val now = Instant.now()

    measurementConsumer = measurementConsumerName
    eventGroupReferenceId = "$REFERENCE_ID_PREFIX-$index"
    mediaTypes += MediaType.VIDEO
    mediaTypes += MediaType.DISPLAY
    dataAvailabilityInterval = interval { startTime = now.minus(10, ChronoUnit.DAYS).toProtoTime() }
    eventGroupMetadata = allMetadata[index % allMetadata.size]
  }

  private class CampaignOptions {
    @CommandLine.Option(names = ["--brand-name"], required = true)
    lateinit var brandName: String
      private set

    @CommandLine.Option(names = ["--campaign-name"], required = true)
    lateinit var campaignNames: List<String>
      private set

    fun toMetadata(): List<EventGroupMetadata> =
      campaignNames.map { campaignName ->
        eventGroupMetadata {
          adMetadata =
            EventGroupMetadataKt.adMetadata {
              campaignMetadata =
                EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                  brandName = this@CampaignOptions.brandName
                  this.campaignName = campaignName
                }
            }
        }
      }
  }

  companion object {
    private const val REFERENCE_ID_PREFIX = "load-test"

    @JvmStatic
    fun main(args: Array<String>) {
      val commandLine =
        CommandLine(WriteEventGroups()).apply {
          registerConverter(Duration::class.java) { it.toDuration() }
          setUseSimplifiedAtFiles(true)
        }

      val status: Int = commandLine.execute(*args)
      if (status != 0) {
        exitProcess(status)
      }
    }
  }
}
