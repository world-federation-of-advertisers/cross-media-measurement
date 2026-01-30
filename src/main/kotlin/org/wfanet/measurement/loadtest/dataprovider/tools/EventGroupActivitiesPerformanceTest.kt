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

import io.grpc.ManagedChannel
import java.io.File
import java.time.Duration
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import kotlin.system.exitProcess
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.toDuration
import picocli.CommandLine

@CommandLine.Command(
  name = "EventGroupActivitiesPerformanceTest",
  subcommands = [CommandLine.HelpCommand::class],
)
class EventGroupActivitiesPerformanceTest : Runnable {
  @CommandLine.Option(names = ["--kingdom-public-api-target"], required = true)
  private lateinit var kingdomPublicApiTarget: String

  @CommandLine.Option(names = ["--kingdom-public-api-cert-host"], required = false)
  private var kingdomPublicApiCertHost: String? = null

  @CommandLine.Option(names = ["--reporting-public-api-target"], required = true)
  private lateinit var reportingPublicApiTarget: String

  @CommandLine.Option(names = ["--reporting-public-api-cert-host"], required = false)
  private var reportingPublicApiCertHost: String? = null

  @CommandLine.Option(names = ["--kingdom-tls-cert-file"], required = true)
  private lateinit var kingdomTlsCertFile: File

  @CommandLine.Option(names = ["--kingdom-tls-key-file"], required = true)
  private lateinit var kingdomTlsKeyFile: File

  @CommandLine.Option(names = ["--kingdom-cert-collection-file"], required = true)
  private lateinit var kingdomCertCollectionFile: File

  @CommandLine.Option(names = ["--reporting-tls-cert-file"], required = true)
  private lateinit var reportingTlsCertFile: File

  @CommandLine.Option(names = ["--reporting-tls-key-file"], required = true)
  private lateinit var reportingTlsKeyFile: File

  @CommandLine.Option(names = ["--reporting-cert-collection-file"], required = true)
  private lateinit var reportingCertCollectionFile: File

  @CommandLine.Option(
    names = ["--measurement-consumer"],
    required = true,
    description = ["Resource name of the MeasurementConsumer"],
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Option(
    names = ["--data-provider"],
    required = true,
    description = ["Resource name of the DataProvider"],
  )
  private lateinit var dataProviderName: String

  private fun buildKingdomChannel(): ManagedChannel {
    val clientCerts =
      SigningCerts.fromPemFiles(kingdomTlsCertFile, kingdomTlsKeyFile, kingdomCertCollectionFile)
    return buildMutualTlsChannel(
      kingdomPublicApiTarget,
      clientCerts,
      hostName = kingdomPublicApiCertHost,
    )
  }

  override fun run() {
    CommandLine.usage(this, System.err)
    exitProcess(1)
  }

  @CommandLine.Command(name = "write-perf")
  fun writePerf() = runBlocking {
    val kingdomChannel = buildKingdomChannel()
    val kingdomEventGroupsStub = EventGroupsGrpcKt.EventGroupsCoroutineStub(kingdomChannel)

    val eventGroupName =
      createEventGroup(
        kingdomEventGroupsStub,
        dataProviderName,
        measurementConsumerName,
        "ega-write-perf",
      )
    println("--event-group=$eventGroupName")

    val startDate = LocalDate.of(2024, 1, 1)
    val endDate = startDate.plusWeeks(10).minusDays(1)
    val days = ChronoUnit.DAYS.between(startDate, endDate) + 1
    val batchSize = 7
    val requestsPerIteration = (days + batchSize - 1) / batchSize

    val testDurationSeconds = 60
    val latencies = LongArray(testDurationSeconds)

    println("Starting ega write load test ($testDurationSeconds iterations)...")
    val startTime = System.currentTimeMillis()

    repeat(testDurationSeconds) { i ->
      val start = System.currentTimeMillis()
      val exitCode =
        CommandLine(WriteEventGroups())
          .registerConverter(Duration::class.java) { it.toDuration() }
          .execute(
            "--kingdom-public-api-target=$kingdomPublicApiTarget",
            "--tls-cert-file=$kingdomTlsCertFile",
            "--tls-key-file=$kingdomTlsKeyFile",
            "--cert-collection-file=$kingdomCertCollectionFile",
            "--measurement-consumer=$measurementConsumerName",
            "--max-qps=20",
            "--parallelism=5",
            "--rpc-timeout=60s",
            "create-activities",
            "--event-group=$eventGroupName",
            "--start-date=$startDate",
            "--end-date=$endDate",
            "--batch-size=$batchSize",
          )
      if (exitCode != 0) throw RuntimeException("WriteEventGroups failed with exit code $exitCode")

      val elapsed = System.currentTimeMillis() - start
      latencies[i] = elapsed
      val sleep = 1000 - elapsed
      if (sleep > 0) delay(sleep)
    }
    val totalTimeSeconds = (System.currentTimeMillis() - startTime) / 1000.0

    val mean = latencies.average()
    val max = latencies.maxOrNull() ?: 0
    val totalRequests = testDurationSeconds * requestsPerIteration
    val qps = totalRequests / totalTimeSeconds
    println("Write Load Test Stats (over $testDurationSeconds seconds):")
    println("QPS: %.2f".format(qps))
    println("Mean Latency: %.2f ms".format(mean))
    println("Max Latency: $max ms")
  }

  @CommandLine.Command(name = "read-perf")
  fun readPerf() = runBlocking {
    val kingdomChannel = buildKingdomChannel()
    val kingdomEventGroupsStub = EventGroupsGrpcKt.EventGroupsCoroutineStub(kingdomChannel)

    val totalEventGroups = 1000
    val startDate = LocalDate.of(2024, 1, 1)
    val endDate = startDate.plusDays(364)

    println("Creating $totalEventGroups EventGroups...")
    val eventGroupNames =
      (1..totalEventGroups)
        .map {
          async {
            createEventGroup(
              kingdomEventGroupsStub,
              dataProviderName,
              measurementConsumerName,
              "ega-list-perf-$it",
            )
          }
        }
        .awaitAll()
    eventGroupNames.forEach { println("--event-group=$it") }

    val commonArgs =
      listOf(
        "--kingdom-public-api-target=$kingdomPublicApiTarget",
        "--tls-cert-file=$kingdomTlsCertFile",
        "--tls-key-file=$kingdomTlsKeyFile",
        "--cert-collection-file=$kingdomCertCollectionFile",
        "--measurement-consumer=$measurementConsumerName",
        "--max-qps=50",
        "--parallelism=50",
        "--rpc-timeout=120s",
        "create-activities",
        "--start-date=$startDate",
        "--end-date=$endDate",
        "--batch-size=365",
      )

    println("Populating first ${totalEventGroups / 2} EventGroups (daily activities)...")
    val argsDaily =
      commonArgs +
        listOf("--step-days=1") +
        eventGroupNames.take(totalEventGroups / 2).map { "--event-group=$it" }

    var exitCode =
      CommandLine(WriteEventGroups())
        .registerConverter(Duration::class.java) { it.toDuration() }
        .execute(*argsDaily.toTypedArray())
    if (exitCode != 0) throw RuntimeException("WriteEventGroups (Daily) failed")

    println("Populating next ${totalEventGroups / 2} EventGroups (every other day)...")
    val argsSparse =
      commonArgs +
        listOf("--step-days=2") +
        eventGroupNames.drop(totalEventGroups / 2).map { "--event-group=$it" }

    exitCode =
      CommandLine(WriteEventGroups())
        .registerConverter(Duration::class.java) { it.toDuration() }
        .execute(*argsSparse.toTypedArray())
    if (exitCode != 0) throw RuntimeException("WriteEventGroups (Sparse) failed")

    // Filter for a 90-day window
    val filterStart = startDate.plusDays(100)
    val filterEnd = startDate.plusDays(190)

    println("Benchmarking listEventGroups with activity_contains filter in summary view...")
    exitCode =
      CommandLine(ReadEventGroups())
        .registerConverter(Duration::class.java) { it.toDuration() }
        .execute(
          "--reporting-public-api-target=$reportingPublicApiTarget",
          "--reporting-public-api-cert-host=${reportingPublicApiCertHost ?: ""}",
          "--tls-cert-file=$reportingTlsCertFile",
          "--tls-key-file=$reportingTlsKeyFile",
          "--cert-collection-file=$reportingCertCollectionFile",
          "list",
          "--parent=$measurementConsumerName",
          "--activity-contains-start-date=$filterStart",
          "--activity-contains-end-date=$filterEnd",
          "--view=WITH_ACTIVITY_SUMMARY",
        )
    if (exitCode != 0) throw RuntimeException("ReadEventGroups failed")
  }

  private suspend fun createEventGroup(
    stub: EventGroupsGrpcKt.EventGroupsCoroutineStub,
    parent: String,
    mc: String,
    refId: String,
  ): String {
    val response =
      stub.createEventGroup(
        createEventGroupRequest {
          this.parent = parent
          eventGroup = eventGroup {
            measurementConsumer = mc
            eventGroupReferenceId = refId
          }
        }
      )
    return response.name
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
      val commandLine =
        CommandLine(EventGroupActivitiesPerformanceTest()).apply {
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
