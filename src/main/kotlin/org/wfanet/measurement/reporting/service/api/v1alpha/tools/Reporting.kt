// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.service.api.v1alpha.tools

import io.grpc.ManagedChannel
import java.time.Duration
import java.time.Instant
import kotlin.properties.Delegates
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.DurationFormat
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.createReportRequest
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.getReportRequest
import org.wfanet.measurement.reporting.v1alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportsRequest
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report
import org.wfanet.measurement.reporting.v1alpha.reportingSet
import org.wfanet.measurement.reporting.v1alpha.timeInterval
import org.wfanet.measurement.reporting.v1alpha.timeIntervals
import picocli.CommandLine

private class ReportingApiFlags {
  @CommandLine.Option(
    names = ["--reporting-server-api-target"],
    description = ["gRPC target (authority) of the reporting server's public API"],
    required = true,
  )
  lateinit var apiTarget: String
    private set

  @CommandLine.Option(
    names = ["--reporting-server-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the reporting server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --reporting-server-api-target.",
      ],
    required = false,
  )
  var apiCertHost: String? = null
    private set
}

private class PageParams {
  @CommandLine.Option(
    names = ["--page-size"],
    description = ["The maximum number of items to return. The maximum value is 1000"],
    required = false,
  )
  var pageSize: Int = 1000
    private set

  @CommandLine.Option(
    names = ["--page-token"],
    description = ["Page token from a previous list call to retrieve the next page"],
    defaultValue = "",
    required = false,
  )
  lateinit var pageToken: String
    private set
}

@CommandLine.Command(name = "create", description = ["Creates a reporting set"])
class CreateReportingSetCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportingSetsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Option(
    names = ["--event-group"],
    description = ["List of EventGroup's API resource names"],
    required = true,
  )
  private lateinit var eventGroups: List<String>

  @CommandLine.Option(
    names = ["--filter"],
    description = ["CEL filter predicate that applies to all `event_groups`"],
    required = false,
    defaultValue = ""
  )
  private lateinit var filterExpression: String

  @CommandLine.Option(
    names = ["--display-name"],
    description = ["Human-readable name for display purposes"],
    required = false,
    defaultValue = ""
  )
  private lateinit var displayNameInput: String

  override fun run() {
    val request = createReportingSetRequest {
      parent = measurementConsumerName
      reportingSet = reportingSet {
        eventGroups += this@CreateReportingSetCommand.eventGroups
        filter = filterExpression
        displayName = displayNameInput
      }
    }
    val reportingSet =
      runBlocking(Dispatchers.IO) { parent.reportingSetStub.createReportingSet(request) }
    println(reportingSet)
  }
}

@CommandLine.Command(name = "list", description = ["List reporting sets"])
class ListReportingSetsCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportingSetsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val request = listReportingSetsRequest {
      parent = measurementConsumerName
      pageSize = pageParams.pageSize
      pageToken = pageParams.pageToken
    }

    val response =
      runBlocking(Dispatchers.IO) { parent.reportingSetStub.listReportingSets(request) }

    println(response)
  }
}

@CommandLine.Command(
  name = "reporting-sets",
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreateReportingSetCommand::class,
      ListReportingSetsCommand::class,
    ]
)
class ReportingSetsCommand : Runnable {
  @CommandLine.ParentCommand lateinit var parent: Reporting
  val reportingSetStub: ReportingSetsCoroutineStub by lazy {
    ReportingSetsCoroutineStub(parent.channel)
  }
  override fun run() {}
}

@CommandLine.Command(name = "create", description = ["Create a set operation report"])
class CreateReportCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Option(
    names = ["--idempotency-key"],
    description = ["Used as the prefix of the idempotency keys of measurements"],
    required = true,
  )
  private lateinit var idempotencyKey: String

  class EventGroupInput {
    @CommandLine.Option(
      names = ["--event-group-key"],
      description = ["Event Group Entry's key"],
      required = true,
    )
    lateinit var key: String
      private set

    @CommandLine.Option(
      names = ["--event-group-value"],
      description = ["Event Group Entry's value"],
      defaultValue = "",
      required = false,
    )
    lateinit var value: String
      private set
  }

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Event Group Entries\n")
  private lateinit var eventGroups: List<EventGroupInput>

  class TimeInput {
    class TimeIntervalInput {
      @CommandLine.Option(
        names = ["--interval-start-time"],
        description = ["Start of time interval in ISO 8601 format of UTC"],
        required = true,
      )
      lateinit var intervalStartTime: Instant
        private set

      @CommandLine.Option(
        names = ["--interval-end-time"],
        description = ["End of time interval in ISO 8601 format of UTC"],
        required = true,
      )
      lateinit var intervalEndTime: Instant
        private set
    }

    class PeriodicTimeIntervalInput {
      @CommandLine.Option(
        names = ["--periodic-interval-start-time"],
        description = ["Start of the first time interval in ISO 8601 format of UTC"],
        required = true,
      )
      lateinit var periodicIntervalStartTime: Instant
        private set

      @CommandLine.Option(
        names = ["--periodic-interval-increment"],
        description = ["Increment for each time interval in ISO-8601 format of PnDTnHnMn"],
        required = true
      )
      lateinit var periodicIntervalIncrement: Duration
        private set

      @set:CommandLine.Option(
        names = ["--periodic-interval-count"],
        description = ["Number of periodic intervals"],
        required = true
      )
      var periodicIntervalCount by Delegates.notNull<Int>()
        private set
    }

    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Time intervals\n")
    var timeIntervals: List<TimeIntervalInput>? = null
      private set

    @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1",
      heading = "Periodic time interval specification\n"
    )
    var periodicTimeIntervalInput: PeriodicTimeIntervalInput? = null
      private set
  }

  @CommandLine.ArgGroup(
    exclusive = true,
    multiplicity = "1",
    heading = "Time interval or periodic time interval\n"
  )
  private lateinit var timeInput: TimeInput

  @CommandLine.Option(
    names = ["--metric"],
    description = ["Metric protobuf messages in text format"],
    required = true,
  )
  private lateinit var textFormatMetrics: List<String>

  override fun run() {
    val request = createReportRequest {
      parent = measurementConsumerName
      report = report {
        reportIdempotencyKey = idempotencyKey
        measurementConsumer = measurementConsumerName
        eventGroupUniverse = eventGroupUniverse {
          eventGroups.forEach {
            eventGroupEntries += eventGroupEntry {
              key = it.key
              value = it.value
            }
          }
        }

        // Either timeIntervals or periodicTimeIntervalInput are set.
        if (timeInput.timeIntervals != null) {
          val intervals = checkNotNull(timeInput.timeIntervals)
          timeIntervals = timeIntervals {
            intervals.forEach {
              timeIntervals += timeInterval {
                startTime = it.intervalStartTime.toProtoTime()
                endTime = it.intervalEndTime.toProtoTime()
              }
            }
          }
        } else {
          val periodicIntervals = checkNotNull(timeInput.periodicTimeIntervalInput)
          periodicTimeInterval = periodicTimeInterval {
            startTime = periodicIntervals.periodicIntervalStartTime.toProtoTime()
            increment = periodicIntervals.periodicIntervalIncrement.toProtoDuration()
            intervalCount = periodicIntervals.periodicIntervalCount
          }
        }

        for (textFormatMetric in textFormatMetrics) {
          metrics +=
            textFormatMetric.reader().use { parseTextProto(it, Metric.getDefaultInstance()) }
        }
      }
    }
    val report = runBlocking(Dispatchers.IO) { parent.reportsStub.createReport(request) }

    println(report)
  }
}

@CommandLine.Command(name = "list", description = ["List set operation reports"])
class ListReportsCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val request = listReportsRequest {
      parent = measurementConsumerName
      pageSize = pageParams.pageSize
      pageToken = pageParams.pageToken
    }

    val response = runBlocking(Dispatchers.IO) { parent.reportsStub.listReports(request) }

    response.reportsList.forEach { println(it.name + " " + it.state.toString()) }
    if (response.nextPageToken.isNotEmpty()) {
      println("nextPageToken: ${response.nextPageToken}")
    }
  }
}

@CommandLine.Command(name = "get", description = ["Get a set operation report"])
class GetReportCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: ReportsCommand

  @CommandLine.Parameters(
    description = ["API resource name of the Report"],
  )
  private lateinit var reportName: String

  override fun run() {
    val request = getReportRequest { name = reportName }

    val report = runBlocking(Dispatchers.IO) { parent.reportsStub.getReport(request) }
    println(report)
  }
}

@CommandLine.Command(
  name = "reports",
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreateReportCommand::class,
      ListReportsCommand::class,
      GetReportCommand::class,
    ]
)
class ReportsCommand : Runnable {
  @CommandLine.ParentCommand lateinit var parent: Reporting

  val reportsStub: ReportsCoroutineStub by lazy { ReportsCoroutineStub(parent.channel) }

  override fun run() {}
}

@CommandLine.Command(name = "list", description = ["List event groups"])
class ListEventGroups : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: EventGroupsCommand

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Data Provider"],
    required = true,
  )
  private lateinit var dataProviderName: String

  @CommandLine.Option(
    names = ["--filter"],
    description = ["Result filter in format of raw CEL expression"],
    required = false,
    defaultValue = ""
  )
  private lateinit var celFilter: String

  @CommandLine.Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val request = listEventGroupsRequest {
      parent = dataProviderName
      pageSize = pageParams.pageSize
      pageToken = pageParams.pageToken
      filter = celFilter
    }

    val response = runBlocking(Dispatchers.IO) { parent.eventGroupStub.listEventGroups(request) }

    println(response)
  }
}

@CommandLine.Command(
  name = "event-groups",
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      ListEventGroups::class,
    ]
)
class EventGroupsCommand : Runnable {
  @CommandLine.ParentCommand lateinit var parent: Reporting

  val eventGroupStub: EventGroupsCoroutineStub by lazy { EventGroupsCoroutineStub(parent.channel) }

  override fun run() {}
}

@CommandLine.Command(
  name = "reporting",
  description = ["Reporting CLI tool"],
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      ReportingSetsCommand::class,
      ReportsCommand::class,
      EventGroupsCommand::class,
    ]
)
class Reporting : Runnable {
  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags
  @CommandLine.Mixin private lateinit var apiFlags: ReportingApiFlags

  val channel: ManagedChannel by lazy {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile
      )
    buildMutualTlsChannel(apiFlags.apiTarget, clientCerts, apiFlags.apiCertHost)
      .withShutdownTimeout(Duration.ofSeconds(1))
  }
  override fun run() {}

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(Reporting(), args, DurationFormat.ISO_8601)
  }
}

/**
 * Create, List and Get reporting set or report.
 *
 * Use the `help` command to see usage details.
 */
fun main(args: Array<String>) = commandLineMain(Reporting(), args)
