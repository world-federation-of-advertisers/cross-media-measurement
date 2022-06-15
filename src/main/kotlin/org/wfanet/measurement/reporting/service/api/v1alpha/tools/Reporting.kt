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
import com.google.protobuf.TextFormat
import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import com.google.protobuf.Duration as ProtoDuration
import com.google.protobuf.duration as protoDuration
import java.time.Duration
import java.time.Instant
import kotlin.properties.Delegates
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.createReportRequest
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.getReportRequest
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportsRequest
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report
import org.wfanet.measurement.reporting.v1alpha.reportingSet
import org.wfanet.measurement.reporting.v1alpha.timeInterval
import org.wfanet.measurement.reporting.v1alpha.timeIntervals
import picocli.CommandLine

private const val PAGE_SIZE = 1000

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

@CommandLine.Command(name = "create-reporting-set", description = ["Creates a reporting set"])
class CreateReportingSetCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: Reporting

  @CommandLine.Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  @CommandLine.Option(
    names = ["--event-groups"],
    arity = "1..*",
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
    val reportingSetStub = ReportingSetsCoroutineStub(parent.channel)

    val reportingSet =
      runBlocking(Dispatchers.IO) {
        reportingSetStub.createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerName
            reportingSet = reportingSet {
              eventGroups += this@CreateReportingSetCommand.eventGroups
              if (filterExpression.isNotEmpty()) {
                filter = filterExpression
              }
              if (displayNameInput.isNotEmpty()) displayName = displayNameInput
            }
          }
        )
      }
    print(reportingSet)
  }
}

@CommandLine.Command(name = "list-reporting-sets", description = ["List reporting sets"])
class ListReportingSetsCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: Reporting

  @CommandLine.Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  override fun run() {
    val reportingSetStub = ReportingSetsCoroutineStub(parent.channel)

    val response =
      runBlocking(Dispatchers.IO) {
        reportingSetStub.listReportingSets(
          listReportingSetsRequest { parent = measurementConsumerName }
        )
      }

    val reportingSets = response.reportingSetsList
    print(reportingSets)
  }
}

@CommandLine.Command(name = "create-report", description = ["Create a set operation report"])
class CreateReportCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: Reporting

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

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

  @CommandLine.ArgGroup(
    exclusive = false,
    multiplicity = "1..*",
    heading = "Event Group Entries\n"
  )
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

      @set:CommandLine.Option(
        names = ["--periodic-interval-count"],
        description = ["Number of periodic intervals"],
        required = true
      )
      var periodicIntervalCount by Delegates.notNull<Int>()
        private set
    }

    @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1..*",
      heading = "Time intervals\n"
    )
    var timeIntervals: List<TimeIntervalInput> = emptyList()
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
    description = ["String of serialized Metric"],
    required = true,
  )
  private lateinit var serializedMetrics: List<String>

  private fun convertToTimestamp(instant: Instant): Timestamp {
    return timestamp {
      seconds = instant.epochSecond
      nanos = instant.nano
    }
  }

  private fun convertToProtoDuration(duration: Duration): ProtoDuration {
    return protoDuration {
      seconds = duration.seconds
      nanos = duration.nano
    }
  }

  override fun run() {
    val reportsStub = ReportsCoroutineStub(parent.channel)

    val request = createReportRequest {
      parent = measurementConsumerName
      report = report {
        measurementConsumer = measurementConsumerName
        eventGroupUniverse = eventGroupUniverse {
          eventGroups.map{
            eventGroupEntries += eventGroupEntry {
              key = it.key
              value = it.value
            }
          }
        }
        // Either timeIntervals or periodicTimeIntervalInput is set.
        timeInput.timeIntervals.map {
          timeIntervals = timeIntervals {
            timeIntervals += timeInterval {
              startTime = convertToTimestamp(it.intervalStartTime)
              endTime = convertToTimestamp(it.intervalEndTime)
            }
          }
        }
        timeInput.periodicTimeIntervalInput?.let {
          periodicTimeInterval = periodicTimeInterval {
            startTime = convertToTimestamp(it.periodicIntervalStartTime)
            increment = convertToProtoDuration(it.periodicIntervalIncrement)
            intervalCount = it.periodicIntervalCount
          }
        }

        serializedMetrics.map {
          val metricsBuilder = Metric.newBuilder()
          TextFormat.getParser().merge(it, metricsBuilder)
          metrics += metricsBuilder.build()
        }
      }

    }
    val report = runBlocking(Dispatchers.IO) {
      reportsStub.createReport(request)
    }

    println(report)
  }
}

@CommandLine.Command(name = "list-reports", description = ["List set operation reports"])
class ListReportsCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: Reporting

  @CommandLine.Option(
    names = ["--parent"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  override fun run() {
    val reportsStub = ReportsCoroutineStub(parent.channel)

    val request = listReportsRequest {
      parent = measurementConsumerName
      pageSize = PAGE_SIZE
    }

    val response = runBlocking(Dispatchers.IO) {
      reportsStub.listReports(request)
    }

    response.reportsList.map {
      println(it.name + " " + it.state.toString())
    }
  }
}

@CommandLine.Command(name = "get-report", description = ["Get a set operation report"])
class GetReportCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: Reporting

  @CommandLine.Option(
    names = ["--name"],
    description = ["API resource name of the Report"],
    required = true,
  )
  private lateinit var reportName: String

  override fun run() {
    val reportsStub = ReportsCoroutineStub(parent.channel)

    val request = getReportRequest {
      name = reportName
    }

    val report = runBlocking(Dispatchers.IO) {
      reportsStub.getReport(request)
    }
    println(report)
  }
}

@CommandLine.Command(
  name = "SimpleReport",
  description = ["Simple report from Kingdom"],
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreateReportingSetCommand::class,
      ListReportingSetsCommand::class,
      CreateReportCommand::class,
      ListReportsCommand::class,
      GetReportCommand::class,
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
}

/**
 * Create, List and Get reporting set or report.
 *
 * Use the `help` command to see usage details.
 */
fun main(args: Array<String>) = commandLineMain(Reporting(), args)
