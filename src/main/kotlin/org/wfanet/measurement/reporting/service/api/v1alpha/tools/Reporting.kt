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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v1alpha.reportingSet
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

@CommandLine.Command(name = "create-reporting-set", description = ["Creates a reporting set"])
class CreateReportingSetCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: Report

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
  @CommandLine.ParentCommand private lateinit var parent: Report

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
  override fun run() {}
}

@CommandLine.Command(name = "list-reports", description = ["List set operation reports"])
class ListReportsCommand : Runnable {
  override fun run() {}
}

@CommandLine.Command(name = "get-report", description = ["Get a set operation report"])
class GetReportCommand : Runnable {
  override fun run() {}
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
class Report : Runnable {
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
fun main(args: Array<String>) = commandLineMain(Report(), args)
