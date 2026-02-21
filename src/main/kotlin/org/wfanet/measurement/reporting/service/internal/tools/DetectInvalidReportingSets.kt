/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

// DO_NOT_SUBMIT: Not intended to be merged.

package org.wfanet.measurement.reporting.service.internal.tools

import io.grpc.ManagedChannel
import java.time.Duration
import kotlin.system.exitProcess
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.BearerTokenCallCredentials
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSets
import org.wfanet.measurement.reporting.service.api.v2alpha.toExpression
import org.wfanet.measurement.reporting.v2alpha.ListReportsResponse
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import picocli.CommandLine

@CommandLine.Command(
  name = "DetectInvalidReportingSets",
  description = ["Detect Reports with invalid ReportingSets"],
)
class DetectInvalidReportingSets : Runnable {
  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags

  @CommandLine.Option(
    names = ["--bearer-token"],
    description = ["Bearer token for Reporting API auth"],
    required = true,
  )
  private lateinit var bearerToken: String

  @CommandLine.Option(
    names = ["--reporting-api-target"],
    description = ["gRPC target (authority) of the Reporting public API"],
    required = true,
  )
  private lateinit var apiTarget: String

  @CommandLine.Option(
    names = ["--reporting-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the reporting server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --reporting-api-target.",
      ],
    required = false,
  )
  private var apiCertHost: String? = null

  @CommandLine.Option(
    names = ["--reporting-internal-api-target"],
    description = ["gRPC target (authority) of the reporting server's internal API"],
    required = true,
  )
  private lateinit var internalApiTarget: String

  @CommandLine.Option(
    names = ["--reporting-internal-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the reporting server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --reporting-internal-api-target.",
      ],
    required = false,
  )
  private var internalApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--measurement-consumer"],
    description =
      ["Resource name of the MeasurementConsumer", "This can be specified multiple times"],
    required = true,
  )
  private lateinit var measurementConsumerKeys: List<MeasurementConsumerKey>

  @CommandLine.Option(
    names = ["--page-token"],
    description = ["Page token from previous run indicating where to start"],
    required = false,
    defaultValue = "",
  )
  private lateinit var initialPageToken: String

  override fun run() {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )
    val apiChannel: ManagedChannel =
      buildMutualTlsChannel(apiTarget, clientCerts, apiCertHost)
        .withShutdownTimeout(Duration.ofSeconds(1))
    val internalApiChannel: ManagedChannel =
      buildMutualTlsChannel(internalApiTarget, clientCerts, internalApiCertHost)
        .withShutdownTimeout(Duration.ofSeconds(1))
    val reportsStub =
      ReportsGrpcKt.ReportsCoroutineStub(apiChannel)
        .withCallCredentials(BearerTokenCallCredentials(bearerToken))
    val internalReportingSetsStub =
      ReportingSetsGrpcKt.ReportingSetsCoroutineStub(internalApiChannel)

    for (measurementConsumerKey in measurementConsumerKeys) {
      processMeasurementConsumer(reportsStub, internalReportingSetsStub, measurementConsumerKey)
    }
  }

  private fun processMeasurementConsumer(
    reportsStub: ReportsGrpcKt.ReportsCoroutineStub,
    internalReportingSetsStub: ReportingSetsGrpcKt.ReportingSetsCoroutineStub,
    measurementConsumerKey: MeasurementConsumerKey,
  ) {
    val measurementConsumerName = measurementConsumerKey.toName()
    println("Processing MC $measurementConsumerName")

    var pageToken: String = initialPageToken
    initialPageToken = "" // Clear this since page token is only valid for a single MC
    do {
      val listReportsResponse: ListReportsResponse = runBlocking {
        reportsStub.listReports(
          listReportsRequest {
            this.parent = measurementConsumerName
            pageSize = REPORTS_PAGE_SIZE
            this.pageToken = pageToken
          }
        )
      }

      for (report: Report in listReportsResponse.reportsList) {
        val reportingSetKeys: Set<ReportingSetKey> =
          report.reportingMetricEntriesList
            .map { checkNotNull(ReportingSetKey.fromName(it.key)) }
            .toSet()
        val referencedReportingSets: Map<ReportingSetKey, InternalReportingSet> = runBlocking {
          ReportingSets.getReferencedReportingSets(internalReportingSetsStub, reportingSetKeys)
        }
        val referencedReportingSetsByName: Map<String, InternalReportingSet> =
          referencedReportingSets.mapKeys { it.key.toName() }
        for ((key: ReportingSetKey, internalReportingSet: InternalReportingSet) in
          referencedReportingSets) {
          if (!internalReportingSet.hasComposite()) {
            continue
          }
          val weightedSubsetUnions: List<InternalReportingSet.WeightedSubsetUnion> =
            ReportingSets.compileWeightedSubsetUnions(
              internalReportingSet.filter,
              internalReportingSet.composite.toExpression(
                measurementConsumerKey.measurementConsumerId
              ),
              measurementConsumerKey.measurementConsumerId,
              referencedReportingSetsByName,
            )
          if (internalReportingSet.weightedSubsetUnionsList != weightedSubsetUnions) {
            println(
              "Report ${report.name} in state ${report.state} references invalid ReportingSet " +
                key.toName()
            )
          }
        }
      }

      if (listReportsResponse.nextPageToken.isNotEmpty()) {
        println("Next page token: ${listReportsResponse.nextPageToken}")
      }
      pageToken = listReportsResponse.nextPageToken
    } while (pageToken.isNotEmpty())
  }

  companion object {
    private const val REPORTS_PAGE_SIZE = 20

    @JvmStatic
    fun main(args: Array<String>) {
      exitProcess(execute(args))
    }

    fun execute(args: Array<String>): Int {
      return CommandLine(DetectInvalidReportingSets())
        .apply {
          registerConverter(MeasurementConsumerKey::class.java) { value: String ->
            MeasurementConsumerKey.fromName(value)
              ?: throw CommandLine.TypeConversionException("Invalid MC resource name $value")
          }
        }
        .execute(*args)
    }
  }
}
