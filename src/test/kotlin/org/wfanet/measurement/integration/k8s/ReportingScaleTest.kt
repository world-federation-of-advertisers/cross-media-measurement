/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.common.truth.Truth.assertThat
import com.google.devtools.build.runfiles.Runfiles
import com.google.gson.Gson
import com.google.protobuf.util.JsonFormat
import io.grpc.ManagedChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.KeyPair
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.tls.HandshakeCertificates
import okhttp3.tls.HeldCertificate
import okhttp3.tls.decodeCertificatePem
import org.junit.Test
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.loadtest.reporting.testing.ReportingScaleTestData
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.reportingSet

/** Exercises report creation through deployed Reporting APIs at increasing state cardinality. */
class ReportingScaleTest {
  @Test
  fun `CreateBasicReport accepts geographically segmented AMI and MRC reports`(): Unit =
    runBlocking {
      val measurementConsumer = requiredEnvironmentVariable("MC_NAME")
      val reportingTarget = requiredEnvironmentVariable("REPORTING_PUBLIC_API_TARGET")
      val accessTarget = requiredEnvironmentVariable("ACCESS_PUBLIC_API_TARGET")
      val endpoint = requiredEnvironmentVariable("REPORTING_SERVICE_ENDPOINT").toHttpUrl()
      val audience = requiredEnvironmentVariable("REPORTING_TOKEN_AUDIENCE")
      val modelLine = requiredEnvironmentVariable("HIGH_OVERLAP_MODEL_LINE")
      val weeks = System.getenv("REPORTING_SCALE_WEEKS")?.toInt() ?: 13
      val stateCounts =
        (System.getenv("REPORTING_SCALE_STATE_COUNTS") ?: "0,50").split(',').map {
          it.trim().toInt()
        }
      val filterIds =
        (System.getenv("REPORTING_SCALE_FILTER_IDS") ?: "ami,mrc").split(',').map(String::trim)
      val repetitions = System.getenv("REPORTING_SCALE_REPETITIONS")?.toInt() ?: 1
      val deadline =
        Duration.ofSeconds(System.getenv("REPORTING_SCALE_DEADLINE_SECONDS")?.toLong() ?: 60L)
      val reportStart =
        LocalDate.parse(System.getenv("REPORTING_SCALE_REPORT_START") ?: "2026-04-01")
      require(weeks in 1..13 && stateCounts.isNotEmpty() && stateCounts.all { it in 0..51 })
      require(repetitions in 1..10 && deadline.seconds in 1..600)
      val output = Paths.get(requiredEnvironmentVariable("TEST_UNDECLARED_OUTPUTS_DIR"))
      Files.createDirectories(output)
      val records = mutableListOf<MutableMap<String, Any?>>()
      val channels = mutableListOf<ManagedChannel>()
      try {
        val reportingChannel =
          buildMutualTlsChannel(reportingTarget, signingCerts("mc_tls", "reporting_root"), null)
            .also(channels::add)
        val accessChannel =
          buildMutualTlsChannel(accessTarget, signingCerts("access_tls", "reporting_root"), null)
            .also(channels::add)
        val eventGroupsClient =
          EventGroupsCoroutineStub(reportingChannel.withDefaultDeadline(deadline))
        val reportingSetsClient =
          ReportingSetsCoroutineStub(reportingChannel.withDefaultDeadline(deadline))
        val accessToken =
          AbstractCorrectnessTest.reportingAccessTokenProvider(
            measurementConsumer = measurementConsumer,
            accessChannel = accessChannel.withDefaultDeadline(deadline),
            audience = audience,
            scopes = REPORTING_SCOPES,
            ttl = Duration.ofMinutes(60),
          )
        val httpClient = httpClient(deadline)
        val eventGroups = resolveEventGroups(eventGroupsClient, measurementConsumer)
        val dataProviders = eventGroups.map(EventGroup::getCmmsDataProvider).distinct()
        check(dataProviders.size == 3) {
          "Scale test requires three distinct EDPs, got $dataProviders"
        }
        for (stateCount in stateCounts) {
          repeat(repetitions) { repetition ->
            val runId = UUID.randomUUID().toString()
            val caseName = "states-$stateCount-repeat-${repetition + 1}"
            val basicReportId = "reporting-scale-$runId"
            val basicReportName = "$measurementConsumer/basicReports/$basicReportId"
            val record =
              linkedMapOf<String, Any?>(
                  "case" to caseName,
                  "seed" to System.getenv("REPORTING_SCALE_SEED"),
                  "weeks" to weeks,
                  "state_count" to stateCount,
                  "filter_ids" to filterIds,
                  "basic_report" to basicReportName,
                  "deadline_seconds" to deadline.seconds,
                  "started_at" to Instant.now().toString(),
                )
                .also(records::add)
            try {
              record["stage"] = "campaign_group"
              val campaignId = "campaign-scale-$runId"
              val campaign =
                reportingSetsClient.createReportingSet(
                  createReportingSetRequest {
                    parent = measurementConsumer
                    reportingSetId = campaignId
                    reportingSet = reportingSet {
                      displayName = "Reporting scale test $runId"
                      primitive =
                        ReportingSetKt.primitive {
                          cmmsEventGroups += eventGroups.map(EventGroup::getCmmsEventGroup)
                        }
                      campaignGroup = "$measurementConsumer/reportingSets/$campaignId"
                    }
                  }
                )
              val requestMessage =
                ReportingScaleTestData.buildReport(
                  campaignGroup = campaign.name,
                  dataProviders = dataProviders,
                  modelLine = modelLine,
                  reportStart = reportStart,
                  weeks = weeks,
                  stateCount = stateCount,
                  filterIds = filterIds,
                )
              val requestBody = JsonFormat.printer().print(requestMessage)
              record["result_group_spec_count"] = requestMessage.resultGroupSpecsCount
              record["request_bytes"] = requestBody.toByteArray(Charsets.UTF_8).size
              Files.writeString(output.resolve("$caseName-request.json"), requestBody)
              val createUrl =
                endpoint
                  .newBuilder()
                  .addPathSegments("v2alpha/$measurementConsumer/basicReports")
                  .addQueryParameter("basic_report_id", basicReportId)
                  .addQueryParameter("request_id", runId)
                  .build()
              val createRequest =
                Request.Builder()
                  .url(createUrl)
                  .post(requestBody.toRequestBody())
                  .header("Content-Type", "application/json; charset=utf-8")
                  .header("Authorization", "Bearer ${accessToken()}")
                  .build()
              record["stage"] = "create"
              val created =
                exchange(
                  httpClient,
                  createRequest,
                  output.resolve("$caseName-create.json"),
                  record,
                  "create",
                )
              assertThat(created.name).isEqualTo(basicReportName)
              record["stage"] = "readback"
              val getRequest =
                Request.Builder()
                  .url(endpoint.newBuilder().addPathSegments("v2alpha/$basicReportName").build())
                  .get()
                  .header("Authorization", "Bearer ${accessToken()}")
                  .build()
              val retrieved =
                exchange(
                  httpClient,
                  getRequest,
                  output.resolve("$caseName-get.json"),
                  record,
                  "get",
                )
              assertThat(retrieved.name).isEqualTo(basicReportName)
              assertThat(retrieved.resultGroupSpecsList)
                .containsExactlyElementsIn(requestMessage.resultGroupSpecsList)
              record["state"] = retrieved.state.name
              record["outcome"] = "created"
              logger.info(
                "$caseName created in ${record["create_ms"]} ms; readback state=${retrieved.state}"
              )
            } catch (e: Throwable) {
              record["outcome"] = "failed"
              record["error"] = "${e.javaClass.simpleName}: ${e.message}"
              if (record["stage"] == "create") {
                try {
                  val request =
                    Request.Builder()
                      .url(
                        endpoint.newBuilder().addPathSegments("v2alpha/$basicReportName").build()
                      )
                      .get()
                      .header("Authorization", "Bearer ${accessToken()}")
                      .build()
                  val retrieved =
                    exchange(
                      httpClient.newBuilder().callTimeout(Duration.ofSeconds(5)).build(),
                      request,
                      output.resolve("$caseName-after-failure.json"),
                      record,
                      "after_failure_get",
                    )
                  record["after_failure_state"] = retrieved.state.name
                } catch (readError: Exception) {
                  record["after_failure_get_error"] = readError.message
                }
              }
              throw e
            } finally {
              Files.writeString(output.resolve("summary.json"), Gson().toJson(records))
            }
          }
        }
      } finally {
        for (channel in channels) {
          channel.shutdown()
          channel.awaitTermination(5, TimeUnit.SECONDS)
        }
      }
    }

  private suspend fun resolveEventGroups(
    client: EventGroupsCoroutineStub,
    measurementConsumer: String,
  ): List<EventGroup> {
    val references =
      (System.getenv("REPORTING_SCALE_EVENT_GROUP_REFERENCE_IDS")
          ?: DEFAULT_EVENT_GROUP_REFERENCES.joinToString(","))
        .split(',')
        .map(String::trim)
    require(references.size >= 3 && references.distinct().size == references.size)
    val selected = mutableMapOf<String, EventGroup>()
    var pageToken = ""
    do {
      val response =
        client.listEventGroups(
          listEventGroupsRequest {
            parent = measurementConsumer
            pageSize = 500
            this.pageToken = pageToken
            structuredFilter = ListEventGroupsRequestKt.filter { entityTypeIn += "ad_group" }
          }
        )
      for (eventGroup in response.eventGroupsList) {
        if (eventGroup.eventGroupReferenceId in references)
          selected[eventGroup.eventGroupReferenceId] = eventGroup
      }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty() && selected.size < references.size)
    check(selected.keys.containsAll(references)) {
      "Missing seeded EventGroups ${references - selected.keys}. Run the EDPA reporting integration test to seed the high overlap dataset first."
    }
    return references.map(selected::getValue)
  }

  private fun exchange(
    client: OkHttpClient,
    request: Request,
    output: Path,
    record: MutableMap<String, Any?>,
    prefix: String,
  ): BasicReport {
    val started = System.nanoTime()
    try {
      return client.newCall(request).execute().use { response ->
        val body = checkNotNull(response.body).string()
        record["${prefix}_http_status"] = response.code
        record["${prefix}_response_bytes"] = body.toByteArray(Charsets.UTF_8).size
        Files.writeString(output, body)
        check(response.isSuccessful) { "$prefix returned HTTP ${response.code}: $body" }
        BasicReport.newBuilder()
          .also { JsonFormat.parser().ignoringUnknownFields().merge(body, it) }
          .build()
      }
    } finally {
      record["${prefix}_ms"] = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
    }
  }

  private fun httpClient(deadline: Duration): OkHttpClient {
    val certificate =
      runtimePath("src/main/k8s/testing/secretfiles/mc_tls.pem")
        .toFile()
        .readText()
        .decodeCertificatePem()
    val key =
      readPrivateKey(
        runtimePath("src/main/k8s/testing/secretfiles/mc_tls.key").toFile(),
        certificate.publicKey.algorithm,
      )
    val certificates =
      HandshakeCertificates.Builder()
        .addTrustedCertificate(
          runtimePath("src/main/k8s/testing/secretfiles/reporting_root.pem")
            .toFile()
            .readText()
            .decodeCertificatePem()
        )
        .heldCertificate(HeldCertificate(KeyPair(certificate.publicKey, key), certificate))
        .build()
    return OkHttpClient.Builder()
      .sslSocketFactory(certificates.sslSocketFactory(), certificates.trustManager)
      .connectTimeout(deadline)
      .readTimeout(deadline)
      .writeTimeout(deadline)
      .callTimeout(deadline)
      .build()
  }

  private fun signingCerts(name: String, root: String): SigningCerts =
    SigningCerts.fromPemFiles(
      runtimePath("src/main/k8s/testing/secretfiles/$name.pem").toFile(),
      runtimePath("src/main/k8s/testing/secretfiles/$name.key").toFile(),
      runtimePath("src/main/k8s/testing/secretfiles/$root.pem").toFile(),
    )

  private fun runtimePath(path: String): Path =
    checkNotNull(RUNFILES.getRuntimePath(Paths.get("wfa_measurement_system", path))) {
      "Runtime path not found: $path"
    }

  private fun requiredEnvironmentVariable(name: String): String =
    checkNotNull(System.getenv(name)?.takeIf(String::isNotEmpty)) { "$name must be set" }

  companion object {
    private val RUNFILES = Runfiles.preload().withSourceRepository("")
    private val logger = Logger.getLogger(ReportingScaleTest::class.java.name)
    private val DEFAULT_EVENT_GROUP_REFERENCES =
      listOf(
        "ad_group-high-overlap-all4-edp7-1",
        "ad_group-high-overlap-all4-edpa_meta-1",
        "ad_group-high-overlap-all4-edpa_video_pub-1",
      )
    private val REPORTING_SCOPES =
      setOf(
        "reporting.basicReports.create",
        "reporting.basicReports.get",
        "reporting.reports.create",
        "reporting.metrics.create",
        "reporting.metricCalculationSpecs.create",
        "reporting.eventGroups.list",
        "reporting.reportingSets.createPrimitive",
        "reporting.reportingSets.createComposite",
      )
  }
}
