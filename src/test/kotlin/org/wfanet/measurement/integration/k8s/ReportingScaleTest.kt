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
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.tls.HandshakeCertificates
import okhttp3.tls.HeldCertificate
import okhttp3.tls.decodeCertificatePem
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.ListMeasurementsRequestKt
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.loadtest.reporting.BasicReportHttpClient
import org.wfanet.measurement.loadtest.reporting.testing.ReportingScaleTestData
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.batchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.reportingSet

/**
 * Exercises creation and EDPA fulfillment through deployed APIs at increasing state cardinality.
 */
class ReportingScaleTest {
  @Test
  fun `EDPA fulfills geographically segmented AMI and MRC reports`(): Unit = runBlocking {
    val measurementConsumer = requiredEnvironmentVariable("MC_NAME")
    val reportingTarget = requiredEnvironmentVariable("REPORTING_PUBLIC_API_TARGET")
    val kingdomTarget = requiredEnvironmentVariable("KINGDOM_PUBLIC_API_TARGET")
    val apiKey = requiredEnvironmentVariable("MC_API_KEY")
    val accessTarget = requiredEnvironmentVariable("ACCESS_PUBLIC_API_TARGET")
    val endpoint = requiredEnvironmentVariable("REPORTING_SERVICE_ENDPOINT").toHttpUrl()
    val audience = requiredEnvironmentVariable("REPORTING_TOKEN_AUDIENCE")
    val modelLine =
      System.getenv("REPORTING_SCALE_MODEL_LINE")?.takeIf(String::isNotBlank)
        ?: requiredEnvironmentVariable("HIGH_OVERLAP_MODEL_LINE")
    val reportDays = System.getenv("REPORTING_SCALE_REPORT_DAYS")?.toInt() ?: 60
    val demographics =
      (System.getenv("REPORTING_SCALE_DEMOGRAPHIC_FIELDS") ?: "age,gender")
        .split(',')
        .map(String::trim)
        .toSet()
    require(
      demographics == setOf("none") ||
        demographics.isNotEmpty() && demographics.all { it in setOf("age", "gender") }
    )
    val groupByAge = "age" in demographics
    val groupByGender = "gender" in demographics
    val stateCounts =
      (System.getenv("REPORTING_SCALE_STATE_COUNTS") ?: "50").split(',').map { it.trim().toInt() }
    val filterIds =
      (System.getenv("REPORTING_SCALE_FILTER_IDS") ?: "ami,mrc").split(',').map(String::trim)
    val repetitions = System.getenv("REPORTING_SCALE_REPETITIONS")?.toInt() ?: 1
    val deadline =
      Duration.ofSeconds(System.getenv("REPORTING_SCALE_DEADLINE_SECONDS")?.toLong() ?: 60L)
    val fulfillmentTimeout =
      Duration.ofSeconds(
        System.getenv("REPORTING_SCALE_FULFILLMENT_TIMEOUT_SECONDS")?.toLong() ?: 3600L
      )
    require(fulfillmentTimeout.seconds in 60..14400)
    val reportStart = LocalDate.parse(System.getenv("REPORTING_SCALE_REPORT_START") ?: "2026-04-01")
    require(reportDays in 1..91 && stateCounts.isNotEmpty() && stateCounts.all { it in 0..51 })
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
          ttl = fulfillmentTimeout.plus(Duration.ofMinutes(30)),
        )
      val kingdomChannel =
        buildMutualTlsChannel(kingdomTarget, signingCerts("mc_tls", "mc_trusted_certs"), null)
          .also(channels::add)
      val measurementsClient =
        MeasurementsCoroutineStub(kingdomChannel.withDefaultDeadline(Duration.ofSeconds(60)))
          .withAuthenticationKey(apiKey)
      val requisitionsClient =
        RequisitionsCoroutineStub(kingdomChannel.withDefaultDeadline(Duration.ofSeconds(60)))
          .withAuthenticationKey(apiKey)
      val reportsClient =
        ReportsCoroutineStub(reportingChannel.withDefaultDeadline(Duration.ofMinutes(5)))
          .withMaxInboundMessageSize(64 * 1024 * 1024)
      val metricsClient =
        MetricsCoroutineStub(reportingChannel.withDefaultDeadline(Duration.ofSeconds(60)))
          .withMaxInboundMessageSize(64 * 1024 * 1024)
      val httpClient = httpClient(deadline)
      val readClient =
        httpClient
          .newBuilder()
          .callTimeout(Duration.ofMinutes(5))
          .readTimeout(Duration.ofMinutes(5))
          .build()
      val eventGroups = resolveEventGroups(eventGroupsClient, measurementConsumer)
      val dataProviders = eventGroups.map(EventGroup::getCmmsDataProvider).distinct()
      check(dataProviders.size == 3) {
        "Scale test requires three distinct EDPs, got $dataProviders"
      }
      val existingReportName =
        System.getenv("REPORTING_SCALE_EXISTING_BASIC_REPORT")?.takeIf(String::isNotEmpty)
      if (existingReportName != null) {
        require(existingReportName.startsWith("$measurementConsumer/basicReports/reporting-scale-"))
        val getRequest =
          Request.Builder()
            .url(endpoint.newBuilder().addPathSegments("v2alpha/$existingReportName").build())
            .get()
            .header("Authorization", "Bearer ${accessToken()}")
            .build()
        val record =
          linkedMapOf<String, Any?>(
              "case" to "existing-report",
              "basic_report" to existingReportName,
              "state_count" to stateCounts.single(),
              "report_days" to reportDays,
              "group_by_age" to groupByAge,
              "group_by_gender" to groupByGender,
              "creation_reused" to true,
              "stage" to "fulfillment",
            )
            .also(records::add)
        try {
          val report =
            exchange(
              readClient,
              getRequest,
              output.resolve("existing-report-get.json"),
              record,
              "get",
            )
          awaitFulfillment(
            existingReportName,
            report.createTime.toInstant().minusSeconds(1),
            fulfillmentTimeout,
            dataProviders,
            measurementsClient,
            requisitionsClient,
            reportsClient,
            metricsClient,
            readClient,
            endpoint,
            accessToken,
            output,
            "existing-report",
            record,
            records,
          )
          record["outcome"] = "fulfilled_existing"
        } catch (e: Throwable) {
          record["outcome"] = "failed"
          record["error"] = "${e.javaClass.simpleName}: ${e.message}"
          throw e
        } finally {
          Files.writeString(output.resolve("summary.json"), Gson().toJson(records))
        }
        return@runBlocking
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
                "report_days" to reportDays,
                "report_start" to reportStart.toString(),
                "event_group_reference_ids" to
                  eventGroups.map(EventGroup::getEventGroupReferenceId),
                "cmms_event_groups" to eventGroups.map(EventGroup::getCmmsEventGroup),
                "data_providers" to dataProviders,
                "model_line" to modelLine,
                "group_by_age" to groupByAge,
                "group_by_gender" to groupByGender,
                "state_count" to stateCount,
                "filter_ids" to filterIds,
                "basic_report" to basicReportName,
                "deadline_seconds" to deadline.seconds,
                "fulfillment_timeout_seconds" to fulfillmentTimeout.seconds,
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
                reportDays = reportDays,
                stateCount = stateCount,
                filterIds = filterIds,
                groupByAge = groupByAge,
                groupByGender = groupByGender,
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
              exchange(httpClient, getRequest, output.resolve("$caseName-get.json"), record, "get")
            assertThat(retrieved.name).isEqualTo(basicReportName)
            assertThat(retrieved.resultGroupSpecsList)
              .containsExactlyElementsIn(requestMessage.resultGroupSpecsList)
            record["state"] = retrieved.state.name
            record["creation_completed_at"] = Instant.now().toString()
            record["stage"] = "fulfillment"
            awaitFulfillment(
              basicReportName,
              Instant.parse(record.getValue("started_at") as String),
              fulfillmentTimeout,
              dataProviders,
              measurementsClient,
              requisitionsClient,
              reportsClient,
              metricsClient,
              readClient,
              endpoint,
              accessToken,
              output,
              caseName,
              record,
              records,
            )
            record["outcome"] = "succeeded"
            record["stage"] = "complete"
            logger.info(
              "$caseName succeeded; requisitions per EDP=${record["requisitions_per_data_provider"]}"
            )
          } catch (e: Throwable) {
            record["outcome"] = "failed"
            record["error"] = "${e.javaClass.simpleName}: ${e.message}"
            if (record["stage"] == "create") {
              try {
                val request =
                  Request.Builder()
                    .url(endpoint.newBuilder().addPathSegments("v2alpha/$basicReportName").build())
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

  private suspend fun awaitFulfillment(
    basicReportName: String,
    startedAt: Instant,
    timeout: Duration,
    dataProviders: List<String>,
    measurementsClient: MeasurementsCoroutineStub,
    requisitionsClient: RequisitionsCoroutineStub,
    reportsClient: ReportsCoroutineStub,
    metricsClient: MetricsCoroutineStub,
    httpClient: OkHttpClient,
    endpoint: okhttp3.HttpUrl,
    accessToken: () -> String,
    output: Path,
    caseName: String,
    record: MutableMap<String, Any?>,
    records: List<Map<String, Any?>>,
  ) {
    val fulfillmentStarted = System.nanoTime()
    val measurements = linkedMapOf<String, Measurement>()
    val measurementConsumer = basicReportName.substringBefore("/basicReports/")
    try {
      kotlinx.coroutines.withTimeout(timeout.toMillis()) {
        // A bounded sample provides diagnostics and the underlying Report name.
        // Complete IDs come from final metric results, avoiding broad measurement scans.
        val response =
          measurementsClient.listMeasurements(
            listMeasurementsRequest {
              parent = measurementConsumer
              pageSize = 100
              filter =
                ListMeasurementsRequestKt.filter {
                  createdAfter = startedAt.toProtoTime()
                  createdBefore = Instant.now().toProtoTime()
                }
            }
          )
        for (measurement in response.measurementsList) {
          val spec = measurement.measurementSpec.unpack<MeasurementSpec>()
          if (spec.reportingMetadata.basicReport == basicReportName) {
            measurements[measurement.name] = measurement
          }
        }
        record["measurement_listing_truncated"] = response.nextPageToken.isNotEmpty()
        record["observed_measurement_count"] = measurements.size
        check(measurements.isNotEmpty()) { "Creation produced no linked Kingdom measurements" }
        val reportNames =
          measurements.values
            .map { it.measurementSpec.unpack<MeasurementSpec>().reportingMetadata.report }
            .toSet()
        check(reportNames.size == 1 && reportNames.single().isNotEmpty()) {
          "Expected one underlying Report, got $reportNames"
        }
        val reportName = reportNames.single()
        record["report"] = reportName
        val pollingClient =
          BasicReportHttpClient(
            httpClient,
            ExponentialBackoff(Duration.ofSeconds(5), 2.0, 0.0),
            Duration.ofSeconds(20),
            3,
          )
        var completed: BasicReport? = null
        while (completed == null) {
          val states = measurements.values.groupingBy { it.state.name }.eachCount()
          record["observed_measurement_states"] = states
          Files.writeString(
            output.resolve("$caseName-measurements.json"),
            Gson()
              .toJson(
                measurements.values.map {
                  mapOf(
                    "name" to it.name,
                    "state" to it.state.name,
                    "data_providers" to it.dataProvidersList.map { entry -> entry.key },
                    "failure" to it.failure.message,
                  )
                }
              ),
          )
          Files.writeString(output.resolve("summary.json"), Gson().toJson(records))
          record["observed_pending_stage"] =
            when {
              measurements.values.any {
                it.state == Measurement.State.AWAITING_REQUISITION_FULFILLMENT
              } -> "edpa_fulfillment"
              measurements.values.any { it.state == Measurement.State.COMPUTING } ->
                "measurement_computation"
              else -> "report_assembly"
            }
          logger.info("$caseName measurement states=$states")
          val failed =
            measurements.values.filter {
              it.state == Measurement.State.FAILED || it.state == Measurement.State.CANCELLED
            }
          check(failed.isEmpty()) {
            "Kingdom measurement failure: ${failed.take(5).map { "${it.name}: ${it.failure}" }}"
          }
          val request =
            Request.Builder()
              .url(endpoint.newBuilder().addPathSegments("v2alpha/$basicReportName").build())
              .get()
              .header("Authorization", "Bearer ${accessToken()}")
              .build()
          val basicReport = pollingClient.getBasicReport(request)
          Files.writeString(
            output.resolve("$caseName-get.json"),
            JsonFormat.printer().print(basicReport),
          )
          record["state"] = basicReport.state.name
          if (basicReport.state == BasicReport.State.SUCCEEDED) {
            completed = basicReport
          } else {
            check(basicReport.state == BasicReport.State.RUNNING) {
              "BasicReport ${basicReport.state}: $basicReport"
            }
            delay(60_000)
            val pending =
              measurements.values.filter { it.state != Measurement.State.SUCCEEDED }.map { it.name }
            val updated =
              mapBounded(pending.chunked(50)) { names ->
                  val response =
                    measurementsClient.batchGetMeasurements(
                      batchGetMeasurementsRequest {
                        parent = measurementConsumer
                        this.names += names
                      }
                    )
                  check(response.measurementsList.map { it.name }.toSet() == names.toSet()) {
                    "BatchGetMeasurements returned an incomplete batch"
                  }
                  response.measurementsList
                }
                .flatten()
            for (measurement in updated) measurements[measurement.name] = measurement
          }
        }
        record["fulfillment_ms"] =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fulfillmentStarted)
        val verificationStarted = System.nanoTime()
        record["stage"] = "verify_report_metrics"
        val report = reportsClient.getReport(getReportRequest { name = reportName })
        Files.writeString(
          output.resolve("$caseName-report.json"),
          JsonFormat.printer().print(report),
        )
        check(
          report.basicReport == basicReportName &&
            report.state == org.wfanet.measurement.reporting.v2alpha.Report.State.SUCCEEDED
        )
        val attributes = report.metricCalculationResultsList.flatMap { it.resultAttributesList }
        val metricNames = attributes.map { it.metric }.toSet()
        check(metricNames.isNotEmpty() && metricNames.all(String::isNotEmpty)) {
          "Report has no metrics"
        }
        check(attributes.all { it.state == Metric.State.SUCCEEDED }) {
          "Report contains unfinished metrics"
        }
        val metrics =
          mapBounded(metricNames.toList().chunked(100)) { names ->
              metricsClient
                .batchGetMetrics(
                  batchGetMetricsRequest {
                    parent = measurementConsumer
                    this.names += names
                  }
                )
                .metricsList
            }
            .flatten()
        check(metrics.map { it.name }.toSet() == metricNames) { "Missing Report metrics" }
        check(
          metrics.all {
            it.state == Metric.State.SUCCEEDED &&
              it.hasResult() &&
              it.basicReport == basicReportName
          }
        ) {
          "Unfinished, empty, or incorrectly linked metric"
        }
        val resultMeasurementNames = metrics.flatMap { it.result.cmmsMeasurementsList }.toSet()
        check(metrics.all { it.result.cmmsMeasurementsCount > 0 }) {
          "Metric is missing measurements"
        }
        check(resultMeasurementNames.containsAll(measurements.keys)) {
          "Metric/measurement linkage is incomplete"
        }
        record["metric_count"] = metrics.size
        record["metric_states"] = metrics.groupingBy { it.state.name }.eachCount()
        val finalMeasurements =
          mapBounded(resultMeasurementNames.toList().chunked(50)) { names ->
              measurementsClient
                .batchGetMeasurements(
                  batchGetMeasurementsRequest {
                    parent = measurementConsumer
                    this.names += names
                  }
                )
                .measurementsList
            }
            .flatten()
        check(
          finalMeasurements.map { it.name }.toSet() == resultMeasurementNames &&
            finalMeasurements.all { it.state == Measurement.State.SUCCEEDED }
        ) {
          "Missing or unfinished Kingdom measurements"
        }
        measurements.clear()
        for (measurement in finalMeasurements) {
          val metadata = measurement.measurementSpec.unpack<MeasurementSpec>().reportingMetadata
          check(metadata.basicReport == basicReportName && metadata.report == reportName) {
            "Measurement belongs to another report"
          }
          measurements[measurement.name] = measurement
        }
        record["measurement_count"] = measurements.size
        record["expected_requisitions_per_data_provider"] =
          finalMeasurements
            .flatMap { it.dataProvidersList.map { entry -> entry.key } }
            .groupingBy { it }
            .eachCount()
        Files.writeString(
          output.resolve("$caseName-measurements.json"),
          Gson()
            .toJson(
              finalMeasurements.map {
                mapOf(
                  "name" to it.name,
                  "state" to it.state.name,
                  "data_providers" to it.dataProvidersList.map { entry -> entry.key },
                )
              }
            ),
        )
        record["measurement_states"] = finalMeasurements.groupingBy { it.state.name }.eachCount()
        record["stage"] = "verify_requisitions"
        val requisitions =
          mapBounded(measurements.values.toList()) { measurement ->
              val result = mutableListOf<Requisition>()
              var token = ""
              do {
                val response =
                  requisitionsClient.listRequisitions(
                    listRequisitionsRequest {
                      parent = measurement.name
                      pageSize = 100
                      pageToken = token
                    }
                  )
                result += response.requisitionsList
                token = response.nextPageToken
              } while (token.isNotEmpty())
              val expectedProviders = measurement.dataProvidersList.map { it.key }.toSet()
              check(
                result.size == expectedProviders.size &&
                  result
                    .map { it.dataProviderCertificate.substringBefore("/certificates/") }
                    .toSet() == expectedProviders
              ) {
                "Missing or duplicated requisitions for ${measurement.name}"
              }
              check(
                result.all {
                  it.measurement == measurement.name && it.state == Requisition.State.FULFILLED
                }
              ) {
                "Unfulfilled requisitions for ${measurement.name}: ${result.map { "${it.name}: ${it.state}" }}"
              }
              result
            }
            .flatten()
        val counts =
          requisitions
            .groupingBy { it.dataProviderCertificate.substringBefore("/certificates/") }
            .eachCount()
        check(dataProviders.all { (counts[it] ?: 0) > 0 }) {
          "An EDP did not participate in fulfillment"
        }
        record["requisition_count"] = requisitions.size
        record["requisitions_per_data_provider"] = counts
        record["requisition_states"] = requisitions.groupingBy { it.state.name }.eachCount()
        Files.writeString(
          output.resolve("$caseName-requisitions.json"),
          Gson()
            .toJson(
              requisitions.map {
                mapOf(
                  "name" to it.name,
                  "measurement" to it.measurement,
                  "data_provider" to it.dataProviderCertificate.substringBefore("/certificates/"),
                  "state" to it.state.name,
                )
              }
            ),
        )
        check(checkNotNull(completed).resultGroupsCount > 0) { "BasicReport has no result groups" }
        record["result_group_count"] = checkNotNull(completed).resultGroupsCount
        record["verification_ms"] =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - verificationStarted)
        record["completed_at"] = Instant.now().toString()
      }
    } finally {
      record["end_to_end_ms"] = Duration.between(startedAt, Instant.now()).toMillis()
      Files.writeString(output.resolve("summary.json"), Gson().toJson(records))
    }
  }

  /** Limits read concurrency so verification does not become another scale workload. */
  private suspend fun <T, R> mapBounded(items: List<T>, transform: suspend (T) -> R): List<R> =
    items.chunked(4).flatMap { batch ->
      coroutineScope { batch.map { item -> async { transform(item) } }.awaitAll() }
    }

  private suspend fun resolveEventGroups(
    client: EventGroupsCoroutineStub,
    measurementConsumer: String,
  ): List<EventGroup> {
    val references =
      (System.getenv("REPORTING_SCALE_EVENT_GROUP_REFERENCE_IDS")?.takeIf(String::isNotBlank)
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
            // CMMS defaults an empty entity type filter to campaign, rather than all types.
            structuredFilter =
              ListEventGroupsRequestKt.filter {
                entityTypeIn += listOf("campaign", "ad_group", "creative-id")
              }
          }
        )
      for (eventGroup in response.eventGroupsList) {
        if (eventGroup.eventGroupReferenceId in references)
          selected[eventGroup.eventGroupReferenceId] = eventGroup
      }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty() && selected.size < references.size)
    check(selected.keys.containsAll(references)) {
      "Missing seeded EventGroups ${references - selected.keys}. The selected dataset must already be seeded before running this test."
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
        record["${prefix}_http_status"] = response.code
        record["${prefix}_headers_ms"] = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
        val body = checkNotNull(response.body).string()
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
      .retryOnConnectionFailure(false)
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
        "reporting.reports.get",
        "reporting.metrics.get",
        "reporting.metrics.create",
        "reporting.metricCalculationSpecs.create",
        "reporting.eventGroups.list",
        "reporting.reportingSets.createPrimitive",
        "reporting.reportingSets.createComposite",
      )
  }
}
