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
import io.grpc.Status
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
import org.wfanet.measurement.api.v2alpha.cancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.ProtobufServiceConfig
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
    val phase = System.getenv("REPORTING_SCALE_PHASE") ?: "full"
    require(phase in setOf("creation", "full"))
    val cancelFailedCreation =
      System.getenv("REPORTING_SCALE_CANCEL_FAILED_CREATION")?.toBooleanStrict() ?: false
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
    require(repetitions in 1..10 && deadline.seconds in 1..14400)
    val output = Paths.get(requiredEnvironmentVariable("TEST_UNDECLARED_OUTPUTS_DIR"))
    Files.createDirectories(output)
    val records = mutableListOf<MutableMap<String, Any?>>()
    val channels = mutableListOf<ManagedChannel>()
    try {
      val reportingChannel =
        buildMutualTlsChannel(
            reportingTarget,
            signingCerts("mc_tls", "reporting_root"),
            null,
            defaultServiceConfig = serviceConfigWithTimeout(deadline.seconds),
            maxInboundMessageSize = 64 * 1024 * 1024,
          )
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
          ttl = deadline.plus(fulfillmentTimeout).plus(Duration.ofMinutes(30)),
        )
      val kingdomChannel =
        buildMutualTlsChannel(
            kingdomTarget,
            signingCerts("mc_tls", "mc_trusted_certs"),
            null,
            defaultServiceConfig = serviceConfigWithTimeout(600),
            maxInboundMessageSize = 64 * 1024 * 1024,
          )
          .also(channels::add)
      val measurementsClient =
        MeasurementsCoroutineStub(kingdomChannel.withDefaultDeadline(Duration.ofMinutes(10)))
          .withMaxInboundMessageSize(64 * 1024 * 1024)
          .withAuthenticationKey(apiKey)
      val reportsClient =
        ReportsCoroutineStub(reportingChannel.withDefaultDeadline(Duration.ofMinutes(10)))
          .withMaxInboundMessageSize(64 * 1024 * 1024)
      val metricsClient =
        MetricsCoroutineStub(reportingChannel.withDefaultDeadline(Duration.ofMinutes(10)))
          .withMaxInboundMessageSize(64 * 1024 * 1024)
      val httpClient = httpClient(deadline)
      val readClient =
        httpClient
          .newBuilder()
          .callTimeout(Duration.ofMinutes(10))
          .readTimeout(Duration.ofMinutes(10))
          .build()
      val eventGroups = resolveEventGroups(eventGroupsClient, measurementConsumer)
      val dataProviders = eventGroups.map(EventGroup::getCmmsDataProvider).distinct()
      check(dataProviders.size == 3) {
        "Scale test requires three distinct EDPs, got $dataProviders"
      }
      val edpTlsNames =
        (System.getenv("REPORTING_SCALE_EDP_TLS_NAMES") ?: "edp7,edpa_meta,edpa_video_pub")
          .split(',')
          .map(String::trim)
      require(
        edpTlsNames.size == dataProviders.size &&
          edpTlsNames.all { it.matches(Regex("[a-zA-Z0-9_]+")) }
      )
      val providerClients =
        (dataProviders.zip(edpTlsNames) + (requiredEnvironmentVariable("PDP_NAME") to "pdp1"))
          .associate { (provider, cert) ->
            val channel =
              buildMutualTlsChannel(
                  kingdomTarget,
                  signingCerts("${cert}_tls", "mc_trusted_certs"),
                  null,
                  defaultServiceConfig = serviceConfigWithTimeout(600),
                  maxInboundMessageSize = 64 * 1024 * 1024,
                )
                .also(channels::add)
            provider to
              RequisitionsCoroutineStub(channel.withDefaultDeadline(Duration.ofMinutes(10)))
                .withMaxInboundMessageSize(64 * 1024 * 1024)
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
            providerClients,
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
                "phase" to phase,
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
            record["stage"] = "verify_creation"
            verifyCreation(
              basicReportName,
              Instant.parse(record.getValue("started_at") as String),
              dataProviders,
              measurementsClient,
              providerClients,
              reportsClient,
              metricsClient,
              output,
              caseName,
              record,
            )
            record["creation_verified"] = true
            record["creation_verified_at"] = Instant.now().toString()
            Files.writeString(output.resolve("summary.json"), Gson().toJson(records))
            if (phase == "creation") {
              record["outcome"] = "creation_succeeded"
              record["stage"] = "complete"
              return@repeat
            }
            record["stage"] = "fulfillment"
            awaitFulfillment(
              basicReportName,
              Instant.parse(record.getValue("started_at") as String),
              fulfillmentTimeout,
              dataProviders,
              measurementsClient,
              providerClients,
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
            if (cancelFailedCreation && record["creation_verified"] != true) {
              try {
                record["cleanup_started_at"] = Instant.now().toString()
                Files.writeString(output.resolve("summary.json"), Gson().toJson(records))
                cancelFailedReport(
                  basicReportName,
                  Instant.parse(record.getValue("started_at") as String),
                  measurementsClient,
                  output,
                  caseName,
                  record,
                )
              } catch (cleanupError: Exception) {
                record["cleanup_complete"] = false
                record["cleanup_error"] = cleanupError.message
                e.addSuppressed(cleanupError)
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
    providerClients: Map<String, RequisitionsCoroutineStub>,
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
        val requisitions = verifyRequisitions(finalMeasurements, providerClients, true)
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

  /** Enumerates only the fresh creation window, retaining exact signed report links. */
  private suspend fun listReportMeasurements(
    basicReportName: String,
    startedAt: Instant,
    client: MeasurementsCoroutineStub,
  ): List<Measurement> {
    val result = linkedMapOf<String, Measurement>()
    var token = ""
    val cutoff = Instant.now().toProtoTime()
    do {
      val response = retryRpc {
        client.listMeasurements(
          listMeasurementsRequest {
            parent = basicReportName.substringBefore("/basicReports/")
            pageSize = 1000
            pageToken = token
            filter =
              ListMeasurementsRequestKt.filter {
                createdAfter = startedAt.minusSeconds(1).toProtoTime()
                createdBefore = cutoff
              }
          }
        )
      }
      for (measurement in response.measurementsList) {
        if (
          measurement.measurementSpec.unpack<MeasurementSpec>().reportingMetadata.basicReport ==
            basicReportName
        ) {
          result[measurement.name] = measurement
        }
      }
      token = response.nextPageToken
      if (token.isNotEmpty()) delay(250)
    } while (token.isNotEmpty())
    return result.values.toList()
  }

  private suspend fun verifyCreation(
    basicReportName: String,
    startedAt: Instant,
    dataProviders: List<String>,
    measurementsClient: MeasurementsCoroutineStub,
    providerClients: Map<String, RequisitionsCoroutineStub>,
    reportsClient: ReportsCoroutineStub,
    metricsClient: MetricsCoroutineStub,
    output: Path,
    caseName: String,
    record: MutableMap<String, Any?>,
  ) {
    val started = System.nanoTime()
    val measurements = listReportMeasurements(basicReportName, startedAt, measurementsClient)
    check(measurements.isNotEmpty()) { "Creation produced no linked Kingdom measurements" }
    val metadata =
      measurements.map { it.measurementSpec.unpack<MeasurementSpec>().reportingMetadata }
    val reportNames = metadata.map { it.report }.toSet()
    check(reportNames.size == 1 && reportNames.single().isNotEmpty()) {
      "Missing or inconsistent underlying Report link"
    }
    val report = reportsClient.getReport(getReportRequest { name = reportNames.single() })
    check(report.basicReport == basicReportName) {
      "Underlying Report belongs to another BasicReport"
    }
    Files.writeString(output.resolve("$caseName-report.json"), JsonFormat.printer().print(report))
    val metricNames =
      report.metricCalculationResultsList
        .flatMap { it.resultAttributesList }
        .map { it.metric }
        .toSet()
    check(metricNames.isNotEmpty() && metricNames.all(String::isNotEmpty)) {
      "Report has missing metric names"
    }
    val metrics =
      mapBounded(metricNames.toList().chunked(100)) { names ->
          retryRpc {
            metricsClient
              .batchGetMetrics(
                batchGetMetricsRequest {
                  parent = basicReportName.substringBefore("/basicReports/")
                  this.names += names
                }
              )
              .metricsList
          }
        }
        .flatten()
    check(metrics.size == metricNames.size && metrics.map { it.name }.toSet() == metricNames) {
      "Report metrics were not all created"
    }
    check(
      metrics.all {
        it.basicReport == basicReportName &&
          it.state in setOf(Metric.State.RUNNING, Metric.State.SUCCEEDED)
      }
    ) {
      "Metric creation failed or has an incorrect report link"
    }
    // Fresh campaign groups prevent reuse from another report. Each unique metric must have
    // at least one signed Kingdom measurement link before fetching is resumed.
    check(metadata.map { it.metric }.toSet() == metricNames) {
      "Report metrics are missing Kingdom measurements"
    }
    check(
      measurements.all {
        it.state != Measurement.State.FAILED && it.state != Measurement.State.CANCELLED
      }
    ) {
      "Measurement failed during creation"
    }
    record["metric_count"] = metrics.size
    record["measurement_count"] = measurements.size
    record["report"] = report.name
    record["expected_requisitions_per_data_provider"] =
      measurements
        .flatMap { it.dataProvidersList.map { entry -> entry.key } }
        .groupingBy { it }
        .eachCount()
    record["metrics_and_measurements_creation_verified"] = true
    record["stage"] = "verify_creation_requisitions"
    Files.writeString(output.resolve("summary.json"), Gson().toJson(listOf(record)))
    val requisitions = verifyRequisitions(measurements, providerClients, false)
    val counts =
      requisitions
        .groupingBy { it.dataProviderCertificate.substringBefore("/certificates/") }
        .eachCount()
    check(dataProviders.all { (counts[it] ?: 0) > 0 }) { "An EDP has no requisitions" }
    record["report"] = report.name
    record["metric_count"] = metrics.size
    record["metric_states"] = metrics.groupingBy { it.state.name }.eachCount()
    record["measurement_count"] = measurements.size
    record["measurement_states"] = measurements.groupingBy { it.state.name }.eachCount()
    record["requisition_count"] = requisitions.size
    record["requisitions_per_data_provider"] = counts
    record["requisition_states"] = requisitions.groupingBy { it.state.name }.eachCount()
    record["creation_verification_ms"] = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
    Files.writeString(
      output.resolve("$caseName-created-measurements.json"),
      Gson()
        .toJson(
          measurements.map {
            mapOf(
              "name" to it.name,
              "state" to it.state.name,
              "metric" to it.measurementSpec.unpack<MeasurementSpec>().reportingMetadata.metric,
              "data_providers" to it.dataProvidersList.map { entry -> entry.key },
            )
          }
        ),
    )
    Files.writeString(
      output.resolve("$caseName-created-requisitions.json"),
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
  }

  /** Lists 500 requisitions per provider call instead of one call per measurement. */
  private suspend fun verifyRequisitions(
    measurements: List<Measurement>,
    providerClients: Map<String, RequisitionsCoroutineStub>,
    requireFulfilled: Boolean,
  ): List<Requisition> = coroutineScope {
    val expected =
      measurements
        .flatMap { measurement -> measurement.dataProvidersList.map { measurement.name to it.key } }
        .toSet()
    check(providerClients.keys.containsAll(expected.map { it.second })) {
      "Missing provider TLS fixture for requisition verification"
    }
    val requisitions =
      providerClients
        .map { (provider, client) ->
          async {
            val needed = expected.filter { it.second == provider }.toSet()
            val found = linkedMapOf<String, Requisition>()
            // PENDING_PARAMS requisitions are not visible in the public API. Give duchy parameter
            // setup a bounded interval, without requiring any requisition fulfillment.
            kotlinx.coroutines.withTimeout(Duration.ofMinutes(30).toMillis()) {
              do {
                var token = ""
                do {
                  val response = retryRpc {
                    client.listRequisitions(
                      listRequisitionsRequest {
                        parent = provider
                        pageSize = 500
                        pageToken = token
                      }
                    )
                  }
                  for (requisition in response.requisitionsList) {
                    if ((requisition.measurement to provider) in needed) {
                      // Retain verification fields rather than every encrypted payload for a large
                      // report.
                      found[requisition.name] =
                        Requisition.newBuilder()
                          .setName(requisition.name)
                          .setMeasurement(requisition.measurement)
                          .setDataProviderCertificate(requisition.dataProviderCertificate)
                          .setState(requisition.state)
                          .build()
                    }
                  }
                  token = response.nextPageToken
                  if (token.isNotEmpty()) delay(250)
                } while (token.isNotEmpty() && found.size < needed.size)
                if (found.size < needed.size) delay(5_000)
              } while (found.size < needed.size)
            }
            check(
              found.size == needed.size &&
                found.values.map { it.measurement to provider }.toSet() == needed
            ) {
              "Missing or duplicated provider requisitions"
            }
            check(
              found.values.all {
                it.dataProviderCertificate.substringBefore("/certificates/") == provider
              }
            ) {
              "Incorrect requisition provider"
            }
            check(
              found.values.all {
                if (requireFulfilled) it.state == Requisition.State.FULFILLED
                else
                  it.state != Requisition.State.REFUSED && it.state != Requisition.State.WITHDRAWN
              }
            ) {
              "Unexpected requisition state for $provider"
            }
            found.values.toList()
          }
        }
        .awaitAll()
        .flatten()
    check(
      requisitions
        .map { it.measurement to it.dataProviderCertificate.substringBefore("/certificates/") }
        .toSet() == expected
    ) {
      "Incomplete requisition coverage"
    }
    requisitions
  }

  private suspend fun cancelFailedReport(
    basicReportName: String,
    startedAt: Instant,
    client: MeasurementsCoroutineStub,
    output: Path,
    caseName: String,
    record: MutableMap<String, Any?>,
  ) {
    val terminal =
      setOf(Measurement.State.SUCCEEDED, Measurement.State.FAILED, Measurement.State.CANCELLED)
    val cancelled = linkedSetOf<String>()
    kotlinx.coroutines.withTimeout(Duration.ofMinutes(15).toMillis()) {
      var quietScans = 0
      while (quietScans < 2) {
        val observed = listReportMeasurements(basicReportName, startedAt, client)
        record["cleanup_observed_measurement_count"] = observed.size
        record["cleanup_observed_measurement_states"] =
          observed.groupingBy { it.state.name }.eachCount()
        record["cleanup_expected_requisitions_per_data_provider"] =
          observed
            .flatMap { it.dataProvidersList.map { entry -> entry.key } }
            .groupingBy { it }
            .eachCount()
        val pending = observed.filter { it.state !in terminal }
        record["cleanup_active_measurement_count"] = pending.size
        if (pending.isEmpty()) quietScans++ else quietScans = 0
        for (measurement in pending) {
          val final =
            try {
              retryRpc {
                client.cancelMeasurement(cancelMeasurementRequest { name = measurement.name })
              }
            } catch (e: Exception) {
              if (Status.fromThrowable(e).code != Status.Code.FAILED_PRECONDITION) throw e
              retryRpc { client.getMeasurement(getMeasurementRequest { name = measurement.name }) }
            }
          check(final.state in terminal) { "Failed to cancel ${measurement.name}" }
          if (final.state == Measurement.State.CANCELLED) cancelled += final.name
          delay(250)
        }
        if (quietScans < 2) delay(5_000)
      }
    }
    record["cleanup_complete"] = true
    record["cleanup_completed_at"] = Instant.now().toString()
    record["cancelled_measurement_count"] = cancelled.size
    Files.writeString(
      output.resolve("$caseName-cancelled-measurements.json"),
      Gson().toJson(cancelled),
    )
  }

  private suspend fun <T> retryRpc(block: suspend () -> T): T {
    repeat(6) { attempt ->
      try {
        return block()
      } catch (e: Exception) {
        val status = Status.fromThrowable(e)
        if (
          attempt == 5 ||
            status.code !in setOf(Status.Code.UNAVAILABLE, Status.Code.RESOURCE_EXHAUSTED)
        )
          throw e
        delay(minOf(30_000L, 1000L shl attempt))
      }
    }
    error("Unreachable")
  }

  /** Keeps reads within the synthetic consumer's five-request-per-second rate limit. */
  private suspend fun <T, R> mapBounded(items: List<T>, transform: suspend (T) -> R): List<R> =
    items.chunked(4).flatMap { batch ->
      val result = coroutineScope { batch.map { item -> async { transform(item) } }.awaitAll() }
      delay(1000)
      result
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

  private fun serviceConfigWithTimeout(timeoutSeconds: Long): ProtobufServiceConfig {
    val timeout = com.google.protobuf.Duration.newBuilder().setSeconds(timeoutSeconds).build()
    return ProtobufServiceConfig.DEFAULT.copy {
      for (index in methodConfig.indices) {
        methodConfig[index] = methodConfig[index].toBuilder().setTimeout(timeout).build()
      }
    }
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
