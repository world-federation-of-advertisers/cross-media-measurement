/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.Date
import com.google.type.Interval
import com.google.type.copy
import com.google.type.date
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.time.DateTimeException
import java.time.LocalDate
import java.time.Period
import java.time.temporal.ChronoField
import java.time.temporal.ChronoUnit
import java.time.temporal.Temporal
import java.time.temporal.TemporalAdjusters
import java.time.zone.ZoneRulesException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.math.min
import kotlin.random.Random
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.projectnessie.cel.Env
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.access.client.v1alpha.withForwardedTrustedCredentials
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequest as InternalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec as InternalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportRequest as internalGetReportRequest
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.reporting.service.api.submitBatchRequests
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportScheduleInfoServerInterceptor.Companion.reportScheduleInfoFromCurrentContext
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.BatchGetMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.CreateReportRequest
import org.wfanet.measurement.reporting.v2alpha.GetReportRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportsPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.ListReportsRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportsResponse
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.batchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listReportsPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportsResponse
import org.wfanet.measurement.reporting.v2alpha.report

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

private const val BATCH_CREATE_METRICS_LIMIT = 1000
private const val BATCH_GET_METRICS_LIMIT = 1000

private typealias InternalReportingMetricEntries =
  Map<String, InternalReport.ReportingMetricCalculationSpec>

class ReportsService(
  private val internalReportsStub: ReportsCoroutineStub,
  private val internalMetricCalculationSpecsStub: MetricCalculationSpecsCoroutineStub,
  private val metricsStub: MetricsCoroutineStub,
  private val metricSpecConfig: MetricSpecConfig,
  private val authorization: Authorization,
  private val secureRandom: Random,
  private val allowSamplingIntervalWrapping: Boolean = false,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ReportsCoroutineImplBase(coroutineContext) {
  private data class CreateReportInfo(
    val parent: String,
    val requestId: String,
    val timeIntervals: List<Interval>?,
    val reportingInterval: Report.ReportingInterval?,
  )

  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }
    val listReportsPageToken = request.toListReportsPageToken()

    authorization.check(request.parent, Permission.LIST)

    val streamInternalReportsRequest: StreamReportsRequest =
      listReportsPageToken.toStreamReportsRequest()

    val results: List<InternalReport> =
      try {
        internalReportsStub.streamReports(streamInternalReportsRequest).toList()
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .withDescription("Unable to list Reports.")
          .asRuntimeException()
      }

    if (results.isEmpty()) {
      return ListReportsResponse.getDefaultInstance()
    }

    val nextPageToken: ListReportsPageToken? =
      if (results.size > listReportsPageToken.pageSize) {
        listReportsPageToken.copy {
          lastReport =
            ListReportsPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = results[results.lastIndex - 1].cmmsMeasurementConsumerId
              createTime = results[results.lastIndex - 1].createTime
              externalReportId = results[results.lastIndex - 1].externalReportId
            }
        }
      } else {
        null
      }

    val subResults: List<InternalReport> =
      results.subList(0, min(results.size, listReportsPageToken.pageSize))

    // Get metrics.
    val metricNames: Flow<String> = flow {
      buildSet {
        for (internalReport in subResults) {
          for (reportingMetricEntry in internalReport.reportingMetricEntriesMap) {
            for (metricCalculationSpecReportingMetrics in
              reportingMetricEntry.value.metricCalculationSpecReportingMetricsList) {
              for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
                if (reportingMetric.externalMetricId.isEmpty()) {
                  continue
                }

                val name =
                  MetricKey(
                      internalReport.cmmsMeasurementConsumerId,
                      reportingMetric.externalMetricId,
                    )
                    .toName()

                if (!contains(name)) {
                  emit(name)
                  add(name)
                }
              }
            }
          }
        }
      }
    }

    val callRpc: suspend (List<String>) -> BatchGetMetricsResponse = { items ->
      batchGetMetrics(request.parent, items)
    }
    val externalIdToMetricMap: Map<String, Metric> = buildMap {
      submitBatchRequests(metricNames, BATCH_GET_METRICS_LIMIT, callRpc) { response ->
          response.metricsList
        }
        .collect { metrics: List<Metric> ->
          for (metric in metrics) {
            computeIfAbsent(checkNotNull(MetricKey.fromName(metric.name)).metricId) { metric }
          }
        }
    }

    return listReportsResponse {
      reports +=
        filterReports(
          subResults.map { internalReport ->
            convertInternalReportToPublic(internalReport, externalIdToMetricMap)
          },
          request.filter,
        )

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  override suspend fun getReport(request: GetReportRequest): Report {
    val reportKey =
      grpcRequireNotNull(ReportKey.fromName(request.name)) {
        "Report name is either unspecified or invalid"
      }
    val parent: String = reportKey.parentKey.toName()

    authorization.check(listOf(request.name, parent), Permission.GET)

    val internalReport =
      try {
        internalReportsStub.getReport(
          internalGetReportRequest {
            cmmsMeasurementConsumerId = reportKey.cmmsMeasurementConsumerId
            externalReportId = reportKey.reportId
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            Status.Code.NOT_FOUND -> Status.NOT_FOUND
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .withDescription("Unable to get Report.")
          .asRuntimeException()
      }

    // Get metrics.
    val metricNames: Flow<String> = flow {
      buildSet {
        for (reportingMetricEntry in internalReport.reportingMetricEntriesMap) {
          for (metricCalculationSpecReportingMetrics in
            reportingMetricEntry.value.metricCalculationSpecReportingMetricsList) {
            for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
              if (reportingMetric.externalMetricId.isEmpty()) {
                continue
              }

              val name =
                MetricKey(
                    internalReport.cmmsMeasurementConsumerId,
                    reportingMetric.externalMetricId,
                  )
                  .toName()

              if (!contains(name)) {
                emit(name)
                add(name)
              }
            }
          }
        }
      }
    }

    val callRpc: suspend (List<String>) -> BatchGetMetricsResponse = { items ->
      batchGetMetrics(parent, items)
    }
    val externalIdToMetricMap: Map<String, Metric> = buildMap {
      submitBatchRequests(metricNames, BATCH_GET_METRICS_LIMIT, callRpc) { response ->
          response.metricsList
        }
        .collect { metrics: List<Metric> ->
          for (metric in metrics) {
            computeIfAbsent(checkNotNull(MetricKey.fromName(metric.name)).metricId) { metric }
          }
        }
    }

    // Convert the internal report to public and return.
    return convertInternalReportToPublic(internalReport, externalIdToMetricMap)
  }

  private suspend fun batchGetMetrics(
    parent: String,
    metricNames: List<String>,
  ): BatchGetMetricsResponse {
    return try {
      metricsStub
        .withForwardedTrustedCredentials()
        .batchGetMetrics(
          batchGetMetricsRequest {
            this.parent = parent
            names += metricNames
          }
        )
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT ->
            Status.INVALID_ARGUMENT.withDescription("Unable to get Metrics.\n${e.message}")
          Status.Code.PERMISSION_DENIED ->
            Status.PERMISSION_DENIED.withDescription("Unable to get Metrics.\n${e.message}")
          else -> Status.UNKNOWN.withDescription("Unable to get Metrics.\n${e.message}")
        }
        .withCause(e)
        .asRuntimeException()
    }
  }

  override suspend fun createReport(request: CreateReportRequest): Report {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    grpcRequire(request.hasReport()) { "Report is not specified." }
    grpcRequire(request.reportId.matches(RESOURCE_ID_REGEX)) { "Report ID is invalid." }

    grpcRequire(request.report.reportingMetricEntriesList.isNotEmpty()) {
      "No ReportingMetricEntry is specified."
    }

    validateTime(request.report)

    val externalMetricCalculationSpecIds: List<String> =
      request.report.reportingMetricEntriesList.flatMap { reportingMetricEntry ->
        reportingMetricEntry.value.metricCalculationSpecsList.map {
          val key =
            grpcRequireNotNull(MetricCalculationSpecKey.fromName(it)) {
              "MetricCalculationSpec name $it is invalid."
            }
          key.metricCalculationSpecId
        }
      }

    authorization.check(request.parent, Permission.CREATE)

    val externalIdToMetricCalculationSpecMap: Map<String, InternalMetricCalculationSpec> =
      createExternalIdToMetricCalculationSpecMap(
        parentKey.measurementConsumerId,
        externalMetricCalculationSpecIds,
      )

    // Build an internal CreateReportRequest.
    //  The internal report in CreateReportRequest has several
    //  ReportingMetrics without request IDs and external metric IDs.
    val internalCreateReportRequest: InternalCreateReportRequest =
      buildInternalCreateReportRequest(request, externalIdToMetricCalculationSpecMap)

    // Create an internal report
    //  The internal report service will fill request IDs in
    //  MetricCalculationSpec.ReportingMetrics. If there are existing metrics based on the
    //  request IDs, the external metric IDs will also be filled.
    val internalReport =
      try {
        internalReportsStub.createReport(internalCreateReportRequest)
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED ->
              Status.DEADLINE_EXCEEDED.withDescription("Unable to create Report.")
            Status.Code.CANCELLED -> Status.CANCELLED.withDescription("Unable to create Report.")
            Status.Code.FAILED_PRECONDITION ->
              Status.FAILED_PRECONDITION.withDescription(
                "Unable to create Report. The measurement consumer not found."
              )
            Status.Code.ALREADY_EXISTS ->
              Status.ALREADY_EXISTS.withDescription(
                "Report with ID ${request.reportId} already exists under ${request.parent}"
              )
            Status.Code.NOT_FOUND ->
              if (e.message!!.contains("external_report_schedule_id")) {
                Status.NOT_FOUND.withDescription(
                  "ReportSchedule associated with the Report not found."
                )
              } else if (e.message!!.contains("external_metric_calculation_spec_id")) {
                Status.NOT_FOUND.withDescription(
                  "MetricCalculationSpec used in the Report not found."
                )
              } else {
                Status.NOT_FOUND.withDescription("ReportingSet used in the Report not found.")
              }
            else -> Status.UNKNOWN.withDescription("Unable to create Report.")
          }
          .withCause(e)
          .asRuntimeException()
      }

    // Create metrics.
    val createMetricRequests: Flow<CreateMetricRequest> =
      @OptIn(ExperimentalCoroutinesApi::class)
      internalReport.reportingMetricEntriesMap.entries.asFlow().flatMapMerge { entry ->
        entry.value.metricCalculationSpecReportingMetricsList.asFlow().flatMapMerge {
          metricCalculationSpecReportingMetrics ->
          metricCalculationSpecReportingMetrics.reportingMetricsList.asFlow().map {
            val metricCalculationSpec =
              externalIdToMetricCalculationSpecMap.getValue(
                metricCalculationSpecReportingMetrics.externalMetricCalculationSpecId
              )
            it.toCreateMetricRequest(
              parentKey,
              entry.key,
              filter = metricCalculationSpec.details.filter,
              modelLineName = metricCalculationSpec.cmmsModelLine,
              containingReportResourceName =
                ReportKey(internalReport.cmmsMeasurementConsumerId, internalReport.externalReportId)
                  .toName(),
            )
          }
        }
      }

    val callRpc: suspend (List<CreateMetricRequest>) -> BatchCreateMetricsResponse = { items ->
      batchCreateMetrics(request.parent, items)
    }
    val externalIdToMetricMap: Map<String, Metric> = buildMap {
      submitBatchRequests(createMetricRequests, BATCH_CREATE_METRICS_LIMIT, callRpc) { response ->
          response.metricsList
        }
        .collect { metrics: List<Metric> ->
          for (metric in metrics) {
            computeIfAbsent(checkNotNull(MetricKey.fromName(metric.name)).metricId) { metric }
          }
        }
    }

    // Once all metrics are created, get the updated internal report with the metric IDs filled.
    val updatedInternalReport =
      try {
        internalReportsStub.getReport(
          internalGetReportRequest {
            cmmsMeasurementConsumerId = internalReport.cmmsMeasurementConsumerId
            externalReportId = internalReport.externalReportId
          }
        )
      } catch (e: StatusException) {
        throw Status.UNKNOWN.withDescription("Unable to create Report.")
          .withCause(e)
          .withDescription("Report created, but error returning the Report.")
          .asRuntimeException()
      }

    // Convert the internal report to public and return.
    return convertInternalReportToPublic(updatedInternalReport, externalIdToMetricMap)
  }

  /** Returns a map of external IDs to [InternalMetricCalculationSpec]. */
  private suspend fun createExternalIdToMetricCalculationSpecMap(
    cmmsMeasurementConsumerId: String,
    externalMetricCalculationSpecIds: List<String>,
  ): Map<String, InternalMetricCalculationSpec> {
    return try {
      internalMetricCalculationSpecsStub
        .batchGetMetricCalculationSpecs(
          batchGetMetricCalculationSpecsRequest {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalMetricCalculationSpecIds += externalMetricCalculationSpecIds.toHashSet()
          }
        )
        .metricCalculationSpecsList
        .associateBy({ it.externalMetricCalculationSpecId }, { it })
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED ->
            Status.DEADLINE_EXCEEDED.withDescription(
              "Unable to get MetricCalculationSpecs in Report."
            )
          Status.Code.CANCELLED ->
            Status.CANCELLED.withDescription("Unable to get MetricCalculationSpecs in Report.")
          Status.Code.NOT_FOUND ->
            Status.NOT_FOUND.withDescription("MetricCalculationSpec in Report not found.")
          else -> Status.UNKNOWN.withDescription("Unable to get MetricCalculationSpecs in Report.")
        }
        .withCause(e)
        .asRuntimeException()
    }
  }

  /** Converts an internal [InternalReport] to a public [Report]. */
  private suspend fun convertInternalReportToPublic(
    internalReport: InternalReport,
    externalIdToMetricMap: Map<String, Metric>,
  ): Report {
    return report {
      name =
        ReportKey(internalReport.cmmsMeasurementConsumerId, internalReport.externalReportId)
          .toName()

      tags.putAll(internalReport.details.tagsMap)

      reportingMetricEntries +=
        internalReport.reportingMetricEntriesMap.map { internalReportingMetricEntry ->
          internalReportingMetricEntry.toReportingMetricEntry(
            internalReport.cmmsMeasurementConsumerId
          )
        }

      if (internalReport.details.hasReportingInterval()) {
        reportingInterval = internalReport.details.reportingInterval.toReportingInterval()
      } else {
        timeIntervals = internalReport.details.timeIntervals.toTimeIntervals()
      }

      val metrics: List<Metric> = buildList {
        for (externalMetricId in internalReport.externalMetricIds) {
          if (externalIdToMetricMap.containsKey(externalMetricId)) {
            add(externalIdToMetricMap.getValue(externalMetricId))
          } else {
            state = Report.State.FAILED
          }
        }
      }

      if (state != Report.State.FAILED) {
        state = inferReportState(metrics)
      }
      createTime = internalReport.createTime

      if (state == Report.State.SUCCEEDED || state == Report.State.FAILED) {
        val externalMetricCalculationSpecIds =
          internalReport.reportingMetricEntriesMap.flatMap { reportingMetricCalculationSpec ->
            reportingMetricCalculationSpec.value.metricCalculationSpecReportingMetricsList.map {
              it.externalMetricCalculationSpecId
            }
          }

        val externalIdToMetricCalculationMap: Map<String, InternalMetricCalculationSpec> =
          createExternalIdToMetricCalculationSpecMap(
            internalReport.cmmsMeasurementConsumerId,
            externalMetricCalculationSpecIds,
          )

        this.metricCalculationResults +=
          buildMetricCalculationResults(
            internalReport.cmmsMeasurementConsumerId,
            internalReport.reportingMetricEntriesMap,
            externalIdToMetricMap,
            externalIdToMetricCalculationMap,
          )
      }

      if (internalReport.externalReportScheduleId.isNotEmpty()) {
        reportSchedule =
          ReportScheduleKey(
              internalReport.cmmsMeasurementConsumerId,
              internalReport.externalReportScheduleId,
            )
            .toName()
      }
    }
  }

  /** Builds [Report.MetricCalculationResult]s. */
  private fun buildMetricCalculationResults(
    cmmsMeasurementConsumerId: String,
    internalReportingMetricEntries: InternalReportingMetricEntries,
    externalIdToMetricMap: Map<String, Metric>,
    externalIdToMetricCalculationMap: Map<String, InternalMetricCalculationSpec>,
  ): List<Report.MetricCalculationResult> {
    return internalReportingMetricEntries.flatMap { (reportingSetId, reportingMetricCalculationSpec)
      ->
      val reportingSetName = ReportingSetKey(cmmsMeasurementConsumerId, reportingSetId).toName()

      reportingMetricCalculationSpec.metricCalculationSpecReportingMetricsList.map {
        metricCalculationSpecReportingMetrics ->
        val metricCalculationSpec =
          externalIdToMetricCalculationMap.getValue(
            metricCalculationSpecReportingMetrics.externalMetricCalculationSpecId
          )
        ReportKt.metricCalculationResult {
          this.metricCalculationSpec =
            MetricCalculationSpecKey(
                metricCalculationSpec.cmmsMeasurementConsumerId,
                metricCalculationSpec.externalMetricCalculationSpecId,
              )
              .toName()
          displayName = metricCalculationSpec.details.displayName
          reportingSet = reportingSetName
          for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
            if (externalIdToMetricMap.containsKey(reportingMetric.externalMetricId)) {
              val metric = externalIdToMetricMap.getValue(reportingMetric.externalMetricId)
              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  this.metric = metric.name
                  groupingPredicates += reportingMetric.details.groupingPredicatesList
                  filter = metricCalculationSpec.details.filter
                  metricSpec = metric.metricSpec
                  timeInterval = metric.timeInterval
                  state = metric.state
                  if (metric.state == Metric.State.SUCCEEDED) {
                    metricResult = metric.result
                  }
                }
            }
          }
        }
      }
    }
  }

  /** Creates a batch of [Metric]s. */
  private suspend fun batchCreateMetrics(
    parent: String,
    createMetricRequests: List<CreateMetricRequest>,
  ): BatchCreateMetricsResponse {
    return try {
      metricsStub
        .withForwardedTrustedCredentials()
        .batchCreateMetrics(
          batchCreateMetricsRequest {
            this.parent = parent
            requests += createMetricRequests
          }
        )
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.PERMISSION_DENIED -> Status.PERMISSION_DENIED.withDescription(e.message)
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT.withDescription(e.message)
          Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription(e.message)
          Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION.withDescription(e.message)
          else -> Status.UNKNOWN.withDescription("Unable to create Metrics.")
        }
        .withCause(e)
        .asRuntimeException()
    }
  }

  /** Builds an [InternalCreateReportRequest]. */
  private fun buildInternalCreateReportRequest(
    request: CreateReportRequest,
    externalIdToMetricCalculationMap: Map<String, InternalMetricCalculationSpec>,
  ): InternalCreateReportRequest {
    val cmmsMeasurementConsumerId =
      checkNotNull(MeasurementConsumerKey.fromName(request.parent)).measurementConsumerId

    return internalCreateReportRequest {
      report = internalReport {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        details =
          InternalReportKt.details {
            tags.putAll(request.report.tagsMap)
            when (request.report.timeCase) {
              Report.TimeCase.TIME_INTERVALS -> {
                timeIntervals = request.report.timeIntervals.toInternal()
              }
              Report.TimeCase.REPORTING_INTERVAL -> {
                reportingInterval = request.report.reportingInterval.toInternal()
              }
              Report.TimeCase.TIME_NOT_SET ->
                failGrpc(Status.INVALID_ARGUMENT) { "The time in Report is not specified." }
            }
          }

        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        reportingMetricEntries.putAll(
          buildInternalReportingMetricEntries(request, externalIdToMetricCalculationMap)
        )
      }
      requestId = request.requestId
      externalReportId = request.reportId

      val reportScheduleInfo: ReportScheduleInfoServerInterceptor.ReportScheduleInfo? =
        reportScheduleInfoFromCurrentContext
      if (reportScheduleInfo != null) {
        val reportScheduleKey =
          grpcRequireNotNull(ReportScheduleKey.fromName(reportScheduleInfo.name)) {
            "reportScheduleName is invalid"
          }

        this.reportScheduleInfo =
          CreateReportRequestKt.reportScheduleInfo {
            externalReportScheduleId = reportScheduleKey.reportScheduleId
            nextReportCreationTime = reportScheduleInfo.nextReportCreationTime
          }
      }
    }
  }

  /** Builds an [InternalReportingMetricEntries] from a [CreateReportRequest]. */
  private fun buildInternalReportingMetricEntries(
    request: CreateReportRequest,
    externalIdToMetricCalculationMap: Map<String, InternalMetricCalculationSpec>,
  ): InternalReportingMetricEntries {
    val measurementConsumerKey = checkNotNull(MeasurementConsumerKey.fromName(request.parent))
    val createReportInfo =
      if (request.report.hasTimeIntervals()) {
        CreateReportInfo(
          request.parent,
          request.requestId,
          request.report.timeIntervals.timeIntervalsList,
          null,
        )
      } else {
        CreateReportInfo(request.parent, request.requestId, null, request.report.reportingInterval)
      }

    return request.report.reportingMetricEntriesList.associate { reportingMetricEntry ->
      val reportingSetKey =
        grpcRequireNotNull(ReportingSetKey.fromName(reportingMetricEntry.key)) {
          "Invalid reporting set name ${reportingMetricEntry.key}."
        }
      if (
        reportingSetKey.cmmsMeasurementConsumerId != measurementConsumerKey.measurementConsumerId
      ) {
        failGrpc(Status.PERMISSION_DENIED) {
          "Cannot access reporting set ${reportingMetricEntry.key}."
        }
      }

      grpcRequire(reportingMetricEntry.hasValue()) {
        "Value in ReportingMetricEntry with key-${reportingMetricEntry.key} is not set."
      }
      grpcRequire(reportingMetricEntry.value.metricCalculationSpecsList.isNotEmpty()) {
        "There is no MetricCalculationSpec associated to ${reportingMetricEntry.key}."
      }

      val reportingSetId = reportingSetKey.reportingSetId

      reportingSetId to
        InternalReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecReportingMetrics +=
            reportingMetricEntry.value.metricCalculationSpecsList.map { metricCalculationSpecName ->
              val metricCalculationSpecKey =
                grpcRequireNotNull(MetricCalculationSpecKey.fromName(metricCalculationSpecName)) {
                  "MetricCalculationSpec name $metricCalculationSpecName is invalid."
                }
              buildInternalMetricCalculationSpecReportingMetrics(
                externalIdToMetricCalculationMap.getValue(
                  metricCalculationSpecKey.metricCalculationSpecId
                ),
                createReportInfo,
              )
            }
        }
    }
  }

  /**
   * Builds an [InternalReport.MetricCalculationSpecReportingMetrics] from a
   * [Report.ReportingMetricEntry].
   */
  private fun buildInternalMetricCalculationSpecReportingMetrics(
    internalMetricCalculationSpec: InternalMetricCalculationSpec,
    createReportInfo: CreateReportInfo,
  ): InternalReport.MetricCalculationSpecReportingMetrics {
    val timeIntervals: List<Interval> =
      if (createReportInfo.timeIntervals != null) {
        grpcRequire(!internalMetricCalculationSpec.details.hasMetricFrequencySpec()) {
          "metric_calculation_spec with metric_frequency_spec set not allowed when time_intervals is set."
        }
        createReportInfo.timeIntervals
      } else {
        if (!internalMetricCalculationSpec.details.hasMetricFrequencySpec()) {
          generateTimeIntervals(checkNotNull(createReportInfo.reportingInterval), null, null)
        } else {
          val trailingWindow: InternalMetricCalculationSpec.TrailingWindow? =
            if (internalMetricCalculationSpec.details.hasTrailingWindow()) {
              internalMetricCalculationSpec.details.trailingWindow
            } else {
              null
            }
          val generatedTimeIntervals: List<Interval> =
            generateTimeIntervals(
              checkNotNull(createReportInfo.reportingInterval),
              internalMetricCalculationSpec.details.metricFrequencySpec,
              trailingWindow,
            )
          generatedTimeIntervals
        }
      }

    // Expand groupings to predicate groups in Cartesian product
    val groupings: List<List<String>> =
      internalMetricCalculationSpec.details.groupingsList.map { it.predicatesList }
    val groupingsCartesianProduct: List<List<String>> = cartesianProduct(groupings)

    return InternalReportKt.metricCalculationSpecReportingMetrics {
      externalMetricCalculationSpecId =
        internalMetricCalculationSpec.externalMetricCalculationSpecId
      // Fan out to a list of reportingMetrics with the Cartesian product of metric specs,
      // predicate groups, and time intervals.
      reportingMetrics +=
        timeIntervals.flatMap { timeInterval ->
          internalMetricCalculationSpec.details.metricSpecsList.flatMap { metricSpec ->
            groupingsCartesianProduct.map { groupingPredicates ->
              InternalReportKt.reportingMetric {
                details =
                  InternalReportKt.ReportingMetricKt.details {
                    this.metricSpec =
                      try {
                        metricSpec
                          .toMetricSpec()
                          .withDefaults(
                            metricSpecConfig,
                            secureRandom,
                            allowSamplingIntervalWrapping,
                          )
                          .toInternal()
                      } catch (e: MetricSpecDefaultsException) {
                        failGrpc(Status.INVALID_ARGUMENT) {
                          listOfNotNull("Invalid metric spec.", e.message, e.cause?.message)
                            .joinToString(separator = "\n")
                        }
                      } catch (e: Exception) {
                        failGrpc(Status.UNKNOWN) { "Failed to read the metric spec." }
                      }
                    this.timeInterval = timeInterval
                    this.groupingPredicates += groupingPredicates
                  }
              }
            }
          }
        }
    }
  }

  /**
   * Outputs the Cartesian product of any number of lists with elements in the same type.
   *
   * Note that Cartesian product allows duplicates.
   *
   * @param lists contains the lists that will be used to generate the Cartesian product. All lists
   *   must not be empty.
   * @return the Cartesian product of the given lists. If [lists] is empty, return a list containing
   *   an empty list.
   */
  private fun <T> cartesianProduct(lists: List<List<T>>): List<List<T>> {
    if (lists.isEmpty()) return listOf(listOf())

    var result: List<List<T>> = lists.first().map { listOf(it) }

    for (list in lists.drop(1)) {
      result = result.flatMap { xList -> list.map { yVal -> xList + listOf(yVal) } }
    }

    return result
  }

  private fun filterReports(reports: List<Report>, filter: String): List<Report> {
    return try {
      filterList(ENV, reports, filter)
    } catch (e: IllegalArgumentException) {
      throw Status.INVALID_ARGUMENT.withDescription(e.message).asRuntimeException()
    }
  }

  object Permission {
    const val GET = "reporting.reports.get"
    const val LIST = "reporting.reports.list"
    const val CREATE = "reporting.reports.create"
  }

  companion object {
    private val RESOURCE_ID_REGEX = ResourceIds.AIP_122_REGEX
    private val ENV: Env = buildCelEnvironment(Report.getDefaultInstance())
  }
}

/** Infers the [Report.State] based on the [Metric]s. */
private fun inferReportState(metrics: Collection<Metric>): Report.State {
  if (metrics.isEmpty()) {
    return Report.State.RUNNING
  }

  val metricStates = metrics.map { it.state }
  return if (metricStates.all { it == Metric.State.SUCCEEDED }) {
    Report.State.SUCCEEDED
  } else if (metricStates.any { it == Metric.State.FAILED || it == Metric.State.INVALID }) {
    Report.State.FAILED
  } else {
    Report.State.RUNNING
  }
}

/** Gets all external metric IDs used in the [InternalReport]. */
private val InternalReport.externalMetricIds: List<String>
  get() =
    reportingMetricEntriesMap.flatMap { (_, reportingMetricCalculationSpec) ->
      reportingMetricCalculationSpec.metricCalculationSpecReportingMetricsList.flatMap {
        it.reportingMetricsList.map { reportingMetric -> reportingMetric.externalMetricId }
      }
    }

/** Converts a public [ListReportsRequest] to a [ListReportsPageToken]. */
private fun ListReportsRequest.toListReportsPageToken(): ListReportsPageToken {
  grpcRequire(pageSize >= 0) { "Page size cannot be less than 0" }

  val source = this
  val parentKey: MeasurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(parent)) {
      "Parent is either unspecified or invalid."
    }
  val cmmsMeasurementConsumerId = parentKey.measurementConsumerId

  return if (pageToken.isNotBlank()) {
    ListReportsPageToken.parseFrom(pageToken.base64UrlDecode()).copy {
      grpcRequire(this.cmmsMeasurementConsumerId == cmmsMeasurementConsumerId) {
        "Arguments must be kept the same when using a page token"
      }

      if (source.pageSize in MIN_PAGE_SIZE..MAX_PAGE_SIZE) {
        pageSize = source.pageSize
      }
    }
  } else {
    listReportsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    }
  }
}

/**
 * Validate [Report] time fields.
 *
 * @throws
 * * [StatusRuntimeException] with [Status.INVALID_ARGUMENT] if error found.
 */
private fun validateTime(report: Report) {
  if (report.hasTimeIntervals()) {
    grpcRequire(report.timeIntervals.timeIntervalsList.isNotEmpty()) {
      "TimeIntervals timeIntervalsList is empty."
    }
    report.timeIntervals.timeIntervalsList.forEach {
      grpcRequire(it.startTime.seconds > 0 || it.startTime.nanos > 0) {
        "TimeInterval startTime is unspecified."
      }
      grpcRequire(it.endTime.seconds > 0 || it.endTime.nanos > 0) {
        "TimeInterval endTime is unspecified."
      }
      grpcRequire(
        it.endTime.seconds > it.startTime.seconds || it.endTime.nanos > it.startTime.nanos
      ) {
        "TimeInterval endTime is not later than startTime."
      }
    }
  } else if (report.hasReportingInterval()) {
    grpcRequire(report.reportingInterval.hasReportStart()) { "report_start is missing." }
    val reportStart = report.reportingInterval.reportStart
    grpcRequire(
      reportStart.year > 0 &&
        reportStart.month > 0 &&
        reportStart.day > 0 &&
        (reportStart.hasUtcOffset() || reportStart.hasTimeZone())
    ) {
      "report_start missing either year, month, day, or time_offset."
    }

    grpcRequire(report.reportingInterval.hasReportEnd()) { "report_end is missing." }
    val reportEnd = report.reportingInterval.reportEnd
    grpcRequire(reportEnd.year > 0 && reportEnd.month > 0 && reportEnd.day > 0) {
      "report_end not a full date."
    }

    grpcRequire(
      date {
          year = reportStart.year
          month = reportStart.month
          day = reportStart.day
        }
        .isBefore(reportEnd)
    ) {
      "report_end is not after report_start."
    }
  } else {
    failGrpc { "time is not set." }
  }
}

private fun Date.isBefore(other: Date): Boolean {
  return LocalDate.of(this.year, this.month, this.day)
    .isBefore(LocalDate.of(other.year, other.month, other.day))
}

/**
 * Generate a list of time intervals using the [Report.ReportingInterval], the
 * [MetricCalculationSpec.MetricFrequencySpec], and the [MetricCalculationSpec.TrailingWindow].
 */
private fun generateTimeIntervals(
  reportingInterval: Report.ReportingInterval,
  frequencySpec: MetricCalculationSpec.MetricFrequencySpec?,
  trailingWindow: MetricCalculationSpec.TrailingWindow? = null,
): List<Interval> {
  val reportEndDateTime =
    reportingInterval.reportStart.copy {
      year = reportingInterval.reportEnd.year
      month = reportingInterval.reportEnd.month
      day = reportingInterval.reportEnd.day
    }

  return if (reportingInterval.reportStart.hasUtcOffset()) {
    val offsetDateTime =
      try {
        reportingInterval.reportStart.toOffsetDateTime()
      } catch (e: DateTimeException) {
        throw Status.INVALID_ARGUMENT.withDescription(
            "report_start.utc_offset is not in valid range."
          )
          .asRuntimeException()
      }
    if (frequencySpec == null) {
      listOf(
        interval {
          startTime = offsetDateTime.toInstant().toProtoTime()
          endTime = reportEndDateTime.toOffsetDateTime().toInstant().toProtoTime()
        }
      )
    } else {
      generateTimeIntervals(
        offsetDateTime,
        reportEndDateTime.toOffsetDateTime(),
        frequencySpec,
        trailingWindow,
      )
    }
  } else {
    val zonedDateTime =
      try {
        reportingInterval.reportStart.toZonedDateTime()
      } catch (e: ZoneRulesException) {
        throw Status.INVALID_ARGUMENT.withDescription("report_start.time_zone.id is invalid")
          .asRuntimeException()
      }
    if (frequencySpec == null) {
      listOf(
        interval {
          startTime = zonedDateTime.toInstant().toProtoTime()
          endTime = reportEndDateTime.toZonedDateTime().toInstant().toProtoTime()
        }
      )
    } else {
      generateTimeIntervals(
        zonedDateTime,
        reportEndDateTime.toZonedDateTime(),
        frequencySpec,
        trailingWindow,
      )
    }
  }
}

/**
 * Generate a list of time intervals using [Temporal]s representing the start and end of the time
 * range to create time intervals for, the [MetricCalculationSpec.MetricFrequencySpec], and the
 * [MetricCalculationSpec.TrailingWindow].
 */
private fun generateTimeIntervals(
  reportStartTemporal: Temporal,
  reportEndTemporal: Temporal,
  frequencySpec: MetricCalculationSpec.MetricFrequencySpec,
  trailingWindow: MetricCalculationSpec.TrailingWindow? = null,
): List<Interval> {
  val firstTimeIntervalEndTemporal: Temporal =
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (frequencySpec.frequencyCase) {
      MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.DAILY -> reportStartTemporal
      MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.WEEKLY ->
        reportStartTemporal.with(
          TemporalAdjusters.nextOrSame(
            java.time.DayOfWeek.valueOf(frequencySpec.weekly.dayOfWeek.name)
          )
        )
      MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.MONTHLY -> {
        val lastDayOfMonthTemporal = reportStartTemporal.with(TemporalAdjusters.lastDayOfMonth())
        val lastDayOfMonth = lastDayOfMonthTemporal.get(ChronoField.DAY_OF_MONTH)
        if (
          frequencySpec.monthly.dayOfMonth <= lastDayOfMonth &&
            reportStartTemporal.get(ChronoField.DAY_OF_MONTH) <= frequencySpec.monthly.dayOfMonth
        ) {
          reportStartTemporal.with(
            TemporalAdjusters.ofDateAdjuster { date: LocalDate ->
              date.withDayOfMonth(frequencySpec.monthly.dayOfMonth)
            }
          )
        } else if (frequencySpec.monthly.dayOfMonth > lastDayOfMonth) {
          lastDayOfMonthTemporal
        } else {
          val nextMonthEndTemporal =
            reportStartTemporal.plus(Period.ofMonths(1)).with(TemporalAdjusters.lastDayOfMonth())
          nextMonthEndTemporal.with(
            TemporalAdjusters.ofDateAdjuster { date: LocalDate ->
              date.withDayOfMonth(
                minOf(
                  nextMonthEndTemporal.get(ChronoField.DAY_OF_MONTH),
                  frequencySpec.monthly.dayOfMonth,
                )
              )
            }
          )
        }
      }
      MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.FREQUENCY_NOT_SET -> {
        throw Status.INVALID_ARGUMENT.withDescription("frequency is not set").asRuntimeException()
      }
    }

  val isWindowReportStart = trailingWindow == null

  val reportStartTimestamp = reportStartTemporal.toTimestamp()
  val reportEndTimestamp = reportEndTemporal.toTimestamp()

  return buildList {
    var nextTimeIntervalEndTemporal = firstTimeIntervalEndTemporal
    // Set a cap to prevent an infinite loop in case of an error.
    val maxNumTimeIntervals = ChronoUnit.DAYS.between(reportStartTemporal, reportEndTemporal) + 1
    for (i in 1..maxNumTimeIntervals) {
      // An interval is only created if there is a period of time between the report start and the
      // potential end of the interval.
      if (ChronoUnit.SECONDS.between(reportStartTemporal, nextTimeIntervalEndTemporal) > 0L) {
        val nextTimeIntervalEndTimestamp = nextTimeIntervalEndTemporal.toTimestamp()

        val nextTimeIntervalStartTimestamp: Timestamp =
          if (isWindowReportStart) {
            reportStartTimestamp
          } else {
            val newTimestamp =
              buildReportTimeIntervalStartTimestamp(
                checkNotNull(trailingWindow),
                nextTimeIntervalEndTemporal,
              )

            // The start of any interval to be created is bounded by the report start.
            if (Timestamps.compare(reportStartTimestamp, newTimestamp) >= 0) {
              reportStartTimestamp
            } else {
              newTimestamp
            }
          }

        if (Timestamps.compare(nextTimeIntervalEndTimestamp, reportEndTimestamp) > 0) {
          if (Timestamps.compare(nextTimeIntervalStartTimestamp, reportEndTimestamp) < 0) {
            add(
              interval {
                startTime = nextTimeIntervalStartTimestamp
                endTime = reportEndTimestamp
              }
            )
          }
          break
        }

        add(
          interval {
            startTime = nextTimeIntervalStartTimestamp
            endTime = nextTimeIntervalEndTimestamp
          }
        )
      }

      nextTimeIntervalEndTemporal =
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (frequencySpec.frequencyCase) {
          MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.DAILY -> {
            nextTimeIntervalEndTemporal.plus(Period.ofDays(1))
          }
          MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.WEEKLY -> {
            nextTimeIntervalEndTemporal.with(
              TemporalAdjusters.next(
                java.time.DayOfWeek.valueOf(frequencySpec.weekly.dayOfWeek.name)
              )
            )
          }
          MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.MONTHLY -> {
            val nextMonthEndTemporal =
              nextTimeIntervalEndTemporal
                .plus(Period.ofMonths(1))
                .with(TemporalAdjusters.lastDayOfMonth())
            nextMonthEndTemporal.with(
              TemporalAdjusters.ofDateAdjuster { date: LocalDate ->
                date.withDayOfMonth(
                  minOf(
                    nextMonthEndTemporal.get(ChronoField.DAY_OF_MONTH),
                    frequencySpec.monthly.dayOfMonth,
                  )
                )
              }
            )
          }
          MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.FREQUENCY_NOT_SET -> {
            throw Status.FAILED_PRECONDITION.withDescription("frequency is not set")
              .asRuntimeException()
          }
        }
    }
  }
}

/**
 * Given a [MetricCalculationSpec.TrailingWindow] and a [Temporal] that represents the end of the
 * interval, create a [Timestamp] that represents the start of the interval.
 */
private fun buildReportTimeIntervalStartTimestamp(
  trailingWindow: MetricCalculationSpec.TrailingWindow,
  intervalEndTemporal: Temporal,
): Timestamp {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf case fields cannot be null.
  return when (trailingWindow.increment) {
    MetricCalculationSpec.TrailingWindow.Increment.DAY ->
      intervalEndTemporal.minus(Period.ofDays(trailingWindow.count))
    MetricCalculationSpec.TrailingWindow.Increment.WEEK ->
      intervalEndTemporal.minus(Period.ofWeeks(trailingWindow.count))
    MetricCalculationSpec.TrailingWindow.Increment.MONTH ->
      intervalEndTemporal.minus(Period.ofMonths(trailingWindow.count))
    MetricCalculationSpec.TrailingWindow.Increment.INCREMENT_UNSPECIFIED,
    MetricCalculationSpec.TrailingWindow.Increment.UNRECOGNIZED ->
      error("trailing_window missing increment")
  }.toTimestamp()
}
