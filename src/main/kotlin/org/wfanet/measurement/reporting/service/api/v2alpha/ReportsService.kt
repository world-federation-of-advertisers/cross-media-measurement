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

import com.google.type.Interval
import com.google.type.copy
import io.grpc.Status
import io.grpc.StatusException
import kotlin.math.min
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import org.projectnessie.cel.Env
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequest as InternalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportRequest as internalGetReportRequest
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.reporting.service.api.submitBatchRequests
import org.wfanet.measurement.reporting.service.api.v2alpha.MetadataPrincipalServerInterceptor.Companion.withPrincipalName
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
private const val BATCH_GET_METRICS_LIMIT = 100

private typealias InternalReportingMetricEntries =
  Map<String, InternalReport.ReportingMetricCalculationSpec>

class ReportsService(
  private val internalReportsStub: ReportsCoroutineStub,
  private val metricsStub: MetricsCoroutineStub,
  private val metricSpecConfig: MetricSpecConfig,
) : ReportsCoroutineImplBase() {

  private data class InternalTimeRange(
    val canBeCumulative: Boolean,
    val timeIntervals: List<Interval>,
    val cumulativeTimeIntervals: List<Interval>
  )

  private data class CreateReportInfo(
    val parent: String,
    val requestId: String,
    val internalTimeRange: InternalTimeRange,
  )

  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    val listReportsPageToken = request.toListReportsPageToken()

    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list Reports belonging to other MeasurementConsumers."
          }
        }
      }
    }

    val streamInternalReportsRequest: StreamReportsRequest =
      listReportsPageToken.toStreamReportsRequest()

    val results: List<InternalReport> =
      try {
        internalReportsStub.streamReports(streamInternalReportsRequest).toList()
      } catch (e: StatusException) {
        throw Exception("Unable to list Reports.", e)
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
    val metricNames: Flow<String> =
      subResults.flatMap { internalReport -> internalReport.metricNames }.distinct().asFlow()

    val callRpc: suspend (List<String>) -> BatchGetMetricsResponse = { items ->
      batchGetMetrics(principal.resourceKey.toName(), items)
    }
    val externalIdToMetricMap: Map<String, Metric> =
      submitBatchRequests(metricNames, BATCH_GET_METRICS_LIMIT, callRpc) { response ->
          response.metricsList
        }
        .toList()
        .associateBy { checkNotNull(MetricKey.fromName(it.name)).metricId }

    return listReportsResponse {
      reports +=
        filterReports(
          subResults.map { internalReport ->
            convertInternalReportToPublic(internalReport, externalIdToMetricMap)
          },
          request.filter
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

    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (reportKey.cmmsMeasurementConsumerId != principal.resourceKey.measurementConsumerId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get Report belonging to other MeasurementConsumers."
          }
        }
      }
    }

    val internalReport =
      try {
        internalReportsStub.getReport(
          internalGetReportRequest {
            cmmsMeasurementConsumerId = reportKey.cmmsMeasurementConsumerId
            externalReportId = reportKey.reportId
          }
        )
      } catch (e: StatusException) {
        throw Exception("Unable to get Report.", e)
      }

    // Get metrics.
    val metricNames: Flow<String> = internalReport.metricNames.distinct().asFlow()

    val callRpc: suspend (List<String>) -> BatchGetMetricsResponse = { items ->
      batchGetMetrics(principal.resourceKey.toName(), items)
    }
    val externalIdToMetricMap: Map<String, Metric> =
      submitBatchRequests(metricNames, BATCH_GET_METRICS_LIMIT, callRpc) { response ->
          response.metricsList
        }
        .toList()
        .associateBy { checkNotNull(MetricKey.fromName(it.name)).metricId }

    // Convert the internal report to public and return.
    return convertInternalReportToPublic(internalReport, externalIdToMetricMap)
  }

  private suspend fun batchGetMetrics(
    parent: String,
    metricNames: List<String>,
  ): BatchGetMetricsResponse {
    return try {
      metricsStub
        .withPrincipalName(parent)
        .batchGetMetrics(
          batchGetMetricsRequest {
            this.parent = parent
            names += metricNames
          }
        )
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT.withDescription(e.message)
          Status.Code.PERMISSION_DENIED -> Status.PERMISSION_DENIED.withDescription(e.message)
          else -> Status.UNKNOWN.withDescription("Unable to get Metrics.")
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

    val principal: ReportingPrincipal = principalFromCurrentContext

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a Report for another MeasurementConsumer."
          }
        }
      }
    }

    grpcRequire(request.hasReport()) { "Report is not specified." }
    grpcRequire(request.reportId.matches(RESOURCE_ID_REGEX)) { "Report ID is invalid." }

    grpcRequire(request.report.reportingMetricEntriesList.isNotEmpty()) {
      "No ReportingMetricEntry is specified."
    }

    // Build an internal CreateReportRequest.
    //  The internal report in CreateReportRequest has several
    //  MetricCalculationSpec.ReportingMetrics without request IDs and external metric IDs.
    val internalCreateReportRequest: InternalCreateReportRequest =
      buildInternalCreateReportRequest(request)

    // Create an internal report
    //  The internal report service will fill request IDs in
    //  MetricCalculationSpec.ReportingMetrics. If there are existing metrics based on the
    //  request IDs, the external metric IDs will also be filled.
    val internalReport =
      try {
        internalReportsStub.createReport(internalCreateReportRequest)
      } catch (e: StatusException) {
        throw Exception("Unable to create Report.", e)
      }

    // Create metrics.
    val createMetricRequests: Flow<CreateMetricRequest> =
      internalReport.reportingMetricEntriesMap
        .flatMap { (_, reportingMetricCalculationSpec) ->
          reportingMetricCalculationSpec.metricCalculationSpecsList.flatMap { metricCalculationSpec
            ->
            metricCalculationSpec.reportingMetricsList.map {
              it.toCreateMetricRequest(principal.resourceKey, metricCalculationSpec.details.filter)
            }
          }
        }
        .asFlow()

    val callRpc: suspend (List<CreateMetricRequest>) -> BatchCreateMetricsResponse = { items ->
      batchCreateMetrics(request.parent, items)
    }
    val externalIdToMetricMap: Map<String, Metric> =
      submitBatchRequests(createMetricRequests, BATCH_CREATE_METRICS_LIMIT, callRpc) {
          response: BatchCreateMetricsResponse ->
          response.metricsList
        }
        .toList()
        .associateBy { checkNotNull(MetricKey.fromName(it.name)).metricId }

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
        throw when (e.status.code) {
            Status.Code.ALREADY_EXISTS ->
              Status.ALREADY_EXISTS.withDescription(
                "Metric with ID ${request.reportId} already exists under ${request.parent}"
              )
            Status.Code.NOT_FOUND ->
              Status.NOT_FOUND.withDescription("ReportingSet used in the report not found.")
            Status.Code.FAILED_PRECONDITION ->
              Status.FAILED_PRECONDITION.withDescription(
                "Unable to create Report. The measurement consumer not found."
              )
            else -> Status.UNKNOWN.withDescription("Unable to create Report.")
          }
          .withCause(e)
          .asRuntimeException()
      }

    // Convert the internal report to public and return.
    return convertInternalReportToPublic(updatedInternalReport, externalIdToMetricMap)
  }

  /** Converts an internal [InternalReport] to a public [Report]. */
  private fun convertInternalReportToPublic(
    internalReport: InternalReport,
    externalIdToMetricMap: Map<String, Metric>,
  ): Report {
    return report {
      name =
        ReportKey(internalReport.cmmsMeasurementConsumerId, internalReport.externalReportId)
          .toName()

      reportingMetricEntries +=
        internalReport.reportingMetricEntriesMap.map { internalReportingMetricEntry ->
          internalReportingMetricEntry.toReportingMetricEntry(
            internalReport.cmmsMeasurementConsumerId
          )
        }

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (internalReport.timeCase) {
        InternalReport.TimeCase.TIME_INTERVALS ->
          this.timeIntervals = internalReport.timeIntervals.toTimeIntervals()
        InternalReport.TimeCase.PERIODIC_TIME_INTERVAL ->
          this.periodicTimeInterval = internalReport.periodicTimeInterval.toPeriodicTimeInterval()
        InternalReport.TimeCase.TIME_NOT_SET ->
          error("The time in the internal report should've been set.")
      }

      val metrics: List<Metric> =
        internalReport.externalMetricIds.map { externalMetricId ->
          externalIdToMetricMap.getValue(externalMetricId)
        }

      state = inferReportState(metrics)
      createTime = internalReport.createTime

      if (state == Report.State.SUCCEEDED) {
        this.metricCalculationResults +=
          buildMetricCalculationResults(
            internalReport.cmmsMeasurementConsumerId,
            internalReport.reportingMetricEntriesMap,
            externalIdToMetricMap
          )
      }
    }
  }

  /** Builds [Report.MetricCalculationResult]s. */
  private fun buildMetricCalculationResults(
    cmmsMeasurementConsumerId: String,
    internalReportingMetricEntries: InternalReportingMetricEntries,
    externalIdToMetricMap: Map<String, Metric>,
  ): List<Report.MetricCalculationResult> {
    return internalReportingMetricEntries.flatMap { (reportingSetId, reportingMetricCalculationSpec)
      ->
      val reportingSetName = ReportingSetKey(cmmsMeasurementConsumerId, reportingSetId).toName()

      reportingMetricCalculationSpec.metricCalculationSpecsList.map { metricCalculationSpec ->
        ReportKt.metricCalculationResult {
          displayName = metricCalculationSpec.details.displayName
          reportingSet = reportingSetName
          cumulative = metricCalculationSpec.details.cumulative
          resultAttributes +=
            metricCalculationSpec.reportingMetricsList.map { reportingMetric ->
              val metric =
                externalIdToMetricMap[reportingMetric.externalMetricId]
                  ?: error("Got a metric not associated with the report.")
              ReportKt.MetricCalculationResultKt.resultAttribute {
                groupingPredicates += reportingMetric.details.groupingPredicatesList
                filter = metricCalculationSpec.details.filter
                metricSpec = metric.metricSpec
                timeInterval = metric.timeInterval
                metricResult = metric.result
              }
            }
        }
      }
    }
  }

  /** Creates a batch of [Metric]s. */
  private suspend fun batchCreateMetrics(
    parent: String,
    createMetricRequests: List<CreateMetricRequest>
  ): BatchCreateMetricsResponse {
    return try {
      metricsStub
        .withPrincipalName(parent)
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

  /** Converts a [Report] to an [InternalTimeRange]. */
  private fun Report.internalTimeRange(): InternalTimeRange {
    val source = this
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (source.timeCase) {
      Report.TimeCase.TIME_INTERVALS -> {
        grpcRequire(source.timeIntervals.timeIntervalsList.isNotEmpty()) {
          "TimeIntervals timeIntervalsList is empty."
        }
        source.timeIntervals.timeIntervalsList.forEach {
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

        InternalTimeRange(
          false,
          source.timeIntervals.timeIntervalsList,
          listOf(),
        )
      }
      Report.TimeCase.PERIODIC_TIME_INTERVAL -> {
        grpcRequire(
          source.periodicTimeInterval.startTime.seconds > 0 ||
            source.periodicTimeInterval.startTime.nanos > 0
        ) {
          "PeriodicTimeInterval startTime is unspecified."
        }
        grpcRequire(
          source.periodicTimeInterval.increment.seconds > 0 ||
            source.periodicTimeInterval.increment.nanos > 0
        ) {
          "PeriodicTimeInterval increment is unspecified."
        }
        grpcRequire(source.periodicTimeInterval.intervalCount > 0) {
          "PeriodicTimeInterval intervalCount is unspecified."
        }

        val timeIntervals = source.periodicTimeInterval.toTimeIntervalsList()
        val cumulativeTimeIntervals =
          timeIntervals.map { timeInterval ->
            timeInterval.copy { startTime = timeIntervals.first().startTime }
          }
        InternalTimeRange(true, timeIntervals, cumulativeTimeIntervals)
      }
      Report.TimeCase.TIME_NOT_SET -> {
        failGrpc(Status.INVALID_ARGUMENT) { "Time in the report is not set." }
      }
    }
  }

  /** Builds an [InternalCreateReportRequest]. */
  private fun buildInternalCreateReportRequest(
    request: CreateReportRequest,
  ): InternalCreateReportRequest {
    val cmmsMeasurementConsumerId =
      checkNotNull(MeasurementConsumerKey.fromName(request.parent)).measurementConsumerId

    return internalCreateReportRequest {
      report = internalReport {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        reportingMetricEntries.putAll(buildInternalReportingMetricEntries(request))

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (request.report.timeCase) {
          Report.TimeCase.TIME_INTERVALS -> {
            this.timeIntervals = request.report.timeIntervals.toInternal()
          }
          Report.TimeCase.PERIODIC_TIME_INTERVAL -> {
            this.periodicTimeInterval = request.report.periodicTimeInterval.toInternal()
          }
          Report.TimeCase.TIME_NOT_SET ->
            failGrpc(Status.INVALID_ARGUMENT) { "The time in Report is not specified." }
        }
      }
      requestId = request.requestId
      externalReportId = request.reportId
    }
  }

  /** Builds an [InternalReportingMetricEntries] from a [CreateReportRequest]. */
  private fun buildInternalReportingMetricEntries(
    request: CreateReportRequest,
  ): InternalReportingMetricEntries {
    val internalTimeRange: InternalTimeRange = request.report.internalTimeRange()
    val measurementConsumerKey = checkNotNull(MeasurementConsumerKey.fromName(request.parent))
    val createReportInfo = CreateReportInfo(request.parent, request.requestId, internalTimeRange)

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
          metricCalculationSpecs +=
            reportingMetricEntry.value.metricCalculationSpecsList.map { metricCalculationSpec ->
              buildInternalMetricCalculationSpec(
                reportingSetId,
                metricCalculationSpec,
                createReportInfo,
              )
            }
        }
    }
  }

  /** Builds an [InternalReport.MetricCalculationSpec] from a [Report.MetricCalculationSpec]. */
  private fun buildInternalMetricCalculationSpec(
    reportingSetId: String,
    metricCalculationSpec: Report.MetricCalculationSpec,
    createReportInfo: CreateReportInfo,
  ): InternalReport.MetricCalculationSpec {
    grpcRequire(metricCalculationSpec.displayName.isNotEmpty()) {
      "Display name of MetricCalculationSpec must be set."
    }
    grpcRequire(metricCalculationSpec.metricSpecsList.isNotEmpty()) {
      "No metric spec in MetricCalculationSpec [${metricCalculationSpec.displayName}] is specified."
    }

    val timeIntervals: List<Interval> =
      if (metricCalculationSpec.cumulative) {
        grpcRequire(createReportInfo.internalTimeRange.canBeCumulative) {
          "Cumulative can only be used with PeriodicTimeInterval."
        }
        createReportInfo.internalTimeRange.cumulativeTimeIntervals
      } else {
        createReportInfo.internalTimeRange.timeIntervals
      }

    // Expand groupings to predicate groups in Cartesian product
    val groupings: List<List<String>> =
      metricCalculationSpec.groupingsList.map {
        grpcRequire(it.predicatesList.isNotEmpty()) {
          "The predicates in Grouping must be specified."
        }
        it.predicatesList
      }
    val allGroupingPredicates = groupings.flatten()
    grpcRequire(allGroupingPredicates.size == allGroupingPredicates.distinct().size) {
      "Cannot have duplicate predicates in different groupings."
    }
    val groupingsCartesianProduct: List<List<String>> = cartesianProduct(groupings)

    return InternalReportKt.metricCalculationSpec {
      val internalMetricSpecs = mutableListOf<InternalMetricSpec>()
      // Fan out to a list of reportingMetrics with the Cartesian product of metric specs,
      // predicate groups, and time intervals.
      reportingMetrics +=
        timeIntervals.flatMap { timeInterval ->
          metricCalculationSpec.metricSpecsList.flatMap { metricSpec ->
            groupingsCartesianProduct.map { groupingPredicates ->
              InternalReportKt.reportingMetric {
                details =
                  InternalReportKt.ReportingMetricKt.details {
                    this.externalReportingSetId = reportingSetId
                    this.metricSpec =
                      try {
                        metricSpec.withDefaults(metricSpecConfig).toInternal()
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
                    internalMetricSpecs += this.metricSpec
                  }
              }
            }
          }
        }

      details =
        InternalReportKt.MetricCalculationSpecKt.details {
          displayName = metricCalculationSpec.displayName
          metricSpecs += internalMetricSpecs.distinct()
          this.groupings +=
            metricCalculationSpec.groupingsList.map { grouping ->
              InternalReportKt.MetricCalculationSpecKt.grouping {
                this.predicates += grouping.predicatesList
              }
            }
          if (metricCalculationSpec.filter.isNotBlank()) {
            filter = metricCalculationSpec.filter
          }
          cumulative = metricCalculationSpec.cumulative
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

  companion object {
    private val RESOURCE_ID_REGEX = Regex("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")
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
  } else if (metricStates.any { it == Metric.State.FAILED }) {
    Report.State.FAILED
  } else {
    Report.State.RUNNING
  }
}

/** Gets all external metric IDs used in the [InternalReport]. */
private val InternalReport.externalMetricIds: List<String>
  get() =
    reportingMetricEntriesMap.flatMap { (_, reportingMetricCalculationSpec) ->
      reportingMetricCalculationSpec.metricCalculationSpecsList.flatMap { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.map { reportingMetric ->
          reportingMetric.externalMetricId
        }
      }
    }

private val InternalReport.metricNames: List<String>
  get() =
    externalMetricIds.map { externalMetricId ->
      MetricKey(cmmsMeasurementConsumerId, externalMetricId).toName()
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
