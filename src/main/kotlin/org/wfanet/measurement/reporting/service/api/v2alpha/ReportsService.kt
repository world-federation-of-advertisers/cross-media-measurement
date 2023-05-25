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

import io.grpc.Status
import io.grpc.StatusException
import kotlin.math.min
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequest as InternalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportRequest as internalGetReportRequest
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.CreateReportRequest
import org.wfanet.measurement.reporting.v2alpha.GetReportRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.batchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.report

private const val MAX_BATCH_SIZE_FOR_BATCH_CREATE_METRICS = 1000
private const val MAX_BATCH_SIZE_FOR_BATCH_GET_METRICS = 100

private typealias InternalReportingMetricEntries =
  Map<Long, InternalReport.ReportingMetricCalculationSpec>

class ReportsService(
  private val internalReportsStub: ReportsCoroutineStub,
  private val metricsStub: MetricsCoroutineStub,
  metricSpecConfig: MetricSpecConfig,
) : ReportsCoroutineImplBase() {

  private data class InternalTimeRange(
    val canBeCumulative: Boolean,
    val timeIntervals: List<InternalTimeInterval>,
    val cumulativeTimeIntervals: List<InternalTimeInterval>
  )

  private data class CreateReportInfo(
    val parent: String,
    val requestId: String,
    val internalTimeRange: InternalTimeRange,
  )

  private val metricSpecBuilder = MetricSpecBuilder(metricSpecConfig)

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
            externalReportId = apiIdToExternalId(reportKey.reportId)
          }
        )
      } catch (e: StatusException) {
        throw Exception("Unable to get the report from the reporting database.", e)
      }

    // Create metrics.
    val metricNames: List<String> =
      internalReport.reportingMetricEntriesMap.flatMap { (_, reportingMetricCalculationSpec) ->
        reportingMetricCalculationSpec.metricCalculationSpecsList.flatMap { metricCalculationSpec ->
          metricCalculationSpec.reportingMetricsList.map { reportingMetric ->
            MetricKey(
                principal.resourceKey.measurementConsumerId,
                externalIdToApiId(reportingMetric.externalMetricId)
              )
              .toName()
          }
        }
      }
    val metrics: List<Metric> =
      batchGetMetrics(principal.resourceKey.toName(), principal.config.apiKey, metricNames)

    // Convert the internal report to public and return.
    return convertInternalReportToPublic(internalReport, metrics)
  }

  private suspend fun batchGetMetrics(
    parent: String,
    apiAuthenticationKey: String,
    metricNames: List<String>,
  ): List<Metric> {
    val batchGetMetricsRequests = mutableListOf<BatchGetMetricsRequest>()

    while (batchGetMetricsRequests.size * MAX_BATCH_SIZE_FOR_BATCH_GET_METRICS < metricNames.size) {
      val fromIndex = batchGetMetricsRequests.size * MAX_BATCH_SIZE_FOR_BATCH_GET_METRICS
      val toIndex = min(fromIndex + MAX_BATCH_SIZE_FOR_BATCH_GET_METRICS, metricNames.size)

      batchGetMetricsRequests += batchGetMetricsRequest {
        this.parent = parent
        names += metricNames.slice(fromIndex until toIndex)
      }
    }

    return batchGetMetricsRequests.flatMap { batchGetMetricsRequest ->
      try {
        metricsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .batchGetMetrics(batchGetMetricsRequest)
          .metricsList
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT.withDescription(e.message)
            Status.Code.PERMISSION_DENIED -> Status.PERMISSION_DENIED.withDescription(e.message)
            else -> Status.UNKNOWN.withDescription("Unable to create metrics.")
          }
          .withCause(e)
          .asRuntimeException()
      }
    }
  }

  override suspend fun createReport(request: CreateReportRequest): Report {
    grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }

    val principal: ReportingPrincipal = principalFromCurrentContext

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a Report for another MeasurementConsumer."
          }
        }
      }
    }

    grpcRequire(request.hasReport()) { "Report is not specified." }

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
        throw Exception("Unable to create a report in the reporting database.", e)
      }

    // Create metrics.
    val createMetricRequests: List<CreateMetricRequest> =
      internalReport.reportingMetricEntriesMap
        .flatMap { (_, reportingMetricCalculationSpec) ->
          reportingMetricCalculationSpec.metricCalculationSpecsList.flatMap { metricCalculationSpec
            ->
            metricCalculationSpec.reportingMetricsList
          }
        }
        .map { it.toCreateMetricRequest(principal.resourceKey) }
    val metrics: List<Metric> =
      batchCreateMetrics(request.parent, principal.config.apiKey, createMetricRequests)

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
        throw Exception("Unable to create a report in the reporting database.", e)
      }

    // Convert the internal report to public and return.
    return convertInternalReportToPublic(updatedInternalReport, metrics)
  }

  /** Converts an internal [InternalReport] to a public [Report]. */
  private fun convertInternalReportToPublic(
    internalReport: InternalReport,
    metrics: List<Metric>,
  ): Report {
    return report {
      name =
        ReportKey(
            internalReport.cmmsMeasurementConsumerId,
            externalIdToApiId(internalReport.externalReportId)
          )
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

      state = inferReportState(metrics)
      createTime = internalReport.createTime

      if (state == Report.State.SUCCEEDED) {
        this.metricCalculationResults +=
          buildMetricCalculationResults(
            internalReport.cmmsMeasurementConsumerId,
            internalReport.reportingMetricEntriesMap,
            metrics
          )
      }
    }
  }

  /** Builds [Report.MetricCalculationResult]s. */
  private fun buildMetricCalculationResults(
    cmmsMeasurementConsumerId: String,
    internalReportingMetricEntries: InternalReportingMetricEntries,
    metrics: List<Metric>,
  ): List<Report.MetricCalculationResult> {
    val externalIdToMetricMap: Map<Long, Metric> =
      metrics.associateBy { apiIdToExternalId(checkNotNull(MetricKey.fromName(it.name)).metricId) }

    return internalReportingMetricEntries.flatMap {
      (externalReportingSetId, reportingMetricCalculationSpec) ->
      val reportingSetName =
        ReportingSetKey(cmmsMeasurementConsumerId, externalIdToApiId(externalReportingSetId))
          .toName()

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
                groupingPredicates += metric.filtersList
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
    apiAuthenticationKey: String,
    createMetricRequests: List<CreateMetricRequest>
  ): List<Metric> {
    val batchCreateMetricsRequests = mutableListOf<BatchCreateMetricsRequest>()

    while (
      batchCreateMetricsRequests.size * MAX_BATCH_SIZE_FOR_BATCH_CREATE_METRICS <
        createMetricRequests.size
    ) {
      val fromIndex = batchCreateMetricsRequests.size * MAX_BATCH_SIZE_FOR_BATCH_CREATE_METRICS
      val toIndex =
        min(fromIndex + MAX_BATCH_SIZE_FOR_BATCH_CREATE_METRICS, createMetricRequests.size)

      batchCreateMetricsRequests += batchCreateMetricsRequest {
        this.parent = parent
        requests += createMetricRequests.slice(fromIndex until toIndex)
      }
    }

    return batchCreateMetricsRequests.flatMap { batchCreateMetricsRequest ->
      try {
        metricsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .batchCreateMetrics(batchCreateMetricsRequest)
          .metricsList
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.PERMISSION_DENIED -> Status.PERMISSION_DENIED.withDescription(e.message)
            Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT.withDescription(e.message)
            Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription(e.message)
            Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION.withDescription(e.message)
            else -> Status.UNKNOWN.withDescription("Unable to create metrics.")
          }
          .withCause(e)
          .asRuntimeException()
      }
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
          source.timeIntervals.timeIntervalsList.map { it.toInternal() },
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

        val timeIntervals =
          source.periodicTimeInterval.toTimeIntervalsList().map { it.toInternal() }
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

      val externalReportingSetId = apiIdToExternalId(reportingSetKey.reportingSetId)

      externalReportingSetId to
        InternalReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecs +=
            reportingMetricEntry.value.metricCalculationSpecsList.map { metricCalculationSpec ->
              buildInternalMetricCalculationSpec(
                externalReportingSetId,
                metricCalculationSpec,
                createReportInfo,
              )
            }
        }
    }
  }

  /** Builds an [InternalReport.MetricCalculationSpec] from a [Report.MetricCalculationSpec]. */
  private fun buildInternalMetricCalculationSpec(
    externalReportingSetId: Long,
    metricCalculationSpec: Report.MetricCalculationSpec,
    createReportInfo: CreateReportInfo,
  ): InternalReport.MetricCalculationSpec {
    grpcRequire(metricCalculationSpec.displayName.isNotBlank()) {
      "Display name of MetricCalculationSpec must be set."
    }
    grpcRequire(metricCalculationSpec.metricSpecsList.isNotEmpty()) {
      "No metric spec in MetricCalculationSpec [${metricCalculationSpec.displayName}] is specified."
    }

    val timeIntervals: List<InternalTimeInterval> =
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
            groupingsCartesianProduct.map { predicateGroup ->
              InternalReportKt.reportingMetric {
                details =
                  InternalReportKt.ReportingMetricKt.details {
                    this.externalReportingSetId = externalReportingSetId
                    this.metricSpec =
                      try {
                        metricSpecBuilder.buildMetricSpec(metricSpec).toInternal()
                      } catch (e: MetricSpecBuildingException) {
                        failGrpc(Status.INVALID_ARGUMENT) {
                          listOfNotNull("Invalid metric spec.", e.message, e.cause?.message)
                            .joinToString(separator = "\n")
                        }
                      } catch (e: Exception) {
                        failGrpc(Status.UNKNOWN) { "Failed to read the metric spec." }
                      }
                    this.timeInterval = timeInterval
                    filters += predicateGroup

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
