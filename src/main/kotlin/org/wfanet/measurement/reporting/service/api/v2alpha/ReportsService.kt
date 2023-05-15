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
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequest as InternalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.CreateReportRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.TimeInterval
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.report

private const val MAX_BATCH_SIZE_FOR_BATCH_CREATE_METRICS = 1000

private typealias InternalReportingMetricEntries =
  Map<Long, InternalReport.ReportingMetricCalculationSpec>

class ReportsService(
  private val internalReportsStub: ReportsCoroutineStub,
  private val metricsStub: MetricsCoroutineStub
) : ReportsCoroutineImplBase() {

  private data class TimeRange(
    val canBeCumulative: Boolean,
    val timeIntervals: List<TimeInterval>,
    val cumulativeTimeIntervals: List<TimeInterval>
  )

  private data class CreateReportInfo(
    val parent: String,
    val requestId: String,
    val timeRange: TimeRange,
  )

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
    val createMetricRequests: MutableList<CreateMetricRequest> = mutableListOf()
    val intermediateInternalReportingMetricEntries: InternalReportingMetricEntries =
      buildIntermediateInternalReportingMetricEntries(request, createMetricRequests)

    // Send createMetricRequests in batches.
    val metrics: List<Metric> =
      batchCreateMetrics(request.parent, principal.config.apiKey, createMetricRequests)

    // Update intermediate internalReportingMetricEntries with metrics.
    val internalReportingMetricEntries: InternalReportingMetricEntries =
      updateIntermediateInternalReportingMetricEntries(
        intermediateInternalReportingMetricEntries,
        metrics
      )

    val internalCreateReportRequest: InternalCreateReportRequest =
      buildInternalCreateReportRequest(request, internalReportingMetricEntries)
    try {
      val internalReport = internalReportsStub.createReport(internalCreateReportRequest)
      return convertInternalReportToPublic(internalReport, metrics)
    } catch (e: StatusException) {
      throw Exception("Unable to create a report in the reporting database.", e)
    }
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
        internalReport.reportingMetricEntriesMap.map {
          (externalReportingSetId, internalReportingMetricCalculationSpec) ->
          ReportKt.reportingMetricEntry {
            key =
              ReportingSetKey(
                  internalReport.cmmsMeasurementConsumerId,
                  externalIdToApiId(externalReportingSetId)
                )
                .toName()

            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  internalReportingMetricCalculationSpec.metricCalculationSpecsList.map {
                    it.toMetricCalculationSpec()
                  }
              }
          }
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
      this.metricCalculationResults +=
        buildMetricCalculationResults(
          internalReport.cmmsMeasurementConsumerId,
          internalReport.reportingMetricEntriesMap,
          metrics
        )
    }
  }

  /** Builds [Report.MetricCalculationResult]s. */
  private fun buildMetricCalculationResults(
    cmmsMeasurementConsumerId: String,
    internalReportingMetricEntries: InternalReportingMetricEntries,
    metrics: List<Metric>,
  ): List<Report.MetricCalculationResult> {
    val metricsMap: Map<String, Metric> = metrics.associateBy { metric -> metric.name }
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
            metricCalculationSpec.externalMetricIdsList.map { externalMetricId ->
              val metricName =
                MetricKey(cmmsMeasurementConsumerId, externalIdToApiId(externalMetricId)).toName()

              val metric =
                metricsMap[metricName] ?: error("Got a metric not associated with the report.")
              ReportKt.MetricCalculationResultKt.resultAttribute {
                groupingPredicates += metric.filtersList
                timeInterval = metric.timeInterval
                metricResult = metric.result
              }
            }
        }
      }
    }
  }

  /**
   * Updates an intermediate [InternalReportingMetricEntries] by replacing
   * [InternalReport.MetricCalculationSpec]s in it with updated ones.
   */
  private fun updateIntermediateInternalReportingMetricEntries(
    intermediate: InternalReportingMetricEntries,
    metrics: List<Metric>
  ): InternalReportingMetricEntries {
    return intermediate.mapValues { (_, reportingMetricCalculationSpec) ->
      reportingMetricCalculationSpec.copy {
        val updatedMetricCalculationSpecs: List<InternalReport.MetricCalculationSpec> =
          metricCalculationSpecs.map { metricCalculationSpec ->
            updateIntermediateInternalMetricCalculationSpec(metricCalculationSpec, metrics)
          }

        metricCalculationSpecs.clear()
        metricCalculationSpecs += updatedMetricCalculationSpecs
      }
    }
  }

  /**
   * Updates an intermediate [InternalReport.MetricCalculationSpec] by updating the external metric
   * IDs and the metric specs.
   */
  private fun updateIntermediateInternalMetricCalculationSpec(
    intermediate: InternalReport.MetricCalculationSpec,
    metrics: List<Metric>,
  ): InternalReport.MetricCalculationSpec {
    return intermediate.copy {
      val updatedMetricSpecsSet = mutableSetOf<MetricSpec>()
      val updatedExternalMetricIds =
        externalMetricIds.map { index ->
          val metric = metrics[index.toInt()]
          val metricKey =
            MetricKey.fromName(metric.name) ?: error("Created a metric with a corrupted name.")

          updatedMetricSpecsSet += metric.metricSpec
          apiIdToExternalId(metricKey.metricId)
        }
      externalMetricIds.clear()
      // The sequence is conserved because of the way we put the placeholder IDs.
      externalMetricIds += updatedExternalMetricIds

      details =
        details.copy {
          metricSpecs.clear()
          // The sequence is conserved because of the way we fan-out metrics.
          metricSpecs += updatedMetricSpecsSet.toList().map { it.toInternal() }
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
            Status.Code.NOT_FOUND ->
              Status.NOT_FOUND.withDescription("Reporting set used in the metric not found.")
            Status.Code.FAILED_PRECONDITION ->
              Status.FAILED_PRECONDITION.withDescription("Measurement Consumer not found.")
            else -> Status.UNKNOWN.withDescription("Unable to create metrics.")
          }
          .withCause(e)
          .asRuntimeException()
      }
    }
  }

  /** Converts a [Report] to a [TimeRange]. */
  private fun Report.timeRange(): TimeRange {
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

        TimeRange(
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
        TimeRange(true, timeIntervals, cumulativeTimeIntervals)
      }
      Report.TimeCase.TIME_NOT_SET -> {
        failGrpc(Status.INVALID_ARGUMENT) { "Time in the report is not set." }
      }
    }
  }

  /** Builds an [InternalCreateReportRequest]. */
  private fun buildInternalCreateReportRequest(
    request: CreateReportRequest,
    internalReportingMetricEntries: InternalReportingMetricEntries,
  ): InternalCreateReportRequest {
    val cmmsMeasurementConsumerId =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)).measurementConsumerId
    return internalCreateReportRequest {
      report = internalReport {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        reportingMetricEntries.putAll(internalReportingMetricEntries)

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

  /**
   * Builds an intermediate [InternalReportingMetricEntries] from a [CreateReportRequest].
   *
   * The function builds an intermediate [InternalReportingMetricEntries] by converting the list of
   * [Report.ReportingMetricEntry]s in the request to the internal version, with the external metric
   * IDs filled with placeholder IDs. For each metric placeholder ID, the corresponding
   * [CreateMetricRequest] is stored into [createMetricRequests] at the position indexed by the
   * placeholder ID.
   */
  private fun buildIntermediateInternalReportingMetricEntries(
    request: CreateReportRequest,
    createMetricRequests: MutableList<CreateMetricRequest>,
  ): InternalReportingMetricEntries {
    val timeRange: TimeRange = request.report.timeRange()
    val measurementConsumerKey = grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent))
    val createReportInfo = CreateReportInfo(request.parent, request.requestId, timeRange)

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

      apiIdToExternalId(reportingSetKey.reportingSetId) to
        InternalReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecs +=
            reportingMetricEntry.value.metricCalculationSpecsList.map { metricCalculationSpec ->
              buildIntermediateMetricCalculationSpec(
                reportingMetricEntry.key,
                metricCalculationSpec,
                createReportInfo,
                createMetricRequests
              )
            }
        }
    }
  }

  /**
   * Builds an intermediate [InternalReport.MetricCalculationSpec] from a
   * [Report.MetricCalculationSpec].
   *
   * The function builds an intermediate [InternalReport.MetricCalculationSpec] by fanning out the
   * configs of creating metrics and converting [Report.MetricCalculationSpec] to the internal
   * version, with the external metric IDs filled with placeholder IDs. For each metric placeholder
   * ID, the corresponding [CreateMetricRequest] is stored into [createMetricRequests] at the
   * position indexed by the placeholder ID.
   */
  private fun buildIntermediateMetricCalculationSpec(
    reportingSetName: String,
    metricCalculationSpec: Report.MetricCalculationSpec,
    createReportInfo: CreateReportInfo,
    createMetricRequests: MutableList<CreateMetricRequest>,
  ): InternalReport.MetricCalculationSpec {
    grpcRequire(metricCalculationSpec.displayName.isNotBlank()) {
      "Display name of MetricCalculationSpec must be set."
    }
    grpcRequire(metricCalculationSpec.metricSpecsList.isNotEmpty()) {
      "No metric spec in MetricCalculationSpec [${metricCalculationSpec.displayName}] is specified."
    }

    val timeIntervals: List<TimeInterval> =
      if (metricCalculationSpec.cumulative) {
        grpcRequire(createReportInfo.timeRange.canBeCumulative) {
          "Cumulative can only be used with PeriodicTimeInterval."
        }
        createReportInfo.timeRange.cumulativeTimeIntervals
      } else {
        createReportInfo.timeRange.timeIntervals
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
    grpcRequire(allGroupingPredicates.size == allGroupingPredicates.toSet().size) {
      "Cannot have duplicate predicates in different groupings."
    }
    val groupingsCartesianProduct: List<List<String>> = cartesianProduct(groupings)

    // Fan out to a list of internal metrics with the Cartesian product of metric specs, predicate
    // groups, and time intervals. The order is fixed.
    val metrics: List<Metric> =
      timeIntervals.flatMap { timeInterval ->
        metricCalculationSpec.metricSpecsList.flatMap { metricSpec ->
          groupingsCartesianProduct.map { predicateGroup ->
            metric {
              this.reportingSet = reportingSetName
              this.timeInterval = timeInterval
              this.metricSpec = metricSpec
              this.filters += predicateGroup
            }
          }
        }
      }

    return InternalReportKt.metricCalculationSpec {
      externalMetricIds +=
        metrics.map { metric ->
          // A placeholder ID indicates the index of the corresponding createMetricRequest in
          // `createMetricRequests`.
          val index = createMetricRequests.size.toLong()

          createMetricRequests += createMetricRequest {
            parent = createReportInfo.parent
            this.metric = metric
            // Create a request ID for metric by concatenating report request ID, and the
            // placeholder ID.
            requestId =
              listOf(createReportInfo.requestId, "metric", index.toString()).joinToString("_")
          }

          index
        }
      details =
        InternalReportKt.MetricCalculationSpecKt.details {
          displayName = metricCalculationSpec.displayName
          metricSpecs += metricCalculationSpec.metricSpecsList.map { it.toInternal() }
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
   * @return the Cartesian product of the given lists.
   */
  private fun <T> cartesianProduct(lists: List<List<T>>): List<List<T>> {
    if (lists.isEmpty()) return lists

    var result: List<List<T>> = lists.first().map { listOf(it) }

    for (list in lists.drop(1)) {
      result = result.flatMap { xList -> list.map { yVal -> xList + listOf(yVal) } }
    }

    return result
  }
}

/** Converts a [MetricSpec] to an [InternalMetricSpec]. */
private fun MetricSpec.toInternal(): InternalMetricSpec {
  val source = this

  return internalMetricSpec {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (source.typeCase) {
      MetricSpec.TypeCase.REACH -> {
        reach = source.reach.toInternal()
      }
      MetricSpec.TypeCase.FREQUENCY_HISTOGRAM -> {
        frequencyHistogram = source.frequencyHistogram.toInternal()
      }
      MetricSpec.TypeCase.IMPRESSION_COUNT -> {
        impressionCount = source.impressionCount.toInternal()
      }
      MetricSpec.TypeCase.WATCH_DURATION -> {
        watchDuration = source.watchDuration.toInternal()
      }
      MetricSpec.TypeCase.TYPE_NOT_SET ->
        failGrpc(Status.INVALID_ARGUMENT) { "The metric type in Metric is not specified." }
    }

    if (source.hasVidSamplingInterval()) {
      vidSamplingInterval = source.vidSamplingInterval.toInternal()
    }
  }
}

/** Converts a [MetricSpec.WatchDurationParams] to an [InternalMetricSpec.WatchDurationParams]. */
private fun MetricSpec.WatchDurationParams.toInternal(): InternalMetricSpec.WatchDurationParams {
  val source = this
  grpcRequire(source.hasPrivacyParams()) { "privacyParams in watch duration is not set." }
  return InternalMetricSpecKt.watchDurationParams {
    privacyParams = source.privacyParams.toInternal()
    if (source.hasMaximumWatchDurationPerUser()) {
      maximumWatchDurationPerUser = source.maximumWatchDurationPerUser
    }
  }
}

/**
 * Converts a [MetricSpec.ImpressionCountParams] to an [InternalMetricSpec.ImpressionCountParams].
 */
private fun MetricSpec.ImpressionCountParams.toInternal():
  InternalMetricSpec.ImpressionCountParams {
  val source = this
  grpcRequire(source.hasPrivacyParams()) { "privacyParams in impression count is not set." }
  return InternalMetricSpecKt.impressionCountParams {
    privacyParams = source.privacyParams.toInternal()
    if (source.hasMaximumFrequencyPerUser()) {
      maximumFrequencyPerUser = source.maximumFrequencyPerUser
    }
  }
}

/**
 * Converts a [MetricSpec.FrequencyHistogramParams] to an
 * [InternalMetricSpec.FrequencyHistogramParams].
 */
private fun MetricSpec.FrequencyHistogramParams.toInternal():
  InternalMetricSpec.FrequencyHistogramParams {
  val source = this
  grpcRequire(source.hasReachPrivacyParams()) {
    "Reach privacyParams in frequency histogram is not set."
  }
  grpcRequire(source.hasFrequencyPrivacyParams()) {
    "Frequency privacyParams in frequency histogram is not set."
  }
  return InternalMetricSpecKt.frequencyHistogramParams {
    reachPrivacyParams = source.reachPrivacyParams.toInternal()
    frequencyPrivacyParams = source.frequencyPrivacyParams.toInternal()
    if (source.hasMaximumFrequencyPerUser()) {
      maximumFrequencyPerUser = source.maximumFrequencyPerUser
    }
  }
}

/** Converts a [MetricSpec.ReachParams] to an [InternalMetricSpec.ReachParams]. */
private fun MetricSpec.ReachParams.toInternal(): InternalMetricSpec.ReachParams {
  val source = this
  grpcRequire(source.hasPrivacyParams()) { "privacyParams in reach is not set." }
  return InternalMetricSpecKt.reachParams { privacyParams = source.privacyParams.toInternal() }
}

/** Infers the [Report.State] based on the [Metric]s. */
private fun inferReportState(metrics: List<Metric>): Report.State {
  val metricStates = metrics.map { it.state }
  return if (metricStates.all { it == Metric.State.SUCCEEDED }) {
    Report.State.SUCCEEDED
  } else if (metricStates.any { it == Metric.State.FAILED }) {
    Report.State.FAILED
  } else {
    Report.State.RUNNING
  }
}
