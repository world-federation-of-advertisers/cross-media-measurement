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
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequest as InternalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.PeriodicTimeInterval as InternalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.TimeIntervals as InternalTimeIntervals
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.internal.reporting.v2.timeIntervals as internalTimeIntervals
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.CreateReportRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.PeriodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.TimeInterval
import org.wfanet.measurement.reporting.v2alpha.TimeIntervals
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.metric

private const val MAX_BATCH_SIZE_FOR_BATCH_CREATE_METRICS = 1000

class ReportsService(
  private val internalReportsStub: ReportsCoroutineStub,
  private val metricsStub: MetricsCoroutineStub
) : ReportsCoroutineImplBase() {

  private data class TimeRange(
    val canBeCumulative: Boolean,
    val timeIntervals: List<TimeInterval>,
    val cumulativeTimeIntervals: List<TimeInterval>
  )

  private data class CreateMetricInfo(
    val parent: String,
    val requestId: String,
    val reportingSet: String,
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

    // TODO(@riemanli): use internal method to get the report by request ID.

    grpcRequire(request.hasReport()) { "Report is not specified." }

    grpcRequire(request.report.reportingMetricEntriesList.isNotEmpty()) {
      "No ReportingMetricEntry is specified."
    }
    val createMetricRequests: MutableList<CreateMetricRequest> = mutableListOf()
    val intermediateInternalReportingMetricEntries:
      Map<Long, InternalReport.ReportingMetricCalculationSpec> =
      buildIntermediateInternalReportingMetricEntries(request, createMetricRequests)

    // Send createMetricRequests in batches.
    val metrics: List<Metric> =
      batchCreateMetrics(request.parent, principal.config.apiKey, createMetricRequests)

    // Update intermediate internalReportingMetricEntries with real external metric IDs.
    val internalReportingMetricEntries: Map<Long, InternalReport.ReportingMetricCalculationSpec> =
      updateIntermediateInternalReportingMetricEntries(
        intermediateInternalReportingMetricEntries,
        metrics
      )

    val internalCreateReportRequest: InternalCreateReportRequest =
      buildInternalCreateReportRequest(request, internalReportingMetricEntries)
    try {
      return internalReportsStub.createReport(internalCreateReportRequest).toReport()
    } catch (e: StatusException) {
      throw Exception("Unable to create a report in the reporting database.", e)
    }
  }

  private fun updateIntermediateInternalReportingMetricEntries(
    intermediate: Map<Long, InternalReport.ReportingMetricCalculationSpec>,
    metrics: List<Metric>
  ): Map<Long, InternalReport.ReportingMetricCalculationSpec> {
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

  private fun updateIntermediateInternalMetricCalculationSpec(
    intermediate: InternalReport.MetricCalculationSpec,
    metrics: List<Metric>,
  ): InternalReport.MetricCalculationSpec {
    return intermediate.copy {
      val updatedExternalMetricIds =
        externalMetricIds.map { index ->
          val metric = metrics[index.toInt()]
          val metricKey =
            MetricKey.fromName(metric.name) ?: error("Created a metric with a corrupted name.")
          apiIdToExternalId(metricKey.metricId)
        }
      externalMetricIds.clear()
      externalMetricIds += updatedExternalMetricIds
    }
  }

  private suspend fun batchCreateMetrics(
    parent: String,
    apiAuthenticationKey: String,
    createMetricRequests: List<CreateMetricRequest>
  ): List<Metric> {
    val batchCreateMetricsRequests = mutableListOf<BatchCreateMetricsRequest>()

    while (batchCreateMetricsRequests.size < createMetricRequests.size) {
      val fromIndex = batchCreateMetricsRequests.size
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
            Status.Code.NOT_FOUND -> Status.NOT_FOUND
            Status.Code.FAILED_PRECONDITION -> Status.FAILED_PRECONDITION
            else -> Status.UNKNOWN.withDescription("Unable to create metrics.")
          }
          .withCause(e)
          .asRuntimeException()
      }
    }
  }

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

  private fun buildInternalCreateReportRequest(
    request: CreateReportRequest,
    internalReportingMetricEntries: Map<Long, InternalReport.ReportingMetricCalculationSpec>,
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

  private fun buildIntermediateInternalReportingMetricEntries(
    request: CreateReportRequest,
    createMetricRequests: MutableList<CreateMetricRequest>,
  ): Map<Long, InternalReport.ReportingMetricCalculationSpec> {

    val timeRange: TimeRange = request.report.timeRange()
    val measurementConsumerKey = grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent))

    return request.report.reportingMetricEntriesList.associate { reportingMetricEntry ->
      grpcRequire(reportingMetricEntry.hasValue()) {
        "Value in ReportingMetricEntry with key-${reportingMetricEntry.key} is not set."
      }

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

      grpcRequire(reportingMetricEntry.value.metricCalculationSpecsList.isNotEmpty()) {
        "There is no MetricCalculationSpec associated to ${reportingMetricEntry.key}."
      }

      apiIdToExternalId(reportingSetKey.reportingSetId) to
        InternalReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecs +=
            reportingMetricEntry.value.metricCalculationSpecsList.map { metricCalculationSpec ->
              buildIntermediateMetricCalculationSpec(
                metricCalculationSpec,
                CreateMetricInfo(
                  request.parent,
                  request.requestId,
                  reportingMetricEntry.key,
                  timeRange
                ),
                createMetricRequests
              )
            }
        }
    }
  }

  private fun buildIntermediateMetricCalculationSpec(
    metricCalculationSpec: Report.MetricCalculationSpec,
    createMetricInfo: CreateMetricInfo,
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
        grpcRequire(createMetricInfo.timeRange.canBeCumulative) {
          "Cumulative can only be used with PeriodicTimeInterval."
        }
        createMetricInfo.timeRange.cumulativeTimeIntervals
      } else {
        createMetricInfo.timeRange.timeIntervals
      }

    // Expand groupings to predicate groups in Cartesian product
    val groupings: List<List<String>> =
      metricCalculationSpec.groupingsList.map { it.predicatesList }
    val allGroupingPredicates = groupings.flatten()
    grpcRequire(allGroupingPredicates.size == allGroupingPredicates.toSet().size) {
      "Cannot have duplicate predicates in different groupings."
    }
    val predicateCartesianProduct: List<List<String>> = cartesianProduct(groupings)

    // Expand to a list of internal metrics with the Cartesian product of metric specs,
    // predicate groups, and time intervals.
    val metrics: List<Metric> =
      timeIntervals.flatMap { timeInterval ->
        metricCalculationSpec.metricSpecsList.flatMap { metricSpec ->
          predicateCartesianProduct.map { predicateGroup ->
            metric {
              this.reportingSet = reportingSet
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
          // Placeholder. Using metrics.map.also to return the list of metricCalculationSpec and
          // store createMetricRequests
          val index = createMetricRequests.size.toLong()

          createMetricRequests += createMetricRequest {
            parent = createMetricInfo.parent
            this.metric = metric
            requestId =
              listOf(createMetricInfo.requestId, "metric", index.toString()).joinToString("_")
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

  private fun <T> cartesianProduct(lists: List<List<T>>): List<List<T>> {
    if (lists.isEmpty()) return lists

    var result: List<List<T>> = lists.first().map { listOf(it) }

    for (list in lists.drop(1)) {
      result = result.flatMap { xList -> list.map { yVal -> xList + listOf(yVal) } }
    }

    return result
  }

  // /** Builds an [InternalCreateReportRequest] from a public [CreateReportRequest]. */
  // private suspend fun buildInternalCreateReportRequest(
  //   request: CreateReportRequest,
  //   reportInfo: ReportInfo,
  //   namedSetOperationResults: Map<String, SetOperationResult>,
  // ): org.wfanet.measurement.internal.reporting.CreateReportRequest {
  //   val internalReport: org.wfanet.measurement.internal.reporting.Report = report {
  //     this.measurementConsumerReferenceId = reportInfo.measurementConsumerReferenceId
  //
  //     @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  //     when (request.report.timeCase) {
  //       Report.TimeCase.TIME_INTERVALS -> {
  //         this.timeIntervals = request.report.timeIntervals.toInternal()
  //       }
  //
  //       Report.TimeCase.PERIODIC_TIME_INTERVAL -> {
  //         this.periodicTimeInterval = request.report.periodicTimeInterval.toInternal()
  //       }
  //
  //       Report.TimeCase.TIME_NOT_SET ->
  //         failGrpc(Status.INVALID_ARGUMENT) { "The time in Report is not specified." }
  //     }
  //
  //     for (metric in request.report.metricsList) {
  //       this@internalReport.metrics +=
  //         buildInternalMetric(metric, reportInfo, namedSetOperationResults)
  //     }
  //
  //     details =
  //       ReportKt.details { this.eventGroupFilters.putAll(reportInfo.eventGroupFilters) }
  //
  //     this.reportIdempotencyKey = reportInfo.reportIdempotencyKey
  //   }
  //
  //   return createReportRequest {
  //     report = internalReport
  // }
}

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

private fun MetricSpec.ReachParams.toInternal(): InternalMetricSpec.ReachParams {
  val source = this
  grpcRequire(source.hasPrivacyParams()) { "privacyParams in reach is not set." }
  return InternalMetricSpecKt.reachParams { privacyParams = source.privacyParams.toInternal() }
}

private fun MetricSpec.DifferentialPrivacyParams.toInternal():
  InternalMetricSpec.DifferentialPrivacyParams {
  val source = this
  return InternalMetricSpecKt.differentialPrivacyParams {
    if (source.hasEpsilon()) {
      this.epsilon = source.epsilon
    }
    if (source.hasDelta()) {
      this.delta = source.delta
    }
  }
}

/** Converts a public [TimeIntervals] to an [InternalTimeIntervals]. */
private fun TimeIntervals.toInternal(): InternalTimeIntervals {
  val source = this
  return internalTimeIntervals {
    this.timeIntervals += source.timeIntervalsList.map { it.toInternal() }
  }
}

/** Converts a public [PeriodicTimeInterval] to an [InternalPeriodicTimeInterval]. */
private fun PeriodicTimeInterval.toInternal(): InternalPeriodicTimeInterval {
  val source = this
  return internalPeriodicTimeInterval {
    startTime = source.startTime
    increment = source.increment
    intervalCount = source.intervalCount
  }
}
