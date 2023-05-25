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

import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.TimeInterval as CmmsTimeInterval
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.timeInterval as cmmsTimeInterval
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.PeriodicTimeInterval as InternalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequestKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.v2.TimeIntervals as InternalTimeIntervals
import org.wfanet.measurement.internal.reporting.v2.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.streamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.timeInterval as internalTimeInterval
import org.wfanet.measurement.internal.reporting.v2.timeIntervals as internalTimeIntervals
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsPageToken
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.PeriodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.TimeInterval
import org.wfanet.measurement.reporting.v2alpha.TimeIntervals
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.timeInterval
import org.wfanet.measurement.reporting.v2alpha.timeIntervals

/**
 * Converts an [MetricSpecConfig.VidSamplingInterval] to an
 * [InternalMetricSpec.VidSamplingInterval].
 */
fun MetricSpecConfig.VidSamplingInterval.toInternal(): InternalMetricSpec.VidSamplingInterval {
  val source = this
  return InternalMetricSpecKt.vidSamplingInterval {
    start = source.start
    width = source.width
  }
}

/** Converts an [InternalMetricSpec.VidSamplingInterval] to a CMMS [VidSamplingInterval]. */
fun InternalMetricSpec.VidSamplingInterval.toCmmsVidSamplingInterval(): VidSamplingInterval {
  val source = this
  return MeasurementSpecKt.vidSamplingInterval {
    start = source.start
    width = source.width
  }
}

/** Converts an [InternalMetricSpec.VidSamplingInterval] to a [MetricSpec.VidSamplingInterval]. */
fun InternalMetricSpec.VidSamplingInterval.toVidSamplingInterval(): MetricSpec.VidSamplingInterval {
  val source = this
  return MetricSpecKt.vidSamplingInterval {
    start = source.start
    width = source.width
  }
}

/** Converts an [InternalTimeInterval] to a [CmmsTimeInterval]. */
fun InternalTimeInterval.toCmmsTimeInterval(): CmmsTimeInterval {
  val source = this
  return cmmsTimeInterval {
    startTime = source.startTime
    endTime = source.endTime
  }
}

/** Converts a [MetricSpec.VidSamplingInterval] to an [InternalMetricSpec.VidSamplingInterval]. */
fun MetricSpec.VidSamplingInterval.toInternal(): InternalMetricSpec.VidSamplingInterval {
  val source = this
  return InternalMetricSpecKt.vidSamplingInterval {
    start = source.start
    width = source.width
  }
}

/** Converts a public [TimeInterval] to an [InternalTimeInterval]. */
fun TimeInterval.toInternal(): InternalTimeInterval {
  val source = this
  return internalTimeInterval {
    startTime = source.startTime
    endTime = source.endTime
  }
}

/** Convert an [PeriodicTimeInterval] to a list of [TimeInterval]s. */
fun PeriodicTimeInterval.toTimeIntervalsList(): List<TimeInterval> {
  val source = this
  var startTime = checkNotNull(source.startTime)
  return (0 until source.intervalCount).map {
    timeInterval {
      this.startTime = startTime
      this.endTime = Timestamps.add(startTime, source.increment)
      startTime = this.endTime
    }
  }
}

/** Converts an [InternalMetricSpec] to a public [MetricSpec]. */
fun InternalMetricSpec.toMetricSpec(): MetricSpec {
  val source = this
  return metricSpec {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.typeCase) {
      InternalMetricSpec.TypeCase.REACH ->
        reach =
          MetricSpecKt.reachParams { privacyParams = source.reach.privacyParams.toPrivacyParams() }
      InternalMetricSpec.TypeCase.FREQUENCY_HISTOGRAM ->
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
            maximumFrequencyPerUser = source.frequencyHistogram.maximumFrequencyPerUser
            reachPrivacyParams = source.frequencyHistogram.reachPrivacyParams.toPrivacyParams()
            frequencyPrivacyParams =
              source.frequencyHistogram.frequencyPrivacyParams.toPrivacyParams()
          }
      InternalMetricSpec.TypeCase.IMPRESSION_COUNT ->
        impressionCount =
          MetricSpecKt.impressionCountParams {
            maximumFrequencyPerUser = source.impressionCount.maximumFrequencyPerUser
            privacyParams = source.impressionCount.privacyParams.toPrivacyParams()
          }
      InternalMetricSpec.TypeCase.WATCH_DURATION ->
        watchDuration =
          MetricSpecKt.watchDurationParams {
            maximumWatchDurationPerUser = source.watchDuration.maximumWatchDurationPerUser
            privacyParams = source.watchDuration.privacyParams.toPrivacyParams()
          }
      InternalMetricSpec.TypeCase.TYPE_NOT_SET ->
        throw IllegalArgumentException("The metric type in Metric is not specified.")
    }
    if (source.hasVidSamplingInterval()) {
      vidSamplingInterval = source.vidSamplingInterval.toVidSamplingInterval()
    }
  }
}

/**
 * Converts an [InternalMetricSpec.DifferentialPrivacyParams] to a public
 * [MetricSpec.DifferentialPrivacyParams].
 */
fun InternalMetricSpec.DifferentialPrivacyParams.toPrivacyParams():
  MetricSpec.DifferentialPrivacyParams {
  val source = this
  return MetricSpecKt.differentialPrivacyParams {
    epsilon = source.epsilon
    delta = source.delta
  }
}

/** Converts an [InternalTimeInterval] to a public [TimeInterval]. */
fun InternalTimeInterval.toTimeInterval(): TimeInterval {
  val source = this
  return timeInterval {
    startTime = source.startTime
    endTime = source.endTime
  }
}

/** Converts an [InternalMetricSpec.DifferentialPrivacyParams] to [DifferentialPrivacyParams]. */
fun InternalMetricSpec.DifferentialPrivacyParams.toCmmsPrivacyParams(): DifferentialPrivacyParams {
  val source = this
  return differentialPrivacyParams {
    epsilon = source.epsilon
    delta = source.delta
  }
}

/** Converts an [InternalMetricSpec.ReachParams] to a [MeasurementSpec.Reach]. */
fun InternalMetricSpec.ReachParams.toReach(): MeasurementSpec.Reach {
  val source = this
  return MeasurementSpecKt.reach { privacyParams = source.privacyParams.toCmmsPrivacyParams() }
}

/**
 * Converts an [InternalMetricSpec.FrequencyHistogramParams] to a
 * [MeasurementSpec.ReachAndFrequency].
 */
fun InternalMetricSpec.FrequencyHistogramParams.toReachAndFrequency():
  MeasurementSpec.ReachAndFrequency {
  val source = this
  return MeasurementSpecKt.reachAndFrequency {
    reachPrivacyParams = source.reachPrivacyParams.toCmmsPrivacyParams()
    frequencyPrivacyParams = source.frequencyPrivacyParams.toCmmsPrivacyParams()
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
  }
}

/** Builds a [MeasurementSpec.ReachAndFrequency] for impression count. */
fun InternalMetricSpec.ImpressionCountParams.toImpression(): MeasurementSpec.Impression {
  val source = this
  return MeasurementSpecKt.impression {
    privacyParams = source.privacyParams.toCmmsPrivacyParams()
    maximumFrequencyPerUser = source.maximumFrequencyPerUser
  }
}

/** Builds a [MeasurementSpec.ReachAndFrequency] for watch duration. */
fun InternalMetricSpec.WatchDurationParams.toDuration(): MeasurementSpec.Duration {
  val source = this
  return MeasurementSpecKt.duration {
    privacyParams = source.privacyParams.toCmmsPrivacyParams()
    maximumWatchDurationPerUser = source.maximumWatchDurationPerUser
  }
}

/** Converts a CMM [Measurement.Failure] to an [InternalMeasurement.Failure]. */
fun Measurement.Failure.toInternal(): InternalMeasurement.Failure {
  val source = this

  return InternalMeasurementKt.failure {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    reason =
      when (source.reason) {
        Measurement.Failure.Reason.REASON_UNSPECIFIED ->
          InternalMeasurement.Failure.Reason.REASON_UNSPECIFIED
        Measurement.Failure.Reason.CERTIFICATE_REVOKED ->
          InternalMeasurement.Failure.Reason.CERTIFICATE_REVOKED
        Measurement.Failure.Reason.REQUISITION_REFUSED ->
          InternalMeasurement.Failure.Reason.REQUISITION_REFUSED
        Measurement.Failure.Reason.COMPUTATION_PARTICIPANT_FAILED ->
          InternalMeasurement.Failure.Reason.COMPUTATION_PARTICIPANT_FAILED
        Measurement.Failure.Reason.UNRECOGNIZED -> InternalMeasurement.Failure.Reason.UNRECOGNIZED
      }
    message = source.message
  }
}

/** Converts a CMM [Measurement.Result] to an [InternalMeasurement.Result]. */
fun Measurement.Result.toInternal(): InternalMeasurement.Result {
  val source = this

  return InternalMeasurementKt.result {
    if (source.hasReach()) {
      this.reach = InternalMeasurementKt.ResultKt.reach { value = source.reach.value }
    }
    if (source.hasFrequency()) {
      this.frequency =
        InternalMeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.putAll(source.frequency.relativeFrequencyDistributionMap)
        }
    }
    if (source.hasImpression()) {
      this.impression =
        InternalMeasurementKt.ResultKt.impression { value = source.impression.value }
    }
    if (source.hasWatchDuration()) {
      this.watchDuration =
        InternalMeasurementKt.ResultKt.watchDuration { value = source.watchDuration.value }
    }
  }
}

/** Converts a [ListMetricsPageToken] to an internal [StreamMetricsRequest]. */
fun ListMetricsPageToken.toStreamMetricsRequest(): StreamMetricsRequest {
  val source = this
  return streamMetricsRequest {
    // get 1 more than the actual page size for deciding whether to set page token
    limit = source.pageSize + 1
    filter =
      StreamMetricsRequestKt.filter {
        cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
        externalMetricIdAfter = source.lastMetric.externalMetricId
      }
  }
}

/** Converts an [InternalReportingSet] to a public [ReportingSet]. */
fun InternalReportingSet.toReportingSet(): ReportingSet {
  val source = this
  return reportingSet {
    name =
      ReportingSetKey(
          cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId,
          reportingSetId = externalIdToApiId(source.externalReportingSetId)
        )
        .toName()

    displayName = source.displayName
    filter = source.filter

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (source.valueCase) {
      InternalReportingSet.ValueCase.PRIMITIVE -> {
        primitive = source.primitive.toPrimitive()
      }
      InternalReportingSet.ValueCase.COMPOSITE -> {
        composite =
          ReportingSetKt.composite {
            expression = source.composite.toExpression(source.cmmsMeasurementConsumerId)
          }
      }
      InternalReportingSet.ValueCase.VALUE_NOT_SET -> {
        error { "ReportingSet [$name] value should've been set." }
      }
    }
  }
}

/** Converts an [InternalReportingSet.SetExpression] to a [ReportingSet.SetExpression]. */
fun InternalReportingSet.SetExpression.toExpression(
  cmmsMeasurementConsumerId: String
): ReportingSet.SetExpression {
  val source = this

  return ReportingSetKt.setExpression {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    operation =
      when (source.operation) {
        InternalReportingSet.SetExpression.Operation.UNION -> {
          ReportingSet.SetExpression.Operation.UNION
        }
        InternalReportingSet.SetExpression.Operation.DIFFERENCE -> {
          ReportingSet.SetExpression.Operation.DIFFERENCE
        }
        InternalReportingSet.SetExpression.Operation.INTERSECTION -> {
          ReportingSet.SetExpression.Operation.INTERSECTION
        }
        InternalReportingSet.SetExpression.Operation.OPERATION_UNSPECIFIED -> {
          error { "Set expression operation type should've been set." }
        }
        InternalReportingSet.SetExpression.Operation.UNRECOGNIZED -> {
          error { "Unrecognized set expression operation type." }
        }
      }

    // Only set the operand when it has a type.
    if (
      source.lhs.operandCase !=
        InternalReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET
    ) {
      lhs = source.lhs.toOperand(cmmsMeasurementConsumerId)
    } else {
      error("Operand type in lhs of set expression should've been set.")
    }
    if (
      source.rhs.operandCase !=
        InternalReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET
    ) {
      rhs = source.rhs.toOperand(cmmsMeasurementConsumerId)
    }
  }
}

/**
 * Converts an [InternalReportingSet.SetExpression.Operand] to a
 * [ReportingSet.SetExpression.Operand].
 */
fun InternalReportingSet.SetExpression.Operand.toOperand(
  cmmsMeasurementConsumerId: String
): ReportingSet.SetExpression.Operand {
  val source = this

  return ReportingSetKt.SetExpressionKt.operand {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (source.operandCase) {
      InternalReportingSet.SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID -> {
        val reportingSetKey =
          ReportingSetKey(
            cmmsMeasurementConsumerId,
            externalIdToApiId(source.externalReportingSetId)
          )
        reportingSet = reportingSetKey.toName()
      }
      InternalReportingSet.SetExpression.Operand.OperandCase.EXPRESSION -> {
        expression = source.expression.toExpression(cmmsMeasurementConsumerId)
      }
      InternalReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        error("Unset operand type in set expression shouldn't reach this code.")
      }
    }
  }
}

/** Converts an [InternalReportingSet.Primitive] to a [ReportingSet.Primitive]. */
fun InternalReportingSet.Primitive.toPrimitive(): ReportingSet.Primitive {
  val source = this
  return ReportingSetKt.primitive {
    eventGroups +=
      source.eventGroupKeysList.map { eventGroupKey ->
        EventGroupKey(
            eventGroupKey.cmmsMeasurementConsumerId,
            eventGroupKey.cmmsDataProviderId,
            eventGroupKey.cmmsEventGroupId
          )
          .toName()
      }
  }
}

/** Converts a [ListReportingSetsPageToken] to an internal [StreamReportingSetsRequest]. */
fun ListReportingSetsPageToken.toStreamReportingSetsRequest(): StreamReportingSetsRequest {
  val source = this
  return streamReportingSetsRequest {
    // get 1 more than the actual page size for deciding whether to set page token
    limit = source.pageSize + 1
    filter =
      StreamReportingSetsRequestKt.filter {
        cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
        externalReportingSetIdAfter = source.lastReportingSet.externalReportingSetId
      }
  }
}

/** Converts an [InternalReport.MetricCalculationSpec] to a [Report.MetricCalculationSpec]. */
fun InternalReport.MetricCalculationSpec.toMetricCalculationSpec(
  requestIdToMetricMap: Map<String, Metric>,
): Report.MetricCalculationSpec {
  val source = this
  val metricSpecs: List<MetricSpec> =
    source.createMetricRequestsList
      .map { request -> requestIdToMetricMap.getValue(request.requestId).metricSpec }
      .distinct()

  return ReportKt.metricCalculationSpec {
    displayName = source.details.displayName
    this.metricSpecs += metricSpecs
    groupings +=
      source.details.groupingsList.map { grouping ->
        ReportKt.grouping { predicates += grouping.predicatesList }
      }
    cumulative = source.details.cumulative
  }
}

/** Converts an [InternalPeriodicTimeInterval] to a [PeriodicTimeInterval]. */
fun InternalPeriodicTimeInterval.toPeriodicTimeInterval(): PeriodicTimeInterval {
  val source = this
  return periodicTimeInterval {
    startTime = source.startTime
    increment = source.increment
    intervalCount = source.intervalCount
  }
}

/** Converts an [InternalTimeIntervals] to a [TimeIntervals]. */
fun InternalTimeIntervals.toTimeIntervals(): TimeIntervals {
  val source = this
  return timeIntervals {
    this.timeIntervals += source.timeIntervalsList.map { it.toTimeInterval() }
  }
}

/**
 * Converts a [MetricSpec.DifferentialPrivacyParams] to an
 * [InternalMetricSpec.DifferentialPrivacyParams].
 */
fun MetricSpec.DifferentialPrivacyParams.toInternal():
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
fun TimeIntervals.toInternal(): InternalTimeIntervals {
  val source = this
  return internalTimeIntervals {
    this.timeIntervals += source.timeIntervalsList.map { it.toInternal() }
  }
}

/** Converts a public [PeriodicTimeInterval] to an [InternalPeriodicTimeInterval]. */
fun PeriodicTimeInterval.toInternal(): InternalPeriodicTimeInterval {
  val source = this
  return internalPeriodicTimeInterval {
    startTime = source.startTime
    increment = source.increment
    intervalCount = source.intervalCount
  }
}

/** Converts an [InternalReport.CreateMetricRequest] to a public [CreateMetricRequest]. */
fun InternalReport.CreateMetricRequest.toCreateMetricRequest(
  measurementConsumerKey: MeasurementConsumerKey
): CreateMetricRequest {
  val source = this
  return createMetricRequest {
    this.parent = measurementConsumerKey.toName()
    metric = metric {
      reportingSet =
        ReportingSetKey(
            measurementConsumerKey.measurementConsumerId,
            externalIdToApiId(source.details.externalReportingSetId)
          )
          .toName()
      this.timeInterval = source.details.timeInterval.toTimeInterval()
      this.metricSpec = source.details.metricSpec.toMetricSpec()
      this.filters += source.details.filtersList
    }
    requestId = source.requestId
  }
}
