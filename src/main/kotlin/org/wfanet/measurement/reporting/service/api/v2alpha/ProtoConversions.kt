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
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Status
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodology
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.LiquidLegionsCountDistinct
import org.wfanet.measurement.api.v2alpha.LiquidLegionsDistribution
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.eventdataprovider.noiser.DpParams as NoiserDpParams
import org.wfanet.measurement.internal.reporting.v2.CustomDirectMethodology as InternalCustomDirectMethodology
import org.wfanet.measurement.internal.reporting.v2.CustomDirectMethodologyKt as InternalCustomDirectMethodologyKt
import org.wfanet.measurement.internal.reporting.v2.DeterministicCount
import org.wfanet.measurement.internal.reporting.v2.DeterministicCountDistinct
import org.wfanet.measurement.internal.reporting.v2.DeterministicDistribution
import org.wfanet.measurement.internal.reporting.v2.DeterministicSum
import org.wfanet.measurement.internal.reporting.v2.LiquidLegionsCountDistinct as InternalLiquidLegionsCountDistinct
import org.wfanet.measurement.internal.reporting.v2.LiquidLegionsDistribution as InternalLiquidLegionsDistribution
import org.wfanet.measurement.internal.reporting.v2.LiquidLegionsV2
import org.wfanet.measurement.internal.reporting.v2.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec as InternalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.NoiseMechanism as InternalNoiseMechanism
import org.wfanet.measurement.internal.reporting.v2.NoiseMechanism
import org.wfanet.measurement.internal.reporting.v2.PeriodicTimeInterval as InternalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.ReachOnlyLiquidLegionsV2
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule as InternalReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequestKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequest
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.TimeIntervals as InternalTimeIntervals
import org.wfanet.measurement.internal.reporting.v2.customDirectMethodology
import org.wfanet.measurement.internal.reporting.v2.liquidLegionsCountDistinct
import org.wfanet.measurement.internal.reporting.v2.liquidLegionsDistribution
import org.wfanet.measurement.internal.reporting.v2.liquidLegionsSketchParams
import org.wfanet.measurement.internal.reporting.v2.liquidLegionsV2
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.reachOnlyLiquidLegionsSketchParams
import org.wfanet.measurement.internal.reporting.v2.reachOnlyLiquidLegionsV2
import org.wfanet.measurement.internal.reporting.v2.streamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.streamReportsRequest
import org.wfanet.measurement.internal.reporting.v2.timeIntervals as internalTimeIntervals
import org.wfanet.measurement.measurementconsumer.stats.NoiseMechanism as StatsNoiseMechanism
import org.wfanet.measurement.measurementconsumer.stats.VidSamplingInterval as StatsVidSamplingInterval
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportsPageToken
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.PeriodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportSchedule
import org.wfanet.measurement.reporting.v2alpha.ReportScheduleKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.TimeIntervals
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportSchedule
import org.wfanet.measurement.reporting.v2alpha.reportingSet
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

/** Converts a [MetricSpec.VidSamplingInterval] to an [InternalMetricSpec.VidSamplingInterval]. */
fun MetricSpec.VidSamplingInterval.toInternal(): InternalMetricSpec.VidSamplingInterval {
  val source = this
  return InternalMetricSpecKt.vidSamplingInterval {
    start = source.start
    width = source.width
  }
}

/** Converts a [MetricSpec] to an [InternalMetricSpec]. */
fun MetricSpec.toInternal(): InternalMetricSpec {
  val source = this

  return internalMetricSpec {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (source.typeCase) {
      MetricSpec.TypeCase.REACH -> {
        reach = source.reach.toInternal()
      }
      MetricSpec.TypeCase.REACH_AND_FREQUENCY -> {
        reachAndFrequency = source.reachAndFrequency.toInternal()
      }
      MetricSpec.TypeCase.IMPRESSION_COUNT -> {
        impressionCount = source.impressionCount.toInternal()
      }
      MetricSpec.TypeCase.WATCH_DURATION -> {
        watchDuration = source.watchDuration.toInternal()
      }
      MetricSpec.TypeCase.POPULATION_COUNT -> {
        populationCount = InternalMetricSpec.PopulationCountParams.getDefaultInstance()
      }
      MetricSpec.TypeCase.TYPE_NOT_SET ->
        throw MetricSpecDefaultsException(
          "Invalid metric spec type",
          IllegalArgumentException("The metric type in Metric is not specified.")
        )
    }

    if (source.hasVidSamplingInterval()) {
      vidSamplingInterval = source.vidSamplingInterval.toInternal()
    }
  }
}

/** Converts a [MetricSpec.WatchDurationParams] to an [InternalMetricSpec.WatchDurationParams]. */
fun MetricSpec.WatchDurationParams.toInternal(): InternalMetricSpec.WatchDurationParams {
  val source = this
  if (!source.hasPrivacyParams()) {
    throw MetricSpecDefaultsException(
      "Invalid privacy params",
      IllegalArgumentException("privacyParams in watch duration is not set.")
    )
  }
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
fun MetricSpec.ImpressionCountParams.toInternal(): InternalMetricSpec.ImpressionCountParams {
  val source = this
  if (!source.hasPrivacyParams()) {
    throw MetricSpecDefaultsException(
      "Invalid privacy params",
      IllegalArgumentException("privacyParams in impression count is not set.")
    )
  }
  return InternalMetricSpecKt.impressionCountParams {
    privacyParams = source.privacyParams.toInternal()
    if (source.hasMaximumFrequencyPerUser()) {
      maximumFrequencyPerUser = source.maximumFrequencyPerUser
    }
  }
}

/**
 * Converts a [MetricSpec.ReachAndFrequencyParams] to an
 * [InternalMetricSpec.ReachAndFrequencyParams].
 */
fun MetricSpec.ReachAndFrequencyParams.toInternal(): InternalMetricSpec.ReachAndFrequencyParams {
  val source = this
  if (!source.hasReachPrivacyParams()) {
    throw MetricSpecDefaultsException(
      "Invalid privacy params",
      IllegalArgumentException("reachPrivacyParams in reach-and-frequency is not set.")
    )
  }
  if (!source.hasFrequencyPrivacyParams()) {
    throw MetricSpecDefaultsException(
      "Invalid privacy params",
      IllegalArgumentException("frequencyPrivacyParams in reach-and-frequency is not set.")
    )
  }
  return InternalMetricSpecKt.reachAndFrequencyParams {
    reachPrivacyParams = source.reachPrivacyParams.toInternal()
    frequencyPrivacyParams = source.frequencyPrivacyParams.toInternal()
    maximumFrequency = source.maximumFrequency
  }
}

/** Converts a [MetricSpec.ReachParams] to an [InternalMetricSpec.ReachParams]. */
fun MetricSpec.ReachParams.toInternal(): InternalMetricSpec.ReachParams {
  val source = this
  if (!source.hasPrivacyParams()) {
    throw MetricSpecDefaultsException(
      "Invalid privacy params",
      IllegalArgumentException("privacyParams in reach is not set.")
    )
  }
  return InternalMetricSpecKt.reachParams { privacyParams = source.privacyParams.toInternal() }
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

/** Convert an [PeriodicTimeInterval] to a list of [Interval]s. */
fun PeriodicTimeInterval.toTimeIntervalsList(): List<Interval> {
  val source = this
  var startTime = checkNotNull(source.startTime)
  return (0 until source.intervalCount).map {
    interval {
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
      InternalMetricSpec.TypeCase.REACH_AND_FREQUENCY ->
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            maximumFrequency = source.reachAndFrequency.maximumFrequency
            reachPrivacyParams = source.reachAndFrequency.reachPrivacyParams.toPrivacyParams()
            frequencyPrivacyParams =
              source.reachAndFrequency.frequencyPrivacyParams.toPrivacyParams()
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
      InternalMetricSpec.TypeCase.POPULATION_COUNT -> {
        populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
      }
      InternalMetricSpec.TypeCase.TYPE_NOT_SET ->
        throw IllegalArgumentException("The metric type in Metric is not specified.")
    }
    if (source.hasVidSamplingInterval()) {
      vidSamplingInterval = source.vidSamplingInterval.toVidSamplingInterval()
    }
    // TODO(jojijac0b): To add model line check and assignment
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
 * Converts an [InternalMetricSpec.ReachAndFrequencyParams] to a
 * [MeasurementSpec.ReachAndFrequency].
 */
fun InternalMetricSpec.ReachAndFrequencyParams.toReachAndFrequency():
  MeasurementSpec.ReachAndFrequency {
  val source = this
  return MeasurementSpecKt.reachAndFrequency {
    reachPrivacyParams = source.reachPrivacyParams.toCmmsPrivacyParams()
    frequencyPrivacyParams = source.frequencyPrivacyParams.toCmmsPrivacyParams()
    maximumFrequency = source.maximumFrequency
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
fun Measurement.Result.toInternal(protocolConfig: ProtocolConfig): InternalMeasurement.Result {
  val source = this

  return InternalMeasurementKt.result {
    if (source.hasReach()) {
      reach = source.reach.toInternal(protocolConfig)
    }
    if (source.hasFrequency()) {
      frequency = source.frequency.toInternal(protocolConfig)
    }
    if (source.hasImpression()) {
      impression = source.impression.toInternal(protocolConfig)
    }
    if (source.hasWatchDuration()) {
      watchDuration = source.watchDuration.toInternal(protocolConfig)
    }
    if (source.hasPopulation()) {
      // Methodology in protocolCOnfig is not set for Population so it is not needed to convert to internal Populaiton
      population =  InternalMeasurementKt.ResultKt.population {
        value = source.population.value
      }
    }
  }
}

/**
 * Converts a [Measurement.Result.WatchDuration] to an internal
 * [InternalMeasurement.Result.WatchDuration].
 */
private fun Measurement.Result.WatchDuration.toInternal(
  protocolConfig: ProtocolConfig
): InternalMeasurement.Result.WatchDuration {
  val source = this

  return InternalMeasurementKt.ResultKt.watchDuration {
    value = source.value

    if (protocolConfig.protocolsList.any { it.hasDirect() }) {
      noiseMechanism = source.noiseMechanism.toInternal()
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (source.methodologyCase) {
        Measurement.Result.WatchDuration.MethodologyCase.METHODOLOGY_NOT_SET -> {}
        Measurement.Result.WatchDuration.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
          customDirectMethodology = source.customDirectMethodology.toInternal()
        }
        Measurement.Result.WatchDuration.MethodologyCase.DETERMINISTIC_SUM -> {
          deterministicSum = DeterministicSum.getDefaultInstance()
        }
      }
    } else {
      error("Measurement protocol is not set or not supported.")
    }
  }
}

/**
 * Converts a [Measurement.Result.Impression] to an internal
 * [InternalMeasurement.Result.Impression].
 */
private fun Measurement.Result.Impression.toInternal(
  protocolConfig: ProtocolConfig
): InternalMeasurement.Result.Impression {
  val source = this

  return InternalMeasurementKt.ResultKt.impression {
    value = source.value

    if (protocolConfig.protocolsList.any { it.hasDirect() }) {
      noiseMechanism = source.noiseMechanism.toInternal()
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (source.methodologyCase) {
        Measurement.Result.Impression.MethodologyCase.METHODOLOGY_NOT_SET -> {}
        Measurement.Result.Impression.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
          customDirectMethodology = source.customDirectMethodology.toInternal()
        }
        Measurement.Result.Impression.MethodologyCase.DETERMINISTIC_COUNT -> {
          deterministicCount = DeterministicCount.getDefaultInstance()
        }
      }
    } else {
      error("Measurement protocol is not set or not supported.")
    }
  }
}

/**
 * Converts a [Measurement.Result.Frequency] to an internal [InternalMeasurement.Result.Frequency].
 */
private fun Measurement.Result.Frequency.toInternal(
  protocolConfig: ProtocolConfig
): InternalMeasurement.Result.Frequency {
  val source = this

  return InternalMeasurementKt.ResultKt.frequency {
    relativeFrequencyDistribution.putAll(source.relativeFrequencyDistributionMap)

    if (protocolConfig.protocolsList.any { it.hasDirect() }) {
      noiseMechanism = source.noiseMechanism.toInternal()
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (source.methodologyCase) {
        Measurement.Result.Frequency.MethodologyCase.METHODOLOGY_NOT_SET -> {}
        Measurement.Result.Frequency.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
          customDirectMethodology = source.customDirectMethodology.toInternal()
        }
        Measurement.Result.Frequency.MethodologyCase.DETERMINISTIC_DISTRIBUTION -> {
          deterministicDistribution = DeterministicDistribution.getDefaultInstance()
        }
        Measurement.Result.Frequency.MethodologyCase.LIQUID_LEGIONS_DISTRIBUTION -> {
          liquidLegionsDistribution = source.liquidLegionsDistribution.toInternal()
        }
      }
    } else if (protocolConfig.protocolsList.any { it.hasLiquidLegionsV2() }) {
      val cmmsProtocol =
        protocolConfig.protocolsList.first { it.hasLiquidLegionsV2() }.liquidLegionsV2
      noiseMechanism = cmmsProtocol.noiseMechanism.toInternal()
      liquidLegionsV2 = cmmsProtocol.toInternal()
    } else {
      error("Measurement protocol is not set or not supported.")
    }
  }
}

/** Converts a [Measurement.Result.Reach] to an internal [InternalMeasurement.Result.Reach]. */
private fun Measurement.Result.Reach.toInternal(
  protocolConfig: ProtocolConfig
): InternalMeasurement.Result.Reach {
  val source = this

  return InternalMeasurementKt.ResultKt.reach {
    value = source.value

    if (protocolConfig.protocolsList.any { it.hasDirect() }) {
      noiseMechanism = source.noiseMechanism.toInternal()
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (source.methodologyCase) {
        Measurement.Result.Reach.MethodologyCase.METHODOLOGY_NOT_SET -> {}
        Measurement.Result.Reach.MethodologyCase.CUSTOM_DIRECT_METHODOLOGY -> {
          customDirectMethodology = source.customDirectMethodology.toInternal()
        }
        Measurement.Result.Reach.MethodologyCase.DETERMINISTIC_COUNT_DISTINCT -> {
          deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
        }
        Measurement.Result.Reach.MethodologyCase.LIQUID_LEGIONS_COUNT_DISTINCT -> {
          liquidLegionsCountDistinct = source.liquidLegionsCountDistinct.toInternal()
        }
      }
    } else if (protocolConfig.protocolsList.any { it.hasLiquidLegionsV2() }) {
      val cmmsProtocol =
        protocolConfig.protocolsList.first { it.hasLiquidLegionsV2() }.liquidLegionsV2
      noiseMechanism = cmmsProtocol.noiseMechanism.toInternal()
      liquidLegionsV2 = cmmsProtocol.toInternal()
    } else if (protocolConfig.protocolsList.any { it.hasReachOnlyLiquidLegionsV2() }) {
      val cmmsProtocol =
        protocolConfig.protocolsList
          .first { it.hasReachOnlyLiquidLegionsV2() }
          .reachOnlyLiquidLegionsV2
      noiseMechanism = cmmsProtocol.noiseMechanism.toInternal()
      reachOnlyLiquidLegionsV2 = cmmsProtocol.toInternal()
    } else {
      error("Measurement protocol is not set or not supported.")
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
          reportingSetId = source.externalReportingSetId
        )
        .toName()

    displayName = source.displayName
    tags.putAll(source.details.tagsMap)
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
          ReportingSetKey(cmmsMeasurementConsumerId, source.externalReportingSetId)
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
    cmmsEventGroups +=
      source.eventGroupKeysList.map { eventGroupKey ->
        CmmsEventGroupKey(eventGroupKey.cmmsDataProviderId, eventGroupKey.cmmsEventGroupId).toName()
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

/** Converts an [InternalMetricCalculationSpec] to a [MetricCalculationSpec]. */
fun InternalMetricCalculationSpec.toMetricCalculationSpec(): MetricCalculationSpec {
  val source = this

  return metricCalculationSpec {
    displayName = source.details.displayName
    metricSpecs += source.details.metricSpecsList.map(InternalMetricSpec::toMetricSpec)
    groupings +=
      source.details.groupingsList.map { grouping ->
        MetricCalculationSpecKt.grouping { predicates += grouping.predicatesList }
      }
    filter = source.details.filter
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
  return timeIntervals { this.timeIntervals += source.timeIntervalsList }
}

/** Converts a public [TimeIntervals] to an [InternalTimeIntervals]. */
fun TimeIntervals.toInternal(): InternalTimeIntervals {
  val source = this
  return internalTimeIntervals { this.timeIntervals += source.timeIntervalsList }
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

/** Converts an [InternalReport.ReportingMetric] to a public [CreateMetricRequest]. */
fun InternalReport.ReportingMetric.toCreateMetricRequest(
  measurementConsumerKey: MeasurementConsumerKey,
  externalReportingSetId: String,
  filter: String,
): CreateMetricRequest {
  val source = this
  return createMetricRequest {
    this.parent = measurementConsumerKey.toName()
    metric = metric {
      reportingSet =
        ReportingSetKey(measurementConsumerKey.measurementConsumerId, externalReportingSetId)
          .toName()
      timeInterval = source.details.timeInterval
      metricSpec = source.details.metricSpec.toMetricSpec()
      filters += (source.details.groupingPredicatesList + filter).filter { it.isNotBlank() }
    }
    requestId = source.createMetricRequestId
    metricId = "a" + source.createMetricRequestId
  }
}

/**
 * Converts an internal ReportingMetricEntry Map.Entry<Long,
 * [InternalReport.ReportingMetricCalculationSpec]> to an [Report.ReportingMetricEntry].
 */
fun Map.Entry<String, InternalReport.ReportingMetricCalculationSpec>.toReportingMetricEntry(
  cmmsMeasurementConsumerId: String
): Report.ReportingMetricEntry {
  val source = this

  return ReportKt.reportingMetricEntry {
    key = ReportingSetKey(cmmsMeasurementConsumerId, source.key).toName()

    value =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecs +=
          source.value.metricCalculationSpecReportingMetricsList.map {
            MetricCalculationSpecKey(
                cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                metricCalculationSpecId = it.externalMetricCalculationSpecId
              )
              .toName()
          }
      }
  }
}

/** Converts a [ListReportsPageToken] to an internal [StreamReportsRequest]. */
fun ListReportsPageToken.toStreamReportsRequest(): StreamReportsRequest {
  val source = this
  return streamReportsRequest {
    // get one more than the actual page size for deciding whether to set page token
    limit = pageSize + 1
    filter =
      StreamReportsRequestKt.filter {
        cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
        if (source.hasLastReport()) {
          after =
            StreamReportsRequestKt.afterFilter {
              createTime = source.lastReport.createTime
              externalReportId = source.lastReport.externalReportId
            }
        }
      }
  }
}

/** Converts a CMMS [ProtocolConfig.NoiseMechanism] to an internal [InternalNoiseMechanism]. */
fun ProtocolConfig.NoiseMechanism.toInternal(): InternalNoiseMechanism {
  return when (this) {
    ProtocolConfig.NoiseMechanism.NONE -> InternalNoiseMechanism.NONE
    ProtocolConfig.NoiseMechanism.GEOMETRIC -> InternalNoiseMechanism.GEOMETRIC
    ProtocolConfig.NoiseMechanism.DISCRETE_GAUSSIAN -> InternalNoiseMechanism.DISCRETE_GAUSSIAN
    ProtocolConfig.NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED ->
      InternalNoiseMechanism.NOISE_MECHANISM_UNSPECIFIED
    ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE -> InternalNoiseMechanism.CONTINUOUS_LAPLACE
    ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN -> InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
    ProtocolConfig.NoiseMechanism.UNRECOGNIZED -> {
      throw NoiseMechanismUnrecognizedException("Noise mechanism $this is not recognized.")
    }
  }
}

/** Converts a CMMS [CustomDirectMethodology] to an internal [InternalCustomDirectMethodology]. */
fun CustomDirectMethodology.toInternal(): InternalCustomDirectMethodology {
  val source = this
  require(source.hasVariance()) { "Variance in CustomDirectMethodology is not set." }
  return customDirectMethodology {
    variance =
      InternalCustomDirectMethodologyKt.variance {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        when (source.variance.typeCase) {
          CustomDirectMethodology.Variance.TypeCase.SCALAR -> {
            scalar = source.variance.scalar
          }
          CustomDirectMethodology.Variance.TypeCase.FREQUENCY -> {
            frequency =
              InternalCustomDirectMethodologyKt.VarianceKt.frequencyVariances {
                variances.putAll(source.variance.frequency.variancesMap)
                kPlusVariances.putAll(source.variance.frequency.kPlusVariancesMap)
              }
          }
          CustomDirectMethodology.Variance.TypeCase.UNAVAILABLE -> {
            unavailable =
              InternalCustomDirectMethodologyKt.VarianceKt.unavailable {
                reason = source.variance.unavailable.reason.toInternal()
              }
          }
          CustomDirectMethodology.Variance.TypeCase.TYPE_NOT_SET -> {
            error("Variance in CustomDirectMethodology is not set.")
          }
        }
      }
  }
}

/**
 * Converts a CMMS [CustomDirectMethodology.Variance.Unavailable.Reason] to an internal
 * [InternalCustomDirectMethodology.Variance.Unavailable.Reason].
 */
private fun CustomDirectMethodology.Variance.Unavailable.Reason.toInternal():
  InternalCustomDirectMethodology.Variance.Unavailable.Reason {
  return when (this) {
    CustomDirectMethodology.Variance.Unavailable.Reason.REASON_UNSPECIFIED -> {
      error(
        "There is no reason specified about the unavailable variance in CustomDirectMethodology."
      )
    }
    CustomDirectMethodology.Variance.Unavailable.Reason.UNDERIVABLE -> {
      InternalCustomDirectMethodology.Variance.Unavailable.Reason.UNDERIVABLE
    }
    CustomDirectMethodology.Variance.Unavailable.Reason.INACCESSIBLE -> {
      InternalCustomDirectMethodology.Variance.Unavailable.Reason.INACCESSIBLE
    }
    CustomDirectMethodology.Variance.Unavailable.Reason.UNRECOGNIZED -> {
      error("Unrecognized reason of unavailable variance in CustomDirectMethodology.")
    }
  }
}

/**
 * Converts a CMMS [LiquidLegionsCountDistinct] to an internal [InternalLiquidLegionsCountDistinct].
 */
fun LiquidLegionsCountDistinct.toInternal(): InternalLiquidLegionsCountDistinct {
  val source = this
  return liquidLegionsCountDistinct {
    decayRate = source.decayRate
    maxSize = source.maxSize
  }
}

/** Converts a CMMS [ProtocolConfig.LiquidLegionsV2] to an internal [LiquidLegionsV2]. */
fun ProtocolConfig.LiquidLegionsV2.toInternal(): LiquidLegionsV2 {
  val source = this
  return liquidLegionsV2 {
    sketchParams = liquidLegionsSketchParams {
      decayRate = source.sketchParams.decayRate
      maxSize = source.sketchParams.maxSize
      samplingIndicatorSize = source.sketchParams.samplingIndicatorSize
    }
  }
}

/**
 * Converts a CMMS [ProtocolConfig.ReachOnlyLiquidLegionsV2] to an internal
 * [ReachOnlyLiquidLegionsV2].
 */
fun ProtocolConfig.ReachOnlyLiquidLegionsV2.toInternal(): ReachOnlyLiquidLegionsV2 {
  val source = this
  return reachOnlyLiquidLegionsV2 {
    sketchParams = reachOnlyLiquidLegionsSketchParams {
      decayRate = source.sketchParams.decayRate
      maxSize = source.sketchParams.maxSize
    }
  }
}

/**
 * Converts a CMMS [LiquidLegionsDistribution] to an internal [InternalLiquidLegionsDistribution].
 */
fun LiquidLegionsDistribution.toInternal(): InternalLiquidLegionsDistribution {
  val source = this
  return liquidLegionsDistribution {
    decayRate = source.decayRate
    maxSize = source.maxSize
  }
}

/** Converts an internal [InternalNoiseMechanism] to a [StatsNoiseMechanism]. */
fun InternalNoiseMechanism.toStatsNoiseMechanism(): StatsNoiseMechanism {
  return when (this) {
    NoiseMechanism.NONE -> StatsNoiseMechanism.NONE
    NoiseMechanism.GEOMETRIC,
    NoiseMechanism.CONTINUOUS_LAPLACE -> StatsNoiseMechanism.LAPLACE
    NoiseMechanism.DISCRETE_GAUSSIAN,
    NoiseMechanism.CONTINUOUS_GAUSSIAN -> StatsNoiseMechanism.GAUSSIAN
    NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED -> {
      throw NoiseMechanismUnspecifiedException("Internal noise mechanism should've been specified.")
    }
    NoiseMechanism.UNRECOGNIZED -> {
      throw NoiseMechanismUnrecognizedException("Internal noise mechanism $this is unrecognized.")
    }
  }
}

/** Converts an [InternalMetricSpec.VidSamplingInterval] to [StatsVidSamplingInterval]. */
fun InternalMetricSpec.VidSamplingInterval.toStatsVidSamplingInterval(): StatsVidSamplingInterval {
  val source = this
  return StatsVidSamplingInterval(source.start.toDouble(), source.width.toDouble())
}

/** Converts an [InternalMetricSpec.DifferentialPrivacyParams] to [NoiserDpParams]. */
fun InternalMetricSpec.DifferentialPrivacyParams.toNoiserDpParams(): NoiserDpParams {
  val source = this
  return NoiserDpParams(source.epsilon, source.delta)
}

/** Converts an internal [InternalReportSchedule] to a public [ReportSchedule]. */
fun InternalReportSchedule.toPublic(): ReportSchedule {
  val source = this

  val reportScheduleName =
    ReportScheduleKey(source.cmmsMeasurementConsumerId, source.externalReportScheduleId).toName()
  val reportTemplate = report {
    reportingMetricEntries +=
      source.details.reportTemplate.reportingMetricEntriesMap.map { internalReportingMetricEntry ->
        internalReportingMetricEntry.toReportingMetricEntry(source.cmmsMeasurementConsumerId)
      }
    tags.putAll(source.details.reportTemplate.details.tagsMap)
  }

  return reportSchedule {
    name = reportScheduleName
    displayName = source.details.displayName
    description = source.details.description
    this.reportTemplate = reportTemplate
    eventStart = source.details.eventStart
    eventEnd = source.details.eventEnd
    frequency = source.details.frequency.toPublic()
    reportWindow = source.details.reportWindow.toPublic()
    state = source.state.toPublic()
    nextReportCreationTime = source.nextReportCreationTime
    createTime = source.createTime
    updateTime = source.updateTime
  }
}

/** Converts an internal [InternalReportSchedule.State] to a public [ReportSchedule.State]. */
private fun InternalReportSchedule.State.toPublic(): ReportSchedule.State {
  return when (this) {
    InternalReportSchedule.State.ACTIVE -> ReportSchedule.State.ACTIVE
    InternalReportSchedule.State.STOPPED -> ReportSchedule.State.STOPPED
    InternalReportSchedule.State.STATE_UNSPECIFIED -> ReportSchedule.State.STATE_UNSPECIFIED
    InternalReportSchedule.State.UNRECOGNIZED ->
      // State is set by the system so if this is reached, something went wrong.
      throw Status.UNKNOWN.withDescription("There is an unknown problem with the ReportSchedule")
        .asRuntimeException()
  }
}

/**
 * Converts an internal [InternalReportSchedule.Frequency] to a public [ReportSchedule.Frequency].
 */
private fun InternalReportSchedule.Frequency.toPublic(): ReportSchedule.Frequency {
  val source = this
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (source.frequencyCase) {
    InternalReportSchedule.Frequency.FrequencyCase.DAILY ->
      ReportScheduleKt.frequency { daily = ReportSchedule.Frequency.Daily.getDefaultInstance() }
    InternalReportSchedule.Frequency.FrequencyCase.WEEKLY ->
      ReportScheduleKt.frequency {
        weekly = ReportScheduleKt.FrequencyKt.weekly { dayOfWeek = source.weekly.dayOfWeek }
      }
    InternalReportSchedule.Frequency.FrequencyCase.MONTHLY ->
      ReportScheduleKt.frequency {
        monthly = ReportScheduleKt.FrequencyKt.monthly { dayOfMonth = source.monthly.dayOfMonth }
      }
    InternalReportSchedule.Frequency.FrequencyCase.FREQUENCY_NOT_SET ->
      throw Status.FAILED_PRECONDITION.withDescription("ReportSchedule missing frequency")
        .asRuntimeException()
  }
}

/**
 * Converts an internal [InternalReportSchedule.ReportWindow] to a public
 * [ReportSchedule.ReportWindow].
 */
private fun InternalReportSchedule.ReportWindow.toPublic(): ReportSchedule.ReportWindow {
  val source = this
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
  return when (source.windowCase) {
    InternalReportSchedule.ReportWindow.WindowCase.TRAILING_WINDOW ->
      ReportScheduleKt.reportWindow {
        trailingWindow =
          ReportScheduleKt.ReportWindowKt.trailingWindow {
            count = source.trailingWindow.count
            increment =
              ReportSchedule.ReportWindow.TrailingWindow.Increment.forNumber(
                source.trailingWindow.increment.number
              )
                ?: throw Status.UNKNOWN.withDescription(
                    "There is an unknown problem with the ReportSchedule"
                  )
                  .asRuntimeException()
          }
      }
    InternalReportSchedule.ReportWindow.WindowCase.FIXED_WINDOW ->
      ReportScheduleKt.reportWindow { fixedWindow = source.fixedWindow }
    InternalReportSchedule.ReportWindow.WindowCase.WINDOW_NOT_SET ->
      throw Status.FAILED_PRECONDITION.withDescription("ReportSchedule missing report_window")
        .asRuntimeException()
  }
}
