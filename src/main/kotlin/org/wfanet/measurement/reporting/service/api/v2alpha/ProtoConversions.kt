// Copyright 2023 The Cross-Media Measurement Authors/*
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.TimeInterval as CmmsTimeInterval
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.timeInterval as cmmsTimeInterval
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2.MetricResult as InternalMetricResult
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.v2.timeInterval as internalTimeInterval
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.HistogramResultKt.bin
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.HistogramResultKt.binResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.histogramResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.impressionCountResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.reachResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.watchDurationResult
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.TimeInterval
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.timeInterval

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

/** Converts an [InternalMetric] to a public [Metric]. */
fun InternalMetric.toMetric(): Metric {
  val source = this
  return metric {
    name =
      MetricKey(
          cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId,
          metricId = externalIdToApiId(source.externalMetricId)
        )
        .toName()
    reportingSet =
      ReportingSetKey(
          source.cmmsMeasurementConsumerId,
          externalIdToApiId(source.externalReportingSetId)
        )
        .toName()
    timeInterval = source.timeInterval.toTimeInterval()
    metricSpec = source.metricSpec.toMetricSpec()
    filters += source.details.filtersList
    state = source.state.toState()
    createTime = source.createTime
    if (source.details.hasResult()) {
      result = source.details.result.toResult()
    }
  }
}

/** Converts an [InternalMetricResult] to a public [MetricResult]. */
fun InternalMetricResult.toResult(): MetricResult {
  val source = this

  return metricResult {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (source.resultCase) {
      InternalMetricResult.ResultCase.REACH -> {
        reach = source.reach.toReachResult()
      }
      InternalMetricResult.ResultCase.FREQUENCY_HISTOGRAM -> {
        frequencyHistogram = source.frequencyHistogram.toHistogramResult()
      }
      InternalMetricResult.ResultCase.IMPRESSION_COUNT -> {
        impressionCount = source.impressionCount.toImpressionCountResult()
      }
      InternalMetricResult.ResultCase.WATCH_DURATION -> {
        watchDuration = source.watchDuration.toWatchDurationResult()
      }
      InternalMetricResult.ResultCase
        .RESULT_NOT_SET, -> {} // No action if the result hasn't been set yet.
    }
  }
}

/**
 * Converts an [InternalMetricResult.WatchDurationResult] to a public
 * [MetricResult.WatchDurationResult].
 */
fun InternalMetricResult.WatchDurationResult.toWatchDurationResult():
  MetricResult.WatchDurationResult {
  val source = this
  return watchDurationResult { value = source.value }
}

/**
 * Converts an [InternalMetricResult.ImpressionCountResult] to a public
 * [MetricResult.ImpressionCountResult].
 */
fun InternalMetricResult.ImpressionCountResult.toImpressionCountResult():
  MetricResult.ImpressionCountResult {
  val source = this
  return impressionCountResult { value = source.value }
}

/** Converts an [InternalMetricResult.ReachResult] to a public [MetricResult.ReachResult]. */
fun InternalMetricResult.ReachResult.toReachResult(): MetricResult.ReachResult {
  val source = this
  return reachResult { value = source.value }
}

/**
 * Converts an [InternalMetricResult.HistogramResult] to a public [MetricResult.HistogramResult].
 */
fun InternalMetricResult.HistogramResult.toHistogramResult(): MetricResult.HistogramResult {
  val source = this
  return histogramResult {
    bins +=
      source.binsList.map { internalBin ->
        bin {
          label = internalBin.label
          binResult = binResult { value = internalBin.binResult.value }
        }
      }
  }
}

/** Converts an [InternalMetric.State] to a public [Metric.State]. */
fun InternalMetric.State.toState(): Metric.State {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (this) {
    InternalMetric.State.RUNNING -> Metric.State.RUNNING
    InternalMetric.State.SUCCEEDED -> Metric.State.SUCCEEDED
    InternalMetric.State.FAILED -> Metric.State.FAILED
    InternalMetric.State.STATE_UNSPECIFIED -> error("Metric state should've been set.")
    InternalMetric.State.UNRECOGNIZED -> error("Unrecognized metric state.")
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
    vidSamplingInterval = source.vidSamplingInterval.toVidSamplingInterval()
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
