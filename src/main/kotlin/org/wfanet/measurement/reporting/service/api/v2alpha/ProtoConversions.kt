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

import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.TimeInterval as CmmsTimeInterval
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.timeInterval as cmmsTimeInterval
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequestKt
import org.wfanet.measurement.internal.reporting.v2.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.v2.streamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.timeInterval as internalTimeInterval
import org.wfanet.measurement.reporting.v2alpha.ListMetricsPageToken
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.TimeInterval
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
