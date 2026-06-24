/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.validation

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/**
 * OpenTelemetry metrics for the EDP impression validation post-processor.
 *
 * @param meter the OpenTelemetry [Meter] to use for creating instruments.
 */
class EdpValidationMetrics(meter: Meter = Instrumentation.meter) {

  /** Histogram recording the deviation fraction between reported and publisher values. */
  val deviationFractionHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edp_validation.deviation_fraction")
      .setDescription("Deviation fraction between reported and publisher impression counts")
      .build()

  /** Counter for validation queries by verdict. */
  val queriesCounter: LongCounter =
    meter
      .counterBuilder("edp_validation.queries_total")
      .setDescription("Total validation queries by verdict")
      .build()

  /** Counter for reports failed due to validation. */
  val reportFailuresCounter: LongCounter =
    meter
      .counterBuilder("edp_validation.report_failures")
      .setDescription("Reports failed due to validation exceeding failure threshold")
      .build()

  /** Counter for skipped validation queries. */
  val skippedQueriesCounter: LongCounter =
    meter
      .counterBuilder("edp_validation.skipped_queries")
      .setDescription("Validation queries skipped by reason")
      .build()

  /** Histogram recording cloud function call duration in seconds. */
  val requestDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edp_validation.request_duration")
      .setDescription("Cloud function call duration")
      .setUnit("s")
      .build()

  /** Counter for cloud function errors. */
  val cloudFunctionErrorsCounter: LongCounter =
    meter
      .counterBuilder("edp_validation.cf_errors")
      .setDescription("Cloud function call errors by type")
      .build()

  companion object {
    val DATA_PROVIDER_KEY: AttributeKey<String> =
      AttributeKey.stringKey("edp_validation.data_provider")
    val VERDICT_KEY: AttributeKey<String> = AttributeKey.stringKey("edp_validation.verdict")
    val SKIP_REASON_KEY: AttributeKey<String> = AttributeKey.stringKey("edp_validation.skip_reason")
    val ERROR_TYPE_KEY: AttributeKey<String> = AttributeKey.stringKey("edp_validation.error_type")
  }
}
