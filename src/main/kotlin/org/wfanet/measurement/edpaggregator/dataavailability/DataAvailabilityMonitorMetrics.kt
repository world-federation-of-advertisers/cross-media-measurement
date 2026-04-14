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

package org.wfanet.measurement.edpaggregator.dataavailability

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongGauge
import org.wfanet.measurement.common.Instrumentation

/** Encapsulates the OpenTelemetry instruments used by [DataAvailabilityMonitor]. */
object DataAvailabilityMonitorMetrics {
  /**
   * Number of days since the latest upload for a model line.
   *
   * Keyed by `edpa.data_availability_monitor.model_line` and
   * `edpa.data_availability_monitor.edp_impression_path`. The gauge value is the number of days
   * between the current date and the most recent date with a completed upload.
   */
  val staleDaysGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.data_availability.stale_days")
        .setDescription("Number of days since the latest upload for a model line")
        .ofLongs()
        .build()

  /**
   * Cumulative count of dates by availability status for a model line.
   *
   * Keyed by `edpa.data_availability_monitor.model_line`,
   * `edpa.data_availability_monitor.edp_impression_path`, and [DATE_STATUS_ATTR]. Each invocation
   * adds the number of dates found in that check run. Use `rate()` or `increase()` in queries to
   * isolate per-run values.
   */
  val dateStatusCounter: LongCounter
    get() =
      Instrumentation.meter
        .counterBuilder("edpa.data_availability.date_count")
        .setDescription("Number of dates by availability status")
        .setUnit("{date}")
        .build()

  val DATE_STATUS_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.data_availability_monitor.date_status")

  const val STATUS_GAP = "gap"
  const val STATUS_ZERO_IMPRESSION = "zero_impression"
  const val STATUS_WITHOUT_DONE_BLOB = "without_done_blob"
  const val STATUS_LATE_ARRIVING = "late_arriving"
  const val STATUS_HEALTHY = "healthy"
}
