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

import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongGauge
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/** Encapsulates the OpenTelemetry instruments used by [DataAvailabilityMonitor]. */
class DataAvailabilityMonitorMetrics(meter: Meter = Instrumentation.meter) {
  /**
   * Number of days since the latest upload for a model line.
   *
   * Keyed by `edpa.data_availability_monitor.model_line` and
   * `edpa.data_availability_monitor.edp_impression_path`. The gauge value is the number of days
   * between the current date and the most recent date with a completed upload.
   */
  val staleDaysGauge: LongGauge =
    meter
      .gaugeBuilder("edpa.data_availability.stale_days")
      .setDescription("Number of days since the latest upload for a model line")
      .ofLongs()
      .build()

  /**
   * Cumulative count of missing date gaps detected for a model line.
   *
   * Keyed by `edpa.data_availability_monitor.model_line` and
   * `edpa.data_availability_monitor.edp_impression_path`. Each invocation adds the number of gap
   * dates found in that check run. Use `rate()` or `increase()` in queries to isolate per-run
   * values.
   */
  val gapCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.gaps")
      .setDescription("Number of missing date gaps detected for a model line")
      .build()

  /**
   * Cumulative count of dates with a "done" blob but no data files.
   *
   * Keyed by `edpa.data_availability_monitor.model_line` and
   * `edpa.data_availability_monitor.edp_impression_path`. Each invocation adds the number of
   * zero-impression dates found in that check run.
   */
  val zeroImpressionDatesCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.zero_impression_dates")
      .setDescription("Number of dates with done blob but no data files")
      .build()

  /**
   * Cumulative count of date folders missing a "done" blob.
   *
   * Keyed by `edpa.data_availability_monitor.model_line` and
   * `edpa.data_availability_monitor.edp_impression_path`. Each invocation adds the number of
   * affected dates found in that check run.
   */
  val datesWithoutDoneBlobCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.dates_without_done_blob")
      .setDescription("Number of date folders missing a done blob")
      .build()

  /**
   * Cumulative count of dates where data files were updated after the "done" blob.
   *
   * Keyed by `edpa.data_availability_monitor.model_line` and
   * `edpa.data_availability_monitor.edp_impression_path`. Each invocation adds the number of
   * late-arriving dates found in that check run.
   */
  val lateArrivingDatesCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.late_arriving_dates")
      .setDescription("Number of dates with files updated after the done blob")
      .build()

  /**
   * Cumulative count of fully healthy dates (done blob present, data files present, no late
   * arrivals).
   *
   * Keyed by `edpa.data_availability_monitor.model_line` and
   * `edpa.data_availability_monitor.edp_impression_path`. Each invocation adds the number of
   * healthy dates found in that check run.
   */
  val healthyDatesCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.healthy_dates")
      .setDescription("Number of dates with done blob, impressions, and no late arrivals")
      .build()
}
