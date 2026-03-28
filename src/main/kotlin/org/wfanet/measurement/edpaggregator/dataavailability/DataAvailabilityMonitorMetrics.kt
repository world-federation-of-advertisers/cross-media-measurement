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
  val staleDaysGauge: LongGauge =
    meter
      .gaugeBuilder("edpa.data_availability.stale_days")
      .setDescription("Number of days since the latest upload for a model line")
      .ofLongs()
      .build()

  val gapCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.gaps")
      .setDescription("Number of missing date gaps detected for a model line")
      .build()

  val zeroImpressionDatesCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.zero_impression_dates")
      .setDescription("Number of dates with done blob but no data files")
      .build()

  val datesWithoutDoneBlobCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.dates_without_done_blob")
      .setDescription("Number of date folders missing a done blob")
      .build()

  val lateArrivingDatesCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.late_arriving_dates")
      .setDescription("Number of dates with files updated after the done blob")
      .build()

  val healthyDatesCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.healthy_dates")
      .setDescription("Number of dates with done blob, impressions, and no late arrivals")
      .build()
}
