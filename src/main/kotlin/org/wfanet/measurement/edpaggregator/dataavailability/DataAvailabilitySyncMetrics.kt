/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/** Encapsulates the OpenTelemetry instruments used by [DataAvailabilitySync]. */
class DataAvailabilitySyncMetrics(meter: Meter = Instrumentation.meter) {
  val syncDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edpa.data_availability.sync_duration")
      .setDescription("Duration of data availability sync fetch and write cycle")
      .setUnit("s")
      .build()

  val recordsSyncedCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.records_synced")
      .setDescription("Number of impression metadata records synced per EDP")
      .build()

  val cmmsRpcErrorsCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.cmms_rpc_errors")
      .setDescription("Number of CMMS API call errors")
      .build()
}
