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

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/** Encapsulates the OpenTelemetry instruments used by DataAvailabilityCleanupFunction. */
class DataAvailabilityCleanupMetrics(meter: Meter = Instrumentation.meter) {
  val cleanupDurationHistogram: DoubleHistogram =
    meter
      .histogramBuilder("edpa.data_availability.cleanup_duration")
      .setDescription("Duration of data availability cleanup operation")
      .setUnit("s")
      .build()

  val recordsDeletedCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.records_deleted")
      .setDescription("Number of impression metadata records deleted")
      .build()

  val cleanupErrorsCounter: LongCounter =
    meter
      .counterBuilder("edpa.data_availability.cleanup_errors")
      .setDescription("Number of cleanup errors (e.g., record not found)")
      .build()

  companion object {
    val DATA_PROVIDER_KEY_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_cleanup.data_provider_key")
    val CLEANUP_STATUS_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_cleanup.cleanup_status")
    val ERROR_TYPE_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_cleanup.error_type")

    const val CLEANUP_STATUS_SUCCESS = "success"
    const val CLEANUP_STATUS_FAILED = "failed"
    const val CLEANUP_STATUS_SKIPPED = "skipped"
    const val ERROR_TYPE_NOT_FOUND = "not_found"
    const val ERROR_TYPE_RPC_ERROR = "rpc_error"
    const val ERROR_TYPE_MULTIPLE_MATCHES = "multiple_matches"
  }
}
