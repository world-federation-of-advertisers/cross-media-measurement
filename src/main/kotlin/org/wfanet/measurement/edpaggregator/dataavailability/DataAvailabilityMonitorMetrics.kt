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
   * `edpa.data_availability_monitor.edp_impression_path`, [DATE_STATUS_ATTR], and [SOURCE_ATTR].
   * Monitor-emitted issue points are also keyed by [DATA_DATE_ATTR]. Date-based statuses add one
   * for each affected date, while spurious-deletion points add the number of affected resources
   * grouped by date. Summing across that attribute preserves the count by status. Healthy-date and
   * legitimate-deletion points omit [DATA_DATE_ATTR]. A spurious-deletion point has a date when its
   * blob URI follows the expected date-folder convention.
   *
   * Both [DataAvailabilityMonitor] (periodic, full status set) and [DataAvailabilitySync]
   * (per-batch, gap/without-done-blob/healthy only) write to this counter; the [SOURCE_ATTR]
   * distinguishes them so dashboards can filter or split by emitter. Use `rate()` or `increase()`
   * in queries to isolate per-run values.
   */
  val dateStatusCounter: LongCounter
    get() =
      Instrumentation.meter
        .counterBuilder("edpa.data_availability.date_count")
        .setDescription("Number of dates by availability status")
        .setUnit("{date}")
        .build()

  /**
   * Number of spurious deletions whose blob URI has no canonical data-date folder.
   *
   * Keyed by `edpa.data_availability_monitor.model_line`,
   * `edpa.data_availability_monitor.edp_impression_path`, and [SOURCE_ATTR]. A nonzero value means
   * the monitor could not attach [DATA_DATE_ATTR] to those spurious-deletion points; inspect the
   * warning logs for their resource names and blob URIs.
   */
  val noncanonicalSpuriousDeletionCountGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.data_availability.noncanonical_spurious_deletion_count")
        .setDescription("Number of spurious deletions without a canonical data date")
        .setUnit("{resource}")
        .ofLongs()
        .build()

  val MODEL_LINE_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.data_availability_monitor.model_line")
  val EDP_IMPRESSION_PATH_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.data_availability_monitor.edp_impression_path")
  val DATE_STATUS_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.data_availability_monitor.date_status")
  val SOURCE_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.data_availability_monitor.source")
  val DATA_DATE_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.data_availability_monitor.data_date")

  const val SOURCE_MONITOR = "monitor"
  const val SOURCE_SYNC = "sync"

  const val STATUS_GAP = "gap"
  const val STATUS_ZERO_IMPRESSION = "zero_impression"
  const val STATUS_WITHOUT_DONE_BLOB = "without_done_blob"
  const val STATUS_LATE_ARRIVING = "late_arriving"
  const val STATUS_UNPROCESSED_DONE = "unprocessed_done"
  const val STATUS_UNPUBLISHED_AVAILABILITY = "unpublished_availability"
  const val STATUS_HEALTHY = "healthy"
  const val STATUS_SPURIOUS_DELETION = "spurious_deletion"
  const val STATUS_LEGITIMATE_DELETION = "legitimate_deletion"
}
