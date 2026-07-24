/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.tools

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongGauge
import org.wfanet.measurement.common.Instrumentation

/** OpenTelemetry instruments recorded by [DashboardComplianceCheckFunction]. */
object DashboardComplianceMetrics {
  /**
   * Number of failed dashboard compliance checks in the most recent scheduled run, keyed by
   * [SECTION_ATTR] (e.g. "Data Isolation [meta]").
   *
   * Recorded once per section per run (0 when a section is clean), exported to Cloud Monitoring. A
   * value > 0 for any section indicates that area's per-EDP isolation / IAM / drift posture has
   * regressed against live state; a metric-based alert policy fires on it and the [SECTION_ATTR]
   * label identifies where.
   */
  val failedChecksGauge: LongGauge
    get() =
      Instrumentation.meter
        .gaugeBuilder("edpa.dashboard_compliance.failed_checks")
        .setDescription("Number of failed dashboard compliance checks in the most recent run")
        .ofLongs()
        .build()

  /**
   * Cumulative count of function execution errors (bad setup or an uncaught exception during a
   * run).
   *
   * Incremented in the function's catch block so a metric-based alert fires on genuine crashes —
   * distinct from a completed run that merely found failed checks (which returns HTTP 200 and is
   * captured by [failedChecksGauge]).
   *
   * Primed to 0 each run so the metric (and its alert policy) exists before any error occurs.
   */
  val errorsCounter: LongCounter
    get() =
      Instrumentation.meter
        .counterBuilder("edpa.dashboard_compliance.errors")
        .setDescription("Number of DashboardComplianceCheckFunction execution errors")
        .build()

  /** The compliance section a failed-check count belongs to (e.g. "Data Isolation [meta]"). */
  val SECTION_ATTR: AttributeKey<String> =
    AttributeKey.stringKey("edpa.dashboard_compliance.section")
}
