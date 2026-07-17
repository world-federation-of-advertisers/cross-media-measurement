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

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import io.opentelemetry.api.common.Attributes
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.EdpConfig
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry

/**
 * Cloud Function that runs [DashboardComplianceRunner] on a schedule.
 *
 * Where the post-deploy CI check ([DashboardComplianceCheck]) validates the freshly-reconciled
 * state right after `terraform apply`, this function runs independently against the *live* state
 * (daily via Cloud Scheduler) to catch drift between deploys — e.g. manual IAM changes or state
 * issues that a deploy would silently re-reconcile.
 *
 * ## Alerting
 * Mirrors the sibling `DataAvailabilityMonitorFunction`: a completed run always returns HTTP 200,
 * and HTTP 500 is reserved for genuine function failures (the catch block), so a found isolation
 * regression does not make Cloud Scheduler treat the run as failed and retry. Alerting is
 * metric-based (exported to Cloud Monitoring):
 * - `edpa.dashboard_compliance.failed_checks`: per-section count of failed checks (labeled by
 *   section); a metric-based alert fires when any section is > 0.
 * - `edpa.dashboard_compliance.errors`: incremented on a genuine execution error so an alert fires
 *   on crashes without relying on HTTP 500.
 *
 * Each failed check is also logged once at [Level.SEVERE] with an `ALERT:` prefix for triage
 * detail.
 *
 * ## Environment Variables
 * - `GOOGLE_CLOUD_PROJECT`: Required. GCP project hosting the dashboard dataset.
 * - `BIGQUERY_REGION`: Required. BigQuery region.
 * - `DASHBOARD_EDPS`: Required. Semicolon-separated `name:resourceId` pairs, e.g.
 *   `meta:AbCdEf_12345;google:GhIjKl_67890`. (Semicolons are used so the value can travel in a
 *   comma-delimited Cloud Function env-var string.)
 * - `IMPERSONATE_SERVICE_ACCOUNT`: Optional. SA the checks run as (the least-privilege
 *   `dashboard-compliance` SA); defaults to ADC if unset.
 * - `DASHBOARD_DATASET`: Optional. BigQuery dataset ID (defaults to `dashboard`). The Terraform
 *   deployment always sets it explicitly.
 * - OpenTelemetry variables (`OTEL_METRICS_EXPORTER`, etc.): Configure metric export; set by the
 *   Terraform deployment.
 */
class DashboardComplianceCheckFunction : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.info(
        "Starting DashboardComplianceCheckFunction (project=$project, region=$region, " +
          "edps=${edps.map { it.name }})"
      )

      val report =
        DashboardComplianceRunner.run(project, dataset, region, impersonateServiceAccount, edps)

      // Record per-section failed-check counts (0 when a section is clean) so a metric-based alert
      // fires on the specific area/EDP that regressed. A found violation is data about a completed
      // run, not a function error.
      for (section in report.sections) {
        DashboardComplianceMetrics.failedChecksGauge.set(
          section.results.count { !it.passed }.toLong(),
          Attributes.of(DashboardComplianceMetrics.SECTION_ATTR, section.name),
        )
      }

      // One SEVERE ("ALERT:") log per failed check for triage detail.
      for (result in report.results.filter { !it.passed }) {
        logger.log(Level.SEVERE, "ALERT: dashboard compliance check failed: ${result.message}")
      }

      // A completed run is a successful invocation regardless of check outcomes.
      response.setStatusCode(200)
      response.writer.write(
        if (report.allPassed) {
          "All ${report.passed} dashboard compliance checks passed."
        } else {
          "${report.failed} of ${report.passed + report.failed} dashboard compliance checks " +
            "failed. See logs (ALERT entries) and the edpa.dashboard_compliance.failed_checks " +
            "metric."
        }
      )
    } catch (e: Exception) {
      // A genuine execution error (bad setup / uncaught exception): increment the error metric so a
      // metric-based alert fires. HTTP 500 alone is not the alert signal, since a completed run
      // with failed checks returns 200.
      DashboardComplianceMetrics.errorsCounter.add(1)
      logger.log(Level.SEVERE, "ALERT: DashboardComplianceCheckFunction execution error", e)
      response.setStatusCode(500)
      response.writer.write("Internal error: ${e.message}")
    } finally {
      EdpaTelemetry.flush()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val project: String = EnvVars.checkNotNullOrEmpty("GOOGLE_CLOUD_PROJECT")
    private val region: String = EnvVars.checkNotNullOrEmpty("BIGQUERY_REGION")
    private val dataset: String =
      System.getenv("DASHBOARD_DATASET")?.takeIf { it.isNotEmpty() } ?: "dashboard"
    private val impersonateServiceAccount: String? = System.getenv("IMPERSONATE_SERVICE_ACCOUNT")
    private val edps: List<EdpConfig> =
      DashboardComplianceRunner.parseEdps(EnvVars.checkNotNullOrEmpty("DASHBOARD_EDPS"))
  }
}
