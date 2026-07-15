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
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.EdpConfig

/**
 * Cloud Function that runs [DashboardComplianceRunner] on a schedule (issue #3930).
 *
 * Where the post-deploy CI check ([DashboardComplianceCheck]) validates the freshly-reconciled
 * state right after `terraform apply`, this function runs independently against the *live* state
 * (daily via Cloud Scheduler) to catch drift between deploys — e.g. manual IAM changes or state
 * issues that a deploy would silently re-reconcile.
 *
 * ## Alerting
 * Each failed check is logged once at [Level.SEVERE] with an `ALERT:` prefix, and the function
 * returns HTTP 500 when any check fails, so both a Cloud Monitoring log-based alert policy and the
 * Cloud Scheduler job's failure status surface the problem. A successful run returns HTTP 200.
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
 */
class DashboardComplianceCheckFunction : HttpFunction {
  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.info(
        "Starting DashboardComplianceCheckFunction (project=$project, region=$region, " +
          "edps=${edps.map { it.name }})"
      )

      val report =
        DashboardComplianceRunner.run(project, dataset, region, impersonateServiceAccount, edps)

      if (report.allPassed) {
        response.setStatusCode(200)
        response.writer.write("All ${report.passed} dashboard compliance checks passed.")
      } else {
        // One SEVERE ("ALERT:") log per failed check — these drive the log-based alert policy.
        for (result in report.results.filter { !it.passed }) {
          logger.log(Level.SEVERE, "ALERT: dashboard compliance check failed: ${result.message}")
        }
        response.setStatusCode(500)
        response.writer.write(
          "${report.failed} of ${report.passed + report.failed} dashboard compliance checks " +
            "failed. See logs (ALERT entries) for details."
        )
      }
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "ALERT: DashboardComplianceCheckFunction execution error", e)
      response.setStatusCode(500)
      response.writer.write("Internal error: ${e.message}")
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
