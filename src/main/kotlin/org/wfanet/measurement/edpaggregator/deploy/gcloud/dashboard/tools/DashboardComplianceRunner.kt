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

import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.ImpersonatedCredentials
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import java.util.logging.Logger
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.CheckResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.EdpConfig

/**
 * Runs the EDPA dashboard compliance/isolation checks and returns a structured [Report].
 *
 * Shared by both entry points so there is a single source of truth for the checks:
 * - [DashboardComplianceCheck] — the interactive CLI.
 * - [DashboardComplianceCheckFunction] — the scheduled Cloud Function (issue #3930).
 *
 * This class performs no printing and never calls [System.exit]; callers decide how to present the
 * result.
 */
object DashboardComplianceRunner {
  private val logger = Logger.getLogger(this::class.java.name)

  private val SCOPES = listOf("https://www.googleapis.com/auth/cloud-platform")

  /** A named group of [CheckResult]s (mirrors the CLI's section layout). */
  data class Section(val name: String, val results: List<CheckResult>)

  /** The full outcome of a compliance run. */
  data class Report(val sections: List<Section>) {
    val results: List<CheckResult>
      get() = sections.flatMap { it.results }

    val passed: Int
      get() = results.count { it.passed }

    val failed: Int
      get() = results.count { !it.passed }

    val allPassed: Boolean
      get() = failed == 0
  }

  /**
   * Runs all checks for the given [edps] and returns the [Report].
   *
   * @param impersonateServiceAccount if set, all checks run as this SA (via impersonation of ADC);
   *   otherwise ADC is used directly. Per-EDP checks additionally impersonate each
   *   `edp-<name>-dashboard` SA.
   */
  fun run(
    project: String,
    dataset: String,
    region: String,
    impersonateServiceAccount: String?,
    edps: List<EdpConfig>,
  ): Report {
    val checks = DashboardIsolationChecks(project, dataset, region)
    val baseCredentials = buildBaseCredentials(impersonateServiceAccount)
    val sections = mutableListOf<Section>()

    // Per-EDP checks, run as each EDP's dashboard SA.
    for (edp in edps) {
      val bq = bigQueryAsEdp(baseCredentials, project, edp)
      sections.add(Section("Data Isolation [${edp.name}]", checks.checkDataIsolation(bq, edp)))
      sections.add(Section("IAM Boundary [${edp.name}]", checks.checkIamBoundary(bq, edp)))
      sections.add(
        Section("EXTERNAL_QUERY Bypass [${edp.name}]", checks.checkExternalQueryBypass(bq, edp))
      )
      sections.add(Section("Data Correctness [${edp.name}]", checks.checkDataCorrectness(bq, edp)))
    }

    // Platform checks, run as the base (impersonated) identity.
    val bq = bigQueryDefault(baseCredentials, project)
    sections.add(Section("UDF Output Validation", checks.checkUdfOutputValidation(bq)))
    sections.add(Section("Drift Detection", checks.checkDriftDetection(bq, edps)))
    sections.add(Section("Data Freshness", checks.checkFreshness(bq)))

    return Report(sections)
  }

  /**
   * Parses `name:resourceId` pairs separated by `;` (e.g. `meta:AbCdEf_12345;google:GhIjKl_67890`).
   *
   * Blank entries are skipped. Each non-blank entry must contain a `:` and a non-empty resourceId;
   * a malformed entry throws [IllegalArgumentException] with an actionable message rather than
   * failing opaquely later.
   */
  fun parseEdps(raw: String): List<EdpConfig> {
    return raw
      .split(";")
      .map { it.trim() }
      .filter { it.isNotEmpty() }
      .map { entry ->
        val parts = entry.split(":", limit = 2)
        require(parts.size == 2 && parts[1].isNotEmpty()) {
          "Malformed DASHBOARD_EDPS entry (expected name:resourceId): '$entry'"
        }
        EdpConfig(parts[0], parts[1])
      }
  }

  private fun buildBaseCredentials(impersonateServiceAccount: String?): GoogleCredentials {
    val adc = GoogleCredentials.getApplicationDefault().createScoped(SCOPES)
    return if (impersonateServiceAccount.isNullOrEmpty()) {
      adc
    } else {
      ImpersonatedCredentials.create(adc, impersonateServiceAccount, null, SCOPES, 300)
    }
  }

  private fun bigQueryDefault(credentials: GoogleCredentials, project: String): BigQuery {
    return BigQueryOptions.newBuilder()
      .setCredentials(credentials)
      .setProjectId(project)
      .build()
      .service
  }

  private fun bigQueryAsEdp(
    credentials: GoogleCredentials,
    project: String,
    edp: EdpConfig,
  ): BigQuery {
    val saEmail = "edp-${edp.name}-dashboard@$project.iam.gserviceaccount.com"
    val impersonated = ImpersonatedCredentials.create(credentials, saEmail, null, SCOPES, 300)
    impersonated.refreshIfExpired()
    logger.info("Successfully impersonated $saEmail")
    return BigQueryOptions.newBuilder()
      .setCredentials(impersonated)
      .setProjectId(project)
      .build()
      .service
  }
}
