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
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.CheckResult
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.EdpConfig
import picocli.CommandLine

@CommandLine.Command(
  name = "DashboardComplianceCheck",
  description = ["Verifies EDP isolation and dashboard compliance for any environment."],
  sortOptions = false,
)
class DashboardComplianceCheck : Runnable {

  @CommandLine.Option(names = ["--project"], required = true, description = ["GCP project ID"])
  private lateinit var project: String

  @CommandLine.Option(
    names = ["--dataset"],
    required = false,
    description = ["BigQuery dataset ID"],
    defaultValue = "dashboard",
  )
  private lateinit var dataset: String

  @CommandLine.Option(
    names = ["--edp"],
    required = true,
    description = ["EDP config as name:resourceId (repeatable)"],
    split = ",",
  )
  private lateinit var edpFlags: List<String>

  @CommandLine.Option(names = ["--region"], required = true, description = ["BigQuery region"])
  private lateinit var region: String

  @CommandLine.Option(
    names = ["--impersonate-service-account"],
    required = false,
    description = ["Service account to run all checks as (defaults to ADC if unset)"],
  )
  private var impersonateServiceAccount: String? = null

  private val edps by lazy {
    edpFlags.map { flag ->
      val (name, resourceId) = flag.split(":", limit = 2)
      EdpConfig(name, resourceId)
    }
  }

  private var passed = 0
  private var failed = 0

  override fun run() {
    println("=== EDPA Dashboard Compliance Check ===")
    println("Project: $project")
    println("Dataset: $dataset")
    println("EDPs: ${edps.map { it.name }}")
    println()

    val checks = DashboardIsolationChecks(project, dataset, region)

    // Per-EDP checks via impersonation
    for (edp in edps) {
      val bq = bigQueryAsEdp(edp)
      println("[Data Isolation]")
      report(checks.checkDataIsolation(bq, edp))
      println()
      println("[IAM Boundary]")
      report(checks.checkIamBoundary(bq, edp))
      println()
      println("[EXTERNAL_QUERY Bypass]")
      report(checks.checkExternalQueryBypass(bq, edp))
      println()
      println("[Data Correctness]")
      report(checks.checkDataCorrectness(bq, edp))
      println()
    }

    // Platform checks via default credentials
    val bq = bigQueryDefault()
    println("[UDF Output Validation]")
    report(checks.checkUdfOutputValidation(bq))
    println()
    println("[Drift Detection]")
    report(checks.checkDriftDetection(bq, edps))
    println()
    println("[Data Freshness]")
    report(checks.checkFreshness(bq))
    println()

    val total = passed + failed
    if (failed == 0) {
      println("=== RESULT: ALL CHECKS PASSED ($passed/$total) ===")
    } else {
      println(
        "=== RESULT: $failed CHECKS FAILED ($passed passed, $failed failed out of $total) ==="
      )
      System.exit(1)
    }
  }

  private fun report(results: List<CheckResult>) {
    for (result in results) {
      if (result.passed) {
        println("  OK  ${result.message}")
        passed++
      } else {
        println("  FAIL  ${result.message}")
        failed++
      }
    }
  }

  private val credentials: GoogleCredentials by lazy {
    val scopes =
      arrayOf(
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
      )
    val adc = GoogleCredentials.getApplicationDefault().createScoped(*scopes)
    val target = impersonateServiceAccount
    if (target.isNullOrEmpty()) {
      adc
    } else {
      ImpersonatedCredentials.create(adc, target, null, scopes.toList(), 300)
    }
  }

  private fun bigQueryDefault(): BigQuery {
    return BigQueryOptions.newBuilder()
      .setCredentials(credentials)
      .setProjectId(project)
      .build()
      .service
  }

  private fun bigQueryAsEdp(edp: EdpConfig): BigQuery {
    val saEmail = "edp-${edp.name}-dashboard@$project.iam.gserviceaccount.com"
    val impersonatedCredentials =
      ImpersonatedCredentials.create(
        credentials,
        saEmail,
        null,
        listOf(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/cloud-platform",
        ),
        300,
      )
    impersonatedCredentials.refreshIfExpired()
    logger.info("Successfully impersonated $saEmail")
    return BigQueryOptions.newBuilder()
      .setCredentials(impersonatedCredentials)
      .setProjectId(project)
      .build()
      .service
  }

  companion object {
    private val logger = Logger.getLogger(DashboardComplianceCheck::class.java.name)

    @JvmStatic fun main(args: Array<String>) = commandLineMain(DashboardComplianceCheck(), args)
  }
}
