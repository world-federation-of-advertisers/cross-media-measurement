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

import org.wfanet.measurement.common.commandLineMain
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

  override fun run() {
    println("=== EDPA Dashboard Compliance Check ===")
    println("Project: $project")
    println("Dataset: $dataset")
    println("EDPs: ${edps.map { it.name }}")
    println()

    val report =
      DashboardComplianceRunner.run(project, dataset, region, impersonateServiceAccount, edps)

    for (section in report.sections) {
      println("[${section.name}]")
      for (result in section.results) {
        if (result.passed) {
          println("  OK  ${result.message}")
        } else {
          println("  FAIL  ${result.message}")
        }
      }
      println()
    }

    val total = report.passed + report.failed
    if (report.allPassed) {
      println("=== RESULT: ALL CHECKS PASSED (${report.passed}/$total) ===")
    } else {
      println(
        "=== RESULT: ${report.failed} CHECKS FAILED " +
          "(${report.passed} passed, ${report.failed} failed out of $total) ==="
      )
      System.exit(1)
    }
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(DashboardComplianceCheck(), args)
  }
}
