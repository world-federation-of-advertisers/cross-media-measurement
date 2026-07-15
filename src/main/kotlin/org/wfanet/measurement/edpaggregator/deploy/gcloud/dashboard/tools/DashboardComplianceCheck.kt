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

import java.io.File
import java.io.IOException
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine

@CommandLine.Command(
  name = "DashboardComplianceCheck",
  description = ["Verifies EDP isolation and dashboard compliance for any environment."],
  sortOptions = false,
)
class DashboardComplianceCheck : Runnable {

  @CommandLine.Spec private lateinit var spec: CommandLine.Model.CommandSpec

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
    names = ["--dashboard-config"],
    required = true,
    description =
      [
        "Path to a DASHBOARD_CONFIG_CONTENT JSON file supplying the EDPs and BigQuery region, " +
          "parsed against the DashboardConfig proto schema."
      ],
  )
  private lateinit var dashboardConfigFile: File

  @CommandLine.Option(
    names = ["--impersonate-service-account"],
    required = false,
    description = ["Service account to run all checks as (defaults to ADC if unset)"],
  )
  private var impersonateServiceAccount: String? = null

  override fun run() {
    val config =
      try {
        DashboardComplianceRunner.parseDashboardConfig(dashboardConfigFile.readText())
      } catch (e: IOException) {
        throw CommandLine.ParameterException(
          spec.commandLine(),
          "Cannot read --dashboard-config file '${dashboardConfigFile.path}': ${e.message}",
        )
      } catch (e: IllegalArgumentException) {
        throw CommandLine.ParameterException(spec.commandLine(), e.message)
      }

    println("=== EDPA Dashboard Compliance Check ===")
    println("Project: $project")
    println("Dataset: $dataset")
    println("EDPs: ${config.edps.map { it.name }}")
    println()

    val report =
      DashboardComplianceRunner.run(
        project,
        dataset,
        config.region,
        impersonateServiceAccount,
        config.edps,
      )

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
