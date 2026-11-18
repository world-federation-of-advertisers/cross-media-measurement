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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.testing

import java.nio.file.Files
import java.nio.file.Paths

/**
 * Reads and renders the dashboard scheduled-query templates under
 * `src/main/terraform/gcloud/cmms/sql` for tests.
 *
 * Shared so every test renders the templates the same way: a test with its own copy of [render]
 * could drift from the others and silently check different SQL than Terraform deploys.
 */
object DashboardSqlTemplates {
  private const val SQL_DIR = "src/main/terraform/gcloud/cmms/sql"

  /**
   * Returns the raw template [fileName] from the test's runfiles. The calling test target must
   * declare the file in its `data`.
   */
  fun readSqlFile(fileName: String): String {
    val runfilesDir = System.getenv("TEST_SRCDIR") ?: "."
    val workspace = System.getenv("TEST_WORKSPACE") ?: "__main__"
    return Files.readString(Paths.get(runfilesDir, workspace, SQL_DIR, fileName))
  }

  /**
   * Renders a templatefile-style SQL template by resolving the `include_platform_columns`
   * conditionals. Handles both the positive (`%{ if include_platform_columns }`) and negated (`%{
   * if !include_platform_columns }`) directives, plus `%{ else }`. Assumes the conditionals are not
   * nested (true for the dashboard SQL). Other template variables are left in place.
   */
  fun render(template: String, platformEnabled: Boolean): String {
    val result = StringBuilder()
    var inConditional = false
    var conditionMet = false
    var keep = true

    for (line in template.lines()) {
      val trimmed = line.trim()
      when {
        trimmed == "%{ if include_platform_columns }" -> {
          inConditional = true
          conditionMet = platformEnabled
          keep = conditionMet
        }
        trimmed == "%{ if !include_platform_columns }" -> {
          inConditional = true
          conditionMet = !platformEnabled
          keep = conditionMet
        }
        trimmed == "%{ else }" -> keep = inConditional && !conditionMet
        trimmed == "%{ endif }" -> {
          inConditional = false
          keep = true
        }
        keep -> result.appendLine(line)
      }
    }
    return result.toString()
  }
}
