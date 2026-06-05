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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard

import com.google.common.truth.Truth.assertThat
import java.nio.file.Files
import java.nio.file.Paths
import org.junit.Test

/** Validates dashboard SQL templates for correct EDP isolation structure. */
class DashboardViewIsolationLocalTest {

  companion object {
    private val PLATFORM_ONLY_COLUMNS = setOf("CoveragePercent", "TotalMcs", "EdpCount")

    private val SQL_FILES =
      listOf("mc_details.sql", "report_detail.sql", "requisition_overview.sql")
  }

  private fun readSqlFile(fileName: String): String {
    val runfilesDir = System.getenv("TEST_SRCDIR") ?: "."
    val workspace = System.getenv("TEST_WORKSPACE") ?: "__main__"
    val path = Paths.get(runfilesDir, workspace, "src/main/terraform/gcloud/cmms/sql", fileName)
    return Files.readString(path)
  }

  private fun renderWithPlatformColumnsDisabled(template: String): String {
    val result = StringBuilder()
    var inIfBlock = false
    var inElseBlock = false

    for (line in template.lines()) {
      val trimmed = line.trim()
      when {
        trimmed == "%{ if include_platform_columns }" -> inIfBlock = true
        trimmed == "%{ else }" -> {
          inIfBlock = false
          inElseBlock = true
        }
        trimmed == "%{ endif }" -> {
          inIfBlock = false
          inElseBlock = false
        }
        inIfBlock -> {}
        else -> result.appendLine(line)
      }
    }
    return result.toString()
  }

  @Test
  fun platformColumnsOnlyInsideConditionalBlocks() {
    for (fileName in listOf("mc_details.sql", "report_detail.sql")) {
      val sql = readSqlFile(fileName)
      assertThat(sql).contains("include_platform_columns")

      val rendered = renderWithPlatformColumnsDisabled(sql)

      for (col in PLATFORM_ONLY_COLUMNS) {
        assertThat(rendered).doesNotContain(col)
      }
    }
  }

  @Test
  fun requisitionOverviewHasNoPlatformOnlyColumns() {
    val sql = readSqlFile("requisition_overview.sql")
    for (col in PLATFORM_ONLY_COLUMNS) {
      assertThat(sql).doesNotContain(col)
    }
  }

  @Test
  fun allSqlFilesUseExternalQuery() {
    for (fileName in SQL_FILES) {
      val sql = readSqlFile(fileName)
      assertThat(sql).contains("EXTERNAL_QUERY")
    }
  }
}
