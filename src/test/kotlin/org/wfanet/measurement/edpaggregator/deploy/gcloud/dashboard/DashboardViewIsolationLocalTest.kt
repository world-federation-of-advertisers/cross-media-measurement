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

/**
 * Local test for dashboard SQL validation.
 *
 * Validates SQL template files pre-merge:
 * 1. EDP table SQL excludes forbidden platform-only columns
 * 2. Platform table SQL includes expected platform-only columns
 * 3. All SQL files contain expected EXTERNAL_QUERY connections
 *
 * Runs in Bazel with no cloud resources needed.
 */
class DashboardViewIsolationLocalTest {

  companion object {
    private val FORBIDDEN_EDP_COLUMNS =
      setOf(
        "CoveragePercent",
        "TotalMcs",
        "EdpCount",
      )

    private val FORBIDDEN_EDP_PATTERNS =
      listOf(
        Regex("(?i).*edp.?count.*"),
        Regex("(?i).*total.?mcs.*"),
        Regex("(?i).*coverage.?percent.*"),
      )

    private val EDP_SQL_FILES =
      listOf(
        "mc_details_edp.sql",
        "report_detail_edp.sql",
        "requisition_overview.sql",
      )

    private val PLATFORM_SQL_FILES =
      listOf(
        "mc_details.sql",
        "report_detail.sql",
      )
  }

  private fun readSqlFile(fileName: String): String {
    val runfilesDir = System.getenv("TEST_SRCDIR") ?: "."
    val workspace = System.getenv("TEST_WORKSPACE") ?: "__main__"
    val path =
      Paths.get(
        runfilesDir,
        workspace,
        "src/main/terraform/gcloud/cmms/sql",
        fileName,
      )
    return Files.readString(path)
  }

  @Test
  fun edpSqlFilesHaveNoForbiddenColumns() {
    val violations = mutableListOf<String>()

    for (fileName in EDP_SQL_FILES) {
      val sql = readSqlFile(fileName)

      val aliasPattern = Regex("""(?i)\bAS\s+(\w+)""")
      val aliases = aliasPattern.findAll(sql).map { it.groupValues[1] }.toList()

      for (alias in aliases) {
        if (alias in FORBIDDEN_EDP_COLUMNS) {
          violations.add("$fileName: contains forbidden column alias '$alias'")
        }
        for (pattern in FORBIDDEN_EDP_PATTERNS) {
          if (pattern.matches(alias)) {
            violations.add(
              "$fileName: alias '$alias' matches forbidden pattern '${pattern.pattern}'"
            )
          }
        }
      }
    }

    assertThat(violations).isEmpty()
  }

  @Test
  fun platformMcDetailsIncludesPlatformColumns() {
    val sql = readSqlFile("mc_details.sql")
    assertThat(sql).contains("TotalMcs")
    assertThat(sql).contains("CoveragePercent")
  }

  @Test
  fun platformReportDetailIncludesEdpCount() {
    val sql = readSqlFile("report_detail.sql")
    assertThat(sql).contains("EdpCount")
  }

  @Test
  fun allSqlFilesUseExternalQuery() {
    for (fileName in EDP_SQL_FILES + PLATFORM_SQL_FILES) {
      val sql = readSqlFile(fileName)
      assertThat(sql).contains("EXTERNAL_QUERY")
    }
  }

  @Test
  fun sqlFilesDoNotContainPerEdpWhereFilter() {
    for (fileName in EDP_SQL_FILES + PLATFORM_SQL_FILES) {
      val sql = readSqlFile(fileName)
      assertThat(sql).doesNotContain("data_provider_id")
    }
  }
}
