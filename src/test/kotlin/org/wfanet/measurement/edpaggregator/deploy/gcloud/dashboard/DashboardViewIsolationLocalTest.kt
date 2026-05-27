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
 * Local test for dashboard view SQL isolation.
 *
 * Validates SQL template files pre-merge:
 * 1. Per-EDP SQL templates contain required WHERE clauses
 * 2. No forbidden columns appear in per-EDP view SQL
 *
 * Runs in Bazel with no cloud resources needed.
 */
class DashboardViewIsolationLocalTest {

  companion object {
    private val FORBIDDEN_COLUMNS =
      setOf(
        "cross_publisher",
        "edp_distribution",
        "EdpCount",
        "TotalMcs",
        "CoveragePercent",
        "ReportingSetFilter",
        "MetricCalcSpecDetails",
        "SucceededMetrics",
        "FailedMetrics",
        "result_group_specs",
        "data_provider_keys",
      )

    private val FORBIDDEN_PATTERNS =
      listOf(
        Regex("(?i).*edp.?count.*"),
        Regex("(?i).*cross.?publisher.*"),
        Regex("(?i).*data.?provider.?key.*"),
        Regex("(?i).*total.?mcs.*"),
        Regex("(?i).*edp.?distribution.*"),
      )

    private val SQL_FILES =
      listOf("requisition_overview.sql", "mc_details.sql", "report_detail.sql")
  }

  private fun readSqlFile(fileName: String): String {
    val runfilesDir = System.getenv("TEST_SRCDIR") ?: "."
    val workspace = System.getenv("TEST_WORKSPACE") ?: "__main__"
    val path = Paths.get(runfilesDir, workspace, "src/main/terraform/gcloud/cmms/sql", fileName)
    return Files.readString(path)
  }

  @Test
  fun perEdpSqlTemplatesContainWhereClause() {
    for (fileName in SQL_FILES) {
      val sql = readSqlFile(fileName)

      assertThat(sql).contains("data_provider_id")
      assertThat(sql).contains("%{ if data_provider_id != \"\" }")
      assertThat(sql).contains("%{ endif }")
    }
  }

  @Test
  fun perEdpSqlTemplatesHaveNoForbiddenColumns() {
    val violations = mutableListOf<String>()

    for (fileName in SQL_FILES) {
      val sql = readSqlFile(fileName)

      val aliasPattern = Regex("""(?i)\bAS\s+(\w+)""")
      val aliases = aliasPattern.findAll(sql).map { it.groupValues[1] }.toList()

      for (alias in aliases) {
        if (alias in FORBIDDEN_COLUMNS) {
          violations.add("$fileName: SELECT contains forbidden column alias '$alias'")
        }
        for (pattern in FORBIDDEN_PATTERNS) {
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
  fun requisitionOverviewFiltersOnDataProviderResourceId() {
    val sql = readSqlFile("requisition_overview.sql")
    assertThat(sql).contains("r.DataProviderResourceId = '\${data_provider_id}'")
  }

  @Test
  fun mcDetailsFiltersOnDataProviderId() {
    val sql = readSqlFile("mc_details.sql")
    assertThat(sql).contains("externalIdToApiId")
    assertThat(sql).contains("DataProviderId")
    assertThat(sql).contains("\${data_provider_id}")
  }

  @Test
  fun reportDetailFiltersOnCmmsDataProviderId() {
    val sql = readSqlFile("report_detail.sql")
    assertThat(sql).contains("cmmsDataProviderId")
    assertThat(sql).contains("\${data_provider_id}")
  }
}
