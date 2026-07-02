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

  /**
   * Renders a templatefile-style SQL template by resolving the `include_platform_columns`
   * conditionals. Handles both the positive (`%{ if include_platform_columns }`) and negated (`%{
   * if !include_platform_columns }`) directives, plus `%{ else }`. Assumes the conditionals are not
   * nested (true for the dashboard SQL).
   */
  private fun render(template: String, platformEnabled: Boolean): String {
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

  @Test
  fun platformColumnsOnlyInsideConditionalBlocks() {
    for (fileName in listOf("mc_details.sql", "report_detail.sql")) {
      val sql = readSqlFile(fileName)
      assertThat(sql).contains("include_platform_columns")

      val rendered = render(sql, platformEnabled = false)

      for (col in PLATFORM_ONLY_COLUMNS) {
        assertThat(rendered).doesNotContain(col)
      }
    }
  }

  @Test
  fun entityColumnsAreEdpOnly() {
    // Entity columns are EDP-specific and must appear only in the EDP variant,
    // never in the platform variant (per PR #3755 review: entity keys are
    // EDP-specific data).
    run {
      val sql = readSqlFile("report_detail.sql")
      assertThat(render(sql, platformEnabled = false)).contains("AS EntityTypes")
      assertThat(render(sql, platformEnabled = false)).contains("AS EntityIds")
      assertThat(render(sql, platformEnabled = true)).doesNotContain("AS EntityTypes")
      assertThat(render(sql, platformEnabled = true)).doesNotContain("AS EntityIds")
    }
    // mc_details: match the outer ARRAY_AGG aggregations, since the inner
    // EXTERNAL_QUERY also aliases EntityMetadata.
    run {
      val sql = readSqlFile("mc_details.sql")
      val edp = render(sql, platformEnabled = false)
      val platform = render(sql, platformEnabled = true)
      val edpOnlyAggregations =
        listOf(
          "ARRAY_AGG(IFNULL(eg.EntityType, '')) AS EntityTypes",
          "ARRAY_AGG(IFNULL(eg.EntityId, '')) AS EntityIds",
          "ARRAY_AGG(IFNULL(eg.EntityMetadata, '')) AS EntityMetadata",
        )
      for (expr in edpOnlyAggregations) {
        assertThat(edp).contains(expr)
        assertThat(platform).doesNotContain(expr)
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

  @Test
  fun externalIdToApiIdAlgorithm() {
    // The UDF converts int64 to base64url-encoded big-endian bytes (no padding).
    // Validate the algorithm with known-good pairs.
    fun externalIdToApiId(id: Long): String {
      val bytes = ByteArray(8)
      var v = id
      for (i in 7 downTo 0) {
        bytes[i] = (v and 0xFF).toByte()
        v = v shr 8
      }
      val lookup = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
      val result = StringBuilder()
      var i = 0
      while (i < bytes.size) {
        val c1 = bytes[i].toInt() and 0xFF
        val c2 = if (i + 1 < bytes.size) bytes[i + 1].toInt() and 0xFF else 0
        val c3 = if (i + 2 < bytes.size) bytes[i + 2].toInt() and 0xFF else 0
        result.append(lookup[c1 shr 2])
        result.append(lookup[((c1 and 3) shl 4) or (c2 shr 4)])
        if (i + 1 < bytes.size) result.append(lookup[((c2 and 15) shl 2) or (c3 shr 6)])
        if (i + 2 < bytes.size) result.append(lookup[c3 and 63])
        i += 3
      }
      return result.toString()
    }

    // Test with synthetic known values
    assertThat(externalIdToApiId(0)).isEqualTo("AAAAAAAAAAA")
    assertThat(externalIdToApiId(1)).isEqualTo("AAAAAAAAAAE")
    // Test with real DataProvider ExternalId from Kingdom Spanner (halo-cmm-dev)
    assertThat(externalIdToApiId(2846180192195638458L)).isEqualTo("J3-pzhqS9Lo")
    // Verify output is URL-safe (no +, /, or = characters)
    val result = externalIdToApiId(123456789L)
    assertThat(result).isNotEmpty()
    assertThat(result).doesNotContain("+")
    assertThat(result).doesNotContain("/")
    assertThat(result).doesNotContain("=")
  }
}
