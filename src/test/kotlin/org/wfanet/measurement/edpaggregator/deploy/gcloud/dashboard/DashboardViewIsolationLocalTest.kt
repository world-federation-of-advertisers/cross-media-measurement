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

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.file.Files
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata as EdpaSchemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.Schemata as KingdomSchemata

/**
 * Local test for dashboard view SQL isolation.
 *
 * Uses Spanner emulator to validate:
 * 1. Per-EDP Spanner queries return only that EDP's data
 * 2. SQL template files contain required WHERE clauses
 * 3. No forbidden columns appear in per-EDP view SQL
 *
 * Runs in Bazel with no cloud resources needed.
 */
class DashboardViewIsolationLocalTest {

  companion object {
    private const val EDP_A_RESOURCE_ID = "edp-a-resource-id"
    private const val EDP_B_RESOURCE_ID = "edp-b-resource-id"
    private const val EDP_A_DATA_PROVIDER_ID = 100L
    private const val EDP_B_DATA_PROVIDER_ID = 200L
    private const val MC_ID = 1000L
    private const val SHARED_REPORT = "measurementConsumers/mc1/reports/report1"

    private val FORBIDDEN_COLUMNS = setOf(
      "cross_publisher", "edp_distribution", "EdpCount", "TotalMcs",
      "CoveragePercent", "ReportingSetFilter", "MetricCalcSpecDetails",
      "SucceededMetrics", "FailedMetrics", "result_group_specs", "data_provider_keys",
    )

    private val FORBIDDEN_PATTERNS = listOf(
      Regex("(?i).*edp.?count.*"),
      Regex("(?i).*cross.?publisher.*"),
      Regex("(?i).*data.?provider.?key.*"),
      Regex("(?i).*total.?mcs.*"),
      Regex("(?i).*edp.?distribution.*"),
    )

    private val SQL_DIR = "src/main/terraform/gcloud/cmms/sql"

    @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @JvmStatic
    val edpaDatabase = SpannerEmulatorDatabaseRule(spannerEmulator, EdpaSchemata.EDP_AGGREGATOR_CHANGELOG_PATH)

    @JvmStatic
    val kingdomDatabase = SpannerEmulatorDatabaseRule(spannerEmulator, KingdomSchemata.KINGDOM_CHANGELOG_PATH)

    @get:ClassRule
    @JvmStatic
    val ruleChain: TestRule = chainRulesSequentially(spannerEmulator, edpaDatabase, kingdomDatabase)
  }

  @Before
  fun insertTestData() {
    // Insert requisition data for both EDPs sharing a report
    val edpaClient = edpaDatabase.databaseClient
    edpaClient.write(
      listOf(
        Mutation.newInsertBuilder("RequisitionMetadata")
          .set("DataProviderResourceId").to(EDP_A_RESOURCE_ID)
          .set("RequisitionMetadataId").to(1L)
          .set("Report").to(SHARED_REPORT)
          .set("State").to(4L) // FULFILLED
          .set("CmmsCreateTime").to(Value.COMMIT_TIMESTAMP)
          .set("StoredTime").to(Value.COMMIT_TIMESTAMP)
          .build(),
        Mutation.newInsertBuilder("RequisitionMetadata")
          .set("DataProviderResourceId").to(EDP_B_RESOURCE_ID)
          .set("RequisitionMetadataId").to(2L)
          .set("Report").to(SHARED_REPORT)
          .set("State").to(4L) // FULFILLED
          .set("CmmsCreateTime").to(Value.COMMIT_TIMESTAMP)
          .set("StoredTime").to(Value.COMMIT_TIMESTAMP)
          .build(),
      )
    )

    // Insert kingdom EventGroups for both EDPs under the same MC
    val kingdomClient = kingdomDatabase.databaseClient
    kingdomClient.write(
      listOf(
        Mutation.newInsertBuilder("DataProviders")
          .set("DataProviderId").to(EDP_A_DATA_PROVIDER_ID)
          .set("ExternalDataProviderId").to(100L)
          .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          .build(),
        Mutation.newInsertBuilder("DataProviders")
          .set("DataProviderId").to(EDP_B_DATA_PROVIDER_ID)
          .set("ExternalDataProviderId").to(200L)
          .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          .build(),
        Mutation.newInsertBuilder("MeasurementConsumers")
          .set("MeasurementConsumerId").to(MC_ID)
          .set("ExternalMeasurementConsumerId").to(MC_ID)
          .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          .build(),
        Mutation.newInsertBuilder("EventGroups")
          .set("DataProviderId").to(EDP_A_DATA_PROVIDER_ID)
          .set("EventGroupId").to(1L)
          .set("MeasurementConsumerId").to(MC_ID)
          .set("ExternalEventGroupId").to(1L)
          .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          .build(),
        Mutation.newInsertBuilder("EventGroups")
          .set("DataProviderId").to(EDP_B_DATA_PROVIDER_ID)
          .set("EventGroupId").to(2L)
          .set("MeasurementConsumerId").to(MC_ID)
          .set("ExternalEventGroupId").to(2L)
          .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          .build(),
      )
    )
  }

  /** Spanner query for edp-aggregator returns only the filtered EDP's requisitions. */
  @Test
  fun edpAggregatorQueryFiltersCorrectly() {
    val client = edpaDatabase.databaseClient
    val resultSet = client.singleUse().executeQuery(
      com.google.cloud.spanner.Statement.of(
        """
        SELECT rm.DataProviderResourceId, rm.Report
        FROM RequisitionMetadata rm
        WHERE rm.DataProviderResourceId = '$EDP_A_RESOURCE_ID'
        """.trimIndent()
      )
    )

    val resourceIds = mutableListOf<String>()
    while (resultSet.next()) {
      resourceIds.add(resultSet.getString("DataProviderResourceId"))
    }

    assertThat(resourceIds).isNotEmpty()
    assertThat(resourceIds).containsExactly(EDP_A_RESOURCE_ID)
  }

  /** Kingdom EventGroups query filtered by DataProviderId returns only that EDP's groups. */
  @Test
  fun kingdomEventGroupQueryFiltersCorrectly() {
    val client = kingdomDatabase.databaseClient
    val resultSet = client.singleUse().executeQuery(
      com.google.cloud.spanner.Statement.of(
        """
        SELECT eg.DataProviderId, eg.MeasurementConsumerId
        FROM EventGroups eg
        WHERE eg.DataProviderId = $EDP_A_DATA_PROVIDER_ID
        """.trimIndent()
      )
    )

    val dataProviderIds = mutableListOf<Long>()
    while (resultSet.next()) {
      dataProviderIds.add(resultSet.getLong("DataProviderId"))
    }

    assertThat(dataProviderIds).isNotEmpty()
    assertThat(dataProviderIds).containsExactly(EDP_A_DATA_PROVIDER_ID)
  }

  /** Both EDPs share the same report but filtered queries only return their own rows. */
  @Test
  fun crossPublisherReportIsFilteredPerEdp() {
    val client = edpaDatabase.databaseClient

    // EDP A sees only its own requisition for the shared report
    val resultA = client.singleUse().executeQuery(
      com.google.cloud.spanner.Statement.of(
        """
        SELECT rm.DataProviderResourceId, rm.Report
        FROM RequisitionMetadata rm
        WHERE rm.DataProviderResourceId = '$EDP_A_RESOURCE_ID'
          AND rm.Report = '$SHARED_REPORT'
        """.trimIndent()
      )
    )
    var countA = 0
    while (resultA.next()) {
      assertThat(resultA.getString("DataProviderResourceId")).isEqualTo(EDP_A_RESOURCE_ID)
      countA++
    }
    assertThat(countA).isEqualTo(1)

    // EDP B sees only its own requisition for the same report
    val resultB = client.singleUse().executeQuery(
      com.google.cloud.spanner.Statement.of(
        """
        SELECT rm.DataProviderResourceId, rm.Report
        FROM RequisitionMetadata rm
        WHERE rm.DataProviderResourceId = '$EDP_B_RESOURCE_ID'
          AND rm.Report = '$SHARED_REPORT'
        """.trimIndent()
      )
    )
    var countB = 0
    while (resultB.next()) {
      assertThat(resultB.getString("DataProviderResourceId")).isEqualTo(EDP_B_RESOURCE_ID)
      countB++
    }
    assertThat(countB).isEqualTo(1)
  }

  /** SQL template files have WHERE clauses for per-EDP filtering. */
  @Test
  fun perEdpSqlTemplatesContainWhereClause() {
    val sqlFiles = listOf("requisition_overview.sql", "mc_details.sql", "report_detail.sql")

    for (fileName in sqlFiles) {
      val path = Paths.get(SQL_DIR, fileName)
      val sql = Files.readString(path)

      assertThat(sql).contains("data_provider_id")
      assertThat(sql).contains("%{ if data_provider_id != \"\" }")
      assertThat(sql).contains("%{ endif }")
    }
  }

  /** Per-EDP SQL templates do not expose forbidden columns. */
  @Test
  fun perEdpSqlTemplatesHaveNoForbiddenColumns() {
    val sqlFiles = listOf("requisition_overview.sql", "mc_details.sql", "report_detail.sql")
    val violations = mutableListOf<String>()

    for (fileName in sqlFiles) {
      val path = Paths.get(SQL_DIR, fileName)
      val sql = Files.readString(path)

      // Extract SELECT column aliases (the AS <name> parts)
      val aliasPattern = Regex("""(?i)\bAS\s+(\w+)""")
      val aliases = aliasPattern.findAll(sql).map { it.groupValues[1] }.toList()

      for (alias in aliases) {
        if (alias in FORBIDDEN_COLUMNS) {
          violations.add("$fileName: SELECT contains forbidden column alias '$alias'")
        }
        for (pattern in FORBIDDEN_PATTERNS) {
          if (pattern.matches(alias)) {
            violations.add("$fileName: alias '$alias' matches forbidden pattern '${pattern.pattern}'")
          }
        }
      }
    }

    assertThat(violations).isEmpty()
  }
}
