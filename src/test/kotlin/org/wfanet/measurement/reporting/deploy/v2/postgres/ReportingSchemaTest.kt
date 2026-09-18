// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.deploy.v2.postgres

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata

@RunWith(JUnit4::class)
class ReportingSchemaTest {
  private val databaseClient: PostgresDatabaseClient = databaseProvider.createDatabase()

  @Test
  fun `all index changesets are applied`(): Unit = runBlocking {
    val indexChangeSetIds =
      queryColumn(query = "SELECT id FROM databasechangelog", columnName = "id").filter {
        it in EXPECTED_INDEX_CHANGESET_IDS
      }

    assertThat(indexChangeSetIds).containsExactlyElementsIn(EXPECTED_INDEX_CHANGESET_IDS)
  }

  @Test
  fun `all reporting indexes are created`(): Unit = runBlocking {
    val reportingIndexNames =
      queryColumn(
          query = "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
          columnName = "indexname",
        )
        .filter { it in EXPECTED_REPORTING_INDEX_NAMES }

    assertThat(reportingIndexNames).containsExactlyElementsIn(EXPECTED_REPORTING_INDEX_NAMES)
  }

  private suspend fun queryColumn(query: String, columnName: String): List<String> {
    return databaseClient
      .readTransaction()
      .executeQuery(boundStatement(query))
      .consume { row: ResultRow -> row.get<String>(columnName) }
      .toList()
  }

  companion object {
    private val EXPECTED_INDEX_CHANGESET_IDS =
      listOf(
        "add-report-create-time-index",
        "add-metric-calculation-spec-reporting-metrics-update-index",
        "add-metrics-comparison-index",
        "add-metric-calculation-spec-reporting-metrics-metric-id-index",
        "recreate-metrics-comparison-index",
        "add-reporting-sets-campaign-group-id-index",
        "add-metric-calculation-specs-campaign-group-id-index",
      )

    private val EXPECTED_REPORTING_INDEX_NAMES =
      listOf(
        "report_create_time",
        // PostgreSQL truncates identifiers to 63 bytes.
        "metric_calculation_spec_reporting_metrics_create_metric_request",
        "metrics_comparison",
        "metric_calculation_spec_reporting_metrics_metric_id",
        "reporting_sets_campaign_group_id_index",
        "metric_calculation_specs_campaign_group_id_index",
      )

    @get:ClassRule
    @JvmStatic
    val databaseProvider = PostgresDatabaseProviderRule(Schemata.REPORTING_CHANGELOG_PATH)
  }
}
