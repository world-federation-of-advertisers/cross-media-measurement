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

package org.wfanet.measurement.integration.k8s

import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.testing.DashboardSqlTemplates.readSqlFile
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.testing.DashboardSqlTemplates.render

/**
 * Builds the production `report_detail.sql` source query with each federated read replaced by
 * seeded fixture rows, so the BigQuery-side joins, grouping and `externalIdToApiId` UDF can be run
 * against data that cannot exist in a real deployment.
 *
 * The Kingdom assigns `ExternalEventGroupId` itself, so two DataProviders cannot be made to share
 * one through the API. The fixture does exactly that, with deliberately different metadata per
 * provider, which is the only way to observe whether the Kingdom metadata join is scoped by
 * DataProvider. It likewise reuses one report ID and one campaign-group ID across two
 * MeasurementConsumers.
 *
 * The reporting Postgres fixture stands in for the output of the composite campaign-group walk; the
 * walk itself is covered by `ReportDetailCampaignGroupQueryTest`.
 */
class ReportDetailFixtureQuery(private val project: String, private val region: String) {
  private val udf = "`$project.dashboard.externalIdToApiId`"

  /** Fixture rows keyed by the main table each pushed-down query reads. */
  private val fixtures: Map<String, String> =
    mapOf(
      // Reporting Spanner: report -> campaign group.
      "BasicReports" to
        """
        SELECT * FROM UNNEST([
          STRUCT<CmmsMeasurementConsumerId STRING, ExternalReportId STRING,
                 ExternalCampaignGroupId STRING, State INT64>
          ('$MC_ALPHA', '$REPORT_SHARED', '$CAMPAIGN_GROUP_SHARED', 4),
          ('$MC_ALPHA', '$REPORT_RIGHT_NESTED', 'cg-right-nested', 4),
          ('$MC_ALPHA', '$REPORT_INTERSECTION', 'cg-intersection', 5),
          ('$MC_BETA', '$REPORT_SHARED', '$CAMPAIGN_GROUP_SHARED', 4)])
        """,
      // Reporting Postgres: campaign group -> (DataProvider, EventGroup), as resolved by the
      // composite walk. cg-right-nested and cg-intersection reach both providers.
      "reportingsets" to
        """
        SELECT * FROM UNNEST([
          STRUCT<CmmsMeasurementConsumerId STRING, ExternalCampaignGroupId STRING,
                 CmmsDataProvider STRING, CmmsEventGroupId STRING>
          ('$MC_ALPHA', '$CAMPAIGN_GROUP_SHARED', $udf($META_DP), $udf($SHARED_EG)),
          ('$MC_ALPHA', '$CAMPAIGN_GROUP_SHARED', $udf($OTHER_DP), $udf($SHARED_EG)),
          ('$MC_ALPHA', 'cg-right-nested', $udf($META_DP), $udf($SHARED_EG)),
          ('$MC_ALPHA', 'cg-right-nested', $udf($OTHER_DP), $udf($SHARED_EG)),
          ('$MC_ALPHA', 'cg-right-nested', $udf($META_DP), $udf($META_ONLY_EG)),
          ('$MC_ALPHA', 'cg-intersection', $udf($META_DP), $udf($SHARED_EG)),
          ('$MC_ALPHA', 'cg-intersection', $udf($OTHER_DP), $udf($SHARED_EG)),
          ('$MC_ALPHA', 'cg-intersection', $udf($OTHER_DP), $udf($OTHER_ONLY_EG)),
          ('$MC_BETA', '$CAMPAIGN_GROUP_SHARED', $udf($OTHER_DP), $udf($OTHER_ONLY_EG))])
        """,
      // Kingdom EventGroups: SHARED_EG exists under both providers with different metadata.
      "EventGroups" to
        """
        SELECT * FROM UNNEST([
          STRUCT<DataProviderId INT64, ExternalEventGroupId INT64, EntityType STRING,
                 EntityId STRING, CampaignName STRING, BrandName STRING>
          (1, $SHARED_EG, 'campaign', 'meta-entity-shared', 'Meta Campaign Shared', 'Meta Brand'),
          (2, $SHARED_EG, 'line_item', 'other-entity-shared', 'Other Campaign Shared', 'Other Brand'),
          (1, $META_ONLY_EG, 'campaign', 'meta-entity-solo', 'Meta Campaign Solo', 'Meta Brand'),
          (2, $OTHER_ONLY_EG, 'line_item', 'other-entity-solo', 'Other Campaign Solo', 'Other Brand')])
        """,
      // Kingdom DataProviders: internal ID -> external ID.
      "DataProviders" to
        """
        SELECT * FROM UNNEST([
          STRUCT<DataProviderId INT64, ExternalDataProviderId INT64>
          (1, $META_DP), (2, $OTHER_DP)])
        """,
    )

  /**
   * Returns the `report_detail` source query, rendered for [platform] and with every federated read
   * replaced by fixture rows.
   */
  fun sourceQuery(platform: Boolean): String {
    val rendered = render(readSqlFile("report_detail.sql"), platform)
    val source =
      rendered.substring(
        rendered.indexOf("USING (") + "USING (".length,
        rendered.lastIndexOf(") S"),
      )
    return substituteFederatedReads(source)
  }

  /** Query returning the API IDs the UDF assigns to the fixture's external IDs. */
  fun apiIdsQuery(): String =
    "SELECT $udf($META_DP) AS meta_dp, $udf($OTHER_DP) AS other_dp, " +
      "$udf($SHARED_EG) AS shared_eg, $udf($META_ONLY_EG) AS meta_only_eg, " +
      "$udf($OTHER_ONLY_EG) AS other_only_eg"

  private fun substituteFederatedReads(sql: String): String {
    val result = StringBuilder()
    val used = mutableListOf<String>()
    var cursor = 0
    while (true) {
      val call = sql.indexOf(EXTERNAL_QUERY, cursor)
      if (call < 0) {
        result.append(sql, cursor, sql.length)
        break
      }
      val queryStart = sql.indexOf("'''", call)
      val queryEnd = sql.indexOf("'''", queryStart + 3)
      val callEnd = sql.indexOf(")", queryEnd + 3)
      check(queryStart >= 0 && queryEnd >= 0 && callEnd >= 0) {
        "Malformed EXTERNAL_QUERY call at offset $call"
      }
      // The last FROM in a pushed-down query names its main table; earlier ones may sit in CTEs.
      val table = FROM_TABLE.findAll(sql.substring(queryStart + 3, queryEnd)).last().groupValues[1]
      val fixture = checkNotNull(fixtures[table]) { "No fixture for federated read of $table" }
      used += table
      result.append(sql, cursor, call).append("(").append(fixture).append(")")
      cursor = callEnd + 1
    }
    // A surviving EXTERNAL_QUERY would read real data, so the substitution must be total.
    check(used.sorted() == fixtures.keys.sorted()) {
      "Expected one federated read per fixture ${fixtures.keys.sorted()}, found ${used.sorted()}"
    }
    return result.toString().replace("\${project_id}", project).replace("\${region}", region)
  }

  companion object {
    const val MC_ALPHA = "mc-alpha"
    const val MC_BETA = "mc-beta"
    const val REPORT_SHARED = "r-shared"
    const val REPORT_RIGHT_NESTED = "r-right-nested"
    const val REPORT_INTERSECTION = "r-intersection"
    private const val CAMPAIGN_GROUP_SHARED = "cg-shared"

    private const val META_DP = 1001L
    private const val OTHER_DP = 1002L
    private const val SHARED_EG = 555L
    private const val META_ONLY_EG = 556L
    private const val OTHER_ONLY_EG = 557L

    private const val EXTERNAL_QUERY = "EXTERNAL_QUERY("
    private val FROM_TABLE = Regex("""\bFROM\s+(\w+)""")
  }
}
