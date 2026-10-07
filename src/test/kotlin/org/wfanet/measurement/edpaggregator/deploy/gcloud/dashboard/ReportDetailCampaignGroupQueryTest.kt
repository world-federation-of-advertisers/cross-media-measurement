/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata

/**
 * Executes the campaign-group subquery that `report_detail.sql` pushes down to the reporting
 * Postgres database, against a real database with seeded ReportingSets.
 *
 * [DashboardViewIsolationLocalTest] only matches tokens in the rendered template, so it cannot tell
 * whether the recursive CTE returns anything, follows every operand, terminates on a cycle, or
 * keeps MeasurementConsumers apart. This test runs the production SQL and asserts exact rows.
 *
 * Scope: the pushed-down Postgres query only. The BasicReport join, the final grouping and the
 * `EdpCount` window all execute in BigQuery and are covered structurally in
 * [DashboardViewIsolationLocalTest].
 */
@RunWith(JUnit4::class)
class ReportDetailCampaignGroupQueryTest {
  private lateinit var client: PostgresDatabaseClient

  /** One output row of the pushed-down query. */
  private data class Resolved(
    val measurementConsumer: String,
    val campaignGroup: String,
    val dataProvider: String,
    val eventGroup: String,
  )

  @Before
  fun seedDatabase() = runBlocking {
    client = databaseProvider.createDatabase()
    val txn = client.readWriteTransaction()

    suspend fun exec(sql: String) = txn.executeStatement(boundStatement(sql))

    // Two MeasurementConsumers, so campaign-group IDs can collide across them.
    exec(
      """
      INSERT INTO MeasurementConsumers (MeasurementConsumerId, CmmsMeasurementConsumerId)
      VALUES (1, 'mc-alpha'), (2, 'mc-beta')
      """
    )

    // EventGroups. 104 and 105 deliberately reuse one CmmsEventGroupId across two
    // DataProviders, which the schema permits: UNIQUE (CmmsDataProviderId, CmmsEventGroupId).
    exec(
      """
      INSERT INTO EventGroups
        (MeasurementConsumerId, EventGroupId, CmmsDataProviderId, CmmsEventGroupId)
      VALUES
        (1, 101, 'dp-a', 'eg-1'),
        (1, 102, 'dp-a', 'eg-2'),
        (1, 103, 'dp-b', 'eg-3'),
        (1, 104, 'dp-a', 'eg-dup'),
        (1, 105, 'dp-b', 'eg-dup'),
        (2, 201, 'dp-a', 'eg-beta')
      """
    )

    // ReportingSets are inserted with a NULL SetExpressionId first: ReportingSets and
    // SetExpressions reference each other, so the link is set by UPDATE below.
    exec(
      """
      INSERT INTO ReportingSets (MeasurementConsumerId, ReportingSetId, ExternalReportingSetId)
      VALUES
        (1, 1,  'primitive-1'),
        (1, 2,  'primitive-2'),
        (1, 3,  'primitive-3'),
        (1, 4,  'composite-direct'),
        (1, 5,  'composite-nested'),
        (1, 6,  'composite-of-composite'),
        (1, 7,  'composite-difference'),
        (1, 8,  'composite-shared-leaf'),
        (1, 9,  'cycle-a'),
        (1, 10, 'cycle-b'),
        (1, 11, 'primitive-shared-event-group-id'),
        (1, 12, 'composite-left-operand-only'),
        (2, 100, 'primitive-1')
      """
    )

    exec(
      """
      INSERT INTO ReportingSetEventGroups (MeasurementConsumerId, ReportingSetId, EventGroupId)
      VALUES
        (1, 1, 101),
        (1, 2, 102),
        (1, 3, 103),
        (1, 11, 104),
        (1, 11, 105),
        (2, 100, 201)
      """
    )

    // Operation values are wfa.measurement.internal.reporting.v2.SetExpression.Operation:
    // UNION = 1, DIFFERENCE = 2, INTERSECTION = 3. The query ignores them by design.
    exec(
      """
      INSERT INTO SetExpressions
        (MeasurementConsumerId, ReportingSetId, SetExpressionId, Operation,
         LeftHandSetExpressionId, LeftHandReportingSetId,
         RightHandSetExpressionId, RightHandReportingSetId)
      VALUES
        -- direct ReportingSet operands on both sides
        (1, 4, 10, 1, NULL, 1,    NULL, 2),
        -- nested expression on the left, no right-hand operand
        (1, 5, 20, 1, 21,   NULL, NULL, NULL),
        (1, 5, 21, 1, NULL, 1,    NULL, 3),
        -- composite referencing another composite
        (1, 6, 30, 1, NULL, 4,    NULL, 3),
        -- DIFFERENCE: documented to yield a superset
        (1, 7, 40, 2, NULL, 1,    NULL, 2),
        -- same primitive leaf reached twice
        (1, 8, 50, 1, NULL, 1,    NULL, 1),
        -- mutual reference between two composites
        (1, 9,  60, 1, NULL, 10,  NULL, 1),
        (1, 10, 70, 1, NULL, 9,   NULL, 2),
        -- left operand only
        (1, 12, 80, 1, NULL, 1,   NULL, NULL)
      """
    )

    exec(
      """
      UPDATE ReportingSets SET SetExpressionId = CASE ReportingSetId
        WHEN 4  THEN 10
        WHEN 5  THEN 20
        WHEN 6  THEN 30
        WHEN 7  THEN 40
        WHEN 8  THEN 50
        WHEN 9  THEN 60
        WHEN 10 THEN 70
        WHEN 12 THEN 80
      END
      WHERE MeasurementConsumerId = 1 AND ReportingSetId IN (4, 5, 6, 7, 8, 9, 10, 12)
      """
    )
    txn.commit()
  }

  /** Rows the production query returns for [campaignGroup]. */
  private fun resolve(campaignGroup: String): Set<Resolved> =
    allRows().filter { it.campaignGroup == campaignGroup }.toSet()

  private fun allRows(): List<Resolved> = runBlocking {
    val readContext = client.singleUse()
    try {
      readContext
        .executeQuery(boundStatement(PUSHED_DOWN_QUERY))
        .consume { row: ResultRow ->
          Resolved(
            measurementConsumer = row["cmmsmeasurementconsumerid"],
            campaignGroup = row["externalcampaigngroupid"],
            dataProvider = row["cmmsdataprovider"],
            eventGroup = row["cmmseventgroupid"],
          )
        }
        .toList()
    } finally {
      readContext.close()
    }
  }

  @Test
  fun primitiveCampaignGroupResolvesItsOwnEventGroups() {
    assertThat(resolve("primitive-2"))
      .containsExactly(Resolved("mc-alpha", "primitive-2", "dp-a", "eg-2"))
  }

  @Test
  fun compositeWithDirectReportingSetOperandsResolvesBothSides() {
    assertThat(resolve("composite-direct"))
      .containsExactly(
        Resolved("mc-alpha", "composite-direct", "dp-a", "eg-1"),
        Resolved("mc-alpha", "composite-direct", "dp-a", "eg-2"),
      )
  }

  @Test
  fun compositeWithNestedExpressionOperandResolvesThroughTheNesting() {
    assertThat(resolve("composite-nested"))
      .containsExactly(
        Resolved("mc-alpha", "composite-nested", "dp-a", "eg-1"),
        Resolved("mc-alpha", "composite-nested", "dp-b", "eg-3"),
      )
  }

  @Test
  fun compositeReferencingAnotherCompositeResolvesThroughBothLevels() {
    assertThat(resolve("composite-of-composite"))
      .containsExactly(
        Resolved("mc-alpha", "composite-of-composite", "dp-a", "eg-1"),
        Resolved("mc-alpha", "composite-of-composite", "dp-a", "eg-2"),
        Resolved("mc-alpha", "composite-of-composite", "dp-b", "eg-3"),
      )
  }

  @Test
  fun compositeWithOnlyALeftOperandResolves() {
    assertThat(resolve("composite-left-operand-only"))
      .containsExactly(Resolved("mc-alpha", "composite-left-operand-only", "dp-a", "eg-1"))
  }

  @Test
  fun differenceOperationYieldsTheDocumentedSuperset() {
    // The walk ignores Operation, so a DIFFERENCE campaign group reports every reachable
    // event group rather than the set the report actually measured. Documented in
    // report_detail.sql; this pins the behaviour so a change is deliberate.
    assertThat(resolve("composite-difference"))
      .containsExactly(
        Resolved("mc-alpha", "composite-difference", "dp-a", "eg-1"),
        Resolved("mc-alpha", "composite-difference", "dp-a", "eg-2"),
      )
  }

  @Test
  fun leafReachedThroughMultiplePathsIsReturnedOnce() {
    assertThat(resolve("composite-shared-leaf"))
      .containsExactly(Resolved("mc-alpha", "composite-shared-leaf", "dp-a", "eg-1"))
  }

  @Test
  fun referenceCycleTerminatesAndResolvesEveryReachableLeaf() {
    // cycle-a and cycle-b reference each other. UNION in the closure dedupes, so the
    // recursion terminates; both resolve to the union of the leaves they can reach.
    assertThat(resolve("cycle-a"))
      .containsExactly(
        Resolved("mc-alpha", "cycle-a", "dp-a", "eg-1"),
        Resolved("mc-alpha", "cycle-a", "dp-a", "eg-2"),
      )
    assertThat(resolve("cycle-b"))
      .containsExactly(
        Resolved("mc-alpha", "cycle-b", "dp-a", "eg-1"),
        Resolved("mc-alpha", "cycle-b", "dp-a", "eg-2"),
      )
  }

  @Test
  fun eventGroupIdReusedAcrossDataProvidersStaysAttributedToItsOwnProvider() {
    // dp-a and dp-b both have an event group called eg-dup. Each must appear under its own
    // provider, which is what lets the Kingdom metadata join be scoped by DataProvider.
    assertThat(resolve("primitive-shared-event-group-id"))
      .containsExactly(
        Resolved("mc-alpha", "primitive-shared-event-group-id", "dp-a", "eg-dup"),
        Resolved("mc-alpha", "primitive-shared-event-group-id", "dp-b", "eg-dup"),
      )
  }

  @Test
  fun campaignGroupIdReusedAcrossMeasurementConsumersStaysSeparate() {
    // Regression guard for the unscoped campaign-group join. Both consumers have a campaign
    // group called primitive-1; each must carry its own MeasurementConsumer and resolve only
    // its own event groups, so the BigQuery join can key on the consumer as well.
    assertThat(resolve("primitive-1"))
      .containsExactly(
        Resolved("mc-alpha", "primitive-1", "dp-a", "eg-1"),
        Resolved("mc-beta", "primitive-1", "dp-a", "eg-beta"),
      )
  }

  @Test
  fun compositeCampaignGroupsAreNotSilentlyDropped() {
    // The whole point of the change: before it, every composite resolved to nothing.
    val composites =
      allRows().map { it.campaignGroup }.filter { it.startsWith("composite-") }.toSet()
    assertThat(composites)
      .containsExactly(
        "composite-direct",
        "composite-nested",
        "composite-of-composite",
        "composite-difference",
        "composite-shared-leaf",
        "composite-left-operand-only",
      )
  }

  companion object {
    @get:ClassRule
    @JvmStatic
    val databaseProvider = PostgresDatabaseProviderRule(Schemata.REPORTING_CHANGELOG_PATH)

    /**
     * The campaign-group query `report_detail.sql` pushes to reporting Postgres, lifted verbatim
     * from the Terraform template so the test exercises production SQL rather than a copy.
     */
    private val PUSHED_DOWN_QUERY: String by lazy {
      val runfilesDir = System.getenv("TEST_SRCDIR") ?: "."
      val workspace = System.getenv("TEST_WORKSPACE") ?: "__main__"
      val template =
        Files.readString(
          Paths.get(
            runfilesDir,
            workspace,
            "src/main/terraform/gcloud/cmms/sql",
            "report_detail.sql",
          )
        )
      val connectionIndex = template.indexOf("reporting-postgres-conn")
      check(connectionIndex >= 0) { "reporting-postgres-conn not found in report_detail.sql" }
      val start = template.indexOf("'''", connectionIndex)
      check(start >= 0) { "pushed-down query not found after the connection reference" }
      val end = template.indexOf("'''", start + 3)
      check(end >= 0) { "pushed-down query is not terminated" }
      template.substring(start + 3, end)
    }
  }
}
