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

import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.ImpersonatedCredentials
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import java.util.logging.Logger
import org.junit.BeforeClass
import org.junit.Test
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.EdpConfig
import org.wfanet.measurement.integration.k8s.ReportDetailFixtureQuery.Companion.MC_ALPHA
import org.wfanet.measurement.integration.k8s.ReportDetailFixtureQuery.Companion.MC_BETA
import org.wfanet.measurement.integration.k8s.ReportDetailFixtureQuery.Companion.REPORT_INTERSECTION
import org.wfanet.measurement.integration.k8s.ReportDetailFixtureQuery.Companion.REPORT_RIGHT_NESTED
import org.wfanet.measurement.integration.k8s.ReportDetailFixtureQuery.Companion.REPORT_SHARED

/**
 * Cloud test for EDPA Reporting Dashboard EDP isolation.
 *
 * Uses the two-identity model established by DashboardComplianceCheck:
 * - Per-EDP checks (data isolation, IAM boundary, EXTERNAL_QUERY bypass, data correctness) run as
 *   the EDP service account, which has only row-scoped SELECT via row access policies. These checks
 *   are meaningful only when executed with the identity whose isolation they verify.
 * - Platform checks (UDF output validation, drift detection, freshness) run as the inspector
 *   service account (dashboard-compliance@…) which holds the dashboardComplianceChecker custom
 *   role. They need schema and IAM introspection privileges that EDP identities intentionally lack.
 *
 * See the driver at
 * src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dashboard/tools/DashboardComplianceCheck.kt
 * for the canonical split.
 */
class DashboardIsolationTest {

  @Test
  fun dataIsolation() {
    val results = checks.checkDataIsolation(bigQueryAsEdp, edp)
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

  @Test
  fun iamBoundary() {
    val results = checks.checkIamBoundary(bigQueryAsEdp, edp)
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

  @Test
  fun externalQueryBypass() {
    val results = checks.checkExternalQueryBypass(bigQueryAsEdp, edp)
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

  @Test
  fun dataCorrectness() {
    val results = checks.checkDataCorrectness(bigQueryAsEdp, edp)
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

  @Test
  fun driftDetection() {
    val results = checks.checkDriftDetection(bigQueryAsInspector, listOf(edp))
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

  /** One `report_detail_edp` row as an EDP sees it. */
  private data class EdpRow(
    val measurementConsumer: String,
    val report: String,
    val reportState: String,
    val eventGroups: Set<String>,
    val campaignNames: Set<String>,
    val brandNames: Set<String>,
    val entityTypes: Set<String>,
    val entityIds: Set<String>,
  )

  @Test
  fun reportDetailEdpRowsContainOnlyTheirOwnProvidersEventGroupsAndMetadata() {
    // The row access policy filters on CmmsDataProvider alone, so it cannot stop another
    // provider's event groups or metadata from being embedded in a correctly labelled row. The
    // fixture gives two providers the same ExternalEventGroupId with different metadata and checks
    // what each provider's rows actually contain.
    val ids = fixtureApiIds()
    val edpView = fixture.sourceQuery(platform = false)

    assertThat(edpRowsFor(edpView, ids.getValue("meta_dp")))
      .containsExactly(
        EdpRow(
          MC_ALPHA,
          REPORT_SHARED,
          "SUCCEEDED",
          setOf(ids.getValue("shared_eg")),
          setOf("Meta Campaign Shared"),
          setOf("Meta Brand"),
          setOf("campaign"),
          setOf("meta-entity-shared"),
        ),
        EdpRow(
          MC_ALPHA,
          REPORT_RIGHT_NESTED,
          "SUCCEEDED",
          setOf(ids.getValue("shared_eg"), ids.getValue("meta_only_eg")),
          setOf("Meta Campaign Shared", "Meta Campaign Solo"),
          setOf("Meta Brand"),
          setOf("campaign"),
          setOf("meta-entity-shared", "meta-entity-solo"),
        ),
        EdpRow(
          MC_ALPHA,
          REPORT_INTERSECTION,
          "FAILED",
          setOf(ids.getValue("shared_eg")),
          setOf("Meta Campaign Shared"),
          setOf("Meta Brand"),
          setOf("campaign"),
          setOf("meta-entity-shared"),
        ),
      )

    // MC_BETA reuses MC_ALPHA's report ID and campaign-group ID, and must stay a separate row
    // holding only its own event group.
    assertThat(edpRowsFor(edpView, ids.getValue("other_dp")))
      .containsExactly(
        EdpRow(
          MC_ALPHA,
          REPORT_SHARED,
          "SUCCEEDED",
          setOf(ids.getValue("shared_eg")),
          setOf("Other Campaign Shared"),
          setOf("Other Brand"),
          setOf("line_item"),
          setOf("other-entity-shared"),
        ),
        EdpRow(
          MC_ALPHA,
          REPORT_RIGHT_NESTED,
          "SUCCEEDED",
          setOf(ids.getValue("shared_eg")),
          setOf("Other Campaign Shared"),
          setOf("Other Brand"),
          setOf("line_item"),
          setOf("other-entity-shared"),
        ),
        EdpRow(
          MC_ALPHA,
          REPORT_INTERSECTION,
          "FAILED",
          setOf(ids.getValue("shared_eg"), ids.getValue("other_only_eg")),
          setOf("Other Campaign Shared", "Other Campaign Solo"),
          setOf("Other Brand"),
          setOf("line_item"),
          setOf("other-entity-shared", "other-entity-solo"),
        ),
        EdpRow(
          MC_BETA,
          REPORT_SHARED,
          "SUCCEEDED",
          setOf(ids.getValue("other_only_eg")),
          setOf("Other Campaign Solo"),
          setOf("Other Brand"),
          setOf("line_item"),
          setOf("other-entity-solo"),
        ),
      )
  }

  @Test
  fun reportDetailEdpCountIsScopedByMeasurementConsumer() {
    // Both MeasurementConsumers have a report called REPORT_SHARED. EdpCount must count the
    // providers in each consumer's report, not across the two.
    val edpCounts =
      runFixtureQuery(
          "SELECT CmmsMeasurementConsumer, EdpCount FROM (${fixture.sourceQuery(platform = true)}) " +
            "WHERE ExternalReportId = '$REPORT_SHARED'"
        )
        .associate { it["CmmsMeasurementConsumer"].stringValue to it["EdpCount"].longValue }

    assertThat(edpCounts).containsExactly(MC_ALPHA, 2L, MC_BETA, 1L)
  }

  /** Rows of [edpView] visible to [dataProvider], filtered as its row access policy does. */
  private fun edpRowsFor(edpView: String, dataProvider: String): List<EdpRow> =
    runFixtureQuery("SELECT * FROM ($edpView) WHERE CmmsDataProvider = '$dataProvider'").map {
      EdpRow(
        measurementConsumer = it["CmmsMeasurementConsumer"].stringValue,
        report = it["ExternalReportId"].stringValue,
        reportState = it["ReportState"].stringValue,
        eventGroups = it.strings("CmmsEventGroupIds"),
        campaignNames = it.strings("CampaignNames"),
        brandNames = it.strings("BrandNames"),
        entityTypes = it.strings("EntityTypes"),
        entityIds = it.strings("EntityIds"),
      )
    }

  private fun FieldValueList.strings(field: String): Set<String> =
    get(field).repeatedValue.map { it.stringValue }.toSet()

  private fun fixtureApiIds(): Map<String, String> {
    val row = runFixtureQuery(fixture.apiIdsQuery()).single()
    return listOf("meta_dp", "other_dp", "shared_eg", "meta_only_eg", "other_only_eg")
      .associateWith { row[it].stringValue }
  }

  // Runs as the inspector, which may create query jobs and read the UDF. The fixture query reads
  // no tables, so it neither needs nor exercises any table or row-level grant.
  private fun runFixtureQuery(sql: String): List<FieldValueList> =
    bigQueryAsInspector
      .query(
        QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build(),
        JobId.newBuilder().setLocation(REGION).build(),
      )
      .iterateAll()
      .toList()

  companion object {
    private val logger = Logger.getLogger(DashboardIsolationTest::class.java.name)

    private val PROJECT =
      System.getenv("GOOGLE_CLOUD_PROJECT")
        ?: throw IllegalStateException("GOOGLE_CLOUD_PROJECT not set")
    private val REGION =
      System.getenv("BIGQUERY_REGION") ?: throw IllegalStateException("BIGQUERY_REGION not set")
    private val EDP_NAME =
      System.getenv("EDP_NAME") ?: throw IllegalStateException("EDP_NAME not set")
    private val EDP_RESOURCE_ID =
      System.getenv("EDP_RESOURCE_ID") ?: throw IllegalStateException("EDP_RESOURCE_ID not set")

    // Optional impersonation targets. When set, ADC is used only as the outer credential and
    // each BigQuery client is built by impersonating the named service account. When unset,
    // the client uses ADC directly (supports local-dev runs where the user already has both
    // roles via their own identity).
    private val EDP_IMPERSONATE_SA: String? =
      System.getenv("EDP_IMPERSONATE_SA")?.takeIf { it.isNotBlank() }
    private val INSPECTOR_IMPERSONATE_SA: String? =
      System.getenv("INSPECTOR_IMPERSONATE_SA")?.takeIf { it.isNotBlank() }
    // Optional intermediate SA that the outer ADC identity impersonates before it can token-create
    // on the EDP SA. In CI: the workflow auths as TF SA which can impersonate
    // INSPECTOR_IMPERSONATE_SA
    // (dashboard-compliance@), and dashboard-compliance@ can impersonate the per-EDP SAs — but
    // TF SA cannot impersonate the per-EDP SAs directly (by design; we don't want that grant).
    // Setting EDP_DELEGATES to the inspector SA chains the impersonation as
    // ADC → inspector SA → EDP SA using ImpersonatedCredentials' delegates parameter.
    private val EDP_DELEGATES: List<String> =
      System.getenv("EDP_DELEGATES")?.split(",")?.map { it.trim() }?.filter { it.isNotEmpty() }
        ?: emptyList()

    private const val DATASET = "dashboard"

    private lateinit var bigQueryAsEdp: BigQuery
    private lateinit var bigQueryAsInspector: BigQuery
    private lateinit var checks: DashboardIsolationChecks
    private lateinit var edp: EdpConfig
    private lateinit var fixture: ReportDetailFixtureQuery

    @JvmStatic
    @BeforeClass
    fun setUp() {
      bigQueryAsEdp = buildBigQuery(EDP_IMPERSONATE_SA, EDP_DELEGATES)
      bigQueryAsInspector = buildBigQuery(INSPECTOR_IMPERSONATE_SA, emptyList())
      checks = DashboardIsolationChecks(PROJECT, DATASET, REGION)
      edp = EdpConfig(EDP_NAME, EDP_RESOURCE_ID)
      fixture = ReportDetailFixtureQuery(PROJECT, REGION)
      logger.info(
        "Testing EDP '$EDP_NAME' (resource ID: $EDP_RESOURCE_ID) in project $PROJECT " +
          "(edp SA: ${EDP_IMPERSONATE_SA ?: "ADC"}, inspector SA: ${INSPECTOR_IMPERSONATE_SA ?: "ADC"}, edp delegates: $EDP_DELEGATES)"
      )
    }

    private fun buildBigQuery(impersonateTarget: String?, delegates: List<String>): BigQuery {
      if (impersonateTarget == null) {
        return BigQueryOptions.getDefaultInstance().service
      }
      val scopes = listOf("https://www.googleapis.com/auth/cloud-platform")
      val adc = GoogleCredentials.getApplicationDefault().createScoped(scopes)
      val impersonated =
        ImpersonatedCredentials.create(adc, impersonateTarget, delegates, scopes, 300)
      return BigQueryOptions.newBuilder()
        .setCredentials(impersonated)
        .setProjectId(PROJECT)
        .build()
        .service
    }
  }
}
