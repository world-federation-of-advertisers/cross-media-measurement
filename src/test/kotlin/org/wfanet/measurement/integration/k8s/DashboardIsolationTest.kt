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

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.common.truth.Truth.assertThat
import java.util.logging.Logger
import org.junit.Assert.fail
import org.junit.BeforeClass
import org.junit.Test

/**
 * Cloud test for EDPA Reporting Dashboard EDP isolation.
 *
 * Validates that:
 * 1. EDP service accounts cannot bypass views via EXTERNAL_QUERY
 * 2. EDP service accounts cannot access platform views
 * 3. EDP service accounts can only see their own data through per-EDP views
 * 4. Per-EDP views contain no forbidden columns that leak cross-EDP data
 */
class DashboardIsolationTest {

  companion object {
    private val logger = Logger.getLogger(DashboardIsolationTest::class.java.name)

    private val PROJECT =
      System.getenv("GOOGLE_CLOUD_PROJECT")
        ?: throw IllegalStateException("GOOGLE_CLOUD_PROJECT not set")
    private val REGION = System.getenv("BIGQUERY_REGION") ?: "us-central1"
    private val EDP_NAME =
      System.getenv("EDP_NAME") ?: throw IllegalStateException("EDP_NAME not set")
    private val EDP_RESOURCE_ID =
      System.getenv("EDP_RESOURCE_ID")
        ?: throw IllegalStateException("EDP_RESOURCE_ID not set")

    private const val PLATFORM_DATASET = "dashboard_views"
    private const val EDP_DATASET = "dashboard_views_edp"

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

    private lateinit var bigQuery: BigQuery

    @JvmStatic
    @BeforeClass
    fun setUp() {
      bigQuery = BigQueryOptions.getDefaultInstance().service
      logger.info(
        "Testing as EDP '$EDP_NAME' (resource ID: $EDP_RESOURCE_ID) in project $PROJECT"
      )
    }
  }

  @Test
  fun externalQueryBypassIsDenied() {
    val sql =
      """
      SELECT * FROM EXTERNAL_QUERY(
        'projects/$PROJECT/locations/$REGION/connections/kingdom-conn',
        '''SELECT DataProviderId, MeasurementConsumerId FROM EventGroups LIMIT 10'''
      )
      """
        .trimIndent()

    try {
      val result = bigQuery.query(QueryJobConfiguration.of(sql))
      fail(
        "EXTERNAL_QUERY bypass should have been denied but returned ${result.totalRows} rows"
      )
    } catch (e: BigQueryException) {
      logger.info("EXTERNAL_QUERY bypass correctly denied: ${e.message}")
      assertThat(e.code).isEqualTo(403)
    }
  }

  @Test
  fun platformViewAccessIsDenied() {
    val sql =
      """
      SELECT * FROM `$PROJECT.$PLATFORM_DATASET.requisition_overview_platform` LIMIT 1
      """
        .trimIndent()

    try {
      val result = bigQuery.query(QueryJobConfiguration.of(sql))
      fail(
        "Platform view access should have been denied but returned ${result.totalRows} rows"
      )
    } catch (e: BigQueryException) {
      logger.info("Platform view access correctly denied: ${e.message}")
      assertThat(e.code).isEqualTo(403)
    }
  }

  @Test
  fun requisitionOverviewReturnsOnlyOwnData() {
    val sql =
      """
      SELECT DISTINCT DataProviderResourceId
      FROM `$PROJECT.$EDP_DATASET.requisition_overview_$EDP_NAME`
      """
        .trimIndent()

    val result = bigQuery.query(QueryJobConfiguration.of(sql))
    val resourceIds =
      result.iterateAll().map { it["DataProviderResourceId"].stringValue }

    assertThat(resourceIds).isNotEmpty()
    assertThat(resourceIds).containsExactly(EDP_RESOURCE_ID)
    logger.info(
      "requisition_overview_$EDP_NAME: ${result.totalRows} rows, all for $EDP_RESOURCE_ID"
    )
  }

  @Test
  fun mcDetailsReturnsOnlyOwnData() {
    val sql =
      """
      SELECT DISTINCT CmmsDataProvider
      FROM `$PROJECT.$EDP_DATASET.mc_details_$EDP_NAME`
      """
        .trimIndent()

    val result = bigQuery.query(QueryJobConfiguration.of(sql))
    val dataProviders =
      result.iterateAll().map { it["CmmsDataProvider"].stringValue }

    assertThat(dataProviders).isNotEmpty()
    assertThat(dataProviders).containsExactly(EDP_RESOURCE_ID)
    logger.info(
      "mc_details_$EDP_NAME: ${result.totalRows} rows, all for $EDP_RESOURCE_ID"
    )
  }

  @Test
  fun reportDetailReturnsOnlyOwnData() {
    val sql =
      """
      SELECT DISTINCT CmmsDataProvider
      FROM `$PROJECT.$EDP_DATASET.report_detail_$EDP_NAME`
      """
        .trimIndent()

    val result = bigQuery.query(QueryJobConfiguration.of(sql))
    val dataProviders =
      result.iterateAll().map { it["CmmsDataProvider"].stringValue }

    if (result.totalRows > 0) {
      assertThat(dataProviders).containsExactly(EDP_RESOURCE_ID)
    }
    logger.info(
      "report_detail_$EDP_NAME: ${result.totalRows} rows, all for $EDP_RESOURCE_ID"
    )
  }

  @Test
  fun noForbiddenColumnsInEdpViews() {
    val sql =
      """
      SELECT table_name, column_name
      FROM `$PROJECT.$EDP_DATASET.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name LIKE '%_$EDP_NAME'
      """
        .trimIndent()

    val result = bigQuery.query(QueryJobConfiguration.of(sql))
    val violations = mutableListOf<String>()

    for (row in result.iterateAll()) {
      val tableName = row["table_name"].stringValue
      val columnName = row["column_name"].stringValue

      if (columnName in FORBIDDEN_COLUMNS) {
        violations.add("$tableName: forbidden column '$columnName'")
      }

      for (pattern in FORBIDDEN_PATTERNS) {
        if (pattern.matches(columnName)) {
          violations.add(
            "$tableName: column '$columnName' matches forbidden pattern '${pattern.pattern}'"
          )
        }
      }
    }

    assertThat(violations).isEmpty()
    logger.info("Column exclusion check passed for $EDP_NAME views")
  }

  @Test
  fun cannotAccessOtherEdpViews() {
    val sql =
      """
      SELECT table_name
      FROM `$PROJECT.$EDP_DATASET.INFORMATION_SCHEMA.TABLES`
      WHERE table_name NOT LIKE '%_$EDP_NAME'
      """
        .trimIndent()

    val result = bigQuery.query(QueryJobConfiguration.of(sql))
    val otherViews =
      result.iterateAll().map { it["table_name"].stringValue }.toList()

    for (viewName in otherViews) {
      try {
        val query = "SELECT * FROM `$PROJECT.$EDP_DATASET.$viewName` LIMIT 1"
        bigQuery.query(QueryJobConfiguration.of(query))
        fail("Should not have access to $viewName")
      } catch (e: BigQueryException) {
        assertThat(e.code).isEqualTo(403)
        logger.info("Correctly denied access to $viewName")
      }
    }
  }
}
