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
 * 1. EDP service accounts can only see their own rows via row access policies
 * 2. EDP service accounts get zero rows for other EDPs' data
 * 3. EDP service accounts cannot access platform-only tables (403)
 */
class DashboardIsolationTest {

  companion object {
    private val logger = Logger.getLogger(DashboardIsolationTest::class.java.name)

    private val PROJECT =
      System.getenv("GOOGLE_CLOUD_PROJECT")
        ?: throw IllegalStateException("GOOGLE_CLOUD_PROJECT not set")
    private val EDP_NAME =
      System.getenv("EDP_NAME") ?: throw IllegalStateException("EDP_NAME not set")
    private val EDP_RESOURCE_ID =
      System.getenv("EDP_RESOURCE_ID") ?: throw IllegalStateException("EDP_RESOURCE_ID not set")

    private const val DATASET = "dashboard"

    private lateinit var bigQuery: BigQuery

    @JvmStatic
    @BeforeClass
    fun setUp() {
      bigQuery = BigQueryOptions.getDefaultInstance().service
      logger.info("Testing as EDP '$EDP_NAME' (resource ID: $EDP_RESOURCE_ID) in project $PROJECT")
    }
  }

  @Test
  fun requisitionOverviewReturnsOnlyOwnData() {
    val sql =
      """
      SELECT DISTINCT DataProviderResourceId
      FROM `$PROJECT.$DATASET.requisition_overview`
      """
        .trimIndent()

    val result = bigQuery.query(QueryJobConfiguration.of(sql))
    val resourceIds = result.iterateAll().map { it["DataProviderResourceId"].stringValue }

    assertThat(resourceIds).isNotEmpty()
    assertThat(resourceIds).containsExactly(EDP_RESOURCE_ID)
    logger.info("requisition_overview: ${result.totalRows} rows, all for $EDP_RESOURCE_ID")
  }

  @Test
  fun mcDetailsEdpReturnsOnlyOwnData() {
    val sql =
      """
      SELECT DISTINCT CmmsDataProvider
      FROM `$PROJECT.$DATASET.mc_details_edp`
      """
        .trimIndent()

    val result = bigQuery.query(QueryJobConfiguration.of(sql))
    val dataProviders = result.iterateAll().map { it["CmmsDataProvider"].stringValue }

    assertThat(dataProviders).isNotEmpty()
    assertThat(dataProviders).containsExactly(EDP_RESOURCE_ID)
    logger.info("mc_details_edp: ${result.totalRows} rows, all for $EDP_RESOURCE_ID")
  }

  @Test
  fun reportDetailEdpReturnsOnlyOwnData() {
    val sql =
      """
      SELECT DISTINCT CmmsDataProvider
      FROM `$PROJECT.$DATASET.report_detail_edp`
      """
        .trimIndent()

    val result = bigQuery.query(QueryJobConfiguration.of(sql))
    val dataProviders = result.iterateAll().map { it["CmmsDataProvider"].stringValue }

    if (result.totalRows > 0) {
      assertThat(dataProviders).containsExactly(EDP_RESOURCE_ID)
    }
    logger.info("report_detail_edp: ${result.totalRows} rows, all for $EDP_RESOURCE_ID")
  }

  @Test
  fun otherEdpDataReturnsZeroRows() {
    val sql =
      """
      SELECT COUNT(*) AS cnt
      FROM `$PROJECT.$DATASET.requisition_overview`
      WHERE DataProviderResourceId != '$EDP_RESOURCE_ID'
      """
        .trimIndent()

    val result = bigQuery.query(QueryJobConfiguration.of(sql))
    val count = result.iterateAll().first()["cnt"].longValue

    assertThat(count).isEqualTo(0)
    logger.info("Row access policy correctly filters other EDPs' data")
  }

  @Test
  fun platformMcDetailsAccessIsDenied() {
    val sql =
      """
      SELECT * FROM `$PROJECT.$DATASET.mc_details` LIMIT 1
      """
        .trimIndent()

    try {
      val result = bigQuery.query(QueryJobConfiguration.of(sql))
      fail(
        "Platform mc_details access should have been denied but returned ${result.totalRows} rows"
      )
    } catch (e: BigQueryException) {
      logger.info("Platform mc_details correctly denied: ${e.message}")
      assertThat(e.code).isEqualTo(403)
    }
  }

  @Test
  fun platformReportDetailAccessIsDenied() {
    val sql =
      """
      SELECT * FROM `$PROJECT.$DATASET.report_detail` LIMIT 1
      """
        .trimIndent()

    try {
      val result = bigQuery.query(QueryJobConfiguration.of(sql))
      fail(
        "Platform report_detail access should have been denied but returned ${result.totalRows} rows"
      )
    } catch (e: BigQueryException) {
      logger.info("Platform report_detail correctly denied: ${e.message}")
      assertThat(e.code).isEqualTo(403)
    }
  }
}
