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
import com.google.common.truth.Truth.assertWithMessage
import java.util.logging.Logger
import org.junit.BeforeClass
import org.junit.Test
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks
import org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.DashboardIsolationChecks.EdpConfig

/**
 * Cloud test for EDPA Reporting Dashboard EDP isolation.
 *
 * Validates that:
 * 1. EDP service accounts can only see their own rows via row access policies
 * 2. EDP service accounts get zero rows for other EDPs' data
 * 3. EDP service accounts cannot access platform-only tables (403)
 * 4. EDP service accounts cannot bypass row filtering via EXTERNAL_QUERY
 * 5. Deployed table schemas have no forbidden columns
 * 6. Data correctness: tables have rows and metrics are populated
 */
class DashboardIsolationTest {

  @Test
  fun dataIsolation() {
    val results = checks.checkDataIsolation(bigQuery, edp)
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

  @Test
  fun iamBoundary() {
    val results = checks.checkIamBoundary(bigQuery, edp)
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

  @Test
  fun externalQueryBypass() {
    val results = checks.checkExternalQueryBypass(bigQuery, edp)
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

  @Test
  fun driftDetection() {
    val results = checks.checkDriftDetection(bigQuery, listOf(edp))
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

  @Test
  fun dataCorrectness() {
    val results = checks.checkDataCorrectness(bigQuery, edp)
    val failures = results.filter { !it.passed }
    assertWithMessage(failures.joinToString("\n") { it.message }).that(failures).isEmpty()
  }

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
    // When set, the test runs BigQuery calls as this service account by impersonating it
    // from ADC. CI sets this so the workflow can authenticate once as the terraform SA and
    // fan out per-EDP; local dev leaves it unset and uses ADC directly.
    private val IMPERSONATE_SA: String? =
      System.getenv("EDP_IMPERSONATE_SA")?.takeIf { it.isNotBlank() }

    private const val DATASET = "dashboard"

    private lateinit var bigQuery: BigQuery
    private lateinit var checks: DashboardIsolationChecks
    private lateinit var edp: EdpConfig

    @JvmStatic
    @BeforeClass
    fun setUp() {
      bigQuery = buildBigQuery()
      checks = DashboardIsolationChecks(PROJECT, DATASET, REGION)
      edp = EdpConfig(EDP_NAME, EDP_RESOURCE_ID)
      val identity = IMPERSONATE_SA ?: "ADC"
      logger.info(
        "Testing as EDP '$EDP_NAME' (resource ID: $EDP_RESOURCE_ID) as $identity in project $PROJECT"
      )
    }

    private fun buildBigQuery(): BigQuery {
      val target = IMPERSONATE_SA ?: return BigQueryOptions.getDefaultInstance().service
      val scopes =
        listOf(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/cloud-platform",
        )
      val adc = GoogleCredentials.getApplicationDefault().createScoped(scopes)
      val impersonated = ImpersonatedCredentials.create(adc, target, null, scopes, 300)
      return BigQueryOptions.newBuilder()
        .setCredentials(impersonated)
        .setProjectId(PROJECT)
        .build()
        .service
    }
  }
}
