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

    private const val DATASET = "dashboard"

    private lateinit var bigQueryAsEdp: BigQuery
    private lateinit var bigQueryAsInspector: BigQuery
    private lateinit var checks: DashboardIsolationChecks
    private lateinit var edp: EdpConfig

    @JvmStatic
    @BeforeClass
    fun setUp() {
      bigQueryAsEdp = buildBigQuery(EDP_IMPERSONATE_SA)
      bigQueryAsInspector = buildBigQuery(INSPECTOR_IMPERSONATE_SA)
      checks = DashboardIsolationChecks(PROJECT, DATASET, REGION)
      edp = EdpConfig(EDP_NAME, EDP_RESOURCE_ID)
      logger.info(
        "Testing EDP '$EDP_NAME' (resource ID: $EDP_RESOURCE_ID) in project $PROJECT " +
          "(edp SA: ${EDP_IMPERSONATE_SA ?: "ADC"}, inspector SA: ${INSPECTOR_IMPERSONATE_SA ?: "ADC"})"
      )
    }

    private fun buildBigQuery(impersonateTarget: String?): BigQuery {
      if (impersonateTarget == null) {
        return BigQueryOptions.getDefaultInstance().service
      }
      val scopes =
        listOf(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/cloud-platform",
        )
      val adc = GoogleCredentials.getApplicationDefault().createScoped(scopes)
      val impersonated = ImpersonatedCredentials.create(adc, impersonateTarget, null, scopes, 300)
      return BigQueryOptions.newBuilder()
        .setCredentials(impersonated)
        .setProjectId(PROJECT)
        .build()
        .service
    }
  }
}
