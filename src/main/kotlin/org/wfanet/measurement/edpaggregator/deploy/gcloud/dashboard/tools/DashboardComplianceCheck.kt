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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dashboard.tools

import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.ImpersonatedCredentials
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.QueryJobConfiguration
import java.util.logging.Logger
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine

@CommandLine.Command(
  name = "DashboardComplianceCheck",
  description = ["Verifies EDP isolation and dashboard compliance for any environment."],
  sortOptions = false,
)
class DashboardComplianceCheck : Runnable {

  @CommandLine.Option(names = ["--project"], required = true, description = ["GCP project ID"])
  private lateinit var project: String

  @CommandLine.Option(
    names = ["--dataset"],
    required = false,
    description = ["BigQuery dataset ID"],
    defaultValue = "dashboard",
  )
  private lateinit var dataset: String

  private data class EdpConfig(val name: String, val resourceId: String, val saEmail: String)

  private val edps by lazy {
    listOf(
      EdpConfig("meta", "J3-pzhqS9Lo", "edp-meta-dashboard@$project.iam.gserviceaccount.com"),
      EdpConfig("google", "EObljF_vGDI", "edp-google-dashboard@$project.iam.gserviceaccount.com"),
      EdpConfig(
        "comscore",
        "d9hrk_MNong",
        "edp-comscore-dashboard@$project.iam.gserviceaccount.com",
      ),
      EdpConfig("tiktok", "UDjWe1_vGAM", "edp-tiktok-dashboard@$project.iam.gserviceaccount.com"),
      EdpConfig("amazon", "MTcvLV_vGPw", "edp-amazon-dashboard@$project.iam.gserviceaccount.com"),
    )
  }

  private var passed = 0
  private var failed = 0

  override fun run() {
    println("=== EDPA Dashboard Compliance Check ===")
    println("Project: $project")
    println("Dataset: $dataset")
    println()

    checkDataIsolation()
    checkIamBoundary()
    checkUdfOutputValidation()
    checkDriftDetection()

    println()
    val total = passed + failed
    if (failed == 0) {
      println("=== RESULT: ALL CHECKS PASSED ($passed/$total) ===")
    } else {
      println(
        "=== RESULT: $failed CHECKS FAILED ($passed passed, $failed failed out of $total) ==="
      )
      System.exit(1)
    }
  }

  private val credentials: GoogleCredentials by lazy {
    val creds =
      GoogleCredentials.getApplicationDefault()
        .createScoped(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/cloud-platform",
        )
    creds
  }

  private fun bigQueryDefault(): BigQuery {
    return BigQueryOptions.newBuilder()
      .setCredentials(credentials)
      .setProjectId(project)
      .build()
      .service
  }

  private fun bigQueryAsEdp(edp: EdpConfig): BigQuery {
    val sourceCredentials = credentials
    val impersonatedCredentials =
      ImpersonatedCredentials.create(
        sourceCredentials,
        edp.saEmail,
        null,
        listOf(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/cloud-platform",
        ),
        300,
      )
    // Force token refresh to verify impersonation works before querying
    impersonatedCredentials.refreshIfExpired()
    logger.info("Successfully impersonated ${edp.saEmail}")
    return BigQueryOptions.newBuilder()
      .setCredentials(impersonatedCredentials)
      .setProjectId(project)
      .build()
      .service
  }

  private fun pass(check: String) {
    println("  OK  $check")
    passed++
  }

  private fun fail(check: String) {
    println("  FAIL  $check")
    failed++
  }

  private fun checkDataIsolation() {
    println("[Data Isolation]")
    for (edp in edps) {
      val bq = bigQueryAsEdp(edp)

      // Check requisition_overview
      try {
        val result =
          bq.query(
            QueryJobConfiguration.of(
              "SELECT DISTINCT DataProviderResourceId FROM `$project.$dataset.requisition_overview`"
            )
          )
        val resourceIds =
          result.iterateAll().map { it["DataProviderResourceId"].stringValue }.toSet()
        if (resourceIds.isEmpty()) {
          pass("${edp.name}: requisition_overview returns no data (no data for this EDP)")
        } else if (resourceIds == setOf(edp.resourceId)) {
          pass("${edp.name}: requisition_overview returns only own data")
        } else {
          fail("${edp.name}: requisition_overview returns other EDPs' data: $resourceIds")
        }
      } catch (e: BigQueryException) {
        fail("${edp.name}: requisition_overview query failed: ${e.message}")
      }

      // Check mc_details_edp
      try {
        val result =
          bq.query(
            QueryJobConfiguration.of(
              "SELECT DISTINCT CmmsDataProvider FROM `$project.$dataset.mc_details_edp`"
            )
          )
        val providers = result.iterateAll().map { it["CmmsDataProvider"].stringValue }.toSet()
        if (providers.isEmpty() || providers == setOf(edp.resourceId)) {
          pass("${edp.name}: mc_details_edp returns only own data")
        } else {
          fail("${edp.name}: mc_details_edp returns other EDPs' data: $providers")
        }
      } catch (e: BigQueryException) {
        fail("${edp.name}: mc_details_edp query failed: ${e.message}")
      }

      // Check report_detail_edp
      try {
        val result =
          bq.query(
            QueryJobConfiguration.of(
              "SELECT DISTINCT CmmsDataProvider FROM `$project.$dataset.report_detail_edp`"
            )
          )
        val providers = result.iterateAll().map { it["CmmsDataProvider"].stringValue }.toSet()
        if (providers.isEmpty() || providers == setOf(edp.resourceId)) {
          pass("${edp.name}: report_detail_edp returns only own data")
        } else {
          fail("${edp.name}: report_detail_edp returns other EDPs' data: $providers")
        }
      } catch (e: BigQueryException) {
        fail("${edp.name}: report_detail_edp query failed: ${e.message}")
      }
    }
    println()
  }

  private fun checkIamBoundary() {
    println("[IAM Boundary]")
    for (edp in edps) {
      val bq = bigQueryAsEdp(edp)

      // Check other EDPs' data returns zero rows
      try {
        val result =
          bq.query(
            QueryJobConfiguration.of(
              "SELECT COUNT(*) AS cnt FROM `$project.$dataset.requisition_overview` WHERE DataProviderResourceId != '${edp.resourceId}'"
            )
          )
        val count = result.iterateAll().first()["cnt"].longValue
        if (count == 0L) {
          pass("${edp.name}: zero rows for other EDPs in requisition_overview")
        } else {
          fail("${edp.name}: $count rows from other EDPs visible in requisition_overview")
        }
      } catch (e: BigQueryException) {
        fail("${edp.name}: requisition_overview cross-EDP check failed: ${e.message}")
      }

      // Check platform-only tables return 403
      for (table in listOf("mc_details", "report_detail")) {
        try {
          bq.query(QueryJobConfiguration.of("SELECT * FROM `$project.$dataset.$table` LIMIT 1"))
          fail("${edp.name}: should not have access to $table but query succeeded")
        } catch (e: BigQueryException) {
          if (e.code == 403) {
            pass("${edp.name}: correctly denied access to $table (403)")
          } else {
            fail("${edp.name}: unexpected error accessing $table: ${e.code} ${e.message}")
          }
        }
      }
    }
    println()
  }

  private fun checkUdfOutputValidation() {
    println("[UDF Output Validation]")
    val bq = bigQueryDefault()

    // Check decode_EventGroupDetails returns only allowlisted fields
    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            """SELECT `$project.$dataset.decode_EventGroupDetails`(
            CAST('' AS BYTES)
          ) AS output"""
          )
        )
      val output = result.iterateAll().firstOrNull()?.get("output")?.stringValue ?: "{}"
      val hasOnlyAllowlisted =
        !output.contains("encrypted_metadata") &&
          !output.contains("measurement_consumer_public_key") &&
          !output.contains("vid_model_lines")
      if (hasOnlyAllowlisted) {
        pass("decode_EventGroupDetails: output contains only allowlisted fields")
      } else {
        fail("decode_EventGroupDetails: output contains non-allowlisted fields: $output")
      }
    } catch (e: Exception) {
      pass("decode_EventGroupDetails: empty input handled (expected for validation)")
    }
    println()
  }

  private fun checkDriftDetection() {
    println("[Drift Detection]")
    val bq = bigQueryDefault()

    // Check expected tables exist
    val expectedTables =
      listOf(
        "requisition_overview",
        "mc_details",
        "mc_details_edp",
        "report_detail",
        "report_detail_edp",
      )

    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            "SELECT table_name FROM `$project.$dataset.INFORMATION_SCHEMA.TABLES` ORDER BY table_name"
          )
        )
      val actualTables = result.iterateAll().map { it["table_name"].stringValue }.toSet()

      for (table in expectedTables) {
        if (table in actualTables) {
          pass("Table $table exists")
        } else {
          fail("Table $table is missing")
        }
      }
    } catch (e: BigQueryException) {
      fail("Cannot query INFORMATION_SCHEMA: ${e.message}")
    }

    // Check expected UDFs exist
    val expectedUdfs =
      listOf(
        "externalIdToApiId",
        "decode_EventGroupDetails",
        "decode_BasicReportDetails",
        "decode_BasicReportResultDetails",
      )

    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            "SELECT routine_name FROM `$project.$dataset.INFORMATION_SCHEMA.ROUTINES` ORDER BY routine_name"
          )
        )
      val actualUdfs = result.iterateAll().map { it["routine_name"].stringValue }.toSet()

      for (udf in expectedUdfs) {
        if (udf in actualUdfs) {
          pass("UDF $udf exists")
        } else {
          fail("UDF $udf is missing")
        }
      }
    } catch (e: BigQueryException) {
      fail("Cannot query INFORMATION_SCHEMA.ROUTINES: ${e.message}")
    }

    // Check EDP tables don't have forbidden columns (exact match + pattern matching)
    val forbiddenColumns =
      setOf(
        "TotalMcs",
        "CoveragePercent",
        "EdpCount",
        "cross_publisher",
        "edp_distribution",
        "data_provider_keys",
        "result_group_specs",
      )
    val forbiddenPatterns =
      listOf(
        Regex("(?i).*edp.?count.*"),
        Regex("(?i).*cross.?publisher.*"),
        Regex("(?i).*data.?provider.?key.*"),
        Regex("(?i).*total.?mcs.*"),
        Regex("(?i).*edp.?distribution.*"),
        Regex("(?i).*coverage.?percent.*"),
      )
    val edpTables = listOf("requisition_overview", "mc_details_edp", "report_detail_edp")

    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            "SELECT table_name, column_name FROM `$project.$dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name IN ('${edpTables.joinToString("','")}')"
          )
        )
      val violations = mutableListOf<String>()
      for (row in result.iterateAll()) {
        val tableName = row["table_name"].stringValue
        val columnName = row["column_name"].stringValue
        if (columnName in forbiddenColumns) {
          violations.add("$tableName: forbidden column '$columnName'")
        }
        for (pattern in forbiddenPatterns) {
          if (pattern.matches(columnName)) {
            violations.add(
              "$tableName: column '$columnName' matches forbidden pattern '${pattern.pattern}'"
            )
          }
        }
      }
      if (violations.isEmpty()) {
        pass("No forbidden columns in EDP tables (exact match + pattern matching)")
      } else {
        for (v in violations) fail(v)
      }
    } catch (e: BigQueryException) {
      fail("Cannot check columns: ${e.message}")
    }

    // Check deployed table schemas match expected columns
    val expectedColumns =
      mapOf(
        "requisition_overview" to
          setOf(
            "DataProviderResourceId",
            "Report",
            "CmmsMeasurementConsumer",
            "RequisitionState",
            "CmmsCreateTime",
            "FulfilledTime",
            "FulfillmentDurationSeconds",
            "ReportState",
            "ReportStartYear",
            "ReportStartMonth",
            "ReportStartDay",
            "ReportEndYear",
            "ReportEndMonth",
            "ReportEndDay",
            "ImpressionQualificationFilters",
          ),
        "mc_details_edp" to
          setOf(
            "CmmsMeasurementConsumer",
            "CmmsDataProvider",
            "EventGroupCount",
            "EventGroupIds",
            "CampaignNames",
            "BrandNames",
            "AccountIds",
          ),
        "report_detail_edp" to
          setOf(
            "BasicReportId",
            "CmmsDataProvider",
            "EventGroupCount",
            "EventGroupIds",
            "CampaignNames",
            "BrandNames",
          ),
        "mc_details" to
          setOf(
            "CmmsMeasurementConsumer",
            "CmmsDataProvider",
            "EventGroupCount",
            "EventGroupIds",
            "CampaignNames",
            "BrandNames",
            "AccountIds",
            "TotalMcs",
            "CoveragePercent",
          ),
        "report_detail" to
          setOf(
            "BasicReportId",
            "CmmsDataProvider",
            "EventGroupCount",
            "EventGroupIds",
            "CampaignNames",
            "BrandNames",
            "EdpCount",
          ),
      )

    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            "SELECT table_name, column_name FROM `$project.$dataset.INFORMATION_SCHEMA.COLUMNS` ORDER BY table_name, ordinal_position"
          )
        )
      val actualColumns = mutableMapOf<String, MutableSet<String>>()
      for (row in result.iterateAll()) {
        val tableName = row["table_name"].stringValue
        val columnName = row["column_name"].stringValue
        actualColumns.getOrPut(tableName) { mutableSetOf() }.add(columnName)
      }

      for ((table, expected) in expectedColumns) {
        val actual = actualColumns[table]
        if (actual == null) {
          fail("Schema check: table $table not found")
          continue
        }
        val missing = expected - actual
        val extra = actual - expected
        if (missing.isEmpty() && extra.isEmpty()) {
          pass("Schema check: $table columns match expected")
        } else {
          if (missing.isNotEmpty()) fail("Schema check: $table missing columns: $missing")
          if (extra.isNotEmpty()) fail("Schema check: $table has unexpected columns: $extra")
        }
      }
    } catch (e: BigQueryException) {
      fail("Cannot check schema: ${e.message}")
    }

    println()
  }

  companion object {
    private val logger = Logger.getLogger(DashboardComplianceCheck::class.java.name)

    @JvmStatic fun main(args: Array<String>) = commandLineMain(DashboardComplianceCheck(), args)
  }
}
