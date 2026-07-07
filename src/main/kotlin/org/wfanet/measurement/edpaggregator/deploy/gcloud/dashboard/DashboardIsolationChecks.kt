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

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.TableId
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

class DashboardIsolationChecks(
  private val project: String,
  private val dataset: String,
  private val region: String,
) {

  data class EdpConfig(val name: String, val resourceId: String)

  data class CheckResult(val name: String, val passed: Boolean, val message: String)

  fun checkDataIsolation(bq: BigQuery, edp: EdpConfig): List<CheckResult> {
    val results = mutableListOf<CheckResult>()

    for (table in
      listOf(
        "requisition_overview" to "DataProviderResourceId",
        "mc_details_edp" to "CmmsDataProvider",
        "report_detail_edp" to "CmmsDataProvider",
      )) {
      val (tableName, column) = table
      try {
        val result =
          bq.query(
            QueryJobConfiguration.of("SELECT DISTINCT $column FROM `$project.$dataset.$tableName`")
          )
        val ids = result.iterateAll().map { it[column].stringValue }.toSet()
        if (ids.isEmpty()) {
          results.add(
            CheckResult(
              "${edp.name}: $tableName",
              false,
              "${edp.name}: $tableName is empty (expected data after scheduled queries)",
            )
          )
        } else if (ids == setOf(edp.resourceId)) {
          results.add(
            CheckResult(
              "${edp.name}: $tableName",
              true,
              "${edp.name}: $tableName returns only own data",
            )
          )
        } else {
          results.add(
            CheckResult(
              "${edp.name}: $tableName",
              false,
              "${edp.name}: $tableName returns other EDPs\' data: $ids",
            )
          )
        }
      } catch (e: BigQueryException) {
        results.add(
          CheckResult(
            "${edp.name}: $tableName",
            false,
            "${edp.name}: $tableName query failed: ${e.message}",
          )
        )
      }
    }
    return results
  }

  fun checkIamBoundary(bq: BigQuery, edp: EdpConfig): List<CheckResult> {
    val results = mutableListOf<CheckResult>()

    // Cross-EDP zero rows
    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            "SELECT COUNT(*) AS cnt FROM `$project.$dataset.requisition_overview` WHERE DataProviderResourceId != '${edp.resourceId}'"
          )
        )
      val count = result.iterateAll().first()["cnt"].longValue
      if (count == 0L) {
        results.add(
          CheckResult(
            "${edp.name}: cross-EDP",
            true,
            "${edp.name}: zero rows for other EDPs in requisition_overview",
          )
        )
      } else {
        results.add(
          CheckResult(
            "${edp.name}: cross-EDP",
            false,
            "${edp.name}: $count rows from other EDPs visible in requisition_overview",
          )
        )
      }
    } catch (e: BigQueryException) {
      results.add(
        CheckResult(
          "${edp.name}: cross-EDP",
          false,
          "${edp.name}: cross-EDP check failed: ${e.message}",
        )
      )
    }

    // Platform table denial
    for (table in listOf("mc_details", "report_detail")) {
      try {
        bq.query(QueryJobConfiguration.of("SELECT * FROM `$project.$dataset.$table` LIMIT 1"))
        results.add(
          CheckResult(
            "${edp.name}: $table",
            false,
            "${edp.name}: should not have access to $table but query succeeded",
          )
        )
      } catch (e: BigQueryException) {
        if (e.code == 403) {
          results.add(
            CheckResult(
              "${edp.name}: $table",
              true,
              "${edp.name}: correctly denied access to $table (403)",
            )
          )
        } else {
          results.add(
            CheckResult(
              "${edp.name}: $table",
              false,
              "${edp.name}: unexpected error accessing $table: ${e.code} ${e.message}",
            )
          )
        }
      }
    }
    return results
  }

  fun checkExternalQueryBypass(bq: BigQuery, edp: EdpConfig): List<CheckResult> {
    val results = mutableListOf<CheckResult>()
    val connections =
      listOf("edp-aggregator-conn", "kingdom-conn", "reporting-conn", "reporting-postgres-conn")
    for (conn in connections) {
      try {
        bq.query(
          QueryJobConfiguration.of(
            "SELECT * FROM EXTERNAL_QUERY('projects/$project/locations/$region/connections/$conn', '''SELECT 1\'\'\')" +
              " LIMIT 1"
          )
        )
        results.add(
          CheckResult(
            "${edp.name}: $conn",
            false,
            "${edp.name}: EXTERNAL_QUERY bypass via $conn should have been denied",
          )
        )
      } catch (e: BigQueryException) {
        if (e.code == 403) {
          results.add(
            CheckResult(
              "${edp.name}: $conn",
              true,
              "${edp.name}: correctly denied EXTERNAL_QUERY via $conn (403)",
            )
          )
        } else {
          results.add(
            CheckResult(
              "${edp.name}: $conn",
              false,
              "${edp.name}: unexpected error for EXTERNAL_QUERY via $conn: ${e.code} ${e.message}",
            )
          )
        }
      }
    }
    return results
  }

  fun checkUdfOutputValidation(bq: BigQuery): List<CheckResult> {
    val results = mutableListOf<CheckResult>()

    // Proto decoding now happens Spanner-side via TO_JSON(). Verify
    // externalIdToApiId still works correctly.
    try {
      val result =
        bq.query(
          QueryJobConfiguration.of("SELECT `$project.$dataset.externalIdToApiId`(1) AS output")
        )
      val output = result.iterateAll().firstOrNull()?.get("output")?.stringValue ?: ""
      if (output.isNotEmpty()) {
        results.add(
          CheckResult("externalIdToApiId", true, "externalIdToApiId: produces valid output")
        )
      } else {
        results.add(
          CheckResult("externalIdToApiId", false, "externalIdToApiId: returned empty output")
        )
      }
    } catch (e: Exception) {
      results.add(
        CheckResult("externalIdToApiId", false, "externalIdToApiId: query failed: ${e.message}")
      )
    }
    return results
  }

  fun checkDriftDetection(bq: BigQuery, edps: List<EdpConfig>): List<CheckResult> {
    val results = mutableListOf<CheckResult>()

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
        results.add(
          CheckResult(
            "Table $table",
            table in actualTables,
            if (table in actualTables) "Table $table exists" else "Table $table is missing",
          )
        )
      }
    } catch (e: BigQueryException) {
      results.add(CheckResult("tables", false, "Cannot query INFORMATION_SCHEMA: ${e.message}"))
    }

    // Check expected UDFs exist
    val expectedUdfs = listOf("externalIdToApiId")
    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            "SELECT routine_name FROM `$project.$dataset.INFORMATION_SCHEMA.ROUTINES` ORDER BY routine_name"
          )
        )
      val actualUdfs = result.iterateAll().map { it["routine_name"].stringValue }.toSet()
      for (udf in expectedUdfs) {
        results.add(
          CheckResult(
            "UDF $udf",
            udf in actualUdfs,
            if (udf in actualUdfs) "UDF $udf exists" else "UDF $udf is missing",
          )
        )
      }
    } catch (e: BigQueryException) {
      results.add(
        CheckResult("UDFs", false, "Cannot query INFORMATION_SCHEMA.ROUTINES: ${e.message}")
      )
    }

    // Check EDP tables don\'t have forbidden columns
    val forbiddenColumns =
      setOf(
        "TotalMcs",
        "CoveragePercent",
        "EdpCount",
        "data_provider_keys",
        "reporting_unit",
        "dimension_spec",
      )
    val forbiddenPatterns =
      listOf(
        Regex("(?i).*edp.?count.*"),
        Regex("(?i).*data.?provider.?key.*"),
        Regex("(?i).*total.?mcs.*"),
        Regex("(?i).*coverage.?percent.*"),
      )
    val edpTables = listOf("requisition_overview", "mc_details_edp", "report_detail_edp")
    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            "SELECT table_name, column_name FROM `$project.$dataset.INFORMATION_SCHEMA.COLUMNS` WHERE table_name IN ('${edpTables.joinToString("\',\'")}')"
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
            violations.add("$tableName: column '$columnName' matches forbidden pattern")
          }
        }
      }
      if (violations.isEmpty()) {
        results.add(CheckResult("forbidden columns", true, "No forbidden columns in EDP tables"))
      } else {
        for (v in violations) {
          results.add(CheckResult("forbidden columns", false, v))
        }
      }
    } catch (e: BigQueryException) {
      results.add(CheckResult("forbidden columns", false, "Cannot check columns: ${e.message}"))
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
            "RefusalMessage",
            "CmmsCreateTime",
            "FulfilledTime",
            "FulfillmentDurationSeconds",
            "ReportState",
            "ReportStartDate",
            "ReportEndDate",
            "ImpressionQualificationFilters",
            "ReportTitle",
            "ResultGroupTitles",
            "ResultGroupMetricFrequencies",
          ),
        "mc_details_edp" to
          setOf(
            "CmmsMeasurementConsumer",
            "CmmsDataProvider",
            "EventGroupCount",
            "ProvidedEventGroupIds",
            "EntityTypes",
            "EntityIds",
            "CampaignNames",
            "BrandNames",
            "EventTemplates",
            "EntityMetadata",
            "MediaTypes",
            "AccountIds",
            "DataAvailabilityStartTime",
            "DataAvailabilityEndTime",
          ),
        "report_detail_edp" to
          setOf(
            "ExternalReportId",
            "CmmsDataProvider",
            "EventGroupCount",
            "CmmsEventGroupIds",
            "CampaignNames",
            "BrandNames",
            "EntityTypes",
            "EntityIds",
          ),
        "mc_details" to
          setOf(
            "CmmsMeasurementConsumer",
            "CmmsDataProvider",
            "EventGroupCount",
            "ProvidedEventGroupIds",
            "CampaignNames",
            "BrandNames",
            "EventTemplates",
            "MediaTypes",
            "AccountIds",
            "DataAvailabilityStartTime",
            "DataAvailabilityEndTime",
            "TotalMcs",
            "CoveragePercent",
          ),
        "report_detail" to
          setOf(
            "ExternalReportId",
            "CmmsDataProvider",
            "EventGroupCount",
            "CmmsEventGroupIds",
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
        actualColumns
          .getOrPut(row["table_name"].stringValue) { mutableSetOf() }
          .add(row["column_name"].stringValue)
      }
      for ((table, expected) in expectedColumns) {
        val actual = actualColumns[table]
        if (actual == null) {
          results.add(CheckResult("Schema $table", false, "Schema check: table $table not found"))
          continue
        }
        val missing = expected - actual
        val extra = actual - expected
        if (missing.isEmpty() && extra.isEmpty()) {
          results.add(
            CheckResult("Schema $table", true, "Schema check: $table columns match expected")
          )
        } else {
          if (missing.isNotEmpty())
            results.add(
              CheckResult("Schema $table", false, "Schema check: $table missing columns: $missing")
            )
          if (extra.isNotEmpty())
            results.add(
              CheckResult(
                "Schema $table",
                false,
                "Schema check: $table has unexpected columns: $extra",
              )
            )
        }
      }
    } catch (e: BigQueryException) {
      results.add(CheckResult("schema", false, "Cannot check schema: ${e.message}"))
    }

    // Verify row access policies are present and correctly scoped on the EDP
    // tables. BigQuery has no INFORMATION_SCHEMA view for policies, so list them
    // via the REST API. Each table must have a platform_full_access (TRUE) policy
    // plus a per-EDP filter referencing that EDP's resource id. (Behavioral
    // isolation checks cover predicate drift too; this is the explicit structural
    // assertion.)
    val edpTableColumns =
      mapOf(
        "requisition_overview" to "DataProviderResourceId",
        "mc_details_edp" to "CmmsDataProvider",
        "report_detail_edp" to "CmmsDataProvider",
      )
    for ((tableName, column) in edpTableColumns) {
      try {
        val predicates =
          listRowAccessPolicyPredicates(bq, tableName)
            .map { it.replace(" ", "").lowercase() }
            .toSet()
        val missing = mutableListOf<String>()
        if ("true" !in predicates) {
          missing.add("platform_full_access (TRUE)")
        }
        for (edp in edps) {
          if ("$column='${edp.resourceId}'".lowercase() !in predicates) {
            missing.add("$column = '${edp.resourceId}'")
          }
        }
        if (missing.isEmpty()) {
          results.add(
            CheckResult(
              "Policies $tableName",
              true,
              "Row access policies correct on $tableName (${predicates.size} policies)",
            )
          )
        } else {
          results.add(
            CheckResult(
              "Policies $tableName",
              false,
              "$tableName missing expected row access policies: $missing",
            )
          )
        }
      } catch (e: Exception) {
        results.add(
          CheckResult(
            "Policies $tableName",
            false,
            "Cannot check row access policies on $tableName: ${e.message}",
          )
        )
      }
    }

    // EDP-isolated tables must grant access only to service accounts — never
    // human users or groups (operators are intentionally excluded from these
    // tables). Catches grantee tampering that the predicate check cannot see.
    for (tableName in listOf("mc_details_edp", "report_detail_edp")) {
      try {
        val grantees = listRowAccessPolicyGrantees(bq, tableName)
        val nonServiceAccounts =
          grantees.filter { it.startsWith("user:") || it.startsWith("group:") }
        if (nonServiceAccounts.isEmpty()) {
          results.add(
            CheckResult(
              "Grantees $tableName",
              true,
              "$tableName granted only to service accounts (${grantees.size} grantees)",
            )
          )
        } else {
          results.add(
            CheckResult(
              "Grantees $tableName",
              false,
              "$tableName has non-service-account grantees: $nonServiceAccounts",
            )
          )
        }
      } catch (e: Exception) {
        results.add(
          CheckResult(
            "Grantees $tableName",
            false,
            "Cannot check grantees on $tableName: ${e.message}",
          )
        )
      }
    }

    // Verify only expected BigQuery connections exist. BigQuery exposes no
    // INFORMATION_SCHEMA view for connections, so list them via the Connection
    // REST API.
    val expectedConnections =
      setOf("edp-aggregator-conn", "kingdom-conn", "reporting-conn", "reporting-postgres-conn")
    try {
      val actualConnections = listConnections(bq)
      val unexpected = actualConnections - expectedConnections
      if (unexpected.isEmpty()) {
        results.add(
          CheckResult(
            "Connection inventory",
            true,
            "Only expected connections exist: $actualConnections",
          )
        )
      } else {
        results.add(
          CheckResult("Connection inventory", false, "Unexpected connections found: $unexpected")
        )
      }
    } catch (e: Exception) {
      results.add(
        CheckResult("Connection inventory", false, "Cannot check connections: ${e.message}")
      )
    }

    return results
  }

  fun checkDataCorrectness(bq: BigQuery, edp: EdpConfig): List<CheckResult> {
    val results = mutableListOf<CheckResult>()

    // Check requisition_overview has rows for this EDP
    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            "SELECT COUNT(*) AS cnt FROM `$project.$dataset.requisition_overview`"
          )
        )
      val count = result.iterateAll().first()["cnt"].longValue
      results.add(
        CheckResult(
          "${edp.name}: requisition_overview rows",
          count > 0,
          if (count > 0) "${edp.name}: requisition_overview has $count rows"
          else "${edp.name}: requisition_overview is empty",
        )
      )
    } catch (e: BigQueryException) {
      results.add(
        CheckResult(
          "${edp.name}: requisition_overview rows",
          false,
          "${edp.name}: requisition_overview row count failed: ${e.message}",
        )
      )
    }

    // Check requisition_overview has populated metrics for fulfilled requisitions
    try {
      val result =
        bq.query(
          QueryJobConfiguration.of(
            "SELECT COUNT(*) AS cnt FROM `$project.$dataset.requisition_overview` " +
              "WHERE RequisitionState = 'FULFILLED' AND FulfillmentDurationSeconds IS NOT NULL"
          )
        )
      val count = result.iterateAll().first()["cnt"].longValue
      results.add(
        CheckResult(
          "${edp.name}: fulfilled metrics",
          count > 0,
          if (count > 0) "${edp.name}: $count fulfilled requisitions with populated metrics"
          else "${edp.name}: no fulfilled requisitions with populated metrics",
        )
      )
    } catch (e: BigQueryException) {
      results.add(
        CheckResult(
          "${edp.name}: fulfilled metrics",
          false,
          "${edp.name}: fulfilled metrics check failed: ${e.message}",
        )
      )
    }

    return results
  }

  /** Sends an authenticated request and parses the JSON body, failing on a non-2xx response. */
  private fun sendForJson(client: HttpClient, request: HttpRequest): JsonObject {
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() !in 200..299) {
      throw IOException("HTTP ${response.statusCode()} from ${request.uri()}: ${response.body()}")
    }
    return JsonParser.parseString(response.body()).asJsonObject
  }

  /** Builds a GET (or empty POST) request to [url] with the client's auth headers applied. */
  private fun authorizedRequest(
    credentials: com.google.auth.Credentials,
    url: String,
    post: Boolean = false,
  ): HttpRequest {
    val uri = URI(url)
    val builder = HttpRequest.newBuilder(uri)
    if (post) {
      builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.noBody())
    } else {
      builder.GET()
    }
    for ((name, values) in credentials.getRequestMetadata(uri)) {
      for (value in values) {
        builder.header(name, value)
      }
    }
    return builder.build()
  }

  /**
   * Lists the IAM grantees across all of a table's row access policies via the REST API (one
   * getIamPolicy call per policy). Used to verify EDP tables grant only service accounts.
   */
  private fun listRowAccessPolicyGrantees(bq: BigQuery, tableName: String): Set<String> {
    val credentials = bq.options.credentials
    val client = HttpClient.newHttpClient()
    val base =
      "https://bigquery.googleapis.com/bigquery/v2/projects/$project/datasets/$dataset/tables/$tableName/rowAccessPolicies"
    val policyIds = mutableListOf<String>()
    var pageToken: String? = null
    do {
      val url = if (pageToken.isNullOrEmpty()) base else "$base?pageToken=$pageToken"
      val json = sendForJson(client, authorizedRequest(credentials, url))
      json.getAsJsonArray("rowAccessPolicies")?.forEach { element ->
        policyIds.add(
          element.asJsonObject.getAsJsonObject("rowAccessPolicyReference").get("policyId").asString
        )
      }
      pageToken = json.get("nextPageToken")?.asString
    } while (!pageToken.isNullOrEmpty())

    val grantees = mutableSetOf<String>()
    for (policyId in policyIds) {
      val json =
        sendForJson(
          client,
          authorizedRequest(credentials, "$base/$policyId:getIamPolicy", post = true),
        )
      json.getAsJsonArray("bindings")?.forEach { binding ->
        binding.asJsonObject.getAsJsonArray("members")?.forEach { grantees.add(it.asString) }
      }
    }
    return grantees
  }

  /**
   * Lists the filter predicates of a table's row access policies via the REST API. BigQuery exposes
   * no INFORMATION_SCHEMA view for policies, so this calls the management API directly (handles
   * pagination).
   */
  private fun listRowAccessPolicyPredicates(bq: BigQuery, tableName: String): Set<String> {
    val credentials = bq.options.credentials
    val client = HttpClient.newHttpClient()
    val base =
      "https://bigquery.googleapis.com/bigquery/v2/projects/$project/datasets/$dataset/tables/$tableName/rowAccessPolicies"
    val predicates = mutableSetOf<String>()
    var pageToken: String? = null
    do {
      val url = if (pageToken.isNullOrEmpty()) base else "$base?pageToken=$pageToken"
      val json = sendForJson(client, authorizedRequest(credentials, url))
      json.getAsJsonArray("rowAccessPolicies")?.forEach { element ->
        predicates.add(element.asJsonObject.get("filterPredicate")?.asString ?: "")
      }
      pageToken = json.get("nextPageToken")?.asString
    } while (!pageToken.isNullOrEmpty())
    return predicates
  }

  /**
   * Lists BigQuery connection IDs in the dataset's region via the Connection REST API. BigQuery
   * exposes no INFORMATION_SCHEMA view for connections, so this calls the management API directly
   * (handles pagination).
   */
  private fun listConnections(bq: BigQuery): Set<String> {
    val credentials = bq.options.credentials
    val client = HttpClient.newHttpClient()
    val base =
      "https://bigqueryconnection.googleapis.com/v1/projects/$project/locations/$region/connections"
    val ids = mutableSetOf<String>()
    var pageToken: String? = null
    do {
      val url = if (pageToken.isNullOrEmpty()) base else "$base?pageToken=$pageToken"
      val json = sendForJson(client, authorizedRequest(credentials, url))
      json.getAsJsonArray("connections")?.forEach { element ->
        ids.add(element.asJsonObject.get("name").asString.substringAfterLast("/"))
      }
      pageToken = json.get("nextPageToken")?.asString
    } while (!pageToken.isNullOrEmpty())
    return ids
  }

  /**
   * Verifies dashboard tables were refreshed recently. Freshness is not EDP-specific (a table's
   * last-modified time is identical for every caller), so this runs once as the platform service
   * account rather than per-EDP. Reads last-modified time from table metadata (tables.get) rather
   * than querying __TABLES__/TABLE_OPTIONS, which require data-level access.
   */
  fun checkFreshness(bq: BigQuery): List<CheckResult> {
    val results = mutableListOf<CheckResult>()
    val stalenessThresholdHours = 3
    for (tableName in listOf("requisition_overview", "mc_details", "mc_details_edp")) {
      try {
        val table = bq.getTable(TableId.of(project, dataset, tableName))
        val lastModified = table?.lastModifiedTime
        if (lastModified == null) {
          results.add(
            CheckResult(
              "$tableName freshness",
              false,
              "$tableName not found or missing last-modified time",
            )
          )
          continue
        }
        val hoursStale = (System.currentTimeMillis() - lastModified) / (1000L * 60L * 60L)
        if (hoursStale <= stalenessThresholdHours) {
          results.add(
            CheckResult("$tableName freshness", true, "$tableName updated $hoursStale hours ago")
          )
        } else {
          results.add(
            CheckResult(
              "$tableName freshness",
              false,
              "$tableName is stale (${hoursStale ?: "unknown"} hours old)",
            )
          )
        }
      } catch (e: BigQueryException) {
        results.add(
          CheckResult(
            "$tableName freshness",
            false,
            "$tableName freshness check failed: ${e.message}",
          )
        )
      }
    }
    return results
  }
}
