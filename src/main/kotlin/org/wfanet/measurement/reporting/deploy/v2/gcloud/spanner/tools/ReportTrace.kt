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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.tools

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.logging.LogEntry
import com.google.cloud.logging.Logging
import com.google.cloud.logging.Logging.EntryListOption
import com.google.cloud.logging.Logging.SortingField
import com.google.cloud.logging.Logging.SortingOrder
import com.google.cloud.logging.LoggingOptions
import com.google.cloud.logging.Payload
import com.google.cloud.sql.core.GcpConnectionFactoryProvider
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.protobuf.util.Timestamps
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import java.io.File
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException
import kotlin.properties.Delegates
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.config.reporting.ReportTraceTopologyConfig
import org.wfanet.measurement.gcloud.spanner.SpannerDatabaseConnector
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.reporting.deploy.v2.common.SpannerFlags
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getBasicReportByExternalId
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import picocli.CommandLine

private const val REPORT_NOT_CREATED = "(not created)"

/** Identifiers that connect one BasicReport to work performed by downstream services. */
internal data class ReportTraceContext(
  val basicReportName: String?,
  val basicReportState: String?,
  val reportName: String,
  val metricNames: List<String>,
  val metricStates: Map<String, String>,
  val reusedMetricNames: Set<String>,
  val unresolvedMetricRequestIds: List<String>,
  val measurementNames: List<String>,
  val reusedMeasurementNames: Set<String>,
  val unresolvedMeasurementRequestIds: List<String>,
  val createTime: Instant?,
) {
  val correlationValues: List<String>
    get() =
      buildList {
          if (basicReportName != null) {
            add(basicReportName)
          }
          if (reportName != REPORT_NOT_CREATED) {
            add(reportName)
          }
          addAll(metricNames)
          addAll(unresolvedMetricRequestIds)
          addAll(measurementNames)
          addAll(unresolvedMeasurementRequestIds)
        }
        .distinct()
}

/** A single Cloud Logging entry in an end-to-end report timeline. */
internal data class ReportTraceLogEntry(
  val sourceProject: String,
  val timestamp: Instant,
  val service: String,
  val severity: String,
  val trace: String?,
  val message: String,
)

/** A Cloud Trace v1 span. The read API exposes span labels, but not OTel events or status. */
internal data class ReportTraceSpan(
  val sourceProject: String,
  val traceId: String,
  val spanId: String,
  val parentSpanId: String?,
  val name: String,
  val service: String,
  val startTime: Instant,
  val endTime: Instant?,
  val attributes: Map<String, String>,
)

internal enum class ReportTraceArtifactStatus {
  COMPLETE,
  PARTIAL,
  FAILED,
}

internal enum class ReportTraceExecutionOutcome {
  SUCCEEDED,
  FAILED,
  REFUSED,
  IN_PROGRESS,
  UNKNOWN,
}

internal data class ReportTraceLifecycleStage(
  val name: String,
  val resource: String,
  val status: String,
  val evidence: String,
)

internal data class ReportTraceSourceStatus(
  val project: String,
  val source: String,
  val status: String,
  val fetched: Int,
  val retained: Int,
  val note: String = "",
)

internal fun interface ReportTraceLogReader {
  fun read(
    correlationValues: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<ReportTraceLogEntry>
}

internal fun interface ReportTraceSpanReader {
  fun read(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<ReportTraceSpan>
}

internal fun interface BasicReportTraceResolver {
  suspend fun resolve(basicReportKey: BasicReportKey): ReportTraceContext
}

/** Resolves the durable resource-name chain stored by the Reporting service. */
internal class DatabaseBasicReportTraceResolver(
  private val spannerClient: org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient,
  private val postgresClient: PostgresDatabaseClient,
) : BasicReportTraceResolver {
  override suspend fun resolve(basicReportKey: BasicReportKey): ReportTraceContext {
    val basicReport =
      spannerClient.readOnlyTransaction().use { transaction ->
        transaction
          .getBasicReportByExternalId(
            basicReportKey.cmmsMeasurementConsumerId,
            basicReportKey.basicReportId,
          )
          .basicReport
      }

    if (basicReport.externalReportId.isEmpty()) {
      return ReportTraceContext(
        basicReportName = basicReportKey.toName(),
        basicReportState = basicReport.state.name,
        reportName = REPORT_NOT_CREATED,
        metricNames = emptyList(),
        metricStates = emptyMap(),
        reusedMetricNames = emptySet(),
        unresolvedMetricRequestIds = emptyList(),
        measurementNames = emptyList(),
        reusedMeasurementNames = emptySet(),
        unresolvedMeasurementRequestIds = emptyList(),
        createTime = Instant.ofEpochMilli(Timestamps.toMillis(basicReport.createTime)),
      )
    }

    val reportName =
      ReportKey(basicReport.cmmsMeasurementConsumerId, basicReport.externalReportId).toName()
    val readContext = postgresClient.readTransaction()
    try {
      val reportResult =
        checkNotNull(
          ReportReader(readContext)
            .readReportByExternalId(
              basicReport.cmmsMeasurementConsumerId,
              basicReport.externalReportId,
            )
        ) {
          "Associated Report $reportName was not found"
        }

      val createMetricRequestIds =
        reportResult.report.reportingMetricEntriesMap.values
          .flatMap { it.metricCalculationSpecReportingMetricsList }
          .flatMap { it.reportingMetricsList }
          .map { it.createMetricRequestId }
          .distinct()
      val metricResults =
        MetricReader(readContext)
          .readMetricsByRequestId(reportResult.measurementConsumerId, createMetricRequestIds)
          .toList()
      val resolvedMetricRequestIds = metricResults.mapNotNull { it.createMetricRequestId }.toSet()
      val unresolvedMetricRequestIds =
        createMetricRequestIds.filterNot(resolvedMetricRequestIds::contains)

      val currentBasicReportName = basicReportKey.toName()
      val metricNames = mutableListOf<String>()
      val metricStates = mutableMapOf<String, String>()
      val reusedMetricNames = mutableSetOf<String>()
      val currentMeasurementNames = mutableSetOf<String>()
      val measurementsOnReusedMetrics = mutableSetOf<String>()
      val unresolvedMeasurementRequestIds = mutableSetOf<String>()
      for (metricResult in metricResults) {
        val metric = metricResult.metric
        val metricName =
          MetricKey(basicReport.cmmsMeasurementConsumerId, metric.externalMetricId).toName()
        metricNames += metricName
        metricStates[metricName] = metric.state.name
        val isReused =
          if (metric.details.basicReport.isNotEmpty()) {
            metric.details.basicReport != currentBasicReportName
          } else {
            metric.details.containingReport.isNotEmpty() &&
              metric.details.containingReport != reportName
          }
        if (isReused) {
          reusedMetricNames += metricName
        }
        val associatedMeasurementNames =
          metric.weightedMeasurementsList.mapNotNull { weightedMeasurement ->
            val measurement = weightedMeasurement.measurement
            if (measurement.cmmsMeasurementId.isEmpty()) {
              unresolvedMeasurementRequestIds += measurement.cmmsCreateMeasurementRequestId
              null
            } else {
              MeasurementKey(basicReport.cmmsMeasurementConsumerId, measurement.cmmsMeasurementId)
                .toName()
            }
          }
        if (isReused) {
          measurementsOnReusedMetrics += associatedMeasurementNames
        } else {
          currentMeasurementNames += associatedMeasurementNames
        }
      }
      val measurementNames =
        (currentMeasurementNames + measurementsOnReusedMetrics).distinct().sorted()
      val reusedMeasurementNames = measurementsOnReusedMetrics - currentMeasurementNames

      return ReportTraceContext(
        basicReportName = currentBasicReportName,
        basicReportState = basicReport.state.name,
        reportName = reportName,
        metricNames = metricNames.distinct().sorted(),
        metricStates = metricStates,
        reusedMetricNames = reusedMetricNames,
        unresolvedMetricRequestIds = unresolvedMetricRequestIds.sorted(),
        measurementNames = measurementNames,
        reusedMeasurementNames = reusedMeasurementNames,
        unresolvedMeasurementRequestIds = unresolvedMeasurementRequestIds.sorted(),
        createTime = Instant.ofEpochMilli(Timestamps.toMillis(basicReport.createTime)),
      )
    } finally {
      readContext.close()
    }
  }
}

/** Reads matching entries from Cloud Logging. */
internal class GoogleCloudReportTraceLogReader(
  private val project: String,
  private val logging: Logging,
  private val includeRawPayloads: Boolean,
) : ReportTraceLogReader {
  override fun read(
    correlationValues: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<ReportTraceLogEntry> {
    require(correlationValues.isNotEmpty()) { "At least one correlation value is required" }
    return ReportTraceOutput.buildLogFilters(correlationValues, startTime, endTime)
      .flatMap { filter ->
        logging
          .listLogEntries(
            EntryListOption.filter(filter),
            EntryListOption.pageSize(readLimit(limit).coerceAtMost(MAX_LOG_PAGE_SIZE)),
            EntryListOption.sortOrder(SortingField.TIMESTAMP, SortingOrder.DESCENDING),
          )
          .iterateAll()
          .take(readLimit(limit))
          .map { it.toReportTraceLogEntry() }
      }
      .distinct()
      .sortedByDescending { it.timestamp }
      .take(readLimit(limit))
  }

  private fun LogEntry.toReportTraceLogEntry(): ReportTraceLogEntry {
    val resourceLabels = resource?.labels.orEmpty()
    val service =
      listOf("service_name", "container_name", "job_name", "function_name").firstNotNullOfOrNull {
        resourceLabels[it]
      } ?: resource?.type ?: logName.substringAfterLast('/')
    return ReportTraceLogEntry(
      sourceProject = project,
      timestamp = instantTimestamp ?: Instant.EPOCH,
      service = service,
      severity = severity.name,
      trace = trace?.takeIf(String::isNotEmpty),
      message = ReportTraceOutput.renderLogPayload(getPayload(), includeRawPayloads),
    )
  }

  companion object {
    private const val MAX_LOG_PAGE_SIZE = 1000

    private fun readLimit(limit: Int): Int = if (limit == Int.MAX_VALUE) limit else limit + 1
  }
}

/** Reads complete spans from the Cloud Trace v1 API using Application Default Credentials. */
internal class GoogleCloudReportTraceSpanReader(
  private val credentials: GoogleCredentials,
  private val httpClient: HttpClient,
) : ReportTraceSpanReader {
  constructor() :
    this(
      GoogleCredentials.getApplicationDefault().createScoped(TRACE_READ_SCOPE),
      HttpClient.newHttpClient(),
    )

  override fun read(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<ReportTraceSpan> {
    credentials.refreshIfExpired()
    val entries = mutableListOf<ReportTraceSpan>()
    for (correlationValue in correlationValues.distinct()) {
      for (traceAttribute in traceAttributesFor(correlationValue)) {
        entries +=
          listTraces(project, "+$traceAttribute:\"$correlationValue\"", startTime, endTime, limit)
      }
    }
    for (traceId in traceIds.map { it.substringAfterLast('/') }.distinct()) {
      readTrace(project, traceId)?.let { entries += it }
    }
    return entries.distinct().sortedBy { it.startTime }.take(readLimit(limit))
  }

  private fun listTraces(
    project: String,
    filter: String,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<ReportTraceSpan> {
    val entries = mutableListOf<ReportTraceSpan>()
    var pageToken: String? = null
    do {
      val queryParameters =
        mutableMapOf(
          "view" to "COMPLETE",
          "pageSize" to readLimit(limit).coerceAtMost(MAX_TRACE_PAGE_SIZE).toString(),
          "startTime" to startTime.toString(),
          "endTime" to endTime.toString(),
          "filter" to filter,
        )
      if (pageToken != null) {
        queryParameters["pageToken"] = pageToken
      }
      val query =
        queryParameters.entries.joinToString("&") { (key, value) -> "$key=${urlEncode(value)}" }
      val request =
        HttpRequest.newBuilder()
          .uri(URI.create("https://cloudtrace.googleapis.com/v1/projects/$project/traces?$query"))
          .header("Authorization", "Bearer ${checkNotNull(credentials.accessToken).tokenValue}")
          .timeout(HTTP_REQUEST_TIMEOUT)
          .GET()
          .build()
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      check(response.statusCode() in 200..299) {
        "Cloud Trace API returned HTTP ${response.statusCode()}"
      }
      val root = JsonParser.parseString(response.body()).asJsonObject
      entries += parseResponse(project, response.body(), fallbackTraceId = null)
      pageToken = root.optionalString("nextPageToken")
    } while (pageToken != null && entries.size < readLimit(limit))
    return entries
  }

  private fun readTrace(project: String, traceId: String): List<ReportTraceSpan>? {
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("https://cloudtrace.googleapis.com/v1/projects/$project/traces/$traceId"))
        .header("Authorization", "Bearer ${checkNotNull(credentials.accessToken).tokenValue}")
        .timeout(HTTP_REQUEST_TIMEOUT)
        .GET()
        .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() == 404) return null
    check(response.statusCode() in 200..299) {
      "Cloud Trace API returned HTTP ${response.statusCode()}"
    }
    return parseResponse(project, response.body(), traceId)
  }

  companion object {
    private const val TRACE_READ_SCOPE = "https://www.googleapis.com/auth/trace.readonly"
    private const val BASIC_REPORT_TRACE_ATTRIBUTE = "xmm.basic_report.name"
    private const val REPORT_TRACE_ATTRIBUTE = "xmm.report.name"
    private const val METRIC_TRACE_ATTRIBUTE = "xmm.metric.name"
    private const val METRIC_REQUEST_ID_TRACE_ATTRIBUTE = "xmm.metric.request_id"
    private const val MEASUREMENT_TRACE_ATTRIBUTE = "xmm.measurement.name"
    private const val MEASUREMENT_REQUEST_ID_TRACE_ATTRIBUTE = "xmm.measurement.request_id"
    private const val REQUISITION_TRACE_ATTRIBUTE = "xmm.requisition.name"
    private const val GROUP_TRACE_ATTRIBUTE = "xmm.edpa.group_id"
    private const val WORK_ITEM_TRACE_ATTRIBUTE = "xmm.work_item.name"
    private const val COMPUTATION_TRACE_ATTRIBUTE = "xmm.computation.name"
    private const val MAX_TRACE_PAGE_SIZE = 1000
    private val HTTP_REQUEST_TIMEOUT: Duration = Duration.ofSeconds(30)

    private fun readLimit(limit: Int): Int = if (limit == Int.MAX_VALUE) limit else limit + 1

    private fun traceAttributesFor(value: String): List<String> {
      return when {
        "/basicReports/" in value -> listOf(BASIC_REPORT_TRACE_ATTRIBUTE)
        "/reports/" in value -> listOf(REPORT_TRACE_ATTRIBUTE)
        "/metrics/" in value -> listOf(METRIC_TRACE_ATTRIBUTE)
        "/measurements/" in value -> listOf(MEASUREMENT_TRACE_ATTRIBUTE)
        "/requisitions/" in value -> listOf(REQUISITION_TRACE_ATTRIBUTE)
        value.startsWith("workItems/") -> listOf(WORK_ITEM_TRACE_ATTRIBUTE)
        value.startsWith("computations/") -> listOf(COMPUTATION_TRACE_ATTRIBUTE)
        else ->
          listOf(
            GROUP_TRACE_ATTRIBUTE,
            METRIC_REQUEST_ID_TRACE_ATTRIBUTE,
            MEASUREMENT_REQUEST_ID_TRACE_ATTRIBUTE,
          )
      }
    }

    private fun urlEncode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)

    /** Parses the documented Cloud Trace v1 Trace/Traces response shape. */
    internal fun parseResponse(
      project: String,
      body: String,
      fallbackTraceId: String?,
    ): List<ReportTraceSpan> {
      val root = JsonParser.parseString(body).asJsonObject
      val traces = root.getAsJsonArray("traces")?.map { it.asJsonObject } ?: listOf(root)
      return traces.flatMap { trace ->
        val traceId =
          trace.optionalString("traceId") ?: fallbackTraceId ?: return@flatMap emptyList()
        trace.getAsJsonArray("spans")?.map { spanElement ->
          val span = spanElement.asJsonObject
          val labels = span.getAsJsonObject("labels")
          val service =
            labels?.optionalString("g.co/agent/name")
              ?: labels?.optionalString("service.name")
              ?: labels?.optionalString("/http/host")
              ?: "unknown-service"
          ReportTraceSpan(
            sourceProject = project,
            traceId = traceId,
            spanId = span.requiredString("spanId"),
            parentSpanId = span.optionalString("parentSpanId"),
            name = span.requiredString("name"),
            service = service,
            startTime = Instant.parse(span.requiredString("startTime")),
            endTime = span.optionalString("endTime")?.let(Instant::parse),
            attributes = labels?.stringValues().orEmpty(),
          )
        } ?: emptyList()
      }
    }
  }
}

private fun JsonObject.stringValues(): Map<String, String> {
  return entrySet()
    .mapNotNull { (key, value) -> if (value.isJsonPrimitive) key to value.asString else null }
    .toMap()
}

private fun JsonObject.requiredString(name: String): String = get(name).asString

private fun JsonObject.optionalString(name: String): String? =
  get(name)?.takeUnless { it.isJsonNull }?.asString

internal object ReportTraceOutput {
  fun renderLogPayload(payload: Payload<*>?, includeRawPayloads: Boolean): String {
    if (payload == null) return ""
    if (includeRawPayloads) return payload.toString()
    if (payload.type == Payload.Type.STRING) {
      val text = (payload as Payload.StringPayload).data
      val safeFields =
        SAFE_TEXT_FIELD_PATTERN.findAll(text)
          .map { match -> "${match.groupValues[1]}=${sanitize(match.groupValues[2])}" }
          .toList()
      return if (safeFields.isEmpty()) {
        "[string payload omitted]"
      } else {
        safeFields.joinToString(" ")
      }
    }
    if (payload.type != Payload.Type.JSON) {
      return "[${payload.type.name.lowercase()} payload omitted]"
    }

    val values = (payload as Payload.JsonPayload).dataAsMap
    val safeValues = mutableMapOf<String, String>()
    for (key in SAFE_LOG_FIELDS) {
      values[key]?.let { value -> safeScalar(value)?.let { safeValues[key] = it } }
    }
    val message = values["message"]?.toString()
    if (message != null) {
      SAFE_TEXT_FIELD_PATTERN.findAll(message).forEach { match ->
        val key = match.groups[1]?.value ?: return@forEach
        val value = match.groups[2]?.value ?: return@forEach
        safeValues[key] = sanitize(value)
      }
    }
    val nestedAttributes = values["attributes"] as? Map<*, *>
    if (nestedAttributes != null) {
      for ((key, value) in nestedAttributes) {
        val keyString = key as? String ?: continue
        if (keyString in SAFE_LOG_FIELDS && value != null) {
          safeScalar(value)?.let { safeValues[keyString] = it }
        }
      }
    }
    return if (safeValues.isEmpty()) {
      "[json payload omitted]"
    } else {
      safeValues.entries.sortedBy { it.key }.joinToString(" ") { "${it.key}=${it.value}" }
    }
  }

  fun buildLogFilters(
    correlationValues: Collection<String>,
    startTime: Instant,
    endTime: Instant,
  ): List<String> {
    val timeFilter = "timestamp>=\"$startTime\" AND timestamp<=\"$endTime\""
    val identifierPredicates =
      correlationValues.distinct().map { value ->
        val escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
        "(textPayload:\"$escaped\" OR jsonPayload.message:\"$escaped\" OR " +
          "jsonPayload.\"xmm.basic_report.name\"=\"$escaped\" OR " +
          "jsonPayload.\"xmm.report.name\"=\"$escaped\" OR " +
          "jsonPayload.\"xmm.metric.name\"=\"$escaped\" OR " +
          "jsonPayload.\"xmm.measurement.name\"=\"$escaped\" OR " +
          "jsonPayload.\"xmm.requisition.name\"=\"$escaped\" OR " +
          "jsonPayload.\"xmm.edpa.group_id\"=\"$escaped\" OR " +
          "jsonPayload.\"xmm.work_item.name\"=\"$escaped\" OR " +
          "jsonPayload.\"xmm.computation.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.basic_report.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.report.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.metric.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.measurement.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.requisition.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.edpa.group_id\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.work_item.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.computation.name\"=\"$escaped\" OR " +
          "jsonPayload.\"edpa.report_id\"=\"$escaped\" OR " +
          "jsonPayload.\"edpa.results_fulfiller.report_id\"=\"$escaped\")"
      }
    val filters = mutableListOf<String>()
    var chunk = mutableListOf<String>()
    for (predicate in identifierPredicates) {
      val candidate = buildLogFilter(timeFilter, chunk + predicate)
      if (candidate.length > MAX_LOG_FILTER_LENGTH && chunk.isNotEmpty()) {
        filters += buildLogFilter(timeFilter, chunk)
        require(buildLogFilter(timeFilter, listOf(predicate)).length <= MAX_LOG_FILTER_LENGTH) {
          "One correlation value exceeds the Cloud Logging filter-size limit"
        }
        chunk = mutableListOf(predicate)
      } else {
        require(candidate.length <= MAX_LOG_FILTER_LENGTH) {
          "One correlation value exceeds the Cloud Logging filter-size limit"
        }
        chunk += predicate
      }
    }
    if (chunk.isNotEmpty()) {
      filters += buildLogFilter(timeFilter, chunk)
    }
    return filters
  }

  private fun buildLogFilter(timeFilter: String, identifierPredicates: List<String>): String {
    return "$timeFilter AND (${identifierPredicates.joinToString(" OR ")})"
  }

  fun render(
    context: ReportTraceContext,
    routeResolution: ReportTraceRouteResolution =
      ReportTraceRouteResolution.unresolved(
        measurementNames = context.measurementNames,
        topology = ReportTraceTopology.notSupplied(),
        status = "NOT_ATTEMPTED",
        note = "Kingdom route resolution was not supplied",
      ),
    spans: List<ReportTraceSpan>,
    logEntries: List<ReportTraceLogEntry>,
    sourceStatuses: List<ReportTraceSourceStatus>,
    warnings: List<String>,
    includeRawPayloads: Boolean,
    lifecycleCoverage: List<ReportTraceLifecycleStage> =
      lifecycleCoverage(context, routeResolution, spans, logEntries),
    artifactStatus: ReportTraceArtifactStatus =
      artifactStatus(spans, logEntries, sourceStatuses, lifecycleCoverage),
    startTime: Instant? = null,
    endTime: Instant? = null,
    generatedAt: Instant? = null,
    executionOutcome: ReportTraceExecutionOutcome =
      executionOutcome(context, routeResolution, spans, logEntries),
  ): String = buildString {
    appendLine("# Report execution trace")
    appendLine()
    appendLine("Collection completeness: $artifactStatus")
    appendLine("Execution outcome: $executionOutcome")
    appendLine()
    appendLine("## Identity and collection window")
    appendLine()
    val basicReportName = context.basicReportName
    if (basicReportName != null) {
      append("BasicReport: ").append(basicReportName)
      val basicReportState = context.basicReportState
      if (basicReportState != null) {
        append(" [").append(basicReportState).append(']')
      }
      appendLine()
    }
    appendLine("Report: ${context.reportName}")
    if (startTime != null) appendLine("Collection start: $startTime")
    if (endTime != null) appendLine("Collection end: $endTime")
    if (generatedAt != null) appendLine("Generated at: $generatedAt")
    appendLine("Payload policy: ${if (includeRawPayloads) "RAW-SENSITIVE" else "REDACTED"}")
    if (includeRawPayloads) {
      appendLine("WARNING: This artifact contains raw log payloads and may contain secrets.")
    }
    appendLine()
    appendLine("## Resolved resource chain")
    appendLine()
    appendLine("- Report: ${context.reportName}")
    if (context.metricNames.isEmpty()) {
      appendLine("- Metrics: none resolved")
    } else {
      context.metricNames.forEach { metricName ->
        append("- Metric: ").append(metricName)
        if (metricName in context.reusedMetricNames) append(" [REUSED]")
        appendLine()
      }
    }
    context.unresolvedMetricRequestIds.forEach { requestId ->
      appendLine("- Metric request: $requestId [UNRESOLVED]")
    }
    if (context.measurementNames.isEmpty()) {
      appendLine("- Measurements: none resolved")
    } else {
      context.measurementNames.forEach { measurementName ->
        append("- Measurement: ").append(measurementName)
        if (measurementName in context.reusedMeasurementNames) append(" [REUSED]")
        appendLine()
      }
    }
    context.unresolvedMeasurementRequestIds.forEach { requestId ->
      appendLine("- Measurement request: $requestId [UNRESOLVED]")
    }
    val discoveredResources =
      mapOf(
        "Requisition" to observedAttributeValues(spans, logEntries, "xmm.requisition.name"),
        "EDPA group" to observedAttributeValues(spans, logEntries, "xmm.edpa.group_id"),
        "WorkItem" to observedAttributeValues(spans, logEntries, "xmm.work_item.name"),
        "Computation" to observedAttributeValues(spans, logEntries, "xmm.computation.name"),
        "Duchy participant" to observedAttributeValues(spans, logEntries, "xmm.duchy.id"),
      )
    for ((label, names) in discoveredResources) {
      if (names.isEmpty()) {
        appendLine("- ${label}s: none observed")
      } else {
        names.forEach { appendLine("- $label: $it") }
      }
    }
    appendLine()
    appendLine("## Resolved execution routes")
    appendLine()
    appendLine("Kingdom resolution: ${routeResolution.status}")
    appendLine("DataProvider topology: ${routeResolution.topology.provenance}")
    if (routeResolution.topology.routes.isNotEmpty()) {
      routeResolution.topology.routes.toSortedMap().forEach { (dataProvider, route) ->
        appendLine("- DataProvider: $dataProvider [$route]")
      }
    }
    appendLine()
    appendLine("| Measurement | State | Selected protocol | Duchy path | Expected Duchies |")
    appendLine("| --- | --- | --- | --- | --- |")
    if (routeResolution.measurementRoutes.isEmpty()) {
      appendLine("| none resolved | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |")
    } else {
      for (route in routeResolution.measurementRoutes) {
        val duchies: String =
          when {
            route.route == ReportTraceMeasurementRouteKind.DIRECT -> "NOT_APPLICABLE"
            route.duchyParticipantsResolved -> route.duchyIds.joinToString()
            else -> "UNKNOWN"
          }
        appendLine(
          "| ${route.name} | ${route.state} | ${route.protocol} | ${route.route} | $duchies |"
        )
      }
    }
    appendLine()
    appendLine("| Requisition | State | DataProvider | Fulfillment route |")
    appendLine("| --- | --- | --- | --- |")
    val requisitionRoutes = routeResolution.measurementRoutes.flatMap { it.requisitions }
    if (requisitionRoutes.isEmpty()) {
      appendLine("| none resolved | UNKNOWN | UNKNOWN | UNKNOWN |")
    } else {
      for (route in requisitionRoutes) {
        appendLine("| ${route.name} | ${route.state} | ${route.dataProvider} | ${route.route} |")
      }
    }
    appendLine()
    if (sourceStatuses.isNotEmpty()) {
      appendLine("## Observability source status")
      appendLine()
      appendLine("| Project | Source | Status | Fetched | Retained | Note |")
      appendLine("| --- | --- | --- | ---: | ---: | --- |")
      for (sourceStatus in sourceStatuses) {
        appendLine(
          "| ${sourceStatus.project} | ${sourceStatus.source} | ${sourceStatus.status} | " +
            "${sourceStatus.fetched} | ${sourceStatus.retained} | " +
            "${sanitize(sourceStatus.note)} |"
        )
      }
      appendLine()
    }
    appendLine("## Lifecycle coverage")
    appendLine()
    appendLine("| Stage | Resource | Status | Evidence |")
    appendLine("| --- | --- | --- | --- |")
    for (stage in lifecycleCoverage) {
      appendLine(
        "| ${stage.name} | ${sanitize(stage.resource)} | ${stage.status} | " +
          "${sanitize(stage.evidence)} |"
      )
    }
    appendLine()
    appendLine("## Chronological timeline")
    appendLine()
    if (spans.isEmpty() && logEntries.isEmpty()) {
      appendLine("No matching trace spans or log entries were found in the selected time range.")
    } else {
      val timeline = buildList {
        for (entry in logEntries) {
          add(
            RenderedTimelineEntry(
              timestamp = entry.timestamp,
              text =
                "LOG ${entry.severity.padEnd(7)} [${entry.sourceProject}/${entry.service}] " +
                  entry.message.replace('\n', ' ') +
                  (entry.trace?.let { " trace=${it.substringAfterLast('/')}" } ?: ""),
            )
          )
        }
        for (span in spans) {
          val duration = span.endTime?.let { Duration.between(span.startTime, it).toMillis() }
          val attributes = formatAttributes(span.attributes)
          add(
            RenderedTimelineEntry(
              timestamp = span.startTime,
              text =
                buildString {
                  append("SPAN [${span.sourceProject}/${span.service}] ${span.name}")
                  append(" trace=${span.traceId} span=${span.spanId}")
                  if (span.parentSpanId != null) append(" parent=${span.parentSpanId}")
                  if (duration != null) append(" duration_ms=$duration")
                  if (attributes.isNotEmpty()) append(" ").append(attributes)
                },
            )
          )
        }
      }
      for (entry in timeline.sortedBy { it.timestamp }) {
        append(entry.timestamp).append("  ").appendLine(entry.text)
      }
    }
    appendLine()
    appendLine("## Errors and warnings")
    appendLine()
    if (warnings.isEmpty()) {
      appendLine("None.")
    } else {
      for (warning in warnings) {
        appendLine("- ${sanitize(warning)}")
      }
    }
    appendLine()
    appendLine("## Collection metadata")
    appendLine()
    appendLine("- Tool: report-trace")
    appendLine("- Trace API: Cloud Trace v1 (span labels only; events and status are unavailable)")
  }

  fun lifecycleCoverage(
    context: ReportTraceContext,
    routeResolution: ReportTraceRouteResolution,
    spans: List<ReportTraceSpan>,
    logEntries: List<ReportTraceLogEntry>,
  ): List<ReportTraceLifecycleStage> {
    val observed = mutableMapOf<String, MutableList<LifecycleEvidence>>()
    for (span in spans) {
      val stage = span.attributes["xmm.lifecycle.stage"] ?: inferStage(span.name) ?: continue
      observed.getOrPut(stage) { mutableListOf() } +=
        LifecycleEvidence(
          description = "span ${span.name}",
          outcome = span.attributes["xmm.outcome"],
          attributes = span.attributes,
          timestamp = span.endTime ?: span.startTime,
        )
    }
    for (entry in logEntries) {
      val fields = safeTextFields(entry.message)
      val stage = fields["xmm.lifecycle.stage"]
      if (stage != null) {
        observed.getOrPut(stage) { mutableListOf() } +=
          LifecycleEvidence(
            description = "log ${entry.service}",
            outcome = fields["xmm.outcome"],
            attributes = fields,
            timestamp = entry.timestamp,
          )
      }
    }
    if (context.basicReportState?.uppercase() == "SUCCEEDED") {
      observed.getOrPut("basic_report_available") { mutableListOf() } +=
        LifecycleEvidence(
          description = "durable BasicReport state SUCCEEDED",
          outcome = "succeeded",
          attributes = mapOf("xmm.basic_report.name" to checkNotNull(context.basicReportName)),
          timestamp = Instant.MAX,
        )
    }
    linkComputationEvidenceToMeasurements(observed)
    linkWorkItemEvidenceToRequisitions(observed)
    val expectedOperations =
      expectedOperations(
        context,
        routeResolution,
        observed,
        reportStates =
          latestResourceStates(spans, logEntries, "xmm.report.name", "xmm.report.state"),
        measurementStates =
          latestResourceStates(spans, logEntries, "xmm.measurement.name", "xmm.measurement.state") +
            routeResolution.measurementRoutes.associate { it.name to it.state.uppercase() },
      )
    val expectedStageNames = expectedOperations.mapTo(mutableSetOf()) { it.stage }
    val coverage =
      expectedOperations.map { operation ->
        val stageEvidence = observed[operation.stage].orEmpty().distinct()
        val matchingEvidence =
          stageEvidence.filter { evidence ->
            operation.identifyingAttributes.all { (attribute, value) ->
              evidence.attributes[attribute] == value
            } &&
              (evidence.outcome?.lowercase() == "failed" ||
                operation.requiredPresenceAttributes.all(evidence.attributes::containsKey))
          }
        lifecycleStage(
          operation = operation,
          matchingEvidence = matchingEvidence,
          hasUnattributedEvidence =
            stageEvidence.any { evidence ->
              val exactAttributesMatch: Boolean =
                operation.identifyingAttributes.all { (attribute, value) ->
                  evidence.attributes[attribute]?.let { it == value } ?: true
                }
              val identityIsIncomplete: Boolean =
                (operation.identifyingAttributes.keys + operation.requiredPresenceAttributes).any {
                  attribute ->
                  attribute !in evidence.attributes
                }
              exactAttributesMatch && identityIsIncomplete
            },
          durableTerminalStatus = durableTerminalStatus(operation, routeResolution),
        )
      }
    val unexpected =
      observed.keys
        .filterNot { it in expectedStageNames }
        .sorted()
        .map { stage ->
          lifecycleStage(
            operation =
              ExpectedLifecycleOperation(
                stage = stage,
                resource = "(unresolved)",
                identifyingAttributes = emptyMap(),
                requiredPresenceAttributes = emptySet(),
                requirement = null,
              ),
            matchingEvidence = observed.getValue(stage).distinct(),
            hasUnattributedEvidence = false,
            durableTerminalStatus = null,
          )
        }
    return coverage + unexpected
  }

  private fun linkComputationEvidenceToMeasurements(
    observed: MutableMap<String, MutableList<LifecycleEvidence>>
  ) {
    val measurementsByComputation: Map<String, List<String>> =
      listOf("duchy_computation", "duchy_stage_attempt")
        .flatMap { stage -> observed[stage].orEmpty() }
        .mapNotNull { evidence ->
          val computationName: String =
            evidence.attributes["xmm.computation.name"] ?: return@mapNotNull null
          val measurementName: String =
            evidence.attributes["xmm.measurement.name"] ?: return@mapNotNull null
          computationName to measurementName
        }
        .groupBy(
          keySelector = { (computationName) -> computationName },
          valueTransform = { (_, measurementName) -> measurementName },
        )
    val acceptanceEvidence: MutableList<LifecycleEvidence> =
      observed["kingdom_computation_result_acceptance"] ?: return
    observed["kingdom_computation_result_acceptance"] =
      acceptanceEvidence
        .map { evidence ->
          if ("xmm.measurement.name" in evidence.attributes) {
            evidence
          } else {
            val computationName: String? = evidence.attributes["xmm.computation.name"]
            val measurementNames: List<String> =
              if (computationName == null) {
                emptyList()
              } else {
                measurementsByComputation[computationName].orEmpty().distinct()
              }
            if (measurementNames.size == 1) {
              evidence.copy(
                description = "${evidence.description} (Measurement correlated by computation)",
                attributes =
                  evidence.attributes + ("xmm.measurement.name" to measurementNames.single()),
              )
            } else {
              evidence
            }
          }
        }
        .toMutableList()
  }

  private fun linkWorkItemEvidenceToRequisitions(
    observed: MutableMap<String, MutableList<LifecycleEvidence>>
  ) {
    val requisitionsByWorkItem: Map<String, List<String>> =
      observed["requisition_dispatch"]
        .orEmpty()
        .mapNotNull { evidence ->
          val workItemName: String =
            evidence.attributes["xmm.work_item.name"] ?: return@mapNotNull null
          val requisitionName: String =
            evidence.attributes["xmm.requisition.name"] ?: return@mapNotNull null
          workItemName to requisitionName
        }
        .groupBy(
          keySelector = { (workItemName) -> workItemName },
          valueTransform = { (_, requisitionName) -> requisitionName },
        )
    val workItemEvidence: MutableList<LifecycleEvidence> =
      observed["work_item_processing"] ?: return
    observed["work_item_processing"] =
      workItemEvidence
        .flatMap { evidence ->
          if ("xmm.requisition.name" in evidence.attributes) {
            listOf(evidence)
          } else {
            val workItemName: String? = evidence.attributes["xmm.work_item.name"]
            val requisitions: List<String> =
              if (workItemName == null) {
                emptyList()
              } else {
                requisitionsByWorkItem[workItemName].orEmpty().distinct()
              }
            if (requisitions.isEmpty()) {
              listOf(evidence)
            } else {
              requisitions.map { requisition ->
                evidence.copy(
                  attributes = evidence.attributes + ("xmm.requisition.name" to requisition)
                )
              }
            }
          }
        }
        .toMutableList()
  }

  private fun lifecycleStage(
    operation: ExpectedLifecycleOperation,
    matchingEvidence: List<LifecycleEvidence>,
    hasUnattributedEvidence: Boolean,
    durableTerminalStatus: String?,
  ): ReportTraceLifecycleStage {
    val evidence = matchingEvidence.map { it.description }
    val latestOutcome = matchingEvidence.maxByOrNull { it.timestamp }?.outcome?.lowercase()
    val durableTerminalEvidenceFound =
      when (durableTerminalStatus) {
        "SUCCEEDED" -> matchingEvidence.any { it.outcome?.lowercase() in TERMINAL_SUCCESS_OUTCOMES }
        "REFUSED" -> matchingEvidence.any { it.outcome?.lowercase() == "refused" }
        else -> false
      }
    val requirement = operation.requirement
    return ReportTraceLifecycleStage(
      name = operation.stage,
      resource = operation.resource,
      status =
        when {
          requirement == ReportTraceStageRequirement.NOT_APPLICABLE && evidence.isNotEmpty() ->
            "UNEXPECTED"
          durableTerminalStatus != null && durableTerminalEvidenceFound -> durableTerminalStatus
          durableTerminalStatus != null -> "UNKNOWN"
          latestOutcome?.let {
            it == "failed" || it.startsWith("failed_") || it == "report_failed"
          } == true -> "FAILED"
          latestOutcome == "refused" -> "REFUSED"
          latestOutcome != null && latestOutcome in TERMINAL_SUCCESS_OUTCOMES -> "SUCCEEDED"
          latestOutcome != null && latestOutcome in IN_PROGRESS_OUTCOMES -> "IN_PROGRESS"
          latestOutcome == "unknown" -> "UNKNOWN"
          evidence.isNotEmpty() -> "OBSERVED"
          hasUnattributedEvidence -> "UNKNOWN"
          requirement == ReportTraceStageRequirement.NOT_APPLICABLE -> "NOT_APPLICABLE"
          requirement == ReportTraceStageRequirement.UNKNOWN -> "UNKNOWN"
          requirement == ReportTraceStageRequirement.REUSED -> "REUSED"
          requirement == ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE ->
            "SKIPPED_AFTER_FAILURE"
          requirement == ReportTraceStageRequirement.SKIPPED_AFTER_REFUSAL ->
            "SKIPPED_AFTER_REFUSAL"
          requirement == ReportTraceStageRequirement.REQUIRED -> "MISSING"
          else -> "OPTIONAL"
        },
      evidence =
        if (durableTerminalStatus != null && !durableTerminalEvidenceFound) {
          buildString {
            append("Durable resource state is $durableTerminalStatus, but no matching accepted ")
            append("telemetry was found")
            if (evidence.isNotEmpty()) {
              append("; observed ").append(evidence.joinToString())
            }
          }
        } else {
          evidence.joinToString().ifEmpty {
            when {
              hasUnattributedEvidence -> "Stage evidence did not identify this resource"
              requirement == ReportTraceStageRequirement.NOT_APPLICABLE ->
                "Not applicable for the Kingdom-resolved route"
              requirement == ReportTraceStageRequirement.UNKNOWN ->
                "Route or resource applicability could not be resolved"
              requirement == ReportTraceStageRequirement.REUSED ->
                "Historical operation belongs to the BasicReport that created this reused resource"
              requirement == ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE ->
                "Not reached after an observed terminal failure"
              requirement == ReportTraceStageRequirement.SKIPPED_AFTER_REFUSAL ->
                "Not reached after an observed Requisition refusal"
              else -> "No matching span label or structured log"
            }
          }
        },
    )
  }

  private fun durableTerminalStatus(
    operation: ExpectedLifecycleOperation,
    routeResolution: ReportTraceRouteResolution,
  ): String? {
    if (operation.requirement != ReportTraceStageRequirement.REQUIRED) {
      return null
    }
    return when (operation.stage) {
      "kingdom_computation_result_acceptance" ->
        routeResolution.measurementRoutes
          .singleOrNull { it.name == operation.identifyingAttributes["xmm.measurement.name"] }
          ?.takeIf { it.state.uppercase() == "SUCCEEDED" }
          ?.let { "SUCCEEDED" }
      "kingdom_requisition_result_acceptance" ->
        routeResolution.measurementRoutes
          .flatMap { it.requisitions }
          .singleOrNull { it.name == operation.identifyingAttributes["xmm.requisition.name"] }
          ?.takeIf { it.state.uppercase() == "FULFILLED" }
          ?.let { "SUCCEEDED" }
      "kingdom_requisition_refusal_acceptance" ->
        routeResolution.measurementRoutes
          .flatMap { it.requisitions }
          .singleOrNull { it.name == operation.identifyingAttributes["xmm.requisition.name"] }
          ?.takeIf { it.state.uppercase() == "REFUSED" }
          ?.let { "REFUSED" }
      else -> null
    }
  }

  fun artifactStatus(
    spans: List<ReportTraceSpan>,
    logEntries: List<ReportTraceLogEntry>,
    sourceStatuses: List<ReportTraceSourceStatus>,
    lifecycleCoverage: List<ReportTraceLifecycleStage>,
  ): ReportTraceArtifactStatus {
    if (spans.isEmpty() && logEntries.isEmpty()) {
      return if (sourceStatuses.isNotEmpty() && sourceStatuses.all { it.status == "FAILED" }) {
        ReportTraceArtifactStatus.FAILED
      } else {
        ReportTraceArtifactStatus.PARTIAL
      }
    }
    return if (
      sourceStatuses.any { it.status in setOf("FAILED", "PARTIAL", "TRUNCATED") } ||
        lifecycleCoverage.any {
          it.status in setOf("MISSING", "IN_PROGRESS", "OBSERVED", "UNKNOWN", "UNEXPECTED")
        }
    ) {
      ReportTraceArtifactStatus.PARTIAL
    } else {
      ReportTraceArtifactStatus.COMPLETE
    }
  }

  fun executionOutcome(
    context: ReportTraceContext,
    routeResolution: ReportTraceRouteResolution,
    spans: List<ReportTraceSpan>,
    logEntries: List<ReportTraceLogEntry>,
  ): ReportTraceExecutionOutcome {
    when (context.basicReportState?.uppercase()) {
      "SUCCEEDED" -> return ReportTraceExecutionOutcome.SUCCEEDED
      "FAILED",
      "INVALID" -> return ReportTraceExecutionOutcome.FAILED
    }

    val reportStates =
      latestResourceStates(spans, logEntries, "xmm.report.name", "xmm.report.state").values
    val metricStates =
      context.metricStates.values +
        latestResourceStates(spans, logEntries, "xmm.metric.name", "xmm.metric.state").values
    val measurementStates =
      latestResourceStates(spans, logEntries, "xmm.measurement.name", "xmm.measurement.state")
        .values + routeResolution.measurementRoutes.map { it.state.uppercase() }
    val requisitionStates =
      latestResourceStates(spans, logEntries, "xmm.requisition.name", "xmm.requisition.state")
        .values +
        routeResolution.measurementRoutes.flatMap { measurement ->
          measurement.requisitions.map { it.state.uppercase() }
        }
    if (
      reportStates.any { it == "FAILED" } ||
        metricStates.any { it in setOf("FAILED", "INVALID") } ||
        measurementStates.any { it in setOf("FAILED", "CANCELLED") }
    ) {
      return ReportTraceExecutionOutcome.FAILED
    }
    if (requisitionStates.any { it == "REFUSED" }) {
      return ReportTraceExecutionOutcome.REFUSED
    }

    val outcomes = buildList {
      spans.mapNotNullTo(this) { it.attributes["xmm.outcome"]?.lowercase() }
      logEntries.mapNotNullTo(this) { safeTextFields(it.message)["xmm.outcome"]?.lowercase() }
    }
    val requisitionStatesAuthoritative =
      routeResolution.measurementRoutes.isNotEmpty() &&
        routeResolution.measurementRoutes.all { it.requisitionsResolved }
    if ("refused" in outcomes && !requisitionStatesAuthoritative) {
      return ReportTraceExecutionOutcome.REFUSED
    }
    if ("report_failed" in outcomes) {
      return ReportTraceExecutionOutcome.FAILED
    }
    if (
      context.basicReportState?.uppercase() in
        setOf("CREATED", "REPORT_CREATED", "UNPROCESSED_RESULTS_READY", "RUNNING")
    ) {
      return ReportTraceExecutionOutcome.IN_PROGRESS
    }
    return when {
      reportStates.isNotEmpty() && reportStates.all { it == "SUCCEEDED" } ->
        ReportTraceExecutionOutcome.SUCCEEDED
      outcomes.any { it in IN_PROGRESS_OUTCOMES } -> ReportTraceExecutionOutcome.IN_PROGRESS
      else -> ReportTraceExecutionOutcome.UNKNOWN
    }
  }

  private fun latestResourceStates(
    spans: Collection<ReportTraceSpan>,
    logEntries: Collection<ReportTraceLogEntry>,
    resourceAttribute: String,
    stateAttribute: String,
  ): Map<String, String> {
    val latestStates = mutableMapOf<String, String>()
    val evidence = buildList {
      spans.mapTo(this) { it.startTime to it.attributes }
      logEntries.mapTo(this) { it.timestamp to safeTextFields(it.message) }
    }
    for ((_, fields) in evidence.sortedBy { it.first }) {
      val resource = fields[resourceAttribute] ?: continue
      val state = fields[stateAttribute] ?: continue
      latestStates[resource] = state.uppercase()
    }
    return latestStates
  }

  fun discoveredCorrelationValues(
    spans: Collection<ReportTraceSpan>,
    logEntries: Collection<ReportTraceLogEntry>,
  ): Set<String> {
    return buildSet {
        for (span in spans) {
          DISCOVERABLE_IDENTIFIER_ATTRIBUTES.mapNotNullTo(this) { attribute ->
            span.attributes[attribute]
          }
        }
        for (entry in logEntries) {
          val fields = safeTextFields(entry.message)
          DISCOVERABLE_IDENTIFIER_ATTRIBUTES.mapNotNullTo(this) { attribute -> fields[attribute] }
        }
      }
      .filter(String::isNotBlank)
      .toSet()
  }

  private fun observedAttributeValues(
    spans: Collection<ReportTraceSpan>,
    logEntries: Collection<ReportTraceLogEntry>,
    attribute: String,
  ): List<String> {
    return buildSet {
        spans.mapNotNullTo(this) { it.attributes[attribute] }
        logEntries.mapNotNullTo(this) { safeTextFields(it.message)[attribute] }
      }
      .filter(String::isNotBlank)
      .sorted()
  }

  private fun expectedOperations(
    context: ReportTraceContext,
    routeResolution: ReportTraceRouteResolution,
    observed: Map<String, List<LifecycleEvidence>>,
    reportStates: Map<String, String>,
    measurementStates: Map<String, String>,
  ): List<ExpectedLifecycleOperation> = buildList {
    fun add(
      stage: String,
      resource: String,
      resourceAttribute: String,
      requirement: ReportTraceStageRequirement? = ReportTraceStageRequirement.REQUIRED,
    ) {
      add(
        ExpectedLifecycleOperation(
          stage,
          resource,
          mapOf(resourceAttribute to resource),
          emptySet(),
          requirement,
        )
      )
    }

    fun addWithPresence(
      stage: String,
      resource: String,
      resourceAttribute: String,
      requirement: ReportTraceStageRequirement?,
      requiredPresenceAttributes: Set<String>,
    ) {
      add(
        ExpectedLifecycleOperation(
          stage,
          resource,
          mapOf(resourceAttribute to resource),
          requiredPresenceAttributes,
          requirement,
        )
      )
    }

    fun add(
      stage: String,
      resource: String,
      identifyingAttributes: Map<String, String>,
      requirement: ReportTraceStageRequirement? = ReportTraceStageRequirement.REQUIRED,
    ) {
      add(
        ExpectedLifecycleOperation(stage, resource, identifyingAttributes, emptySet(), requirement)
      )
    }

    fun addWithPresence(
      stage: String,
      resource: String,
      identifyingAttributes: Map<String, String>,
      requirement: ReportTraceStageRequirement?,
      requiredPresenceAttributes: Set<String>,
    ) {
      add(
        ExpectedLifecycleOperation(
          stage,
          resource,
          identifyingAttributes,
          requiredPresenceAttributes,
          requirement,
        )
      )
    }

    fun hasFailedEvidence(stage: String, resourceAttribute: String, resource: String): Boolean {
      val latestEvidence =
        observed[stage]
          .orEmpty()
          .filter { it.attributes[resourceAttribute] == resource }
          .maxByOrNull { it.timestamp }
      val outcome = latestEvidence?.outcome?.lowercase()
      return outcome == "failed" ||
        outcome?.startsWith("failed_") == true ||
        outcome == "report_failed"
    }

    fun refusalOrigin(requisitionName: String): RequisitionRefusalOrigin {
      val origins =
        observed.values
          .flatten()
          .filter { evidence -> evidence.attributes["xmm.requisition.name"] == requisitionName }
          .mapNotNull { evidence ->
            evidence.attributes[ReportTraceAttributes.REFUSAL_ORIGIN_STRING]
          }
          .mapNotNull { value ->
            when (value) {
              ReportTraceAttributes.REQUISITION_FETCHER_REFUSAL_ORIGIN ->
                RequisitionRefusalOrigin.REQUISITION_FETCHER
              ReportTraceAttributes.RESULTS_FULFILLER_REFUSAL_ORIGIN ->
                RequisitionRefusalOrigin.RESULTS_FULFILLER
              else -> null
            }
          }
          .distinct()
      return origins.singleOrNull() ?: RequisitionRefusalOrigin.UNKNOWN
    }

    val basicReportFailed = context.basicReportState?.uppercase() in setOf("FAILED", "INVALID")
    val failedBeforeReport = basicReportFailed && context.reportName == REPORT_NOT_CREATED
    val reportFailed =
      reportStates[context.reportName] == "FAILED" ||
        hasFailedEvidence("report_result_assembly", "xmm.report.name", context.reportName)
    val executionRefused =
      routeResolution.measurementRoutes.any { measurement ->
        measurement.requisitions.any { it.state.uppercase() == "REFUSED" }
      }
    val noiseCorrectionFailed =
      context.basicReportName?.let { basicReportName ->
        hasFailedEvidence("noise_correction", "xmm.basic_report.name", basicReportName)
      } == true

    context.basicReportName?.let { basicReportName ->
      add("basic_report_creation", basicReportName, "xmm.basic_report.name")
      add("basic_report_api_fetch", basicReportName, "xmm.basic_report.name", requirement = null)
    }
    add(
      "report_creation",
      context.reportName,
      "xmm.report.name",
      if (failedBeforeReport) {
        ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
      } else {
        ReportTraceStageRequirement.REQUIRED
      },
    )

    for (metricName in context.metricNames) {
      add(
        "metric_creation",
        metricName,
        "xmm.metric.name",
        if (metricName in context.reusedMetricNames) {
          ReportTraceStageRequirement.REUSED
        } else {
          ReportTraceStageRequirement.REQUIRED
        },
      )
      add(
        "metric_result_sync",
        metricName,
        "xmm.metric.name",
        if (failedBeforeReport) {
          ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
        } else {
          ReportTraceStageRequirement.REQUIRED
        },
      )
    }
    for (requestId in context.unresolvedMetricRequestIds) {
      val resource = "Metric request $requestId"
      val unresolvedMetricRequirement =
        if (failedBeforeReport) {
          ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
        } else {
          ReportTraceStageRequirement.UNKNOWN
        }
      add(
        "metric_creation",
        resource,
        mapOf("xmm.metric.request_id" to requestId),
        unresolvedMetricRequirement,
      )
      add(
        "metric_result_sync",
        resource,
        mapOf("xmm.metric.request_id" to requestId),
        unresolvedMetricRequirement,
      )
    }
    if (context.metricNames.isEmpty() && context.unresolvedMetricRequestIds.isEmpty()) {
      val unresolvedMetricRequirement =
        if (failedBeforeReport) {
          ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
        } else {
          ReportTraceStageRequirement.UNKNOWN
        }
      add("metric_creation", "(unresolved Metric)", "xmm.metric.name", unresolvedMetricRequirement)
      add(
        "metric_result_sync",
        "(unresolved Metric)",
        "xmm.metric.name",
        unresolvedMetricRequirement,
      )
    }

    for (measurement in routeResolution.measurementRoutes) {
      val measurementReused = measurement.name in context.reusedMeasurementNames
      val measurementFailed = measurementStates[measurement.name] in setOf("FAILED", "CANCELLED")
      val measurementRefused = measurement.requisitions.any { it.state.uppercase() == "REFUSED" }
      val historicalRequirement =
        if (measurementReused) {
          ReportTraceStageRequirement.REUSED
        } else {
          ReportTraceStageRequirement.REQUIRED
        }
      add("measurement_creation", measurement.name, "xmm.measurement.name", historicalRequirement)
      add(
        "kingdom_measurement_sync",
        measurement.name,
        "xmm.measurement.name",
        historicalRequirement,
      )
      when (measurement.route) {
        ReportTraceMeasurementRouteKind.DIRECT -> {
          add(
            "duchy_computation",
            measurement.name,
            "xmm.measurement.name",
            ReportTraceStageRequirement.NOT_APPLICABLE,
          )
          add(
            "duchy_stage_attempt",
            measurement.name,
            "xmm.measurement.name",
            ReportTraceStageRequirement.NOT_APPLICABLE,
          )
        }
        ReportTraceMeasurementRouteKind.MPC -> {
          if (measurement.duchyParticipantsResolved) {
            for (duchyId in measurement.duchyIds) {
              val resource = "${measurement.name} @ duchy $duchyId"
              val attributes =
                mapOf("xmm.measurement.name" to measurement.name, "xmm.duchy.id" to duchyId)
              val duchyRequirement =
                when {
                  measurementReused -> ReportTraceStageRequirement.REUSED
                  measurementRefused -> ReportTraceStageRequirement.SKIPPED_AFTER_REFUSAL
                  else -> ReportTraceStageRequirement.REQUIRED
                }
              add("duchy_computation", resource, attributes, duchyRequirement)
              add("duchy_stage_attempt", resource, attributes, duchyRequirement)
            }
          } else {
            val resource = "${measurement.name} @ unresolved Duchy participants"
            val duchyRequirement =
              when {
                measurementReused -> ReportTraceStageRequirement.REUSED
                measurementRefused -> ReportTraceStageRequirement.SKIPPED_AFTER_REFUSAL
                else -> ReportTraceStageRequirement.UNKNOWN
              }
            addWithPresence(
              "duchy_computation",
              resource,
              mapOf("xmm.measurement.name" to measurement.name),
              duchyRequirement,
              requiredPresenceAttributes = setOf("xmm.duchy.id"),
            )
            addWithPresence(
              "duchy_stage_attempt",
              resource,
              mapOf("xmm.measurement.name" to measurement.name),
              duchyRequirement,
              requiredPresenceAttributes = setOf("xmm.duchy.id"),
            )
          }
        }
        ReportTraceMeasurementRouteKind.UNKNOWN -> {
          add(
            "duchy_computation",
            measurement.name,
            "xmm.measurement.name",
            ReportTraceStageRequirement.UNKNOWN,
          )
          add(
            "duchy_stage_attempt",
            measurement.name,
            "xmm.measurement.name",
            ReportTraceStageRequirement.UNKNOWN,
          )
        }
      }

      val computationAcceptanceRequirement: ReportTraceStageRequirement =
        when (measurement.route) {
          ReportTraceMeasurementRouteKind.DIRECT -> ReportTraceStageRequirement.NOT_APPLICABLE
          ReportTraceMeasurementRouteKind.MPC ->
            when {
              measurementReused -> ReportTraceStageRequirement.REUSED
              measurementRefused -> ReportTraceStageRequirement.SKIPPED_AFTER_REFUSAL
              measurementFailed -> ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
              else -> ReportTraceStageRequirement.REQUIRED
            }
          ReportTraceMeasurementRouteKind.UNKNOWN -> ReportTraceStageRequirement.UNKNOWN
        }
      addWithPresence(
        "kingdom_computation_result_acceptance",
        measurement.name,
        "xmm.measurement.name",
        computationAcceptanceRequirement,
        requiredPresenceAttributes = setOf("xmm.computation.name"),
      )

      if (!measurement.requisitionsResolved) {
        for (stage in REQUISITION_LIFECYCLE_STAGES) {
          add(
            stage,
            "${measurement.name} requisitions",
            "xmm.requisition.name",
            ReportTraceStageRequirement.UNKNOWN,
          )
        }
      } else {
        for (requisition in measurement.requisitions) {
          val requisitionState = requisition.state.uppercase()
          val requisitionRefused = requisitionState == "REFUSED"
          val requisitionFulfilled = requisitionState == "FULFILLED"
          val refusalOrigin =
            if (requisitionRefused) {
              refusalOrigin(requisition.name)
            } else {
              null
            }
          add(
            "requisition_available",
            requisition.name,
            "xmm.requisition.name",
            historicalRequirement,
          )
          val edpaRequirement: ReportTraceStageRequirement =
            when (requisition.route) {
              ReportTraceRequisitionRouteKind.EDPA ->
                when {
                  measurementReused -> ReportTraceStageRequirement.REUSED
                  requisitionRefused &&
                    refusalOrigin == RequisitionRefusalOrigin.REQUISITION_FETCHER ->
                    ReportTraceStageRequirement.SKIPPED_AFTER_REFUSAL
                  requisitionRefused &&
                    refusalOrigin == RequisitionRefusalOrigin.RESULTS_FULFILLER ->
                    ReportTraceStageRequirement.REQUIRED
                  requisitionRefused -> ReportTraceStageRequirement.UNKNOWN
                  measurementFailed && !requisitionFulfilled ->
                    ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
                  else -> ReportTraceStageRequirement.REQUIRED
                }
              ReportTraceRequisitionRouteKind.DIRECT_EDP ->
                ReportTraceStageRequirement.NOT_APPLICABLE
              ReportTraceRequisitionRouteKind.UNKNOWN -> ReportTraceStageRequirement.UNKNOWN
            }
          val refusalRequirement =
            when {
              !requisitionRefused -> ReportTraceStageRequirement.NOT_APPLICABLE
              measurementReused -> ReportTraceStageRequirement.REUSED
              requisition.route == ReportTraceRequisitionRouteKind.DIRECT_EDP ->
                ReportTraceStageRequirement.NOT_APPLICABLE
              requisition.route == ReportTraceRequisitionRouteKind.UNKNOWN ->
                ReportTraceStageRequirement.UNKNOWN
              refusalOrigin == RequisitionRefusalOrigin.UNKNOWN ->
                ReportTraceStageRequirement.UNKNOWN
              else -> ReportTraceStageRequirement.REQUIRED
            }
          add("requisition_refusal", requisition.name, "xmm.requisition.name", refusalRequirement)
          addWithPresence(
            "requisition_dispatch",
            requisition.name,
            "xmm.requisition.name",
            edpaRequirement,
            requiredPresenceAttributes = setOf("xmm.edpa.group_id", "xmm.work_item.name"),
          )
          addWithPresence(
            "work_item_processing",
            requisition.name,
            "xmm.requisition.name",
            edpaRequirement,
            requiredPresenceAttributes = setOf("xmm.work_item.name"),
          )
          addWithPresence(
            "results_fulfillment",
            requisition.name,
            "xmm.requisition.name",
            edpaRequirement,
            requiredPresenceAttributes = setOf("xmm.edpa.group_id"),
          )
          val requisitionAcceptanceRequirement: ReportTraceStageRequirement =
            when (measurement.route) {
              ReportTraceMeasurementRouteKind.DIRECT ->
                when {
                  measurementReused -> ReportTraceStageRequirement.REUSED
                  requisitionRefused -> ReportTraceStageRequirement.SKIPPED_AFTER_REFUSAL
                  measurementFailed && !requisitionFulfilled ->
                    ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
                  else -> ReportTraceStageRequirement.REQUIRED
                }
              ReportTraceMeasurementRouteKind.MPC -> ReportTraceStageRequirement.NOT_APPLICABLE
              ReportTraceMeasurementRouteKind.UNKNOWN -> ReportTraceStageRequirement.UNKNOWN
            }
          add(
            "kingdom_requisition_result_acceptance",
            requisition.name,
            "xmm.requisition.name",
            requisitionAcceptanceRequirement,
          )
          val refusalAcceptanceRequirement =
            when {
              requisitionRefused && measurementReused -> ReportTraceStageRequirement.REUSED
              requisitionRefused -> ReportTraceStageRequirement.REQUIRED
              else -> ReportTraceStageRequirement.NOT_APPLICABLE
            }
          add(
            "kingdom_requisition_refusal_acceptance",
            requisition.name,
            "xmm.requisition.name",
            refusalAcceptanceRequirement,
          )
        }
      }
    }
    for (requestId in context.unresolvedMeasurementRequestIds) {
      val measurementResource = "Measurement request $requestId"
      val requisitionResource = "Requisitions for Measurement request $requestId"
      val unresolvedMeasurementRequirement =
        if (failedBeforeReport) {
          ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
        } else {
          ReportTraceStageRequirement.UNKNOWN
        }
      for (stage in MEASUREMENT_LIFECYCLE_STAGES) {
        add(
          stage,
          measurementResource,
          mapOf("xmm.measurement.request_id" to requestId),
          unresolvedMeasurementRequirement,
        )
      }
      for (stage in REQUISITION_LIFECYCLE_STAGES) {
        add(stage, requisitionResource, "xmm.requisition.name", unresolvedMeasurementRequirement)
      }
    }
    if (
      routeResolution.measurementRoutes.isEmpty() &&
        context.unresolvedMeasurementRequestIds.isEmpty()
    ) {
      val unresolvedMeasurementRequirement =
        if (failedBeforeReport) {
          ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
        } else {
          ReportTraceStageRequirement.UNKNOWN
        }
      for (stage in MEASUREMENT_LIFECYCLE_STAGES) {
        add(
          stage,
          "(unresolved Measurement)",
          "xmm.measurement.name",
          unresolvedMeasurementRequirement,
        )
      }
      for (stage in REQUISITION_LIFECYCLE_STAGES) {
        add(
          stage,
          "(unresolved Requisition)",
          "xmm.requisition.name",
          unresolvedMeasurementRequirement,
        )
      }
    }

    add(
      "report_result_assembly",
      context.reportName,
      "xmm.report.name",
      if (failedBeforeReport) {
        ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
      } else {
        ReportTraceStageRequirement.REQUIRED
      },
    )
    context.basicReportName?.let { basicReportName ->
      val postProcessingRequirement =
        when {
          failedBeforeReport || reportFailed -> ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
          executionRefused -> ReportTraceStageRequirement.SKIPPED_AFTER_REFUSAL
          else -> ReportTraceStageRequirement.REQUIRED
        }
      add("noise_correction", basicReportName, "xmm.basic_report.name", postProcessingRequirement)
      val completionRequirement =
        when {
          context.basicReportState?.uppercase() == "SUCCEEDED" ->
            ReportTraceStageRequirement.REQUIRED
          basicReportFailed || reportFailed || noiseCorrectionFailed ->
            ReportTraceStageRequirement.SKIPPED_AFTER_FAILURE
          executionRefused -> ReportTraceStageRequirement.SKIPPED_AFTER_REFUSAL
          else -> null
        }
      add(
        "processed_result_writeback",
        basicReportName,
        "xmm.basic_report.name",
        completionRequirement,
      )
      add("basic_report_available", basicReportName, "xmm.basic_report.name", completionRequirement)
    }
  }

  private fun inferStage(spanName: String): String? =
    when {
      "results_fulfiller" in spanName || "report_fulfillment" in spanName -> "results_fulfillment"
      "sync_results" in spanName -> "report_result_sync"
      "assemble_results" in spanName -> "report_result_assembly"
      else -> null
    }

  private fun formatAttributes(attributes: Map<String, String>): String {
    return attributes
      .filterKeys { key -> key in SAFE_LOG_FIELDS || key in SAFE_TRACE_ATTRIBUTES }
      .entries
      .sortedBy { it.key }
      .joinToString(" ") { (key, value) -> "$key=${sanitize(value)}" }
  }

  fun sanitize(value: String): String {
    var sanitized = value.replace('\n', ' ').replace('\r', ' ')
    for (pattern in SECRET_PATTERNS) {
      sanitized =
        pattern.replace(sanitized) { match ->
          "${match.groupValues.getOrNull(1).orEmpty()}[REDACTED]"
        }
    }
    return sanitized.take(MAX_RENDERED_VALUE_LENGTH)
  }

  private fun safeScalar(value: Any): String? =
    when (value) {
      is String,
      is Number,
      is Boolean -> sanitize(value.toString())
      else -> null
    }

  private data class RenderedTimelineEntry(val timestamp: Instant, val text: String)

  private data class LifecycleEvidence(
    val description: String,
    val outcome: String?,
    val attributes: Map<String, String>,
    val timestamp: Instant,
  )

  private data class ExpectedLifecycleOperation(
    val stage: String,
    val resource: String,
    val identifyingAttributes: Map<String, String>,
    val requiredPresenceAttributes: Set<String>,
    val requirement: ReportTraceStageRequirement?,
  )

  private enum class RequisitionRefusalOrigin {
    REQUISITION_FETCHER,
    RESULTS_FULFILLER,
    UNKNOWN,
  }

  private const val MAX_LOG_FILTER_LENGTH = 20_000
  private const val MAX_RENDERED_VALUE_LENGTH = 1000
  private val MEASUREMENT_LIFECYCLE_STAGES =
    listOf(
      "measurement_creation",
      "kingdom_measurement_sync",
      "duchy_computation",
      "duchy_stage_attempt",
      "kingdom_computation_result_acceptance",
    )
  private val REQUISITION_LIFECYCLE_STAGES =
    listOf(
      "requisition_available",
      "requisition_refusal",
      "requisition_dispatch",
      "work_item_processing",
      "results_fulfillment",
      "kingdom_requisition_result_acceptance",
      "kingdom_requisition_refusal_acceptance",
    )
  private val SAFE_LOG_FIELDS =
    setOf(
      "event",
      "stage",
      "status",
      "state",
      "error_type",
      "exception.type",
      "xmm.basic_report.name",
      "xmm.report.name",
      "xmm.metric.name",
      "xmm.metric.request_id",
      "xmm.measurement.name",
      "xmm.measurement.request_id",
      "xmm.requisition.name",
      "xmm.edpa.group_id",
      "xmm.work_item.name",
      "xmm.computation.name",
      "xmm.duchy.id",
      "xmm.basic_report.state",
      "xmm.report.state",
      "xmm.metric.state",
      "xmm.measurement.state",
      "xmm.requisition.state",
      "xmm.lifecycle.stage",
      "xmm.outcome",
      "xmm.error.type",
      "xmm.refusal.origin",
      "xmm.error.retryable",
      "xmm.operation.result",
    )
  private val DISCOVERABLE_IDENTIFIER_ATTRIBUTES =
    setOf("xmm.requisition.name", "xmm.edpa.group_id", "xmm.work_item.name", "xmm.computation.name")
  private val TERMINAL_SUCCESS_OUTCOMES =
    setOf(
      "succeeded",
      "accepted",
      "returned",
      "results_available",
      "synchronized",
      "no_update_required",
      "already_completed",
      "stale_delivery",
    )
  private val IN_PROGRESS_OUTCOMES = setOf("started", "in_progress", "pending", "retryable_failure")
  private val SAFE_TRACE_ATTRIBUTES =
    setOf("error", "service.name", "g.co/agent/name", "/http/host")
  private val SECRET_PATTERNS =
    listOf(
      Regex("(?i)(bearer\\s+)[A-Za-z0-9._~+/=-]+"),
      Regex(
        "(?i)((?:authorization|cookie|set-cookie|x-api-key|api[_-]?key|access[_-]?token|" +
          "refresh[_-]?token|client[_-]?secret|private[_-]?key|password|passwd|credential|" +
          "session[_-]?(?:id|token))\\s*[:=]\\s*)[^\\s,;]+"
      ),
      Regex("(?i)(https?://[^\\s?]+\\?)[^\\s]+"),
      Regex("(?<![A-Za-z0-9_-])[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+(?![A-Za-z0-9_-])"),
      Regex("(?is)(-----BEGIN [^-]*PRIVATE KEY-----).*?(-----END [^-]*PRIVATE KEY-----)"),
    )
  private val SAFE_TEXT_FIELD_PATTERN =
    Regex("(?:^|\\s)(${SAFE_LOG_FIELDS.joinToString("|") { Regex.escape(it) }})=([^\\s]+)")

  private fun safeTextFields(text: String): Map<String, String> {
    return SAFE_TEXT_FIELD_PATTERN.findAll(text).associate { match ->
      match.groupValues[1] to match.groupValues[2]
    }
  }
}

@CommandLine.Command(
  name = "report-trace",
  description = ["Prints a chronological Cloud Trace and Logging timeline for a report."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
internal class ReportTrace(
  private val logReaderFactory: (String, Boolean) -> ReportTraceLogReader,
  private val spanReaderFactory: () -> ReportTraceSpanReader,
  private val resolverFactory:
    (SpannerDatabaseConnector, PostgresDatabaseClient) -> BasicReportTraceResolver,
  private val resolverOverride: BasicReportTraceResolver?,
  private val routeResolverOverride: ReportTraceRouteResolver?,
  private val clock: Clock,
) : Runnable {
  @CommandLine.Spec private lateinit var spec: CommandLine.Model.CommandSpec

  private val spanReader: ReportTraceSpanReader by lazy(spanReaderFactory)
  private val logReaders = mutableMapOf<Pair<String, Boolean>, ReportTraceLogReader>()

  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server."],
  )
  private var kingdomPublicApiTarget: String? = null

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname in the Kingdom public API TLS certificate.",
        "Overrides derivation from --kingdom-public-api-target.",
      ],
  )
  private var kingdomPublicApiCertHost: String? = null

  @CommandLine.Option(
    names = ["--kingdom-api-key"],
    description = ["API authentication key for the MeasurementConsumer."],
  )
  private var kingdomApiKey: String? = null

  @CommandLine.Option(
    names = ["--tls-cert-file"],
    description = ["MeasurementConsumer TLS certificate file for Kingdom."],
  )
  private var tlsCertFile: File? = null

  @CommandLine.Option(
    names = ["--tls-key-file"],
    description = ["MeasurementConsumer TLS private key file for Kingdom."],
  )
  private var tlsKeyFile: File? = null

  @CommandLine.Option(
    names = ["--cert-collection-file"],
    description = ["Trusted root certificate collection for Kingdom."],
  )
  private var certCollectionFile: File? = null

  @CommandLine.Option(
    names = ["--topology-config-file"],
    description =
      [
        "Complete DataProvider fulfillment topology as a ReportTraceTopologyConfig textproto.",
        "An encountered DataProvider missing from the config produces a PARTIAL artifact.",
        "Required with --basic-report.",
      ],
  )
  private var topologyConfigFile: File? = null

  @CommandLine.Option(
    names = ["--kingdom-resolution-timeout"],
    defaultValue = "PT30S",
    description = ["Maximum Kingdom route-resolution time per report."],
  )
  private lateinit var kingdomResolutionTimeout: Duration

  @set:CommandLine.Option(
    names = ["--kingdom-max-concurrency"],
    defaultValue = "4",
    description = ["Maximum concurrent Kingdom route-resolution RPCs."],
  )
  private var kingdomMaxConcurrency by Delegates.notNull<Int>()

  @set:CommandLine.Option(
    names = ["--kingdom-max-attempts"],
    defaultValue = "3",
    description = ["Maximum attempts for each retryable Kingdom RPC."],
  )
  private var kingdomMaxAttempts by Delegates.notNull<Int>()

  @CommandLine.Option(
    names = ["--kingdom-retry-delay"],
    defaultValue = "PT0.2S",
    description = ["Initial exponential-backoff delay for retryable Kingdom RPCs."],
  )
  private lateinit var kingdomRetryDelay: Duration

  @CommandLine.Option(
    names = ["--observability-project", "--project"],
    required = true,
    description =
      ["Google Cloud project containing trace and log data. Repeat for multiple projects."],
  )
  private var observabilityProjects: List<String> = emptyList()

  @CommandLine.Option(
    names = ["--include-raw-payloads"],
    description = ["Include raw log payloads. The resulting artifact may contain secrets."],
  )
  private var includeRawPayloads: Boolean = false

  @CommandLine.Option(
    names = ["--allow-partial"],
    description =
      ["Exit successfully for a PARTIAL artifact. FAILED artifacts still return a failure."],
  )
  private var allowPartial: Boolean = false

  @CommandLine.Option(
    names = ["--postgres-database"],
    description = ["Name of the Reporting Postgres database. Required with --basic-report."],
  )
  private var postgresDatabase: String? = null

  @CommandLine.Option(
    names = ["--postgres-cloud-sql-connection-name"],
    description = ["Cloud SQL instance connection name. Required with --basic-report."],
  )
  private var postgresCloudSqlConnectionName: String? = null

  @CommandLine.Option(
    names = ["--postgres-user"],
    description = ["Reporting Postgres user. Required with --basic-report."],
  )
  private var postgresUser: String? = null

  @CommandLine.Option(
    names = ["--statement-timeout"],
    description = ["Postgres statement timeout as an ISO-8601 duration."],
    defaultValue = "PT0S",
  )
  private lateinit var statementTimeout: Duration

  @CommandLine.Option(
    names = ["--basic-report"],
    description =
      ["BasicReport resource name. Repeat for multiple reports; resolves downstream names."],
  )
  private var basicReportNames: List<String> = emptyList()

  @CommandLine.Option(
    names = ["--output-dir"],
    description = ["Directory for one Markdown file per BasicReport."],
  )
  private var outputDirectory: Path? = null

  @CommandLine.Option(
    names = ["--report"],
    description = ["Report resource name for direct mode (does not require reporting databases)."],
  )
  private var reportName: String? = null

  @CommandLine.Option(
    names = ["--start-time"],
    description =
      ["Inclusive RFC 3339 start time. Defaults to five minutes before report creation."],
  )
  private var startTime: String? = null

  @CommandLine.Option(
    names = ["--end-time"],
    description = ["Inclusive RFC 3339 end time. Defaults to the current time."],
  )
  private var endTime: String? = null

  @CommandLine.Option(
    names = ["--limit"],
    defaultValue = "1000",
    description = ["Maximum retained spans and logs. Set to 0 to collect without a limit."],
  )
  private lateinit var limit: String

  override fun run() {
    val exitCode = runBlocking { execute() }
    if (exitCode != 0) {
      throw CommandLine.ExecutionException(
        spec.commandLine(),
        "One or more BasicReport trace artifacts could not be generated",
      )
    }
  }

  private suspend fun execute(): Int {
    val requestedBasicReportNames = basicReportNames.distinct()
    if (requestedBasicReportNames.isEmpty() == (reportName == null)) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "Exactly one mode must be specified: one or more --basic-report values, or --report",
      )
    }
    if (reportName != null && outputDirectory != null) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--output-dir can only be used with --basic-report",
      )
    }
    if (requestedBasicReportNames.size > 1 && outputDirectory == null) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--output-dir is required when --basic-report is specified more than once",
      )
    }
    val configuredLimit = limit.toIntOrNull()
    if (configuredLimit == null || configuredLimit < 0) {
      throw CommandLine.ParameterException(spec.commandLine(), "--limit must be non-negative")
    }
    val entryLimit = if (configuredLimit == 0) Int.MAX_VALUE else configuredLimit

    val parsedEndTime = endTime?.let { parseTime("--end-time", it) } ?: clock.instant()
    val explicitStartTime = startTime?.let { parseTime("--start-time", it) }
    if (explicitStartTime?.isAfter(parsedEndTime) == true) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--start-time must not be after --end-time",
      )
    }

    val directReportName = reportName
    if (directReportName != null) {
      if (ReportKey.fromName(directReportName) == null) {
        throw CommandLine.ParameterException(
          spec.commandLine(),
          "Invalid --report resource name: $directReportName",
        )
      }
      val context =
        ReportTraceContext(
          basicReportName = null,
          basicReportState = null,
          reportName = directReportName,
          metricNames = emptyList(),
          metricStates = emptyMap(),
          reusedMetricNames = emptySet(),
          unresolvedMetricRequestIds = emptyList(),
          measurementNames = emptyList(),
          reusedMeasurementNames = emptySet(),
          unresolvedMeasurementRequestIds = emptyList(),
          createTime = null,
        )
      val routeResolution =
        ReportTraceRouteResolution.unresolved(
          measurementNames = emptyList(),
          topology = ReportTraceTopology.notSupplied(),
          status = "NOT_ATTEMPTED",
          note = "Kingdom route resolution is unavailable in direct --report mode",
        )
      val collection =
        collectTimeline(
          context,
          routeResolution,
          explicitStartTime,
          parsedEndTime,
          entryLimit,
          resolutionFailure = null,
        )
      spec
        .commandLine()
        .out
        .print(
          ReportTraceOutput.render(
            context,
            routeResolution,
            collection.spans,
            collection.logEntries,
            collection.sourceStatuses,
            collection.warnings,
            includeRawPayloads,
            collection.lifecycleCoverage,
            collection.status,
            collection.startTime,
            collection.endTime,
            collection.generatedAt,
          )
        )
      return exitCode(collection.status)
    }

    val normalizedOutputDirectory =
      outputDirectory?.toAbsolutePath()?.normalize()?.also { Files.createDirectories(it) }
    val topology = topologyConfigFile?.let(::loadTopology) ?: ReportTraceTopology.notSupplied()
    val resolver = resolverOverride
    if (resolver != null) {
      val routeResolver =
        routeResolverOverride
          ?: ReportTraceRouteResolver { measurementNames, topology ->
            ReportTraceRouteResolution.unresolved(
              measurementNames = measurementNames,
              topology = topology,
              status = "NOT_ATTEMPTED",
              note = "Kingdom route resolver was not configured",
            )
          }
      return processBasicReports(
        requestedBasicReportNames,
        normalizedOutputDirectory,
        resolver,
        routeResolver,
        topology,
        explicitStartTime,
        parsedEndTime,
        entryLimit,
      )
    }

    validateBasicReportFlags()
    val routeResolver = routeResolverOverride ?: buildKingdomRouteResolver()
    val postgresClient =
      PostgresDatabaseClient.fromConnectionFactory(buildPostgresConnectionFactory())
    return spannerFlags.usingSpanner { spanner ->
      processBasicReports(
        requestedBasicReportNames,
        normalizedOutputDirectory,
        resolverFactory(spanner, postgresClient),
        routeResolver,
        topology,
        explicitStartTime,
        parsedEndTime,
        entryLimit,
      )
    }
  }

  private suspend fun processBasicReports(
    names: List<String>,
    outputDirectory: Path?,
    resolver: BasicReportTraceResolver,
    routeResolver: ReportTraceRouteResolver,
    topology: ReportTraceTopology,
    explicitStartTime: Instant?,
    endTime: Instant,
    entryLimit: Int,
  ): Int {
    var failures = 0
    for ((index, name) in names.withIndex()) {
      val basicReportKey = BasicReportKey.fromName(name)
      if (basicReportKey == null) {
        failures++
        val outputPath =
          writeArtifact(
            outputDirectory,
            "invalid-${index + 1}.md",
            renderFailureArtifact(name, "Invalid BasicReport resource name"),
          )
        printBatchResult(name, "FAILED", outputPath)
        continue
      }

      val fileName = outputFileName(basicReportKey)
      try {
        val resolution: Pair<ReportTraceContext, String?> =
          try {
            resolver.resolve(basicReportKey) to null
          } catch (e: CancellationException) {
            throw e
          } catch (e: Exception) {
            ReportTraceContext(
              basicReportName = basicReportKey.toName(),
              basicReportState = null,
              reportName = REPORT_NOT_CREATED,
              metricNames = emptyList(),
              metricStates = emptyMap(),
              reusedMetricNames = emptySet(),
              unresolvedMetricRequestIds = emptyList(),
              measurementNames = emptyList(),
              reusedMeasurementNames = emptySet(),
              unresolvedMeasurementRequestIds = emptyList(),
              createTime = null,
            ) to failureDescription(e)
          }
        val (context, resolutionFailure) = resolution
        val routeResolution =
          if (resolutionFailure == null) {
            try {
              routeResolver.resolve(context.measurementNames, topology)
            } catch (e: CancellationException) {
              throw e
            } catch (e: Exception) {
              ReportTraceRouteResolution.unresolved(
                measurementNames = context.measurementNames,
                topology = topology,
                status = "FAILED",
                note = "Kingdom route resolution failed: ${failureDescription(e)}",
              )
            }
          } else {
            ReportTraceRouteResolution.unresolved(
              measurementNames = context.measurementNames,
              topology = topology,
              status = "NOT_ATTEMPTED",
              note = "Kingdom route resolution skipped because Reporting resolution failed",
            )
          }
        val collection =
          collectTimeline(
            context,
            routeResolution,
            explicitStartTime,
            endTime,
            entryLimit,
            resolutionFailure,
          )
        val outputPath =
          writeArtifact(
            outputDirectory,
            fileName,
            ReportTraceOutput.render(
              context,
              routeResolution,
              collection.spans,
              collection.logEntries,
              collection.sourceStatuses,
              collection.warnings,
              includeRawPayloads,
              collection.lifecycleCoverage,
              collection.status,
              collection.startTime,
              collection.endTime,
              collection.generatedAt,
            ),
          )
        printBatchResult(name, collection.status.name, outputPath)
        if (exitCode(collection.status) != 0) {
          failures++
        }
      } catch (e: Exception) {
        failures++
        val outputPath =
          writeArtifact(
            outputDirectory,
            fileName,
            renderFailureArtifact(name, failureDescription(e)),
          )
        printBatchResult(name, "FAILED", outputPath)
      }
    }
    return if (failures == 0) 0 else 1
  }

  private fun collectTimeline(
    context: ReportTraceContext,
    routeResolution: ReportTraceRouteResolution,
    explicitStartTime: Instant?,
    endTime: Instant,
    entryLimit: Int,
    resolutionFailure: String?,
  ): TimelineCollection {
    val startTime =
      explicitStartTime
        ?: context.createTime?.minus(DEFAULT_LEAD_TIME)
        ?: endTime.minus(DEFAULT_LOOKBACK)
    if (startTime.isAfter(endTime)) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--start-time must not be after --end-time",
      )
    }
    val warnings = mutableListOf<String>()
    if (resolutionFailure != null) {
      warnings += "Reporting resource resolution failed: $resolutionFailure"
    }
    val unresolvedDescendantNote =
      buildList {
          if (context.unresolvedMetricRequestIds.isNotEmpty()) {
            add(
              "Metric requests were not resolved: " +
                context.unresolvedMetricRequestIds.joinToString()
            )
          }
          if (context.unresolvedMeasurementRequestIds.isNotEmpty()) {
            add(
              "Measurement requests do not have Kingdom Measurement IDs: " +
                context.unresolvedMeasurementRequestIds.joinToString()
            )
          }
        }
        .joinToString("; ")
    if (unresolvedDescendantNote.isNotEmpty()) {
      warnings += "Reporting resource resolution was partial: $unresolvedDescendantNote"
    }
    warnings += routeResolution.warnings
    val spanEntries = mutableListOf<ReportTraceSpan>()
    val logEntries = mutableListOf<ReportTraceLogEntry>()
    val traceFailures = mutableMapOf<String, MutableList<String>>()
    val logFailures = mutableMapOf<String, MutableList<String>>()
    val traceTruncatedProjects = mutableSetOf<String>()
    val logTruncatedProjects = mutableSetOf<String>()
    val traceFetchedCounts = mutableMapOf<String, Int>()
    val logFetchedCounts = mutableMapOf<String, Int>()
    val projects = observabilityProjects.distinct()
    val correlationValues =
      (context.correlationValues + routeResolution.correlationValues).distinct()
    val queriedLogCorrelationValues = correlationValues.toMutableSet()
    for (project in projects) {
      try {
        val projectLogEntries =
          logReaders
            .getOrPut(project to includeRawPayloads) {
              logReaderFactory(project, includeRawPayloads)
            }
            .read(correlationValues, startTime, endTime, entryLimit)
        if (projectLogEntries.size > entryLimit) {
          logTruncatedProjects += project
          warnings +=
            "Cloud Logging results were truncated for project $project at $entryLimit entries"
        }
        logFetchedCounts[project] = projectLogEntries.size
        logEntries += retainLogEntries(projectLogEntries, entryLimit)
      } catch (e: Exception) {
        val failure = failureDescription(e)
        logFailures.getOrPut(project) { mutableListOf() } += failure
        warnings += "Cloud Logging query failed for project $project: $failure"
      }
    }

    val logTraceIds = logEntries.mapNotNull { it.trace?.substringAfterLast('/') }.distinct()
    val primaryCorrelationValues =
      listOfNotNull(
        context.basicReportName ?: context.reportName.takeUnless { it == REPORT_NOT_CREATED }
      )
    val queriedTraceCorrelationValues = primaryCorrelationValues.toMutableSet()
    val queriedTraceIds = logTraceIds.toMutableSet()
    for (project in projects) {
      try {
        val projectSpans =
          spanReader.read(
            project,
            primaryCorrelationValues,
            logTraceIds,
            startTime,
            endTime,
            entryLimit,
          )
        if (projectSpans.size > entryLimit) {
          traceTruncatedProjects += project
          warnings += "Cloud Trace results were truncated for project $project at $entryLimit spans"
        }
        traceFetchedCounts[project] =
          traceFetchedCounts.getOrDefault(project, 0) + projectSpans.size
        spanEntries += retainSpans(projectSpans, entryLimit)
      } catch (e: Exception) {
        val failure = failureDescription(e)
        traceFailures.getOrPut(project) { mutableListOf() } += failure
        warnings += "Cloud Trace query failed for project $project: $failure"
      }
    }

    // A trace located by a searchable label in one project may have unlabelled remote spans in
    // another project. Fetch those complete traces by ID in every configured project.
    val spanTraceIds = spanEntries.map { it.traceId }.distinct()
    val newlyDiscoveredTraceIds = spanTraceIds - logTraceIds.toSet()
    if (newlyDiscoveredTraceIds.isNotEmpty()) {
      queriedTraceIds += newlyDiscoveredTraceIds
      for (project in projects) {
        try {
          val projectSpans =
            spanReader.read(
              project,
              emptyList(),
              newlyDiscoveredTraceIds,
              startTime,
              endTime,
              entryLimit,
            )
          if (projectSpans.size > entryLimit) {
            traceTruncatedProjects += project
            warnings +=
              "Cloud Trace ID results were truncated for project $project at $entryLimit spans"
          }
          traceFetchedCounts[project] =
            traceFetchedCounts.getOrDefault(project, 0) + projectSpans.size
          spanEntries += retainSpans(projectSpans, entryLimit)
        } catch (e: Exception) {
          val failure = failureDescription(e)
          traceFailures.getOrPut(project) { mutableListOf() } += failure
          warnings += "Cloud Trace ID lookup failed for project $project: $failure"
        }
      }
    }

    // Resource-name fallback can be expensive for high-cardinality reports. Use it only when the
    // primary lineage query and cross-project trace-ID expansion did not cover the lifecycle.
    val fallbackCorrelationValues = correlationValues - primaryCorrelationValues.toSet()
    if (
      fallbackCorrelationValues.isNotEmpty() &&
        ReportTraceOutput.lifecycleCoverage(context, routeResolution, spanEntries, logEntries).any {
          it.status == "MISSING"
        }
    ) {
      queriedTraceCorrelationValues += fallbackCorrelationValues
      for (project in projects) {
        try {
          val projectSpans =
            spanReader.read(
              project,
              fallbackCorrelationValues,
              emptyList(),
              startTime,
              endTime,
              entryLimit,
            )
          if (projectSpans.size > entryLimit) {
            traceTruncatedProjects += project
            warnings +=
              "Cloud Trace fallback results were truncated for project $project at $entryLimit spans"
          }
          traceFetchedCounts[project] =
            traceFetchedCounts.getOrDefault(project, 0) + projectSpans.size
          spanEntries += retainSpans(projectSpans, entryLimit)
        } catch (e: Exception) {
          val failure = failureDescription(e)
          traceFailures.getOrPut(project) { mutableListOf() } += failure
          warnings += "Cloud Trace fallback query failed for project $project: $failure"
        }
      }
    }

    // Follow identifiers and trace IDs across process boundaries until no new correlation key is
    // found. A fixed round limit bounds request growth for cyclic or unexpectedly large graphs.
    var expansionRounds = 0
    var expansionTruncated = false
    while (true) {
      val knownCorrelationValues =
        correlationValues + ReportTraceOutput.discoveredCorrelationValues(spanEntries, logEntries)
      val pendingLogCorrelationValues = knownCorrelationValues.toSet() - queriedLogCorrelationValues
      val pendingTraceCorrelationValues =
        knownCorrelationValues.toSet() - queriedTraceCorrelationValues
      val pendingTraceIds =
        (spanEntries.map { it.traceId } +
            logEntries.mapNotNull { it.trace?.substringAfterLast('/') })
          .toSet() - queriedTraceIds
      if (
        pendingLogCorrelationValues.isEmpty() &&
          pendingTraceCorrelationValues.isEmpty() &&
          pendingTraceIds.isEmpty()
      ) {
        break
      }
      if (expansionRounds == MAX_CORRELATION_EXPANSION_ROUNDS) {
        expansionTruncated = true
        warnings += "Correlation expansion stopped after $MAX_CORRELATION_EXPANSION_ROUNDS rounds"
        break
      }
      expansionRounds++
      queriedLogCorrelationValues += pendingLogCorrelationValues
      queriedTraceCorrelationValues += pendingTraceCorrelationValues
      queriedTraceIds += pendingTraceIds

      for (project in projects) {
        if (pendingLogCorrelationValues.isNotEmpty()) {
          try {
            val projectLogEntries =
              logReaders
                .getOrPut(project to includeRawPayloads) {
                  logReaderFactory(project, includeRawPayloads)
                }
                .read(pendingLogCorrelationValues, startTime, endTime, entryLimit)
            if (projectLogEntries.size > entryLimit) {
              logTruncatedProjects += project
              warnings +=
                "Cloud Logging correlation-expansion results were truncated for project " +
                  "$project at $entryLimit entries"
            }
            logFetchedCounts[project] =
              logFetchedCounts.getOrDefault(project, 0) + projectLogEntries.size
            logEntries += retainLogEntries(projectLogEntries, entryLimit)
          } catch (e: Exception) {
            val failure = failureDescription(e)
            logFailures.getOrPut(project) { mutableListOf() } += failure
            warnings +=
              "Cloud Logging correlation-expansion query failed for project $project: $failure"
          }
        }
        if (pendingTraceCorrelationValues.isNotEmpty() || pendingTraceIds.isNotEmpty()) {
          try {
            val projectSpans =
              spanReader.read(
                project,
                pendingTraceCorrelationValues,
                pendingTraceIds,
                startTime,
                endTime,
                entryLimit,
              )
            if (projectSpans.size > entryLimit) {
              traceTruncatedProjects += project
              warnings +=
                "Cloud Trace correlation-expansion results were truncated for project " +
                  "$project at $entryLimit spans"
            }
            traceFetchedCounts[project] =
              traceFetchedCounts.getOrDefault(project, 0) + projectSpans.size
            spanEntries += retainSpans(projectSpans, entryLimit)
          } catch (e: Exception) {
            val failure = failureDescription(e)
            traceFailures.getOrPut(project) { mutableListOf() } += failure
            warnings +=
              "Cloud Trace correlation-expansion query failed for project $project: $failure"
          }
        }
      }
    }

    val distinctSpans = spanEntries.distinct()
    val distinctLogEntries = logEntries.distinct()
    val mergedSpansTruncated = distinctSpans.size > entryLimit
    val mergedLogEntriesTruncated = distinctLogEntries.size > entryLimit
    if (mergedSpansTruncated) {
      warnings += "Merged Cloud Trace results were truncated at $entryLimit spans"
    }
    if (mergedLogEntriesTruncated) {
      warnings += "Merged Cloud Logging results were truncated at $entryLimit entries"
    }
    val retainedSpans = retainSpans(distinctSpans, entryLimit)
    val retainedLogEntries = retainLogEntries(distinctLogEntries, entryLimit)
    val sourceStatuses = buildList {
      if (resolutionFailure != null) {
        add(
          ReportTraceSourceStatus(
            project = "reporting",
            source = "Resource resolution",
            status = "FAILED",
            fetched = 0,
            retained = 0,
            note = resolutionFailure,
          )
        )
      } else if (unresolvedDescendantNote.isNotEmpty()) {
        val resolvedDescendantCount = context.metricNames.size + context.measurementNames.size
        add(
          ReportTraceSourceStatus(
            project = "reporting",
            source = "Resource resolution",
            status = "PARTIAL",
            fetched = resolvedDescendantCount,
            retained = resolvedDescendantCount,
            note = unresolvedDescendantNote,
          )
        )
      }
      add(
        ReportTraceSourceStatus(
          project = "kingdom",
          source = "Route resolution",
          status = routeResolution.status,
          fetched = routeResolution.fetchedResourceCount,
          retained = routeResolution.fetchedResourceCount,
          note = routeResolution.note,
        )
      )
      if (expansionTruncated) {
        add(
          ReportTraceSourceStatus(
            project = "collector",
            source = "Correlation expansion",
            status = "TRUNCATED",
            fetched = expansionRounds,
            retained = expansionRounds,
            note = "Stopped at the configured round limit",
          )
        )
      }
      if (mergedSpansTruncated) {
        add(
          ReportTraceSourceStatus(
            project = "collector",
            source = "Merged Cloud Trace",
            status = "TRUNCATED",
            fetched = distinctSpans.size,
            retained = retainedSpans.size,
            note = "Distinct results across query batches exceeded the configured limit",
          )
        )
      }
      if (mergedLogEntriesTruncated) {
        add(
          ReportTraceSourceStatus(
            project = "collector",
            source = "Merged Cloud Logging",
            status = "TRUNCATED",
            fetched = distinctLogEntries.size,
            retained = retainedLogEntries.size,
            note = "Distinct results across query batches exceeded the configured limit",
          )
        )
      }
      for (project in projects) {
        add(
          buildSourceStatus(
            project = project,
            source = "Cloud Trace",
            fetched = traceFetchedCounts.getOrDefault(project, 0),
            retained = retainedSpans.count { it.sourceProject == project },
            truncated = project in traceTruncatedProjects,
            failures = traceFailures[project].orEmpty(),
          )
        )
        add(
          buildSourceStatus(
            project = project,
            source = "Cloud Logging",
            fetched = logFetchedCounts.getOrDefault(project, 0),
            retained = retainedLogEntries.count { it.sourceProject == project },
            truncated = project in logTruncatedProjects,
            failures = logFailures[project].orEmpty(),
          )
        )
      }
    }
    val lifecycleCoverage =
      ReportTraceOutput.lifecycleCoverage(
        context,
        routeResolution,
        retainedSpans,
        retainedLogEntries,
      )
    val status =
      ReportTraceOutput.artifactStatus(
        retainedSpans,
        retainedLogEntries,
        sourceStatuses,
        lifecycleCoverage,
      )
    return TimelineCollection(
      spans = retainedSpans,
      logEntries = retainedLogEntries,
      sourceStatuses = sourceStatuses,
      warnings = warnings,
      status = status,
      lifecycleCoverage = lifecycleCoverage,
      startTime = startTime,
      endTime = endTime,
      generatedAt = clock.instant(),
    )
  }

  private fun buildSourceStatus(
    project: String,
    source: String,
    fetched: Int,
    retained: Int,
    truncated: Boolean,
    failures: List<String>,
  ): ReportTraceSourceStatus {
    val status =
      when {
        failures.isNotEmpty() && fetched > 0 -> "PARTIAL"
        failures.isNotEmpty() -> "FAILED"
        truncated -> "TRUNCATED"
        fetched == 0 -> "NO_MATCHES"
        else -> "SUCCESS"
      }
    val notes = buildList {
      if (truncated) add("Additional results were omitted")
      addAll(failures)
    }
    return ReportTraceSourceStatus(
      project = project,
      source = source,
      status = status,
      fetched = fetched,
      retained = retained,
      note = notes.joinToString("; "),
    )
  }

  private fun retainSpans(spans: List<ReportTraceSpan>, limit: Int): List<ReportTraceSpan> {
    if (spans.size <= limit) return spans.sortedBy { it.startTime }
    val errors =
      spans.filter {
        it.attributes["xmm.outcome"]?.lowercase() in setOf("failed", "failure", "refused", "error")
      }
    return (errors + spans.sortedByDescending { it.startTime }).distinct().take(limit).sortedBy {
      it.startTime
    }
  }

  private fun retainLogEntries(
    entries: List<ReportTraceLogEntry>,
    limit: Int,
  ): List<ReportTraceLogEntry> {
    if (entries.size <= limit) return entries.sortedBy { it.timestamp }
    val errors =
      entries.filter { it.severity in setOf("WARNING", "ERROR", "CRITICAL", "ALERT", "EMERGENCY") }
    return (errors + entries.sortedByDescending { it.timestamp }).distinct().take(limit).sortedBy {
      it.timestamp
    }
  }

  private fun writeArtifact(outputDirectory: Path?, fileName: String, contents: String): Path? {
    if (outputDirectory == null) {
      spec.commandLine().out.print(contents)
      return null
    }
    val outputPath = outputDirectory.resolve(fileName).normalize()
    check(outputPath.parent == outputDirectory) { "Output path escapes --output-dir" }
    Files.writeString(outputPath, contents, StandardCharsets.UTF_8)
    return outputPath
  }

  private fun printBatchResult(name: String, status: String, outputPath: Path?) {
    if (outputPath != null) {
      spec.commandLine().out.println("$status  $name -> $outputPath")
    }
  }

  private fun exitCode(status: ReportTraceArtifactStatus): Int =
    when (status) {
      ReportTraceArtifactStatus.COMPLETE -> 0
      ReportTraceArtifactStatus.PARTIAL -> if (allowPartial) 0 else 1
      ReportTraceArtifactStatus.FAILED -> 1
    }

  private fun failureDescription(exception: Exception): String {
    return if (includeRawPayloads) {
      ReportTraceOutput.sanitize(exception.message ?: exception::class.java.name)
    } else {
      exception::class.java.simpleName
    }
  }

  private fun outputFileName(key: BasicReportKey): String {
    return "${safeFileNamePart(key.cmmsMeasurementConsumerId)}__" +
      "${safeFileNamePart(key.basicReportId)}.md"
  }

  private fun safeFileNamePart(value: String): String {
    return value
      .map { if (it.isLetterOrDigit() || it == '-' || it == '_') it else '_' }
      .joinToString("")
  }

  private fun renderFailureArtifact(name: String, message: String): String = buildString {
    appendLine("# BasicReport trace")
    appendLine()
    appendLine("Collection completeness: FAILED")
    appendLine("Execution outcome: UNKNOWN")
    appendLine()
    appendLine("BasicReport: $name")
    appendLine()
    appendLine("## Collection failure")
    appendLine()
    appendLine(ReportTraceOutput.sanitize(message))
  }

  private fun validateBasicReportFlags() {
    val missing = buildList {
      if (runCatching { spannerFlags.projectName }.isFailure) add("--spanner-project")
      if (runCatching { spannerFlags.instanceName }.isFailure) add("--spanner-instance")
      if (runCatching { spannerFlags.databaseName }.isFailure) add("--spanner-database")
      if (postgresDatabase == null) add("--postgres-database")
      if (postgresCloudSqlConnectionName == null) add("--postgres-cloud-sql-connection-name")
      if (postgresUser == null) add("--postgres-user")
      if (routeResolverOverride == null) {
        if (kingdomPublicApiTarget == null) add("--kingdom-public-api-target")
        if (kingdomApiKey == null) add("--kingdom-api-key")
        if (tlsCertFile == null) add("--tls-cert-file")
        if (tlsKeyFile == null) add("--tls-key-file")
        if (topologyConfigFile == null) add("--topology-config-file")
      }
    }
    if (missing.isNotEmpty()) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "${missing.joinToString()} required with --basic-report",
      )
    }
    if (kingdomResolutionTimeout.isZero || kingdomResolutionTimeout.isNegative) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--kingdom-resolution-timeout must be positive",
      )
    }
    if (kingdomMaxConcurrency <= 0) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--kingdom-max-concurrency must be positive",
      )
    }
    if (kingdomMaxAttempts !in 1..MAX_KINGDOM_RPC_ATTEMPTS) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--kingdom-max-attempts must be between 1 and $MAX_KINGDOM_RPC_ATTEMPTS",
      )
    }
    if (kingdomRetryDelay.isNegative) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--kingdom-retry-delay must be non-negative",
      )
    }
  }

  private fun loadTopology(file: File): ReportTraceTopology {
    return try {
      val config = parseTextProto(file, ReportTraceTopologyConfig.getDefaultInstance())
      ReportTraceTopology.fromConfig(config)
    } catch (e: Exception) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "Invalid --topology-config-file: ${e.message ?: e::class.java.simpleName}",
        e,
      )
    }
  }

  private fun buildKingdomRouteResolver(): ReportTraceRouteResolver {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = checkNotNull(tlsCertFile),
        privateKeyFile = checkNotNull(tlsKeyFile),
        trustedCertCollectionFile = certCollectionFile,
      )
    val channel =
      buildMutualTlsChannel(
          target = checkNotNull(kingdomPublicApiTarget),
          clientCerts = clientCerts,
          hostName = kingdomPublicApiCertHost,
        )
        .withShutdownTimeout(CHANNEL_SHUTDOWN_TIMEOUT)
    val apiKey = checkNotNull(kingdomApiKey)
    return KingdomReportTraceResolver(
      client =
        GrpcKingdomReportTraceClient(
          MeasurementsCoroutineStub(channel).withAuthenticationKey(apiKey),
          RequisitionsCoroutineStub(channel).withAuthenticationKey(apiKey),
        ),
      perReportDeadline = kingdomResolutionTimeout,
      maxConcurrency = kingdomMaxConcurrency,
      maxRpcAttempts = kingdomMaxAttempts,
      initialRetryDelay = kingdomRetryDelay,
    )
  }

  private fun buildPostgresConnectionFactory(): ConnectionFactory {
    return ConnectionFactories.get(
      ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "gcp")
        .option(ConnectionFactoryOptions.PROTOCOL, "postgresql")
        .option(ConnectionFactoryOptions.USER, checkNotNull(postgresUser))
        .option(ConnectionFactoryOptions.PASSWORD, "UNUSED")
        .option(ConnectionFactoryOptions.DATABASE, checkNotNull(postgresDatabase))
        .option(ConnectionFactoryOptions.HOST, checkNotNull(postgresCloudSqlConnectionName))
        .option(GcpConnectionFactoryProvider.ENABLE_IAM_AUTH, true)
        .option(ConnectionFactoryOptions.STATEMENT_TIMEOUT, statementTimeout)
        .build()
    )
  }

  private fun parseTime(option: String, value: String): Instant {
    return try {
      Instant.parse(value)
    } catch (e: DateTimeParseException) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "Invalid $option '$value': expected an RFC 3339 time",
      )
    }
  }

  companion object {
    private val DEFAULT_LEAD_TIME: Duration = Duration.ofMinutes(5)
    private val DEFAULT_LOOKBACK: Duration = Duration.ofDays(7)
    private val CHANNEL_SHUTDOWN_TIMEOUT: Duration = Duration.ofSeconds(5)
    private const val MAX_CORRELATION_EXPANSION_ROUNDS = 4
    private const val MAX_KINGDOM_RPC_ATTEMPTS = 10
  }
}

internal class ReportTraceDependencies(
  val logReaderFactory: (String, Boolean) -> ReportTraceLogReader,
  val spanReaderFactory: () -> ReportTraceSpanReader,
  val resolverFactory:
    (SpannerDatabaseConnector, PostgresDatabaseClient) -> BasicReportTraceResolver,
  val resolverOverride: BasicReportTraceResolver?,
  val clock: Clock,
  val output: java.io.PrintWriter,
  val error: java.io.PrintWriter,
  val routeResolverOverride: ReportTraceRouteResolver? = null,
)

internal fun main(args: Array<String>, dependencies: ReportTraceDependencies): Int {
  return CommandLine(
      ReportTrace(
        dependencies.logReaderFactory,
        dependencies.spanReaderFactory,
        dependencies.resolverFactory,
        dependencies.resolverOverride,
        dependencies.routeResolverOverride,
        dependencies.clock,
      )
    )
    .setOut(dependencies.output)
    .setErr(dependencies.error)
    .execute(*args)
}

fun main(args: Array<String>) =
  commandLineMain(
    ReportTrace(
      logReaderFactory = { project, includeRawPayloads ->
        GoogleCloudReportTraceLogReader(
          project,
          LoggingOptions.newBuilder().setProjectId(project).build().service,
          includeRawPayloads,
        )
      },
      spanReaderFactory = { GoogleCloudReportTraceSpanReader() },
      resolverFactory = { spanner, postgres ->
        DatabaseBasicReportTraceResolver(spanner.databaseClient, postgres)
      },
      resolverOverride = null,
      routeResolverOverride = null,
      clock = Clock.systemUTC(),
    ),
    args,
  )

private data class TimelineCollection(
  val spans: List<ReportTraceSpan>,
  val logEntries: List<ReportTraceLogEntry>,
  val sourceStatuses: List<ReportTraceSourceStatus>,
  val warnings: List<String>,
  val status: ReportTraceArtifactStatus,
  val lifecycleCoverage: List<ReportTraceLifecycleStage>,
  val startTime: Instant,
  val endTime: Instant,
  val generatedAt: Instant,
)
