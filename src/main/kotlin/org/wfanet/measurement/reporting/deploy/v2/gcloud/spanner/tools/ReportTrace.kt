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
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient
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
  val measurementNames: List<String>,
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
          addAll(measurementNames)
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
        measurementNames = emptyList(),
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

      val metricNames =
        metricResults
          .map {
            MetricKey(basicReport.cmmsMeasurementConsumerId, it.metric.externalMetricId).toName()
          }
          .distinct()
          .sorted()
      val measurementNames =
        metricResults
          .flatMap { it.metric.weightedMeasurementsList }
          .mapNotNull { it.measurement.cmmsMeasurementId.takeIf(String::isNotEmpty) }
          .map { MeasurementKey(basicReport.cmmsMeasurementConsumerId, it).toName() }
          .distinct()
          .sorted()

      return ReportTraceContext(
        basicReportName = basicReportKey.toName(),
        basicReportState = basicReport.state.name,
        reportName = reportName,
        metricNames = metricNames,
        measurementNames = measurementNames,
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
      val traceAttribute = traceAttributeFor(correlationValue) ?: continue
      entries +=
        listTraces(project, "+$traceAttribute:\"$correlationValue\"", startTime, endTime, limit)
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
      entries += parseCloudTraceV1Response(project, response.body())
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
    return parseCloudTraceV1Response(project, response.body(), traceId)
  }

  companion object {
    private const val TRACE_READ_SCOPE = "https://www.googleapis.com/auth/trace.readonly"
    private const val BASIC_REPORT_TRACE_ATTRIBUTE = "xmm.basic_report.name"
    private const val REPORT_TRACE_ATTRIBUTE = "xmm.report.name"
    private const val METRIC_TRACE_ATTRIBUTE = "xmm.metric.name"
    private const val MEASUREMENT_TRACE_ATTRIBUTE = "xmm.measurement.name"
    private const val MAX_TRACE_PAGE_SIZE = 1000
    private val HTTP_REQUEST_TIMEOUT: Duration = Duration.ofSeconds(30)

    private fun readLimit(limit: Int): Int = if (limit == Int.MAX_VALUE) limit else limit + 1

    private fun traceAttributeFor(value: String): String? {
      return when {
        "/basicReports/" in value -> BASIC_REPORT_TRACE_ATTRIBUTE
        "/reports/" in value -> REPORT_TRACE_ATTRIBUTE
        "/metrics/" in value -> METRIC_TRACE_ATTRIBUTE
        "/measurements/" in value -> MEASUREMENT_TRACE_ATTRIBUTE
        else -> null
      }
    }

    private fun urlEncode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)
  }
}

/** Parses the documented Cloud Trace v1 Trace/Traces response shape. */
internal fun parseCloudTraceV1Response(
  project: String,
  body: String,
  fallbackTraceId: String? = null,
): List<ReportTraceSpan> {
  val root = JsonParser.parseString(body).asJsonObject
  val traces = root.getAsJsonArray("traces")?.map { it.asJsonObject } ?: listOf(root)
  return traces.flatMap { trace ->
    val traceId = trace.optionalString("traceId") ?: fallbackTraceId ?: return@flatMap emptyList()
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
    spans: List<ReportTraceSpan>,
    logEntries: List<ReportTraceLogEntry>,
    sourceStatuses: List<ReportTraceSourceStatus>,
    warnings: List<String>,
    includeRawPayloads: Boolean,
    lifecycleCoverage: List<ReportTraceLifecycleStage> =
      lifecycleCoverage(context, spans, logEntries),
    artifactStatus: ReportTraceArtifactStatus =
      artifactStatus(spans, logEntries, sourceStatuses, lifecycleCoverage),
    startTime: Instant? = null,
    endTime: Instant? = null,
    generatedAt: Instant? = null,
    executionOutcome: ReportTraceExecutionOutcome = executionOutcome(context, spans, logEntries),
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
      context.metricNames.forEach { appendLine("- Metric: $it") }
    }
    if (context.measurementNames.isEmpty()) {
      appendLine("- Measurements: none resolved")
    } else {
      context.measurementNames.forEach { appendLine("- Measurement: $it") }
    }
    val discoveredResources =
      mapOf(
        "Requisition" to observedAttributeValues(spans, logEntries, "xmm.requisition.name"),
        "EDPA group" to observedAttributeValues(spans, logEntries, "xmm.edpa.group_id"),
        "WorkItem" to observedAttributeValues(spans, logEntries, "xmm.work_item.name"),
        "Computation" to observedAttributeValues(spans, logEntries, "xmm.computation.name"),
      )
    for ((label, names) in discoveredResources) {
      if (names.isEmpty()) {
        appendLine("- ${label}s: none observed")
      } else {
        names.forEach { appendLine("- $label: $it") }
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
    appendLine("| Stage | Status | Evidence |")
    appendLine("| --- | --- | --- |")
    for (stage in lifecycleCoverage) {
      appendLine("| ${stage.name} | ${stage.status} | ${sanitize(stage.evidence)} |")
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
    spans: List<ReportTraceSpan>,
    logEntries: List<ReportTraceLogEntry>,
  ): List<ReportTraceLifecycleStage> {
    val observed = mutableMapOf<String, MutableList<Pair<String, String?>>>()
    for (span in spans) {
      val stage = span.attributes["xmm.lifecycle.stage"] ?: inferStage(span.name) ?: continue
      observed.getOrPut(stage) { mutableListOf() } +=
        "span ${span.name}" to span.attributes["xmm.outcome"]
    }
    for (entry in logEntries) {
      val fields = safeTextFields(entry.message)
      val stage = fields["xmm.lifecycle.stage"]
      if (stage != null) {
        observed.getOrPut(stage) { mutableListOf() } +=
          "log ${entry.service}" to fields["xmm.outcome"]
      }
    }
    val expectedStages = expectedStages(context)
    return (expectedStages + observed.keys.filterNot { it in expectedStages }.sorted()).map { stage
      ->
      val observedEvidence = observed[stage].orEmpty().distinct()
      val evidence = observedEvidence.map { it.first }
      val outcomes = observedEvidence.mapNotNull { it.second?.lowercase() }.toSet()
      ReportTraceLifecycleStage(
        name = stage,
        status =
          when {
            outcomes.any { it == "failed" || it.startsWith("failed_") || it == "report_failed" } ->
              "FAILED"
            "refused" in outcomes -> "REFUSED"
            outcomes.any { it in TERMINAL_SUCCESS_OUTCOMES } -> "SUCCEEDED"
            outcomes.any { it in IN_PROGRESS_OUTCOMES } -> "IN_PROGRESS"
            evidence.isNotEmpty() -> "OBSERVED"
            stage in expectedStages -> "MISSING"
            else -> "OPTIONAL"
          },
        evidence = evidence.joinToString().ifEmpty { "No matching span label or structured log" },
      )
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
        lifecycleCoverage.any { it.status in setOf("MISSING", "IN_PROGRESS") }
    ) {
      ReportTraceArtifactStatus.PARTIAL
    } else {
      ReportTraceArtifactStatus.COMPLETE
    }
  }

  fun executionOutcome(
    context: ReportTraceContext,
    spans: List<ReportTraceSpan>,
    logEntries: List<ReportTraceLogEntry>,
  ): ReportTraceExecutionOutcome {
    when (context.basicReportState?.uppercase()) {
      "SUCCEEDED" -> return ReportTraceExecutionOutcome.SUCCEEDED
      "FAILED",
      "INVALID" -> return ReportTraceExecutionOutcome.FAILED
      "CREATED",
      "REPORT_CREATED",
      "UNPROCESSED_RESULTS_READY",
      "RUNNING" -> return ReportTraceExecutionOutcome.IN_PROGRESS
    }

    val outcomes = buildList {
      spans.mapNotNullTo(this) { it.attributes["xmm.outcome"]?.lowercase() }
      logEntries.mapNotNullTo(this) { safeTextFields(it.message)["xmm.outcome"]?.lowercase() }
    }
    return when {
      outcomes.any { it == "failed" || it.startsWith("failed_") || it == "report_failed" } ->
        ReportTraceExecutionOutcome.FAILED
      "refused" in outcomes -> ReportTraceExecutionOutcome.REFUSED
      outcomes.any { it in IN_PROGRESS_OUTCOMES } -> ReportTraceExecutionOutcome.IN_PROGRESS
      outcomes.any { it in TERMINAL_SUCCESS_OUTCOMES } -> ReportTraceExecutionOutcome.SUCCEEDED
      else -> ReportTraceExecutionOutcome.UNKNOWN
    }
  }

  fun discoveredCorrelationValues(spans: Collection<ReportTraceSpan>): Set<String> {
    return spans
      .flatMap { span ->
        DISCOVERABLE_IDENTIFIER_ATTRIBUTES.mapNotNull { attribute -> span.attributes[attribute] }
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

  private fun expectedStages(context: ReportTraceContext): List<String> = buildList {
    if (context.basicReportName != null) add("basic_report_creation")
    add("report_creation")
    add("measurement_creation")
    add("requisition_creation")
    add("requisition_dispatch")
    add("results_fulfillment")
    add("kingdom_result_acceptance")
    add("metric_result_sync")
    add("report_result_assembly")
    if (context.basicReportName != null) {
      add("noise_correction")
    }
  }

  private fun inferStage(spanName: String): String? =
    when {
      "requisition_fetcher" in spanName -> "requisition_dispatch"
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

  private const val MAX_LOG_FILTER_LENGTH = 20_000
  private const val MAX_RENDERED_VALUE_LENGTH = 1000
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
      "xmm.measurement.name",
      "xmm.requisition.name",
      "xmm.edpa.group_id",
      "xmm.work_item.name",
      "xmm.computation.name",
      "xmm.basic_report.state",
      "xmm.report.state",
      "xmm.metric.state",
      "xmm.measurement.state",
      "xmm.requisition.state",
      "xmm.lifecycle.stage",
      "xmm.outcome",
      "xmm.error.type",
    )
  private val DISCOVERABLE_IDENTIFIER_ATTRIBUTES =
    setOf("xmm.requisition.name", "xmm.edpa.group_id", "xmm.work_item.name", "xmm.computation.name")
  private val TERMINAL_SUCCESS_OUTCOMES = setOf("succeeded", "accepted", "returned")
  private val IN_PROGRESS_OUTCOMES = setOf("started", "in_progress", "pending")
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
  private val clock: Clock,
) : Runnable {
  @CommandLine.Spec private lateinit var spec: CommandLine.Model.CommandSpec

  private val spanReader: ReportTraceSpanReader by lazy(spanReaderFactory)
  private val logReaders = mutableMapOf<Pair<String, Boolean>, ReportTraceLogReader>()

  @CommandLine.Mixin private lateinit var spannerFlags: SpannerFlags

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
          measurementNames = emptyList(),
          createTime = null,
        )
      val collection = collectTimeline(context, explicitStartTime, parsedEndTime, entryLimit)
      spec
        .commandLine()
        .out
        .print(
          ReportTraceOutput.render(
            context,
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
    val resolver = resolverOverride
    if (resolver != null) {
      return processBasicReports(
        requestedBasicReportNames,
        normalizedOutputDirectory,
        resolver,
        explicitStartTime,
        parsedEndTime,
        entryLimit,
      )
    }

    validateDatabaseFlags()
    val postgresClient =
      PostgresDatabaseClient.fromConnectionFactory(buildPostgresConnectionFactory())
    return spannerFlags.usingSpanner { spanner ->
      processBasicReports(
        requestedBasicReportNames,
        normalizedOutputDirectory,
        resolverFactory(spanner, postgresClient),
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
        val context = resolver.resolve(basicReportKey)
        val collection = collectTimeline(context, explicitStartTime, endTime, entryLimit)
        val outputPath =
          writeArtifact(
            outputDirectory,
            fileName,
            ReportTraceOutput.render(
              context,
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
    explicitStartTime: Instant?,
    endTime: Instant,
    entryLimit: Int,
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
    val spanEntries = mutableListOf<ReportTraceSpan>()
    val logEntries = mutableListOf<ReportTraceLogEntry>()
    val traceFailures = mutableMapOf<String, MutableList<String>>()
    val logFailures = mutableMapOf<String, MutableList<String>>()
    val traceTruncatedProjects = mutableSetOf<String>()
    val logTruncatedProjects = mutableSetOf<String>()
    val traceFetchedCounts = mutableMapOf<String, Int>()
    val logFetchedCounts = mutableMapOf<String, Int>()
    val projects = observabilityProjects.distinct()
    for (project in projects) {
      try {
        val projectLogEntries =
          logReaders
            .getOrPut(project to includeRawPayloads) {
              logReaderFactory(project, includeRawPayloads)
            }
            .read(context.correlationValues, startTime, endTime, entryLimit)
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
    val fallbackCorrelationValues = context.correlationValues - primaryCorrelationValues.toSet()
    if (
      fallbackCorrelationValues.isNotEmpty() &&
        ReportTraceOutput.lifecycleCoverage(context, spanEntries, logEntries).any {
          it.status == "MISSING"
        }
    ) {
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

    // Some downstream services can be searched only by an identifier assigned after the Reporting
    // resource chain crosses a process boundary. Pivot back into Logging with those span labels so
    // that v1-invisible exception details are included in the artifact.
    val discoveredCorrelationValues =
      ReportTraceOutput.discoveredCorrelationValues(spanEntries) - context.correlationValues.toSet()
    if (discoveredCorrelationValues.isNotEmpty()) {
      for (project in projects) {
        try {
          val projectLogEntries =
            logReaders
              .getOrPut(project to includeRawPayloads) {
                logReaderFactory(project, includeRawPayloads)
              }
              .read(discoveredCorrelationValues, startTime, endTime, entryLimit)
          if (projectLogEntries.size > entryLimit) {
            logTruncatedProjects += project
            warnings +=
              "Cloud Logging identifier-pivot results were truncated for project $project at " +
                "$entryLimit entries"
          }
          logFetchedCounts[project] =
            logFetchedCounts.getOrDefault(project, 0) + projectLogEntries.size
          logEntries += retainLogEntries(projectLogEntries, entryLimit)
        } catch (e: Exception) {
          val failure = failureDescription(e)
          logFailures.getOrPut(project) { mutableListOf() } += failure
          warnings += "Cloud Logging identifier-pivot query failed for project $project: $failure"
        }
      }
    }

    val distinctSpans = spanEntries.distinct()
    val distinctLogEntries = logEntries.distinct()
    if (distinctSpans.size > entryLimit) {
      warnings += "Merged Cloud Trace results were truncated at $entryLimit spans"
    }
    if (distinctLogEntries.size > entryLimit) {
      warnings += "Merged Cloud Logging results were truncated at $entryLimit entries"
    }
    val retainedSpans = retainSpans(distinctSpans, entryLimit)
    val retainedLogEntries = retainLogEntries(distinctLogEntries, entryLimit)
    val sourceStatuses = buildList {
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
      ReportTraceOutput.lifecycleCoverage(context, retainedSpans, retainedLogEntries)
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

  private fun validateDatabaseFlags() {
    val missing = buildList {
      if (runCatching { spannerFlags.projectName }.isFailure) add("--spanner-project")
      if (runCatching { spannerFlags.instanceName }.isFailure) add("--spanner-instance")
      if (runCatching { spannerFlags.databaseName }.isFailure) add("--spanner-database")
      if (postgresDatabase == null) add("--postgres-database")
      if (postgresCloudSqlConnectionName == null) add("--postgres-cloud-sql-connection-name")
      if (postgresUser == null) add("--postgres-user")
    }
    if (missing.isNotEmpty()) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "${missing.joinToString()} required with --basic-report",
      )
    }
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
)

internal fun main(args: Array<String>, dependencies: ReportTraceDependencies): Int {
  return CommandLine(
      ReportTrace(
        dependencies.logReaderFactory,
        dependencies.spanReaderFactory,
        dependencies.resolverFactory,
        dependencies.resolverOverride,
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
