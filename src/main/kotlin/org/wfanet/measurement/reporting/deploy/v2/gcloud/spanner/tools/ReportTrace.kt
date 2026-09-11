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

/** An annotated event recorded within a Cloud Trace span. */
internal data class ReportTraceEvent(
  val timestamp: Instant,
  val name: String,
  val attributes: Map<String, String>,
)

/** A complete Cloud Trace span retained separately from Cloud Logging entries. */
internal data class ReportTraceSpan(
  val sourceProject: String,
  val traceId: String,
  val spanId: String,
  val parentSpanId: String?,
  val name: String,
  val service: String,
  val startTime: Instant,
  val endTime: Instant?,
  val statusCode: String?,
  val statusMessage: String?,
  val attributes: Map<String, String>,
  val events: List<ReportTraceEvent>,
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
            EntryListOption.pageSize((limit + 1).coerceAtMost(MAX_LOG_PAGE_SIZE)),
            EntryListOption.sortOrder(SortingField.TIMESTAMP, SortingOrder.ASCENDING),
          )
          .iterateAll()
          .take(limit + 1)
          .map { it.toReportTraceLogEntry() }
      }
      .distinct()
      .sortedBy { it.timestamp }
      .take(limit + 1)
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
    return entries.distinct().sortedBy { it.startTime }.take(limit + 1)
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
          "pageSize" to limit.coerceAtMost(MAX_TRACE_PAGE_SIZE).toString(),
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
          .GET()
          .build()
      val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      check(response.statusCode() in 200..299) {
        "Cloud Trace API returned HTTP ${response.statusCode()}: ${response.body().take(1000)}"
      }
      val root = JsonParser.parseString(response.body()).asJsonObject
      val traces = root.getAsJsonArray("traces")
      if (traces != null) {
        entries +=
          traces.flatMap { traceElement ->
            val trace = traceElement.asJsonObject
            val traceId = trace.requiredString("traceId")
            val spans = trace.getAsJsonArray("spans") ?: return@flatMap emptyList()
            spans.map { spanElement -> spanElement.asJsonObject.toTraceSpan(project, traceId) }
          }
      }
      pageToken = root.optionalString("nextPageToken")
    } while (pageToken != null && entries.size <= limit)
    return entries
  }

  private fun readTrace(project: String, traceId: String): List<ReportTraceSpan>? {
    val request =
      HttpRequest.newBuilder()
        .uri(URI.create("https://cloudtrace.googleapis.com/v1/projects/$project/traces/$traceId"))
        .header("Authorization", "Bearer ${checkNotNull(credentials.accessToken).tokenValue}")
        .GET()
        .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() == 404) return null
    check(response.statusCode() in 200..299) {
      "Cloud Trace API returned HTTP ${response.statusCode()}: ${response.body().take(1000)}"
    }
    val trace = JsonParser.parseString(response.body()).asJsonObject
    val responseTraceId = trace.optionalString("traceId") ?: traceId
    val spans = trace.getAsJsonArray("spans") ?: return emptyList()
    return spans.map { it.asJsonObject.toTraceSpan(project, responseTraceId) }
  }

  private fun JsonObject.toTraceSpan(project: String, traceId: String): ReportTraceSpan {
    val labels = getAsJsonObject("labels")
    val service =
      labels?.optionalString("g.co/agent/name")
        ?: labels?.optionalString("service.name")
        ?: labels?.optionalString("/http/host")
        ?: "unknown-service"
    val endTimeValue = optionalString("endTime")
    val endTime = if (endTimeValue == null) null else Instant.parse(endTimeValue)
    val startTime = Instant.parse(requiredString("startTime"))
    val status = getAsJsonObject("status")
    return ReportTraceSpan(
      sourceProject = project,
      traceId = traceId,
      spanId = requiredString("spanId"),
      parentSpanId = optionalString("parentSpanId"),
      name = requiredString("name"),
      service = service,
      startTime = startTime,
      endTime = endTime,
      statusCode = status?.optionalString("code"),
      statusMessage = status?.optionalString("message"),
      attributes = labels?.stringValues().orEmpty(),
      events = parseEvents(),
    )
  }

  private fun JsonObject.parseEvents(): List<ReportTraceEvent> {
    val timeEvents = getAsJsonObject("timeEvents") ?: return emptyList()
    val events = timeEvents.getAsJsonArray("timeEvent") ?: return emptyList()
    return events.mapNotNull { eventElement ->
      val event = eventElement.asJsonObject
      val timestamp = event.optionalString("time")?.let(Instant::parse) ?: return@mapNotNull null
      val annotation = event.getAsJsonObject("annotation") ?: return@mapNotNull null
      val attributes =
        annotation.getAsJsonObject("attributes")?.getAsJsonObject("attributeMap")?.attributeValues()
          ?: emptyMap()
      ReportTraceEvent(
        timestamp = timestamp,
        name = annotation.optionalString("description") ?: "annotation",
        attributes = attributes,
      )
    }
  }

  private fun JsonObject.stringValues(): Map<String, String> {
    return entrySet()
      .mapNotNull { (key, value) -> if (value.isJsonPrimitive) key to value.asString else null }
      .toMap()
  }

  private fun JsonObject.attributeValues(): Map<String, String> {
    return entrySet()
      .mapNotNull { (key, value) ->
        val attribute = value.takeIf { it.isJsonObject }?.asJsonObject ?: return@mapNotNull null
        val rendered =
          listOf("stringValue", "intValue", "boolValue").firstNotNullOfOrNull {
            attribute.optionalString(it)
          } ?: return@mapNotNull null
        key to rendered
      }
      .toMap()
  }

  private fun JsonObject.requiredString(name: String): String = get(name).asString

  private fun JsonObject.optionalString(name: String): String? =
    get(name)?.takeUnless { it.isJsonNull }?.asString

  companion object {
    private const val TRACE_READ_SCOPE = "https://www.googleapis.com/auth/trace.readonly"
    private const val BASIC_REPORT_TRACE_ATTRIBUTE = "xmm.basic_report.name"
    private const val REPORT_TRACE_ATTRIBUTE = "xmm.report.name"
    private const val METRIC_TRACE_ATTRIBUTE = "xmm.metric.name"
    private const val MEASUREMENT_TRACE_ATTRIBUTE = "xmm.measurement.name"
    private const val MAX_TRACE_PAGE_SIZE = 1000

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

internal object ReportTraceOutput {
  fun renderLogPayload(payload: Payload<*>?, includeRawPayloads: Boolean): String {
    if (payload == null) return ""
    if (includeRawPayloads) return payload.toString()
    if (payload.type == Payload.Type.STRING) {
      val text = (payload as Payload.StringPayload).data
      val safeFields = SAFE_TEXT_FIELD_PATTERN.findAll(text).map { sanitize(it.value) }.toList()
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
      val value = values[key]
      if (value != null) {
        safeValues[key] = sanitize(value.toString())
      }
    }
    val nestedAttributes = values["attributes"] as? Map<*, *>
    if (nestedAttributes != null) {
      for ((key, value) in nestedAttributes) {
        val keyString = key as? String ?: continue
        if (keyString in SAFE_LOG_FIELDS || keyString.startsWith("xmm.")) {
          safeValues[keyString] = sanitize(value?.toString().orEmpty())
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
          "jsonPayload.attributes.\"xmm.basic_report.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.report.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.metric.name\"=\"$escaped\" OR " +
          "jsonPayload.attributes.\"xmm.measurement.name\"=\"$escaped\" OR " +
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
  ): String = buildString {
    appendLine("# Report execution trace")
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
    appendLine("Metrics: ${context.metricNames.size}")
    appendLine("Measurements: ${context.measurementNames.size}")
    appendLine("Payload policy: ${if (includeRawPayloads) "RAW-SENSITIVE" else "REDACTED"}")
    if (includeRawPayloads) {
      appendLine("WARNING: This artifact contains raw log payloads and may contain secrets.")
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
    if (warnings.isNotEmpty()) {
      appendLine("## Collection warnings")
      for (warning in warnings) {
        appendLine("- $warning")
      }
      appendLine()
    }
    appendLine("## Timeline")
    appendLine()
    if (spans.isEmpty() && logEntries.isEmpty()) {
      if (warnings.isEmpty()) {
        appendLine("No matching trace spans or log entries were found in the selected time range.")
      } else {
        appendLine("No timeline entries were collected; see the collection warnings above.")
      }
      return@buildString
    }
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
        val status =
          listOfNotNull(span.statusCode, span.statusMessage?.let(ReportTraceOutput::sanitize))
            .joinToString(":")
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
                if (status.isNotEmpty()) append(" status=$status")
                if (attributes.isNotEmpty()) append(" ").append(attributes)
              },
          )
        )
        for (event in span.events) {
          add(
            RenderedTimelineEntry(
              timestamp = event.timestamp,
              text =
                "EVENT [${span.sourceProject}/${span.service}] ${event.name}" +
                  " trace=${span.traceId} span=${span.spanId}" +
                  formatAttributes(event.attributes).let { if (it.isEmpty()) "" else " $it" },
            )
          )
        }
      }
    }
    for (entry in timeline.sortedBy { it.timestamp }) {
      append(entry.timestamp).append("  ").appendLine(entry.text)
    }
  }

  private fun formatAttributes(attributes: Map<String, String>): String {
    return attributes
      .filterKeys { key ->
        key.startsWith("xmm.") ||
          key.startsWith("edpa.") ||
          key in SAFE_TRACE_ATTRIBUTES ||
          key.startsWith("exception.")
      }
      .entries
      .sortedBy { it.key }
      .joinToString(" ") { (key, value) -> "$key=${sanitize(value)}" }
  }

  private fun sanitize(value: String): String {
    var sanitized = value.replace('\n', ' ').replace('\r', ' ')
    for (pattern in SECRET_PATTERNS) {
      sanitized = pattern.replace(sanitized, "$1[REDACTED]")
    }
    return sanitized.take(MAX_RENDERED_VALUE_LENGTH)
  }

  private data class RenderedTimelineEntry(val timestamp: Instant, val text: String)

  private const val MAX_LOG_FILTER_LENGTH = 20_000
  private const val MAX_RENDERED_VALUE_LENGTH = 1000
  private val SAFE_LOG_FIELDS =
    setOf(
      "message",
      "event",
      "stage",
      "status",
      "state",
      "error_type",
      "exception.type",
      "exception.message",
      "xmm.basic_report.name",
      "xmm.report.name",
      "xmm.metric.name",
      "xmm.measurement.name",
      "xmm.requisition.name",
      "xmm.edpa.group_id",
      "xmm.work_item.name",
      "xmm.computation.name",
    )
  private val SAFE_TRACE_ATTRIBUTES =
    setOf("error", "service.name", "g.co/agent/name", "/http/host")
  private val SECRET_PATTERNS =
    listOf(
      Regex("(?i)(bearer\\s+)[A-Za-z0-9._~+/=-]+"),
      Regex(
        "(?i)((?:authorization|cookie|set-cookie|x-api-key|api[_-]?key|access[_-]?token|" +
          "refresh[_-]?token|client[_-]?secret|private[_-]?key)\\s*[:=]\\s*)[^\\s,;]+"
      ),
      Regex("(?is)(-----BEGIN [^-]*PRIVATE KEY-----).*?(-----END [^-]*PRIVATE KEY-----)"),
    )
  private val SAFE_TEXT_FIELD_PATTERN =
    Regex(
      "(?:xmm\\.[a-zA-Z0-9_.-]+|edpa\\.[a-zA-Z0-9_.-]+|error_type|status|state|event|stage)=[^\\s]+"
    )
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
    description = ["Exit successfully when an observability source fails or output is truncated."],
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

  @CommandLine.Option(names = ["--limit"], defaultValue = "1000") private lateinit var limit: String

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
    val entryLimit = limit.toIntOrNull()
    if (entryLimit == null || entryLimit !in 1..MAX_ENTRIES) {
      throw CommandLine.ParameterException(
        spec.commandLine(),
        "--limit must be between 1 and $MAX_ENTRIES",
      )
    }

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
          )
        )
      return if (collection.warnings.isEmpty() || allowPartial) 0 else 1
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
            ),
          )
        printBatchResult(name, if (collection.warnings.isEmpty()) "OK" else "PARTIAL", outputPath)
        if (collection.warnings.isNotEmpty() && !allowPartial) {
          failures++
        }
      } catch (e: Exception) {
        failures++
        val outputPath =
          writeArtifact(
            outputDirectory,
            fileName,
            renderFailureArtifact(name, e.message ?: e::class.java.name),
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
    val spanReader = spanReaderFactory()
    val projects = observabilityProjects.distinct()
    for (project in projects) {
      try {
        val projectLogEntries =
          logReaderFactory(project, includeRawPayloads)
            .read(context.correlationValues, startTime, endTime, entryLimit)
        if (projectLogEntries.size > entryLimit) {
          logTruncatedProjects += project
          warnings +=
            "Cloud Logging results were truncated for project $project at $entryLimit entries"
        }
        logEntries += retainLogEntries(projectLogEntries, entryLimit)
      } catch (e: Exception) {
        val failure = e.message ?: e::class.java.name
        logFailures.getOrPut(project) { mutableListOf() } += failure
        warnings += "Cloud Logging query failed for project $project: $failure"
      }
    }

    val logTraceIds = logEntries.mapNotNull { it.trace?.substringAfterLast('/') }.distinct()
    for (project in projects) {
      try {
        val projectSpans =
          spanReader.read(
            project,
            context.correlationValues,
            logTraceIds,
            startTime,
            endTime,
            entryLimit,
          )
        if (projectSpans.size > entryLimit) {
          traceTruncatedProjects += project
          warnings += "Cloud Trace results were truncated for project $project at $entryLimit spans"
        }
        spanEntries += retainSpans(projectSpans, entryLimit)
      } catch (e: Exception) {
        val failure = e.message ?: e::class.java.name
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
          spanEntries += retainSpans(projectSpans, entryLimit)
        } catch (e: Exception) {
          val failure = e.message ?: e::class.java.name
          traceFailures.getOrPut(project) { mutableListOf() } += failure
          warnings += "Cloud Trace ID lookup failed for project $project: $failure"
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
        val projectSpans = distinctSpans.count { it.sourceProject == project }
        add(
          buildSourceStatus(
            project = project,
            source = "Cloud Trace",
            fetched = projectSpans,
            retained = retainedSpans.count { it.sourceProject == project },
            truncated = project in traceTruncatedProjects || distinctSpans.size > entryLimit,
            failures = traceFailures[project].orEmpty(),
          )
        )
        val projectLogEntries = distinctLogEntries.count { it.sourceProject == project }
        add(
          buildSourceStatus(
            project = project,
            source = "Cloud Logging",
            fetched = projectLogEntries,
            retained = retainedLogEntries.count { it.sourceProject == project },
            truncated = project in logTruncatedProjects || distinctLogEntries.size > entryLimit,
            failures = logFailures[project].orEmpty(),
          )
        )
      }
    }
    return TimelineCollection(
      spans = retainedSpans,
      logEntries = retainedLogEntries,
      sourceStatuses = sourceStatuses,
      warnings = warnings,
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
    val errors = spans.filter { it.statusCode != null && it.statusCode != "0" }
    return (errors + spans.sortedByDescending { it.startTime }).distinct().take(limit).sortedBy {
      it.startTime
    }
  }

  private fun retainLogEntries(
    entries: List<ReportTraceLogEntry>,
    limit: Int,
  ): List<ReportTraceLogEntry> {
    if (entries.size <= limit) return entries.sortedBy { it.timestamp }
    val errors = entries.filter { it.severity == "ERROR" || it.severity == "CRITICAL" }
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
    appendLine("BasicReport: $name")
    appendLine()
    appendLine("## Collection failure")
    appendLine()
    appendLine(message)
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
    private const val MAX_ENTRIES = 1000
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
)
