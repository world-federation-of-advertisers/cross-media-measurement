// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common.telemetry

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.logging.LogEntry
import com.google.cloud.logging.Logging
import com.google.cloud.logging.Logging.EntryListOption
import com.google.cloud.logging.Logging.SortingField
import com.google.cloud.logging.Logging.SortingOrder
import com.google.cloud.logging.LoggingOptions
import com.google.cloud.logging.Payload
import com.google.cloud.logging.Severity
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler

fun buildCloudTelemetryLoggingOptions(
  project: String,
  credentials: GoogleCredentials = GoogleCredentials.getApplicationDefault(),
): LoggingOptions {
  val projectCredentials =
    credentials.createScoped(LOGGING_READ_SCOPE).createWithQuotaProject(project)
  return LoggingOptions.newBuilder()
    .setProjectId(project)
    .setQuotaProjectId(project)
    .setCredentials(projectCredentials)
    .build()
}

/** Bounded Google Cloud Logging reader shared by operator diagnostics. */
class GoogleCloudLogReader(
  private val project: String,
  private val logging: Logging,
  private val safeFields: Set<String>,
  private val correlationFields: Set<String>,
  private val includeGrpcPayloads: Boolean = false,
  private var requestThrottler: Throttler =
    MinimumIntervalThrottler(Clock.systemUTC(), Duration.ZERO),
  private val maxGrpcContextEntries: Int = 5_000,
) : CloudLogReader {
  override fun withRequestThrottler(requestThrottler: Throttler): CloudLogReader {
    this.requestThrottler = requestThrottler
    return this
  }

  override suspend fun read(
    correlationValues: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<CloudLogEntry> {
    require(correlationValues.isNotEmpty()) { "At least one correlation value is required" }
    val predicates =
      correlationValues.distinct().map { value ->
        val escaped = escape(value)
        (listOf(
            "textPayload:\"$escaped\"",
            "jsonPayload.message:\"$escaped\"",
            "jsonPayload.MESSAGE:\"$escaped\"",
          ) +
            correlationFields.flatMap { field ->
              listOf(
                "jsonPayload.\"$field\"=\"$escaped\"",
                "jsonPayload.attributes.\"$field\"=\"$escaped\"",
              )
            })
          .joinToString(" OR ", "(", ")")
      }
    return readPredicates(predicates, startTime, endTime, limit)
  }

  override suspend fun readTraceIds(
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<CloudLogEntry> {
    val predicates =
      traceIds.distinct().map { rawTraceId ->
        val traceId = rawTraceId.substringAfterLast('/')
        require(TRACE_ID_PATTERN.matches(traceId)) { "Invalid Cloud Trace trace ID" }
        "trace=\"projects/$project/traces/$traceId\""
      }
    return readPredicates(predicates, startTime, endTime, limit)
  }

  private suspend fun readPredicates(
    predicates: List<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<CloudLogEntry> {
    if (predicates.isEmpty()) return emptyList()
    require(limit > 0) { "limit must be positive" }
    val timeFilter = "timestamp>=\"$startTime\" AND timestamp<=\"$endTime\""
    val entries = mutableListOf<LogEntry>()
    var truncatedQueries = 0
    val rawLimit =
      (limit.toLong() + maxGrpcContextEntries).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    for (filter in chunkFilters(timeFilter, predicates)) {
      var page = listEntries(filter, rawLimit)
      while (true) {
        for (entry in page.values) {
          if (entries.size == rawLimit) break
          entries += entry
        }
        if (entries.size == rawLimit || !page.hasNextPage()) break
        val currentPage = page
        page =
          requestThrottler.onReady { runInterruptible(Dispatchers.IO) { currentPage.nextPage } }
      }
      if (entries.size == rawLimit || page.hasNextPage()) truncatedQueries++
      if (entries.size == rawLimit) break
    }
    val filtered = filterVerboseGrpcEntries(entries.distinct(), startTime, endTime)
    val rendered =
      filtered.entries
        .mapNotNull(::toCloudLogEntry)
        .distinct()
        .sortedByDescending { it.timestamp }
        .take(limit)
    if (truncatedQueries > 0 || filtered.classificationIncomplete) {
      throw CloudLogCollectionTruncatedException(
        rendered,
        filtered.contextEntriesExamined,
        maxGrpcContextEntries,
        filtered.classificationIncomplete,
        entries.size,
        rawLimit,
        truncatedQueries,
      )
    }
    return rendered
  }

  private suspend fun listEntries(filter: String, pageSize: Int) =
    requestThrottler.onReady {
      runInterruptible(Dispatchers.IO) {
        logging.listLogEntries(
          EntryListOption.filter(filter),
          EntryListOption.pageSize(pageSize.coerceAtMost(MAX_PAGE_SIZE)),
          EntryListOption.sortOrder(SortingField.TIMESTAMP, SortingOrder.DESCENDING),
        )
      }
    }

  private suspend fun filterVerboseGrpcEntries(
    entries: List<LogEntry>,
    startTime: Instant,
    endTime: Instant,
  ): GrpcFilterResult {
    if (includeGrpcPayloads) return GrpcFilterResult(entries, 0, false)
    val retained = mutableListOf<LogEntry>()
    val contextByOrigin = mutableMapOf<LogOrigin, GrpcOriginContext>()
    var contextEntriesExamined = 0
    var classificationIncomplete = false
    for (entry in entries.sortedByDescending { it.instantTimestamp ?: Instant.EPOCH }) {
      val message = entry.rawMessage()
      if (verboseGrpcMarker(message) != null) continue
      if (isLifecycleLog(message)) {
        retained += entry
        continue
      }
      if (isGrpcContinuation(message)) {
        val origin = entry.logOrigin()
        if (origin == null) {
          classificationIncomplete = true
          continue
        }
        var context = contextByOrigin[origin]
        if (context == null) {
          context =
            runBlockingContextRead(
              origin,
              startTime.minus(GRPC_CONTEXT_LOOKBACK),
              endTime,
              maxGrpcContextEntries - contextEntriesExamined,
            )
          contextByOrigin[origin] = context
          contextEntriesExamined += context.entriesExamined
        }
        when (classifyGrpcContinuation(entry.instantTimestamp, context)) {
          GrpcContinuationClassification.GRPC -> continue
          GrpcContinuationClassification.APPLICATION -> Unit
          GrpcContinuationClassification.UNKNOWN -> {
            classificationIncomplete = true
            continue
          }
        }
      }
      retained += entry
    }
    return GrpcFilterResult(retained, contextEntriesExamined, classificationIncomplete)
  }

  private suspend fun runBlockingContextRead(
    origin: LogOrigin,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): GrpcOriginContext {
    if (limit <= 0) return GrpcOriginContext(emptyList(), 0, true)
    val filter = buildGrpcContextFilter(origin, startTime, endTime)
    val entries = mutableListOf<LogEntry>()
    var examined = 0
    var page = listEntries(filter, limit)
    while (true) {
      for (entry in page.values) {
        if (examined == limit) return GrpcOriginContext(entries, examined, true)
        examined++
        if (entry.logOrigin() == origin) entries += entry
      }
      if (!page.hasNextPage()) return GrpcOriginContext(entries, examined, false)
      if (examined == limit) return GrpcOriginContext(entries, examined, true)
      val currentPage = page
      page = requestThrottler.onReady { runInterruptible(Dispatchers.IO) { currentPage.nextPage } }
    }
  }

  private fun classifyGrpcContinuation(
    entryTime: Instant?,
    context: GrpcOriginContext,
  ): GrpcContinuationClassification {
    if (entryTime == null) return GrpcContinuationClassification.UNKNOWN
    val completed = mutableSetOf<GrpcCallKey>()
    val paired = mutableSetOf<GrpcCallKey>()
    for (entry in context.entries.sortedByDescending { it.instantTimestamp ?: Instant.MIN }) {
      val contextTime = entry.instantTimestamp ?: continue
      if (!contextTime.isBefore(entryTime)) continue
      val marker = verboseGrpcMarker(entry.rawMessage())
      if (marker != null) {
        val key = GrpcCallKey(marker.requestId, marker.isClient)
        when (marker.kind) {
          "complete" -> completed += key
          "response" ->
            if (key !in completed && key !in paired) {
              return if (completed.isEmpty()) GrpcContinuationClassification.GRPC
              else GrpcContinuationClassification.UNKNOWN
            }
          "headers",
          "request" ->
            if (completed.remove(key)) paired += key
            else if (key !in paired) {
              return if (completed.isEmpty()) GrpcContinuationClassification.GRPC
              else GrpcContinuationClassification.UNKNOWN
            }
          "error" -> return GrpcContinuationClassification.GRPC
        }
      }
      if (isLifecycleLog(entry.rawMessage())) return GrpcContinuationClassification.APPLICATION
    }
    return if (context.truncated) GrpcContinuationClassification.UNKNOWN
    else GrpcContinuationClassification.APPLICATION
  }

  private fun buildGrpcContextFilter(
    origin: LogOrigin,
    startTime: Instant,
    endTime: Instant,
  ): String {
    val predicates = mutableListOf("timestamp>=\"$startTime\"", "timestamp<=\"$endTime\"")
    if (origin.logName.isNotEmpty()) predicates += "logName=\"${escape(origin.logName)}\""
    if (origin.resourceType != null) {
      predicates += "resource.type=\"${escape(origin.resourceType)}\""
    }
    for ((key, value) in origin.resourceLabels) {
      predicates += "resource.labels.\"$key\"=\"${escape(value)}\""
    }
    if (origin.loggerField != null) {
      predicates += "${origin.loggerField}=\"${escape(origin.loggerIdentity)}\""
    }
    predicates += "\"gRPC\""
    return predicates.joinToString(" AND ")
  }

  private fun LogEntry.rawMessage(): String {
    val payload = getPayload<Payload<*>>() ?: return ""
    return when (payload.type) {
      Payload.Type.STRING -> (payload as Payload.StringPayload).data
      Payload.Type.JSON -> {
        val values = (payload as Payload.JsonPayload).dataAsMap
        (values["message"] ?: values["MESSAGE"])?.toString() ?: payload.toString()
      }
      else -> payload.toString()
    }
  }

  private fun LogEntry.logOrigin(): LogOrigin? {
    val payload = getPayload<Payload<*>>()
    val resourceLabels = resource?.labels.orEmpty().toMap()
    val loggerLabel =
      LOGGER_LABEL_KEYS.firstNotNullOfOrNull { key ->
        labels[key]?.takeIf(String::isNotBlank)?.let { LoggerIdentity(it, "labels.\"$key\"") }
      }
    val sourceFunction = sourceLocation?.function?.takeIf(String::isNotBlank)
    val jsonSource =
      (payload as? Payload.JsonPayload)
        ?.dataAsMap
        ?.get("logging.googleapis.com/sourceLocation")
        ?.let { it as? Map<*, *> }
        ?.let { (it["function"] ?: it["file"])?.toString() }
        ?.takeIf(String::isNotBlank)
    val loggerIdentity =
      loggerLabel
        ?: sourceFunction?.let { LoggerIdentity(it, "sourceLocation.function") }
        ?: jsonSource?.let { LoggerIdentity(it, null) }
    if (loggerIdentity == null && (logName.isEmpty() || resourceLabels.isEmpty())) return null
    return LogOrigin(
      logName,
      resource?.type,
      resourceLabels,
      loggerIdentity?.value.orEmpty(),
      loggerIdentity?.filterField,
    )
  }

  private fun verboseGrpcMarker(text: String): GrpcMarker? {
    val match = VERBOSE_GRPC_PATTERN.find(text) ?: return null
    return GrpcMarker(
      match.groupValues[2],
      match.groupValues[3].lowercase(),
      match.groupValues[1].isNotEmpty(),
    )
  }

  private fun isGrpcContinuation(text: String): Boolean {
    return GRPC_CONTINUATION_PATTERN.matches(LEVEL_PATTERN.replaceFirst(text, ""))
  }

  private fun isLifecycleLog(text: String): Boolean {
    val fields = SafeTelemetryText(safeFields).fields(text)
    return fields.containsKey("event") && fields.containsKey("xmm.lifecycle.stage")
  }

  private fun toCloudLogEntry(entry: LogEntry): CloudLogEntry? {
    val message = renderPayload(entry.getPayload<Payload<*>>()) ?: return null
    val labels = entry.resource?.labels.orEmpty()
    val service =
      listOf("service_name", "container_name", "job_name", "function_name").firstNotNullOfOrNull {
        labels[it]
      } ?: entry.resource?.type ?: entry.logName.orEmpty().substringAfterLast('/')
    return CloudLogEntry(
      project,
      entry.instantTimestamp ?: Instant.EPOCH,
      service,
      effectiveSeverity(entry.severity, message),
      entry.trace?.takeIf(String::isNotEmpty),
      message,
    )
  }

  private fun renderPayload(payload: Payload<*>?): String? {
    if (payload == null) return ""
    if (payload.type == Payload.Type.STRING) {
      val text = (payload as Payload.StringPayload).data
      return text.takeIf { includeGrpcPayloads || !VERBOSE_GRPC_PATTERN.containsMatchIn(it) }
    }
    if (payload.type != Payload.Type.JSON) return payload.toString().takeIf { includeGrpcPayloads }
    if (includeGrpcPayloads) return payload.toString()
    val values = (payload as Payload.JsonPayload).dataAsMap
    val message = (values["message"] ?: values["MESSAGE"])?.toString()
    if (!includeGrpcPayloads && message != null && VERBOSE_GRPC_PATTERN.containsMatchIn(message)) {
      return null
    }
    val fields = mutableMapOf<String, String>()
    for (field in safeFields) scalar(values[field])?.let { fields[field] = it }
    val nested = values["attributes"] as? Map<*, *>
    if (nested != null) {
      for ((key, value) in nested) {
        if (key is String && key in safeFields) scalar(value)?.let { fields[key] = it }
      }
    }
    if (message != null) fields += SafeTelemetryText(safeFields).fields(message)
    val prefix =
      fields.entries.sortedBy { it.key }.joinToString(" ") { (key, value) -> "$key=$value" }
    return listOf(prefix, message.orEmpty()).filter(String::isNotEmpty).joinToString(" ")
  }

  private fun effectiveSeverity(severity: Severity, message: String): String {
    return when (LEVEL_PATTERN.find(message)?.groupValues?.get(1)) {
      "SEVERE" -> "ERROR"
      "WARNING",
      "WARN" -> "WARNING"
      "INFO",
      "CONFIG",
      "FINE",
      "FINER",
      "FINEST" -> "INFO"
      else -> severity.name
    }
  }

  private fun chunkFilters(timeFilter: String, predicates: List<String>): List<String> {
    val filters = mutableListOf<String>()
    var current = mutableListOf<String>()
    for (predicate in predicates) {
      val candidate = buildFilter(timeFilter, current + predicate)
      if (candidate.length > MAX_FILTER_LENGTH && current.isNotEmpty()) {
        filters += buildFilter(timeFilter, current)
        require(buildFilter(timeFilter, listOf(predicate)).length <= MAX_FILTER_LENGTH) {
          "One telemetry value exceeds the Cloud Logging filter-size limit"
        }
        current = mutableListOf(predicate)
      } else {
        require(candidate.length <= MAX_FILTER_LENGTH) {
          "One telemetry value exceeds the Cloud Logging filter-size limit"
        }
        current += predicate
      }
    }
    if (current.isNotEmpty()) filters += buildFilter(timeFilter, current)
    return filters
  }

  private fun buildFilter(timeFilter: String, predicates: List<String>): String =
    "$timeFilter AND (${predicates.joinToString(" OR ")})"

  private fun scalar(value: Any?): String? =
    when (value) {
      is String,
      is Number,
      is Boolean -> value.toString()
      else -> null
    }

  private fun escape(value: String): String = value.replace("\\", "\\\\").replace("\"", "\\\"")

  private data class LogOrigin(
    val logName: String,
    val resourceType: String?,
    val resourceLabels: Map<String, String>,
    val loggerIdentity: String,
    val loggerField: String?,
  )

  private data class LoggerIdentity(val value: String, val filterField: String?)

  private data class GrpcFilterResult(
    val entries: List<LogEntry>,
    val contextEntriesExamined: Int,
    val classificationIncomplete: Boolean,
  )

  private data class GrpcOriginContext(
    val entries: List<LogEntry>,
    val entriesExamined: Int,
    val truncated: Boolean,
  )

  private data class GrpcCallKey(val requestId: String, val isClient: Boolean)

  private data class GrpcMarker(val requestId: String, val kind: String, val isClient: Boolean)

  private enum class GrpcContinuationClassification {
    GRPC,
    APPLICATION,
    UNKNOWN,
  }

  companion object {
    private const val MAX_PAGE_SIZE = 1_000
    private const val MAX_FILTER_LENGTH = 18_000
    private val TRACE_ID_PATTERN = Regex("(?i)[0-9a-f]{32}")
    private val VERBOSE_GRPC_PATTERN =
      Regex("(?i)\\bgRPC(\\s+client)?\\s+(\\S+)\\s+(headers|request|response|complete|error):?")
    private val LEVEL_PATTERN =
      Regex("^\\s*(SEVERE|WARNING|WARN|INFO|CONFIG|FINE|FINER|FINEST):\\s")
    private val GRPC_CONTINUATION_PATTERN =
      Regex(
        "(?s)^\\s*(?:[A-Za-z_][A-Za-z0-9_.-]*\\s*(?::.*|\\{)|[{}]|" +
          "\\[[^]]*]|[A-Za-z0-9_.-]+\\s*=.*)\\s*$"
      )
    private val GRPC_CONTEXT_LOOKBACK: Duration = Duration.ofSeconds(10)
    private val LOGGER_LABEL_KEYS =
      listOf("logger", "logger_name", "loggerName", "logging.googleapis.com/logger")
  }
}

/** Bounded Google Cloud Trace v1 reader shared by operator diagnostics. */
class GoogleCloudTraceReader(
  private val traceAttributesFor: (String) -> Collection<String>,
  private val credentials: GoogleCredentials =
    GoogleCredentials.getApplicationDefault().createScoped(TRACE_READ_SCOPE),
  private val httpClient: HttpClient = HttpClient.newHttpClient(),
  private var maxConcurrency: Int = DEFAULT_MAX_CONCURRENCY,
  private var requestThrottlerFactory: (String) -> Throttler = {
    MinimumIntervalThrottler(Clock.systemUTC(), Duration.ZERO)
  },
) : CloudTraceReader {
  private val requestQuotas = mutableMapOf<String, RequestQuota>()

  override fun withMaxConcurrency(maxConcurrency: Int): CloudTraceReader {
    require(maxConcurrency > 0) { "maxConcurrency must be positive" }
    this.maxConcurrency = maxConcurrency
    return this
  }

  override fun withRequestThrottlerFactory(
    requestThrottlerFactory: (String) -> Throttler
  ): CloudTraceReader {
    this.requestThrottlerFactory = requestThrottlerFactory
    requestQuotas.clear()
    return this
  }

  override suspend fun read(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<CloudTraceSpan> {
    runInterruptible(Dispatchers.IO) { credentials.refreshIfExpired() }
    val quota = requestQuotas.getOrPut(project) { RequestQuota(requestThrottlerFactory(project)) }
    val queries =
      buildList<suspend () -> List<CloudTraceSpan>> {
        for (value in correlationValues.distinct()) {
          for (attribute in traceAttributesFor(value).distinct()) {
            add { listTraces(project, "+$attribute:\"$value\"", startTime, endTime, limit, quota) }
          }
        }
        for (traceId in traceIds.map { it.substringAfterLast('/') }.distinct()) {
          require(TRACE_ID_PATTERN.matches(traceId)) { "Invalid Cloud Trace trace ID" }
          add { readTrace(project, traceId, quota).orEmpty() }
        }
      }
    val spans = coroutineScope {
      val semaphore = Semaphore(maxConcurrency)
      queries.map { query -> async { semaphore.withPermit { query() } } }.awaitAll().flatten()
    }
    return spans
      .filter {
        !it.startTime.isAfter(endTime) && !(it.endTime ?: it.startTime).isBefore(startTime)
      }
      .distinct()
      .sortedByDescending { it.startTime }
      .take(if (limit == Int.MAX_VALUE) limit else limit + 1)
  }

  private suspend fun listTraces(
    project: String,
    filter: String,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
    quota: RequestQuota,
  ): List<CloudTraceSpan> {
    val spans = mutableListOf<CloudTraceSpan>()
    var pageToken: String? = null
    do {
      val parameters =
        mutableMapOf(
          "view" to "COMPLETE",
          "pageSize" to (limit + 1).coerceAtMost(MAX_PAGE_SIZE).toString(),
          "startTime" to startTime.toString(),
          "endTime" to endTime.toString(),
          "filter" to filter,
        )
      if (pageToken != null) parameters["pageToken"] = pageToken
      val query =
        parameters.entries.joinToString("&") { (key, value) -> "$key=${urlEncode(value)}" }
      val request = request(project, "traces?$query")
      val response = send(request, LIST_QUOTA_UNITS, quota)
      check(response.statusCode() in 200..299) {
        "Cloud Trace API returned HTTP ${response.statusCode()}"
      }
      val root = JsonParser.parseString(response.body()).asJsonObject
      spans += parseResponse(project, root, null)
      pageToken = root.optionalString("nextPageToken")
    } while (pageToken != null && spans.size <= limit)
    return spans.take(limit + 1)
  }

  private suspend fun readTrace(
    project: String,
    traceId: String,
    quota: RequestQuota,
  ): List<CloudTraceSpan>? {
    val response = send(request(project, "traces/$traceId"), GET_QUOTA_UNITS, quota)
    if (response.statusCode() == 404) return null
    check(response.statusCode() in 200..299) {
      "Cloud Trace API returned HTTP ${response.statusCode()}"
    }
    return parseResponse(project, JsonParser.parseString(response.body()).asJsonObject, traceId)
  }

  private fun request(project: String, path: String): HttpRequest {
    return HttpRequest.newBuilder()
      .uri(URI.create("https://cloudtrace.googleapis.com/v1/projects/$project/$path"))
      .header("Authorization", "Bearer ${checkNotNull(credentials.accessToken).tokenValue}")
      .header("x-goog-user-project", project)
      .timeout(HTTP_TIMEOUT)
      .GET()
      .build()
  }

  private suspend fun send(
    request: HttpRequest,
    quotaUnits: Int,
    quota: RequestQuota,
  ): HttpResponse<String> {
    quota.mutex.withLock { repeat(quotaUnits) { quota.throttler.onReady {} } }
    return runInterruptible(Dispatchers.IO) {
      httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    }
  }

  private fun parseResponse(
    project: String,
    root: JsonObject,
    fallbackTraceId: String?,
  ): List<CloudTraceSpan> {
    val traces = root.getAsJsonArray("traces")?.map { it.asJsonObject } ?: listOf(root)
    return traces.flatMap { trace ->
      val traceId = trace.optionalString("traceId") ?: fallbackTraceId ?: return@flatMap emptyList()
      trace.getAsJsonArray("spans")?.map { element ->
        val span = element.asJsonObject
        val labels = span.getAsJsonObject("labels")
        CloudTraceSpan(
          project,
          traceId,
          span.requiredString("spanId"),
          span.optionalString("parentSpanId"),
          span.requiredString("name"),
          labels?.optionalString("g.co/agent/name")
            ?: labels?.optionalString("service.name")
            ?: labels?.optionalString("/http/host")
            ?: "unknown-service",
          Instant.parse(span.requiredString("startTime")),
          span.optionalString("endTime")?.let(Instant::parse),
          labels
            ?.entrySet()
            ?.mapNotNull { (key, value) ->
              if (value.isJsonPrimitive) key to value.asString else null
            }
            ?.toMap()
            .orEmpty(),
        )
      } ?: emptyList()
    }
  }

  private class RequestQuota(val throttler: Throttler) {
    val mutex = Mutex()
  }

  companion object {
    private const val TRACE_READ_SCOPE = "https://www.googleapis.com/auth/trace.readonly"
    private const val DEFAULT_MAX_CONCURRENCY = 8
    private const val MAX_PAGE_SIZE = 1_000
    private const val LIST_QUOTA_UNITS = 25
    private const val GET_QUOTA_UNITS = 1
    private val HTTP_TIMEOUT = Duration.ofSeconds(30)
    private val TRACE_ID_PATTERN = Regex("(?i)[0-9a-f]{32}")

    private fun urlEncode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)
  }
}

private fun JsonObject.requiredString(name: String): String = get(name).asString

private fun JsonObject.optionalString(name: String): String? =
  get(name)?.takeUnless { it.isJsonNull }?.asString

private const val LOGGING_READ_SCOPE = "https://www.googleapis.com/auth/logging.read"
