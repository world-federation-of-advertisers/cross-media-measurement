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
import com.google.cloud.logging.LoggingException
import com.google.cloud.logging.LoggingOptions
import com.google.cloud.logging.Payload
import com.google.cloud.logging.Severity
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.io.IOException
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
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
    .setRetrySettings(LoggingOptions.getNoRetrySettings())
    .build()
}

/** Bounded Google Cloud Logging reader shared by operator diagnostics. */
class GoogleCloudLogReader(
  private val project: String,
  private val logging: Logging,
  private val safeFields: Set<String>,
  private val correlationFields: Set<String>,
  private val includeGrpcPayloads: Boolean = false,
  private val correlationFieldsFor: (String) -> Collection<String> = { correlationFields },
  private var requestThrottler: Throttler =
    MinimumIntervalThrottler(Clock.systemUTC(), Duration.ZERO),
  private val maxGrpcContextEntries: Int = 5_000,
  private val retryDelay: suspend (Duration) -> Unit = { delay(it.toMillis()) },
  private val blockingContext: CoroutineContext = Dispatchers.IO,
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
    return requireComplete(readBatch(correlationValues, emptyList(), startTime, endTime, limit))
  }

  override suspend fun readTraceIds(
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<CloudLogEntry> {
    return requireComplete(readBatch(emptyList(), traceIds, startTime, endTime, limit))
  }

  override suspend fun readBatch(
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): CloudTelemetryReadResult<CloudLogEntry> {
    return readBatchInternal(correlationValues, traceIds, startTime, endTime, limit, null)
  }

  override suspend fun readBatch(
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
    timeout: Duration,
  ): CloudTelemetryReadResult<CloudLogEntry> {
    require(!timeout.isNegative && !timeout.isZero) { "timeout must be positive" }
    return readBatchInternal(correlationValues, traceIds, startTime, endTime, limit, timeout)
  }

  private suspend fun readBatchInternal(
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
    timeout: Duration?,
  ): CloudTelemetryReadResult<CloudLogEntry> {
    val correlationPredicates =
      correlationValues.distinct().flatMap { value ->
        val fields = correlationFieldsFor(value).distinct()
        correlationSearchValues(value).map { candidate ->
          val escaped = escape(candidate)
          (listOf(
              "textPayload:\"$escaped\"",
              "jsonPayload.message:\"$escaped\"",
              "jsonPayload.MESSAGE:\"$escaped\"",
            ) +
              fields.flatMap { field ->
                listOf(
                  "jsonPayload.\"$field\"=\"$escaped\"",
                  "jsonPayload.attributes.\"$field\"=\"$escaped\"",
                )
              })
            .joinToString(" OR ", "(", ")")
        }
      }
    val tracePredicates =
      traceIds.distinct().map { rawTraceId ->
        val traceId = rawTraceId.substringAfterLast('/')
        require(TRACE_ID_PATTERN.matches(traceId)) { "Invalid Cloud Trace trace ID" }
        "trace=\"projects/$project/traces/$traceId\""
      }
    val progress = LoggingReadProgress(limit, maxGrpcContextEntries)
    if (timeout == null) {
      readPredicates(correlationPredicates + tracePredicates, startTime, endTime, progress)
      return logResult(progress, timedOut = false)
    }
    val completed =
      withTimeoutOrNull(timeout.toMillis().coerceAtLeast(1L)) {
        readPredicates(correlationPredicates + tracePredicates, startTime, endTime, progress)
        true
      }
    return logResult(progress, timedOut = completed == null)
  }

  private suspend fun readPredicates(
    predicates: List<String>,
    startTime: Instant,
    endTime: Instant,
    progress: LoggingReadProgress,
  ) {
    if (predicates.isEmpty()) return
    val timeFilter = "timestamp>=\"$startTime\" AND timestamp<=\"$endTime\""
    val filters = chunkFilters(timeFilter, predicates)
    for ((filterIndex, filter) in filters.withIndex()) {
      progress.counts.loggingFilterBatches++
      var page =
        try {
          listEntries(filter, progress.rawLimit, progress.counts, context = false)
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          progress.incomplete = true
          progress.failed = true
          progress.failure = preferredFailure(progress.failure, e)
          progress.notes += e::class.simpleName.orEmpty()
          continue
        }
      var currentPageTruncated = false
      while (true) {
        for (entry in page.values) {
          if (progress.entries.size == progress.rawLimit) {
            currentPageTruncated = true
            break
          }
          progress.entries += entry
        }
        if (progress.entries.size == progress.rawLimit || !page.hasNextPage()) break
        val currentPage = page
        page =
          try {
            loggingCall(progress.counts, context = false) { currentPage.nextPage }
          } catch (e: CancellationException) {
            throw e
          } catch (e: Exception) {
            progress.incomplete = true
            progress.failed = true
            progress.failure = preferredFailure(progress.failure, e)
            progress.notes += e::class.simpleName.orEmpty()
            break
          }
      }
      if (
        currentPageTruncated ||
          page.hasNextPage() ||
          (progress.entries.size == progress.rawLimit && filterIndex < filters.lastIndex)
      ) {
        progress.rawQueriesTruncated++
      }
      if (progress.entries.size == progress.rawLimit) break
    }
    val filtered =
      filterVerboseGrpcEntries(progress.entries.distinct(), startTime, endTime, progress)
    progress.filteredEntries = filtered.entries
    progress.contextEntriesExamined = filtered.contextEntriesExamined
    progress.grpcClassificationIncomplete = filtered.classificationIncomplete
    if (filtered.failure != null) {
      progress.incomplete = true
      progress.failed = true
      progress.failure = preferredFailure(progress.failure, filtered.failure)
      progress.notes += filtered.failure::class.simpleName.orEmpty()
    }
  }

  private fun logResult(
    progress: LoggingReadProgress,
    timedOut: Boolean,
  ): CloudTelemetryReadResult<CloudLogEntry> {
    val safeEntries =
      progress.filteredEntries
        ?: progress.entries.distinct().filter { entry -> isLifecycleLog(entry.rawMessage()) }
    val renderedEntries =
      safeEntries.mapNotNull(::toCloudLogEntry).distinct().sortedByDescending { it.timestamp }
    val outputTruncated = renderedEntries.size > progress.limit
    val rendered = renderedEntries.take(progress.limit)
    progress.counts.rowsExamined = progress.entries.size.toLong()
    val complete =
      !timedOut &&
        !progress.incomplete &&
        progress.rawQueriesTruncated == 0 &&
        !progress.grpcClassificationIncomplete &&
        !outputTruncated
    if (timedOut) progress.notes += "source deadline exhausted"
    if (progress.rawQueriesTruncated > 0 || outputTruncated) {
      progress.notes += "configured read bound reached"
    }
    if (progress.grpcClassificationIncomplete) {
      progress.notes += "gRPC classification incomplete"
    }
    return CloudTelemetryReadResult(
      rendered,
      complete,
      progress.counts.snapshot(),
      progress.notes.joinToString(),
      progress.failed,
      timedOut,
      CloudLogReadMetadata(
        progress.contextEntriesExamined,
        maxGrpcContextEntries,
        progress.grpcClassificationIncomplete || timedOut,
        progress.entries.size,
        progress.rawLimit,
        progress.rawQueriesTruncated,
        outputTruncated,
      ),
      progress.failure,
    )
  }

  private suspend fun listEntries(
    filter: String,
    pageSize: Int,
    counts: MutableReadCounts,
    context: Boolean,
  ) =
    loggingCall(counts, context) {
      logging.listLogEntries(
        EntryListOption.filter(filter),
        EntryListOption.pageSize(pageSize.coerceAtMost(MAX_PAGE_SIZE)),
        EntryListOption.sortOrder(SortingField.TIMESTAMP, SortingOrder.DESCENDING),
      )
    }

  private suspend fun filterVerboseGrpcEntries(
    entries: List<LogEntry>,
    startTime: Instant,
    endTime: Instant,
    progress: LoggingReadProgress,
  ): GrpcFilterResult {
    if (includeGrpcPayloads) return GrpcFilterResult(entries, 0, false)
    val retained = mutableListOf<LogEntry>()
    val contextByOrigin = mutableMapOf<LogOrigin, GrpcOriginContext>()
    var contextEntriesExamined = progress.contextEntriesExamined
    var classificationIncomplete = false
    var failure: Throwable? = null
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
            try {
              runBlockingContextRead(
                origin,
                startTime.minus(GRPC_CONTEXT_LOOKBACK),
                endTime,
                maxGrpcContextEntries - progress.contextEntriesExamined,
                progress,
              )
            } catch (e: CancellationException) {
              throw e
            } catch (e: Exception) {
              classificationIncomplete = true
              failure = preferredFailure(failure, e)
              GrpcOriginContext(emptyList(), 0, true)
            }
          contextByOrigin[origin] = context
          contextEntriesExamined = progress.contextEntriesExamined
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
    return GrpcFilterResult(retained, contextEntriesExamined, classificationIncomplete, failure)
  }

  private suspend fun runBlockingContextRead(
    origin: LogOrigin,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
    progress: LoggingReadProgress,
  ): GrpcOriginContext {
    if (limit <= 0) return GrpcOriginContext(emptyList(), 0, true)
    val filter = buildGrpcContextFilter(origin, startTime, endTime)
    val entries = mutableListOf<LogEntry>()
    var examined = 0
    var page = listEntries(filter, limit, progress.counts, context = true)
    while (true) {
      for (entry in page.values) {
        if (examined == limit) return GrpcOriginContext(entries, examined, true)
        examined++
        progress.contextEntriesExamined++
        if (entry.logOrigin() == origin) entries += entry
      }
      if (!page.hasNextPage()) return GrpcOriginContext(entries, examined, false)
      if (examined == limit) return GrpcOriginContext(entries, examined, true)
      val currentPage = page
      page = loggingCall(progress.counts, context = true) { currentPage.nextPage }
    }
  }

  private suspend fun <T> loggingCall(
    counts: MutableReadCounts,
    context: Boolean,
    block: () -> T,
  ): T {
    var backoff = INITIAL_RETRY_DELAY
    repeat(MAX_RETRY_ATTEMPTS) { attempt ->
      try {
        return requestThrottler.onReady {
          counts.physicalRequests++
          if (context) counts.loggingContextRequests++
          runInterruptible(blockingContext) { block() }
        }
      } catch (e: CancellationException) {
        throw e
      } catch (e: LoggingException) {
        if (
          (!e.isRetryable && e.code != RESOURCE_EXHAUSTED_CODE) || attempt == MAX_RETRY_ATTEMPTS - 1
        ) {
          throw e
        }
        counts.retryAttempts++
        retryDelay(backoff)
        backoff = backoff.multipliedBy(2).coerceAtMost(MAX_RETRY_DELAY)
      }
    }
    error("unreachable")
  }

  private fun requireComplete(
    result: CloudTelemetryReadResult<CloudLogEntry>
  ): List<CloudLogEntry> {
    if (result.complete) return result.entries
    result.failure?.let { throw it }
    val metadata = checkNotNull(result.logMetadata)
    throw CloudLogCollectionTruncatedException(
      result.entries,
      metadata.contextEntriesExamined,
      metadata.contextEntryLimit,
      metadata.grpcClassificationIncomplete,
      metadata.rawEntriesExamined,
      metadata.rawEntryLimit,
      metadata.rawQueriesTruncated,
      metadata.outputTruncated,
    )
  }

  private fun preferredFailure(current: Throwable?, candidate: Throwable): Throwable {
    if (candidate is LoggingException && candidate.code == RESOURCE_EXHAUSTED_CODE) {
      return candidate
    }
    return current ?: candidate
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
    val failure: Throwable? = null,
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

  private class MutableReadCounts {
    var physicalRequests = 0L
    var retryAttempts = 0L
    var loggingFilterBatches = 0L
    var loggingContextRequests = 0L
    var rowsExamined = 0L

    fun snapshot(): CloudTelemetryReadCounts =
      CloudTelemetryReadCounts(
        physicalRequests = physicalRequests,
        retryAttempts = retryAttempts,
        loggingFilterBatches = loggingFilterBatches,
        loggingContextRequests = loggingContextRequests,
        rowsExamined = rowsExamined,
      )
  }

  private class LoggingReadProgress(val limit: Int, maxGrpcContextEntries: Int) {
    init {
      require(limit > 0) { "limit must be positive" }
    }

    val rawLimit =
      if (limit == Int.MAX_VALUE) limit
      else (limit.toLong() + maxGrpcContextEntries + 1).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
    val entries = mutableListOf<LogEntry>()
    val counts = MutableReadCounts()
    val notes = linkedSetOf<String>()
    var filteredEntries: List<LogEntry>? = null
    var contextEntriesExamined = 0
    var rawQueriesTruncated = 0
    var grpcClassificationIncomplete = false
    var incomplete = false
    var failed = false
    var failure: Throwable? = null
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
    private val INITIAL_RETRY_DELAY: Duration = Duration.ofSeconds(1)
    private val MAX_RETRY_DELAY: Duration = Duration.ofSeconds(8)
    private const val MAX_RETRY_ATTEMPTS = 3
    private const val RESOURCE_EXHAUSTED_CODE = 8
    private val LOGGER_LABEL_KEYS =
      listOf("logger", "logger_name", "loggerName", "logging.googleapis.com/logger")

    internal fun correlationSearchValues(value: String): List<String> {
      return if (value.startsWith("computations/")) {
        listOf(value, value.substringAfterLast('/'))
      } else {
        listOf(value)
      }
    }
  }
}

/** Bounded Google Cloud Trace v1 reader shared by operator diagnostics. */
class GoogleCloudTraceReader(
  private val traceAttributesFor: (String) -> Collection<String>,
  private val credentials: GoogleCredentials =
    GoogleCredentials.getApplicationDefault().createScoped(TRACE_READ_SCOPE),
  private val httpClient: HttpClient = HttpClient.newHttpClient(),
  private val clock: Clock = Clock.systemUTC(),
  private val retryDelay: suspend (Duration) -> Unit = { delay(it.toMillis()) },
  private val blockingContext: CoroutineContext = Dispatchers.IO,
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
    val result =
      readBatch(
        project,
        correlationValues,
        traceIds,
        startTime,
        endTime,
        spanLimit = limit,
        traceIdLimit = readLimit(limit),
      )
    if (result.failed || (!result.complete && result.entries.size <= limit)) {
      throw IllegalStateException("Cloud Trace collection was incomplete: ${result.note}")
    }
    return result.entries
  }

  override suspend fun readBatch(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    spanLimit: Int,
    traceIdLimit: Int,
  ): CloudTelemetryReadResult<CloudTraceSpan> {
    return readBatchInternal(
      project,
      correlationValues,
      traceIds,
      startTime,
      endTime,
      spanLimit,
      traceIdLimit,
      emptySet(),
      null,
    )
  }

  override suspend fun readBatch(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    spanLimit: Int,
    traceIdLimit: Int,
    requiredTraceIds: Set<String>,
  ): CloudTelemetryReadResult<CloudTraceSpan> =
    readBatchInternal(
      project,
      correlationValues,
      traceIds,
      startTime,
      endTime,
      spanLimit,
      traceIdLimit,
      requiredTraceIds,
      null,
    )

  override suspend fun readBatch(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    spanLimit: Int,
    traceIdLimit: Int,
    timeout: Duration,
  ): CloudTelemetryReadResult<CloudTraceSpan> {
    require(!timeout.isNegative && !timeout.isZero) { "timeout must be positive" }
    return readBatchInternal(
      project,
      correlationValues,
      traceIds,
      startTime,
      endTime,
      spanLimit,
      traceIdLimit,
      emptySet(),
      timeout,
    )
  }

  override suspend fun readBatch(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    spanLimit: Int,
    traceIdLimit: Int,
    requiredTraceIds: Set<String>,
    timeout: Duration,
  ): CloudTelemetryReadResult<CloudTraceSpan> {
    require(!timeout.isNegative && !timeout.isZero) { "timeout must be positive" }
    return readBatchInternal(
      project,
      correlationValues,
      traceIds,
      startTime,
      endTime,
      spanLimit,
      traceIdLimit,
      requiredTraceIds,
      timeout,
    )
  }

  private suspend fun readBatchInternal(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    spanLimit: Int,
    traceIdLimit: Int,
    requiredTraceIds: Set<String>,
    timeout: Duration?,
  ): CloudTelemetryReadResult<CloudTraceSpan> {
    require(spanLimit > 0) { "spanLimit must be positive" }
    require(traceIdLimit > 0) { "traceIdLimit must be positive" }
    val progress = TraceReadProgress(spanLimit, startTime, endTime)
    if (timeout == null) {
      collectTrace(
        project,
        correlationValues,
        traceIds,
        startTime,
        endTime,
        traceIdLimit,
        requiredTraceIds,
        progress,
      )
      return progress.result(timedOut = false)
    }
    val completed =
      withTimeoutOrNull(timeout.toMillis().coerceAtLeast(1L)) {
        collectTrace(
          project,
          correlationValues,
          traceIds,
          startTime,
          endTime,
          traceIdLimit,
          requiredTraceIds,
          progress,
        )
        true
      }
    return progress.result(timedOut = completed == null)
  }

  private suspend fun collectTrace(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    traceIdLimit: Int,
    requiredTraceIds: Set<String>,
    progress: TraceReadProgress,
  ) {
    runInterruptible(blockingContext) { credentials.refreshIfExpired() }
    val quota = requestQuotas.getOrPut(project) { RequestQuota(requestThrottlerFactory(project)) }
    for (value in correlationValues.distinct()) {
      for (attribute in traceAttributesFor(value).distinct()) {
        val result =
          listTraceIds(
            project,
            "+$attribute:\"$value\"",
            startTime,
            endTime,
            traceIdLimit,
            quota,
            progress,
          )
        progress.record(result)
      }
    }

    val requestedTraceIds =
      traceIds
        .map { it.substringAfterLast('/') }
        .distinct()
        .onEach { traceId ->
          require(TRACE_ID_PATTERN.matches(traceId)) { "Invalid Cloud Trace trace ID" }
        }
    val lookupRequirements = linkedMapOf<String, Boolean>()
    for (traceId in requestedTraceIds) lookupRequirements.putIfAbsent(traceId, false)
    for (traceId in progress.discoveredTraceIds()) lookupRequirements[traceId] = true
    for (traceId in requiredTraceIds.map { it.substringAfterLast('/') }) {
      require(TRACE_ID_PATTERN.matches(traceId)) { "Invalid required Cloud Trace trace ID" }
      lookupRequirements[traceId] = true
    }
    val admittedTraceIds = lookupRequirements.entries.take(traceIdLimit)
    if (lookupRequirements.size > admittedTraceIds.size) {
      progress.markIncomplete("trace ID limit reached")
    }
    supervisorScope {
      val semaphore = Semaphore(maxConcurrency)
      admittedTraceIds
        .map { (traceId, required) ->
          async {
            val attempt =
              semaphore.withPermit {
                readTrace(project, traceId, required = required, quota = quota, progress = progress)
              }
            progress.record(attempt)
          }
        }
        .awaitAll()
    }
  }

  private suspend fun listTraceIds(
    project: String,
    filter: String,
    startTime: Instant,
    endTime: Instant,
    traceIdLimit: Int,
    quota: RequestQuota,
    progress: TraceReadProgress,
  ): TraceIdDiscovery {
    val traceIds = linkedSetOf<String>()
    var counts = CloudTelemetryReadCounts()
    val notes = linkedSetOf<String>()
    var complete = true
    var failed = false
    var pageToken: String? = null
    do {
      val parameters =
        mutableMapOf(
          "view" to "MINIMAL",
          "pageSize" to readLimit(traceIdLimit).coerceAtMost(MAX_PAGE_SIZE).toString(),
          "startTime" to startTime.toString(),
          "endTime" to endTime.toString(),
          "filter" to filter,
        )
      if (pageToken != null) parameters["pageToken"] = pageToken
      val query =
        parameters.entries.joinToString("&") { (key, value) -> "$key=${urlEncode(value)}" }
      val request = request(project, "traces?$query")
      val sendResult =
        send(request, LIST_QUOTA_UNITS, RequestKind.LIST_TRACE, quota, progress::addCounts)
      counts += sendResult.counts
      val response = sendResult.response
      if (response == null || response.statusCode() !in 200..299) {
        complete = false
        failed = true
        notes += sendResult.note.ifEmpty { "ListTraces failed" }
        break
      }
      val root = JsonParser.parseString(response.body()).asJsonObject
      val pageTraceIds =
        root
          .getAsJsonArray("traces")
          ?.mapNotNull { element -> element.asJsonObject.optionalString("traceId") }
          .orEmpty()
      for (traceId in pageTraceIds) {
        if (TRACE_ID_PATTERN.matches(traceId)) {
          traceIds += traceId
          progress.addDiscoveredTraceId(traceId)
        } else {
          complete = false
          failed = true
          notes += "ListTraces returned an invalid trace ID"
        }
      }
      pageToken = root.optionalString("nextPageToken")
    } while (pageToken != null && traceIds.size < readLimit(traceIdLimit))
    if (pageToken != null || traceIds.size > traceIdLimit) {
      complete = false
      notes += "trace ID limit reached"
    }
    return TraceIdDiscovery(
      traceIds.take(readLimit(traceIdLimit)),
      complete,
      counts,
      notes.joinToString(),
      failed,
    )
  }

  private suspend fun readTrace(
    project: String,
    traceId: String,
    required: Boolean,
    quota: RequestQuota,
    progress: TraceReadProgress,
  ): TraceReadAttempt {
    val sendResult =
      send(
        request(project, "traces/$traceId"),
        GET_QUOTA_UNITS,
        RequestKind.GET_TRACE,
        quota,
        progress::addCounts,
      )
    val response = sendResult.response
    if (response == null) {
      return TraceReadAttempt(emptyList(), false, sendResult.counts, sendResult.note, true)
    }
    if (response.statusCode() == 404) {
      return TraceReadAttempt(
        emptyList(),
        !required,
        sendResult.counts,
        if (required) "GetTrace returned HTTP 404 for a discovered trace" else "",
        required,
      )
    }
    if (response.statusCode() !in 200..299) {
      return TraceReadAttempt(
        emptyList(),
        false,
        sendResult.counts,
        sendResult.note.ifEmpty { "GetTrace returned HTTP ${response.statusCode()}" },
        true,
      )
    }
    return try {
      TraceReadAttempt(
        parseResponse(project, JsonParser.parseString(response.body()).asJsonObject, traceId),
        true,
        sendResult.counts,
        "",
        false,
      )
    } catch (e: Exception) {
      TraceReadAttempt(emptyList(), false, sendResult.counts, e::class.simpleName.orEmpty(), true)
    }
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
    requestKind: RequestKind,
    quota: RequestQuota,
    count: (CloudTelemetryReadCounts) -> Unit = {},
  ): HttpSendResult {
    var counts = CloudTelemetryReadCounts()
    var backoff = INITIAL_RETRY_DELAY
    repeat(MAX_RETRY_ATTEMPTS) { attempt ->
      quota.mutex.withLock { repeat(quotaUnits) { quota.throttler.onReady {} } }
      val requestCounts =
        CloudTelemetryReadCounts(
          physicalRequests = 1,
          listTraceRequests = if (requestKind == RequestKind.LIST_TRACE) 1 else 0,
          getTraceRequests = if (requestKind == RequestKind.GET_TRACE) 1 else 0,
          traceQuotaUnits = quotaUnits.toLong(),
        )
      counts += requestCounts
      count(requestCounts)
      val response =
        try {
          runInterruptible(blockingContext) {
            httpClient.send(request, HttpResponse.BodyHandlers.ofString())
          }
        } catch (e: CancellationException) {
          throw e
        } catch (e: IOException) {
          if (attempt == MAX_RETRY_ATTEMPTS - 1) {
            return HttpSendResult(null, counts, e::class.simpleName.orEmpty())
          }
          val retryCounts = CloudTelemetryReadCounts(retryAttempts = 1)
          counts += retryCounts
          count(retryCounts)
          retryDelay(backoff)
          backoff = backoff.multipliedBy(2).coerceAtMost(MAX_RETRY_DELAY)
          return@repeat
        } catch (e: Exception) {
          return HttpSendResult(null, counts, e::class.simpleName.orEmpty())
        }
      if (!isRetryable(response.statusCode()) || attempt == MAX_RETRY_ATTEMPTS - 1) {
        return HttpSendResult(
          response,
          counts,
          if (response.statusCode() in 200..299 || response.statusCode() == 404) ""
          else "Cloud Trace API returned HTTP ${response.statusCode()}",
        )
      }
      val retryCounts = CloudTelemetryReadCounts(retryAttempts = 1)
      counts += retryCounts
      count(retryCounts)
      retryDelay(retryAfter(response) ?: backoff)
      backoff = backoff.multipliedBy(2).coerceAtMost(MAX_RETRY_DELAY)
    }
    error("unreachable")
  }

  private fun retryAfter(response: HttpResponse<String>): Duration? {
    val value = response.headers().firstValue("Retry-After").orElse(null) ?: return null
    value.toLongOrNull()?.let {
      return Duration.ofSeconds(it.coerceAtLeast(0L))
    }
    val retryAt =
      runCatching { ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant() }
        .getOrNull() ?: return null
    return Duration.between(clock.instant(), retryAt).coerceAtLeast(Duration.ZERO)
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

  private data class TraceIdDiscovery(
    val traceIds: List<String>,
    val complete: Boolean,
    val counts: CloudTelemetryReadCounts,
    val note: String,
    val failed: Boolean,
  )

  private data class TraceReadAttempt(
    val spans: List<CloudTraceSpan>,
    val complete: Boolean,
    val counts: CloudTelemetryReadCounts,
    val note: String,
    val failed: Boolean,
  )

  private data class HttpSendResult(
    val response: HttpResponse<String>?,
    val counts: CloudTelemetryReadCounts,
    val note: String,
  )

  private enum class RequestKind {
    LIST_TRACE,
    GET_TRACE,
  }

  private class TraceReadProgress(
    private val spanLimit: Int,
    private val startTime: Instant,
    private val endTime: Instant,
  ) {
    private var counts = CloudTelemetryReadCounts()
    private var complete = true
    private var failed = false
    private val notes = linkedSetOf<String>()
    private val discoveredTraceIds = linkedSetOf<String>()
    private val spans = mutableListOf<CloudTraceSpan>()

    @Synchronized
    fun addCounts(value: CloudTelemetryReadCounts) {
      counts += value
    }

    @Synchronized
    fun addDiscoveredTraceId(traceId: String) {
      discoveredTraceIds += traceId
    }

    @Synchronized fun discoveredTraceIds(): Set<String> = discoveredTraceIds.toSet()

    @Synchronized
    fun record(result: TraceIdDiscovery) {
      if (!result.complete) {
        complete = false
        failed = failed || result.failed
        if (result.note.isNotEmpty()) notes += result.note
      }
    }

    @Synchronized
    fun record(attempt: TraceReadAttempt) {
      spans += attempt.spans
      if (!attempt.complete) {
        complete = false
        failed = failed || attempt.failed
        if (attempt.note.isNotEmpty()) notes += attempt.note
      }
    }

    @Synchronized
    fun markIncomplete(note: String) {
      complete = false
      notes += note
    }

    @Synchronized
    fun result(timedOut: Boolean): CloudTelemetryReadResult<CloudTraceSpan> {
      val matchingSpans =
        spans
          .filter {
            !it.startTime.isAfter(endTime) && !(it.endTime ?: it.startTime).isBefore(startTime)
          }
          .distinct()
      if (matchingSpans.size > spanLimit) {
        complete = false
        notes += "span limit reached"
      }
      if (timedOut) {
        complete = false
        notes += "source deadline exhausted"
      }
      return CloudTelemetryReadResult(
        retainSpans(matchingSpans, readLimit(spanLimit)),
        complete,
        counts.copy(rowsExamined = matchingSpans.size.toLong()),
        notes.joinToString(),
        failed,
        timedOut,
      )
    }
  }

  companion object {
    private const val TRACE_READ_SCOPE = "https://www.googleapis.com/auth/trace.readonly"
    private const val DEFAULT_MAX_CONCURRENCY = 8
    private const val MAX_PAGE_SIZE = 1_000
    private const val LIST_QUOTA_UNITS = 25
    private const val GET_QUOTA_UNITS = 1
    private const val MAX_RETRY_ATTEMPTS = 3
    private val HTTP_TIMEOUT = Duration.ofSeconds(30)
    private val INITIAL_RETRY_DELAY = Duration.ofSeconds(1)
    private val MAX_RETRY_DELAY = Duration.ofSeconds(8)
    private val TRACE_ID_PATTERN = Regex("(?i)[0-9a-f]{32}")

    private fun isRetryable(statusCode: Int): Boolean {
      return statusCode == 408 || statusCode == 429 || statusCode in 500..599
    }

    private fun urlEncode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)

    internal fun readLimit(limit: Int): Int = if (limit == Int.MAX_VALUE) limit else limit + 1

    internal fun retainSpans(spans: List<CloudTraceSpan>, limit: Int): List<CloudTraceSpan> {
      if (spans.size <= limit) return spans
      val failures = spans.filter { isFailureOutcome(it.attributes["xmm.outcome"]) }
      return (failures + spans.sortedByDescending { it.startTime }).distinct().take(limit)
    }

    private fun isFailureOutcome(outcome: String?): Boolean {
      val normalized = outcome?.lowercase() ?: return false
      return normalized in
        setOf("failed", "failure", "error", "refused", "report_failed", "permanent_failure") ||
        normalized.startsWith("failed_")
    }
  }
}

private fun JsonObject.requiredString(name: String): String = get(name).asString

private fun JsonObject.optionalString(name: String): String? =
  get(name)?.takeUnless { it.isJsonNull }?.asString

private const val LOGGING_READ_SCOPE = "https://www.googleapis.com/auth/logging.read"
