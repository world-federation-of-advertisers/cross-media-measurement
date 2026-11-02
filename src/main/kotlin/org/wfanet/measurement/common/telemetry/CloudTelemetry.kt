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

import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.withTimeoutOrNull
import org.wfanet.measurement.common.throttler.Throttler

/** Metadata-only representation of a Cloud Logging entry. */
data class CloudLogEntry(
  val sourceProject: String,
  val timestamp: Instant,
  val service: String,
  val severity: String,
  val trace: String?,
  val message: String,
)

/** Metadata-only representation of a Cloud Trace v1 span. */
data class CloudTraceSpan(
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

/** Completeness of one bounded telemetry source read. */
data class CloudTelemetrySourceStatus(
  val project: String,
  val source: String,
  val status: String,
  val fetched: Int,
  val retained: Int,
  val note: String = "",
)

/** Physical API work performed by one bounded telemetry read. */
data class CloudTelemetryReadCounts(
  val physicalRequests: Long = 0,
  val retryAttempts: Long = 0,
  val listTraceRequests: Long = 0,
  val getTraceRequests: Long = 0,
  val traceQuotaUnits: Long = 0,
  val loggingFilterBatches: Long = 0,
  val loggingContextRequests: Long = 0,
  val rowsExamined: Long = 0,
) {
  operator fun plus(other: CloudTelemetryReadCounts): CloudTelemetryReadCounts {
    return CloudTelemetryReadCounts(
      physicalRequests + other.physicalRequests,
      retryAttempts + other.retryAttempts,
      listTraceRequests + other.listTraceRequests,
      getTraceRequests + other.getTraceRequests,
      traceQuotaUnits + other.traceQuotaUnits,
      loggingFilterBatches + other.loggingFilterBatches,
      loggingContextRequests + other.loggingContextRequests,
      rowsExamined + other.rowsExamined,
    )
  }

  fun toMap(): Map<String, Long> = buildMap {
    put("physical_requests", physicalRequests)
    put("retry_attempts", retryAttempts)
    if (listTraceRequests > 0) put("list_trace_requests", listTraceRequests)
    if (getTraceRequests > 0) put("get_trace_requests", getTraceRequests)
    if (traceQuotaUnits > 0) put("trace_quota_units", traceQuotaUnits)
    if (loggingFilterBatches > 0) put("logging_filter_batches", loggingFilterBatches)
    if (loggingContextRequests > 0) put("logging_context_requests", loggingContextRequests)
    put("rows_examined", rowsExamined)
  }
}

/** Exact bounds and observations used by one Cloud Logging read. */
data class CloudLogReadMetadata(
  val contextEntriesExamined: Int,
  val contextEntryLimit: Int,
  val grpcClassificationIncomplete: Boolean,
  val rawEntriesExamined: Int,
  val rawEntryLimit: Int,
  val rawQueriesTruncated: Int,
  val outputTruncated: Boolean = false,
)

/** Result of a telemetry read that can preserve evidence from an incomplete request set. */
data class CloudTelemetryReadResult<T>(
  val entries: List<T>,
  val complete: Boolean,
  val counts: CloudTelemetryReadCounts,
  val note: String = "",
  val failed: Boolean = false,
  val timedOut: Boolean = false,
  val logMetadata: CloudLogReadMetadata? = null,
  val failure: Throwable? = null,
)

class CloudLogCollectionTruncatedException(
  val partialEntries: List<CloudLogEntry>,
  val contextEntriesExamined: Int,
  val contextEntryLimit: Int,
  val grpcClassificationIncomplete: Boolean,
  val rawEntriesExamined: Int,
  val rawEntryLimit: Int,
  val rawQueriesTruncated: Int,
  val outputTruncated: Boolean = false,
) : Exception("Cloud Logging collection was truncated before it could be classified completely")

/** Reads bounded log evidence by stable correlation value or trace identity. */
fun interface CloudLogReader {
  suspend fun read(
    correlationValues: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<CloudLogEntry>

  suspend fun readTraceIds(
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<CloudLogEntry> = emptyList()

  suspend fun readBatch(
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): CloudTelemetryReadResult<CloudLogEntry> {
    val entries =
      (if (correlationValues.isEmpty()) emptyList()
      else read(correlationValues, startTime, endTime, limit)) +
        (if (traceIds.isEmpty()) emptyList() else readTraceIds(traceIds, startTime, endTime, limit))
    return CloudTelemetryReadResult(
      entries.distinct(),
      complete = true,
      CloudTelemetryReadCounts(
        physicalRequests =
          (if (correlationValues.isEmpty()) 0L else 1L) + (if (traceIds.isEmpty()) 0L else 1L),
        rowsExamined = entries.size.toLong(),
      ),
    )
  }

  suspend fun readBatch(
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
    timeout: Duration,
  ): CloudTelemetryReadResult<CloudLogEntry> {
    require(!timeout.isNegative && !timeout.isZero) { "timeout must be positive" }
    var result: CloudTelemetryReadResult<CloudLogEntry>? = null
    val completed =
      withTimeoutOrNull(timeout.toMillis().coerceAtLeast(1L)) {
        result = readBatch(correlationValues, traceIds, startTime, endTime, limit)
        true
      }
    return if (completed == null) {
      CloudTelemetryReadResult(
        emptyList(),
        complete = false,
        CloudTelemetryReadCounts(),
        note = "source deadline exhausted",
        timedOut = true,
      )
    } else {
      checkNotNull(result)
    }
  }

  fun withRequestThrottler(requestThrottler: Throttler): CloudLogReader = this
}

/** Reads bounded spans by stable correlation value or trace identity. */
fun interface CloudTraceReader {
  suspend fun read(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    limit: Int,
  ): List<CloudTraceSpan>

  suspend fun readBatch(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    spanLimit: Int,
    traceIdLimit: Int,
  ): CloudTelemetryReadResult<CloudTraceSpan> {
    val entries = read(project, correlationValues, traceIds, startTime, endTime, spanLimit)
    return CloudTelemetryReadResult(
      entries,
      complete = true,
      CloudTelemetryReadCounts(physicalRequests = 1, rowsExamined = entries.size.toLong()),
    )
  }

  suspend fun readBatch(
    project: String,
    correlationValues: Collection<String>,
    traceIds: Collection<String>,
    startTime: Instant,
    endTime: Instant,
    spanLimit: Int,
    traceIdLimit: Int,
    requiredTraceIds: Set<String>,
  ): CloudTelemetryReadResult<CloudTraceSpan> =
    readBatch(project, correlationValues, traceIds, startTime, endTime, spanLimit, traceIdLimit)

  suspend fun readBatch(
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
    var result: CloudTelemetryReadResult<CloudTraceSpan>? = null
    val completed =
      withTimeoutOrNull(timeout.toMillis().coerceAtLeast(1L)) {
        result =
          readBatch(
            project,
            correlationValues,
            traceIds,
            startTime,
            endTime,
            spanLimit,
            traceIdLimit,
          )
        true
      }
    return if (completed == null) {
      CloudTelemetryReadResult(
        emptyList(),
        complete = false,
        CloudTelemetryReadCounts(),
        note = "source deadline exhausted",
        timedOut = true,
      )
    } else {
      checkNotNull(result)
    }
  }

  suspend fun readBatch(
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
    var result: CloudTelemetryReadResult<CloudTraceSpan>? = null
    val completed =
      withTimeoutOrNull(timeout.toMillis().coerceAtLeast(1L)) {
        result =
          readBatch(
            project,
            correlationValues,
            traceIds,
            startTime,
            endTime,
            spanLimit,
            traceIdLimit,
            requiredTraceIds,
          )
        true
      }
    return if (completed == null) {
      CloudTelemetryReadResult(
        emptyList(),
        complete = false,
        CloudTelemetryReadCounts(),
        note = "source deadline exhausted",
        timedOut = true,
      )
    } else {
      checkNotNull(result)
    }
  }

  fun withMaxConcurrency(maxConcurrency: Int): CloudTraceReader = this

  fun withRequestThrottlerFactory(
    requestThrottlerFactory: (String) -> Throttler
  ): CloudTraceReader = this
}

/** Parses only explicitly allowlisted `key=value` fields from rendered structured logs. */
class SafeTelemetryText(private val fieldNames: Set<String>) {
  private val pattern =
    Regex("(?:^|\\s)(" + fieldNames.joinToString("|") { Regex.escape(it) } + ")=([^\\s]+)")

  fun fields(text: String): Map<String, String> = buildMap {
    for (match in pattern.findAll(text)) {
      putIfAbsent(match.groupValues[1], match.groupValues[2])
    }
  }

  companion object {
    fun markdown(value: String): String {
      return value.replace("|", "\\|").replace("`", "'").replace("\r", " ").replace("\n", " ")
    }
  }
}
