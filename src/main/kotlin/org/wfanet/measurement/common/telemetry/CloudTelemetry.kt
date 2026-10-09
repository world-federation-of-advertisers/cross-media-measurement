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

import java.time.Instant
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

class CloudLogCollectionTruncatedException(
  val partialEntries: List<CloudLogEntry>,
  val contextEntriesExamined: Int,
  val contextEntryLimit: Int,
  val grpcClassificationIncomplete: Boolean,
  val rawEntriesExamined: Int,
  val rawEntryLimit: Int,
  val rawQueriesTruncated: Int,
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
