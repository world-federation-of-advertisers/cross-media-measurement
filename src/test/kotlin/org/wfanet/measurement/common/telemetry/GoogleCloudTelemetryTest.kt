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

import com.google.api.gax.paging.Page
import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.logging.LogEntry
import com.google.cloud.logging.Logging
import com.google.cloud.logging.LoggingException
import com.google.cloud.logging.Payload
import com.google.cloud.logging.SourceLocation
import com.google.common.truth.Truth.assertThat
import java.net.http.HttpClient
import java.net.http.HttpHeaders
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.Collections
import java.util.Date
import kotlin.test.assertFailsWith
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class GoogleCloudTelemetryTest {
  @Test
  fun `logging options disable hidden transport retries`() {
    val options = buildCloudTelemetryLoggingOptions("logging-project", credentials())

    assertThat(options.retrySettings.maxAttempts).isEqualTo(1)
  }

  @Test
  fun `log reader reports rendered output truncation`() = runBlocking {
    val logging = mock<Logging>()
    val page = mock<Page<LogEntry>>()
    val entries =
      (1..2).map { index ->
        LogEntry.newBuilder(
            Payload.StringPayload.of(
              "event=event-$index xmm.lifecycle.stage=label xmm.report.name=reports/report-1"
            )
          )
          .setLogName("projects/logging-project/logs/stdout")
          .setTimestamp(NOW.plusMillis(index.toLong()))
          .build()
      }
    whenever(page.values).thenReturn(entries)
    whenever(page.hasNextPage()).thenReturn(false)
    whenever(logging.listLogEntries(any(), any(), any())).thenReturn(page)
    val reader =
      GoogleCloudLogReader(
        "logging-project",
        logging,
        setOf("event", "xmm.lifecycle.stage", "xmm.report.name"),
        setOf("xmm.report.name"),
      )

    val error =
      assertFailsWith<CloudLogCollectionTruncatedException> {
        reader.read(listOf("reports/report-1"), NOW.minusSeconds(1), NOW.plusSeconds(1), 1)
      }

    assertThat(error.partialEntries).hasSize(1)
    assertThat(error.contextEntriesExamined).isEqualTo(0)
    assertThat(error.contextEntryLimit).isEqualTo(5_000)
    assertThat(error.grpcClassificationIncomplete).isFalse()
    assertThat(error.rawEntriesExamined).isEqualTo(2)
    assertThat(error.rawEntryLimit).isEqualTo(5_002)
    assertThat(error.rawQueriesTruncated).isEqualTo(0)
  }

  @Test
  fun `correlation search preserves bare computation id`() {
    assertThat(GoogleCloudLogReader.correlationSearchValues("computations/abc"))
      .containsExactly("computations/abc", "abc")
      .inOrder()
  }

  @Test
  fun `trace reader preserves unbounded sentinel`() {
    assertThat(GoogleCloudTraceReader.readLimit(Int.MAX_VALUE)).isEqualTo(Int.MAX_VALUE)
  }

  @Test
  fun `trace retention keeps older failures`() {
    val failure = span("failure", NOW.minusSeconds(100), mapOf("xmm.outcome" to "failed_writeback"))
    val permanentFailure =
      span("permanent-failure", NOW.minusSeconds(100), mapOf("xmm.outcome" to "permanent_failure"))
    val newest = span("newest", NOW, emptyMap())

    assertThat(GoogleCloudTraceReader.retainSpans(listOf(newest, failure), 1))
      .containsExactly(failure)
    assertThat(GoogleCloudTraceReader.retainSpans(listOf(newest, permanentFailure), 1))
      .containsExactly(permanentFailure)
  }

  @Test
  fun `log reader preserves ordinary JSON application message`() = runBlocking {
    val logging = mock<Logging>()
    val page = mock<Page<LogEntry>>()
    val logEntry =
      LogEntry.newBuilder(
          Payload.JsonPayload.of(
            mapOf(
              "message" to "The report worker failed validation",
              "xmm.report.name" to "reports/report-1",
            )
          )
        )
        .setLogName("projects/logging-project/logs/stdout")
        .setTimestamp(NOW)
        .build()
    whenever(page.values).thenReturn(listOf(logEntry))
    whenever(page.hasNextPage()).thenReturn(false)
    whenever(logging.listLogEntries(any(), any(), any())).thenReturn(page)
    val reader =
      GoogleCloudLogReader(
        project = "logging-project",
        logging = logging,
        safeFields = setOf("xmm.report.name"),
        correlationFields = setOf("xmm.report.name"),
      )

    val entries =
      reader.read(listOf("reports/report-1"), NOW.minusSeconds(1), NOW.plusSeconds(1), 100)

    assertThat(entries.single().message).contains("The report worker failed validation")
    assertThat(entries.single().message).contains("xmm.report.name=reports/report-1")
    Unit
  }

  @Test
  fun `log reader excludes split gRPC payload using source context`() = runBlocking {
    val logging = mock<Logging>()
    val correlatedPage = mock<Page<LogEntry>>()
    val grpcContextPage = mock<Page<LogEntry>>()
    val applicationContextPage = mock<Page<LogEntry>>()
    val logName = "projects/logging-project/logs/stdout"
    val grpcSource =
      SourceLocation.newBuilder().setFunction("wfa.measurement.Service.Create").build()
    val applicationSource =
      SourceLocation.newBuilder().setFunction("org.example.Worker.process").build()
    val grpcPreamble =
      LogEntry.newBuilder(Payload.StringPayload.of("[grpc-worker] gRPC request-1 request:"))
        .setLogName(logName)
        .setSourceLocation(grpcSource)
        .setTimestamp(NOW.minusMillis(2))
        .build()
    val grpcContinuation =
      LogEntry.newBuilder(Payload.StringPayload.of("basic_report: \"reports/report-1\""))
        .setLogName(logName)
        .setSourceLocation(grpcSource)
        .setTimestamp(NOW.minusMillis(1))
        .build()
    val applicationError =
      LogEntry.newBuilder(Payload.StringPayload.of("status: failed reports/report-1"))
        .setLogName(logName)
        .setSourceLocation(applicationSource)
        .setTimestamp(NOW)
        .build()
    whenever(correlatedPage.values).thenReturn(listOf(applicationError, grpcContinuation))
    whenever(correlatedPage.hasNextPage()).thenReturn(false)
    whenever(applicationContextPage.values).thenReturn(listOf(applicationError))
    whenever(applicationContextPage.hasNextPage()).thenReturn(false)
    whenever(grpcContextPage.values).thenReturn(listOf(grpcContinuation, grpcPreamble))
    whenever(grpcContextPage.hasNextPage()).thenReturn(false)
    whenever(logging.listLogEntries(any(), any(), any()))
      .thenReturn(correlatedPage, applicationContextPage, grpcContextPage)
    val reader =
      GoogleCloudLogReader(
        project = "logging-project",
        logging = logging,
        safeFields = setOf("event", "xmm.lifecycle.stage"),
        correlationFields = setOf("xmm.report.name"),
      )

    val entries =
      reader.read(listOf("reports/report-1"), NOW.minusSeconds(1), NOW.plusSeconds(1), 100)

    assertThat(entries.map { it.message }).containsExactly("status: failed reports/report-1")
    Unit
  }

  @Test
  fun `trace reader discovers 32 shard traces with one minimal list request`() = runBlocking {
    val traceIds = (0 until 32).map { it.toString(16).padStart(32, '0') }
    val requests = Collections.synchronizedList(mutableListOf<HttpRequest>())
    val httpClient = mock<HttpClient>()
    whenever(httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())).thenAnswer {
      invocation ->
      val request = invocation.arguments[0] as HttpRequest
      requests += request
      val traceId = request.uri().path.substringAfterLast('/')
      if (traceId == "traces") {
        httpResponse(
          200,
          "{\"traces\":[" + traceIds.joinToString { "{\"traceId\":\"$it\"}" } + "]}",
        )
      } else {
        httpResponse(
          200,
          """{"traceId":"$traceId","spans":[{"spanId":"span-$traceId","name":"label","startTime":"2026-09-10T12:00:00Z","endTime":"2026-09-10T12:00:01Z","labels":{"xmm.lifecycle.stage":"label"}}]}""",
        )
      }
    }
    val reader =
      GoogleCloudTraceReader(
        traceAttributesFor = { listOf("xmm.edpa.raw_impression_upload.name") },
        credentials = credentials(),
        httpClient = httpClient,
      )

    val result =
      reader.readBatch(
        "trace-project",
        listOf("dataProviders/123/rawImpressionUploads/upload-1"),
        emptyList(),
        NOW.minusSeconds(1),
        NOW.plusSeconds(2),
        spanLimit = 100,
        traceIdLimit = 100,
      )

    assertThat(result.complete).isTrue()
    assertThat(result.entries).hasSize(32)
    assertThat(result.counts.listTraceRequests).isEqualTo(1)
    assertThat(result.counts.getTraceRequests).isEqualTo(32)
    assertThat(result.counts.physicalRequests).isEqualTo(33)
    assertThat(result.counts.traceQuotaUnits).isEqualTo(57)
    assertThat(requests.single { it.uri().path.endsWith("/traces") }.uri().query)
      .contains("view=MINIMAL")
    assertThat(requests.any { it.uri().toString().contains("view=COMPLETE") }).isFalse()
    Unit
  }

  @Test
  fun `trace reader honors Retry-After and counts retry quota`() = runBlocking {
    val httpClient = mock<HttpClient>()
    val throttledResponse = httpResponse(429, "", mapOf("Retry-After" to listOf("7")))
    val successResponse = httpResponse(200, "{\"traces\":[]}")
    whenever(httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()))
      .thenReturn(throttledResponse, successResponse)
    val delays = mutableListOf<Duration>()
    val reader =
      GoogleCloudTraceReader(
        traceAttributesFor = { listOf("xmm.edpa.raw_impression_upload.name") },
        credentials = credentials(),
        httpClient = httpClient,
        retryDelay = { delays += it },
      )

    val result =
      reader.readBatch(
        "trace-project",
        listOf("dataProviders/123/rawImpressionUploads/upload-1"),
        emptyList(),
        NOW.minusSeconds(1),
        NOW.plusSeconds(1),
        spanLimit = 100,
        traceIdLimit = 100,
      )

    assertThat(result.complete).isTrue()
    assertThat(delays).containsExactly(Duration.ofSeconds(7))
    assertThat(result.counts.physicalRequests).isEqualTo(2)
    assertThat(result.counts.retryAttempts).isEqualTo(1)
    assertThat(result.counts.traceQuotaUnits).isEqualTo(50)
    Unit
  }

  @Test
  fun `trace reader preserves successful shards when one GetTrace exhausts retries`() =
    runBlocking {
      val traceIds = (0 until 32).map { it.toString(16).padStart(32, '0') }
      val failedTraceId = traceIds.last()
      val httpClient = mock<HttpClient>()
      whenever(httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())).thenAnswer {
        invocation ->
        val request = invocation.arguments[0] as HttpRequest
        val traceId = request.uri().path.substringAfterLast('/')
        when {
          traceId == "traces" ->
            httpResponse(
              200,
              "{\"traces\":[" + traceIds.joinToString { "{\"traceId\":\"$it\"}" } + "]}",
            )
          traceId == failedTraceId -> httpResponse(503, "")
          else ->
            httpResponse(
              200,
              """{"traceId":"$traceId","spans":[{"spanId":"span-$traceId","name":"label","startTime":"2026-09-10T12:00:00Z","endTime":"2026-09-10T12:00:01Z","labels":{"xmm.lifecycle.stage":"label"}}]}""",
            )
        }
      }
      val reader =
        GoogleCloudTraceReader(
          traceAttributesFor = { listOf("xmm.edpa.raw_impression_upload.name") },
          credentials = credentials(),
          httpClient = httpClient,
          retryDelay = {},
        )

      val result =
        reader.readBatch(
          "trace-project",
          listOf("dataProviders/123/rawImpressionUploads/upload-1"),
          emptyList(),
          NOW.minusSeconds(1),
          NOW.plusSeconds(2),
          spanLimit = 100,
          traceIdLimit = 100,
        )

      assertThat(result.complete).isFalse()
      assertThat(result.entries).hasSize(31)
      assertThat(result.counts.listTraceRequests).isEqualTo(1)
      assertThat(result.counts.getTraceRequests).isEqualTo(34)
      assertThat(result.counts.retryAttempts).isEqualTo(2)
      assertThat(result.note).contains("HTTP 503")
      Unit
    }

  @Test
  fun `trace reader deadline preserves fetched spans and physical counts`() = runTest {
    val traceIds = listOf("1".padStart(32, '0'), "2".padStart(32, '0'))
    val httpClient = mock<HttpClient>()
    var getRequests = 0
    whenever(httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())).thenAnswer {
      invocation ->
      val request = invocation.arguments[0] as HttpRequest
      val traceId = request.uri().path.substringAfterLast('/')
      if (traceId == "traces") {
        httpResponse(
          200,
          "{\"traces\":[" + traceIds.joinToString { "{\"traceId\":\"$it\"}" } + "]}",
        )
      } else if (getRequests++ == 0) {
        httpResponse(
          200,
          """{"traceId":"$traceId","spans":[{"spanId":"span-$traceId","name":"label","startTime":"2026-09-10T12:00:00Z","endTime":"2026-09-10T12:00:01Z","labels":{"xmm.lifecycle.stage":"label"}}]}""",
        )
      } else {
        httpResponse(503, "")
      }
    }
    val reader =
      GoogleCloudTraceReader(
          traceAttributesFor = { listOf("xmm.edpa.raw_impression_upload.name") },
          credentials = credentials(),
          httpClient = httpClient,
          retryDelay = { delay(it.toMillis()) },
          blockingContext = StandardTestDispatcher(testScheduler),
        )
        .withMaxConcurrency(1)

    val result =
      reader.readBatch(
        "trace-project",
        listOf("dataProviders/123/rawImpressionUploads/upload-1"),
        emptyList(),
        NOW.minusSeconds(1),
        NOW.plusSeconds(2),
        spanLimit = 100,
        traceIdLimit = 100,
        timeout = Duration.ofMillis(500),
      )

    assertThat(result.timedOut).isTrue()
    assertThat(result.complete).isFalse()
    assertThat(result.entries).hasSize(1)
    assertThat(result.counts.physicalRequests).isEqualTo(3)
    assertThat(result.counts.listTraceRequests).isEqualTo(1)
    assertThat(result.counts.getTraceRequests).isEqualTo(2)
    assertThat(result.counts.retryAttempts).isEqualTo(1)
    assertThat(result.counts.traceQuotaUnits).isEqualTo(27)
  }

  @Test
  fun `log reader batches 32 shard predicates and counts retry fallback`() = runBlocking {
    val logging = mock<Logging>()
    val page = mock<Page<LogEntry>>()
    val retryableError = mock<LoggingException>()
    whenever(retryableError.isRetryable).thenReturn(false)
    whenever(retryableError.code).thenReturn(8)
    whenever(page.values).thenReturn(emptyList())
    whenever(page.hasNextPage()).thenReturn(false)
    whenever(logging.listLogEntries(any(), any(), any())).thenThrow(retryableError).thenReturn(page)
    val delays = mutableListOf<Duration>()
    val reader =
      GoogleCloudLogReader(
        project = "logging-project",
        logging = logging,
        safeFields = setOf("xmm.edpa.pool_assignment_job.name"),
        correlationFields = setOf("xmm.edpa.pool_assignment_job.name"),
        correlationFieldsFor = { listOf("xmm.edpa.pool_assignment_job.name") },
        retryDelay = { delays += it },
      )
    val shardJobs =
      (0 until 32).map { index ->
        "dataProviders/123/rawImpressionUploads/upload-1/poolAssignmentJobs/shard-$index"
      }

    val result =
      reader.readBatch(shardJobs, emptyList(), NOW.minusSeconds(1), NOW.plusSeconds(1), 100)

    assertThat(result.complete).isTrue()
    assertThat(result.counts.loggingFilterBatches).isLessThan(32)
    assertThat(result.counts.physicalRequests).isEqualTo(result.counts.loggingFilterBatches + 1)
    assertThat(result.counts.retryAttempts).isEqualTo(1)
    assertThat(delays).containsExactly(Duration.ofSeconds(1))
    Unit
  }

  @Test
  fun `log reader deadline preserves fetched entries and exact metadata`() = runTest {
    val logging = mock<Logging>()
    val page = mock<Page<LogEntry>>()
    val retryableError = mock<LoggingException>()
    whenever(retryableError.isRetryable).thenReturn(true)
    val entry =
      LogEntry.newBuilder(
          Payload.StringPayload.of(
            "event=pool_assignment xmm.lifecycle.stage=pool_assignment " +
              "xmm.edpa.pool_assignment_job.name=job-1"
          )
        )
        .setLogName("projects/logging-project/logs/stdout")
        .setTimestamp(NOW)
        .build()
    whenever(page.values).thenReturn(listOf(entry))
    whenever(page.hasNextPage()).thenReturn(false)
    whenever(logging.listLogEntries(any(), any(), any())).thenReturn(page).thenThrow(retryableError)
    val fields = (0 until 15).map { "field-$it" }.toSet()
    val reader =
      GoogleCloudLogReader(
        project = "logging-project",
        logging = logging,
        safeFields = setOf("event", "xmm.lifecycle.stage", "xmm.edpa.pool_assignment_job.name"),
        correlationFields = fields,
        correlationFieldsFor = { fields },
        maxGrpcContextEntries = 0,
        retryDelay = { delay(it.toMillis()) },
        blockingContext = StandardTestDispatcher(testScheduler),
      )
    val shardJobs =
      (0 until 32).map { index ->
        "dataProviders/123/rawImpressionUploads/upload-1/poolAssignmentJobs/shard-$index"
      }

    val result =
      reader.readBatch(
        shardJobs,
        emptyList(),
        NOW.minusSeconds(1),
        NOW.plusSeconds(1),
        100,
        Duration.ofMillis(500),
      )

    assertThat(result.timedOut).isTrue()
    assertThat(result.complete).isFalse()
    assertThat(result.entries).hasSize(1)
    assertThat(result.counts.physicalRequests).isEqualTo(2)
    assertThat(result.counts.retryAttempts).isEqualTo(1)
    assertThat(result.counts.loggingFilterBatches).isGreaterThan(1)
    val metadata = checkNotNull(result.logMetadata)
    assertThat(metadata.contextEntriesExamined).isEqualTo(0)
    assertThat(metadata.contextEntryLimit).isEqualTo(0)
    assertThat(metadata.grpcClassificationIncomplete).isTrue()
    assertThat(metadata.rawEntriesExamined).isEqualTo(1)
    assertThat(metadata.rawEntryLimit).isEqualTo(101)
    assertThat(metadata.rawQueriesTruncated).isEqualTo(0)
  }

  @Test
  fun `log reader preserves safe lifecycle entries when context lookup fails`() = runBlocking {
    val logging = mock<Logging>()
    val correlatedPage = mock<Page<LogEntry>>()
    val lifecycle =
      LogEntry.newBuilder(
          Payload.StringPayload.of(
            "event=registered xmm.lifecycle.stage=upload_registration " +
              "xmm.edpa.raw_impression_upload.name=uploads/1"
          )
        )
        .setTimestamp(NOW)
        .build()
    val continuation =
      LogEntry.newBuilder(Payload.StringPayload.of("xmm.edpa.raw_impression_upload.name=uploads/1"))
        .setLogName("projects/logging-project/logs/stdout")
        .setSourceLocation(SourceLocation.newBuilder().setFunction("grpc.Service.Call").build())
        .setTimestamp(NOW.minusMillis(1))
        .build()
    val contextError = mock<LoggingException>()
    whenever(contextError.isRetryable).thenReturn(false)
    whenever(contextError.code).thenReturn(13)
    whenever(correlatedPage.values).thenReturn(listOf(lifecycle, continuation))
    whenever(correlatedPage.hasNextPage()).thenReturn(false)
    whenever(logging.listLogEntries(any(), any(), any()))
      .thenReturn(correlatedPage)
      .thenThrow(contextError)
    val reader =
      GoogleCloudLogReader(
        project = "logging-project",
        logging = logging,
        safeFields = setOf("event", "xmm.lifecycle.stage", "xmm.edpa.raw_impression_upload.name"),
        correlationFields = setOf("xmm.edpa.raw_impression_upload.name"),
      )

    val result =
      reader.readBatch(
        listOf("uploads/1"),
        emptyList(),
        NOW.minusSeconds(1),
        NOW.plusSeconds(1),
        10,
      )

    assertThat(result.entries).hasSize(1)
    assertThat(result.entries.single().message).contains("event=registered")
    assertThat(result.complete).isFalse()
    assertThat(result.failed).isTrue()
    assertThat(result.failure).isSameInstanceAs(contextError)
  }

  @Test
  fun `log reader prefers later quota failure and counts only issued filters`() = runBlocking {
    val logging = mock<Logging>()
    val ordinaryError = mock<LoggingException>()
    whenever(ordinaryError.isRetryable).thenReturn(false)
    whenever(ordinaryError.code).thenReturn(13)
    val quotaError = mock<LoggingException>()
    whenever(quotaError.isRetryable).thenReturn(false)
    whenever(quotaError.code).thenReturn(8)
    whenever(logging.listLogEntries(any(), any(), any()))
      .thenThrow(ordinaryError, quotaError, quotaError, quotaError)
    val reader =
      GoogleCloudLogReader(
        project = "logging-project",
        logging = logging,
        safeFields = emptySet(),
        correlationFields = emptySet(),
        includeGrpcPayloads = true,
        correlationFieldsFor = { emptyList() },
        retryDelay = {},
      )
    val values = listOf("a".repeat(5_000), "b".repeat(5_000))

    val result = reader.readBatch(values, emptyList(), NOW.minusSeconds(1), NOW.plusSeconds(1), 10)

    assertThat(result.complete).isFalse()
    assertThat(result.failure).isSameInstanceAs(quotaError)
    assertThat(result.counts.loggingFilterBatches).isEqualTo(2)
    assertThat(result.counts.physicalRequests).isEqualTo(4)
    assertThat(result.counts.retryAttempts).isEqualTo(2)
  }

  @Test
  fun `log reader does not count filters skipped after reaching the entry bound`() = runBlocking {
    val logging = mock<Logging>()
    val page = mock<Page<LogEntry>>()
    val entries =
      listOf(
        LogEntry.newBuilder(Payload.StringPayload.of("first")).setTimestamp(NOW).build(),
        LogEntry.newBuilder(Payload.StringPayload.of("second")).setTimestamp(NOW).build(),
        LogEntry.newBuilder(Payload.StringPayload.of("third")).setTimestamp(NOW).build(),
      )
    whenever(page.values).thenReturn(entries)
    whenever(page.hasNextPage()).thenReturn(false)
    whenever(logging.listLogEntries(any(), any(), any())).thenReturn(page)
    val reader =
      GoogleCloudLogReader(
        project = "logging-project",
        logging = logging,
        safeFields = emptySet(),
        correlationFields = emptySet(),
        includeGrpcPayloads = true,
        correlationFieldsFor = { emptyList() },
        maxGrpcContextEntries = 0,
      )
    val values = listOf("a".repeat(5_000), "b".repeat(5_000))

    val result = reader.readBatch(values, emptyList(), NOW.minusSeconds(1), NOW.plusSeconds(1), 1)

    assertThat(result.complete).isFalse()
    assertThat(result.counts.loggingFilterBatches).isEqualTo(1)
    assertThat(result.counts.physicalRequests).isEqualTo(1)
    assertThat(checkNotNull(result.logMetadata).rawQueriesTruncated).isEqualTo(1)
  }

  companion object {
    private val NOW = Instant.parse("2026-09-10T12:00:00Z")

    private fun credentials(): GoogleCredentials =
      GoogleCredentials.create(
        AccessToken("token", Date.from(Instant.parse("2100-01-01T00:00:00Z")))
      )

    private fun httpResponse(
      statusCode: Int,
      body: String,
      headers: Map<String, List<String>> = emptyMap(),
    ): HttpResponse<String> {
      val response = mock<HttpResponse<String>>()
      whenever(response.statusCode()).thenReturn(statusCode)
      whenever(response.body()).thenReturn(body)
      whenever(response.headers()).thenReturn(HttpHeaders.of(headers) { _, _ -> true })
      return response
    }

    private fun span(
      id: String,
      startTime: Instant,
      attributes: Map<String, String>,
    ): CloudTraceSpan =
      CloudTraceSpan("project", "trace", id, null, id, "service", startTime, null, attributes)
  }
}
