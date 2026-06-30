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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.LongPointData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile
import org.wfanet.measurement.storage.ParquetRow
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetRow
import org.wfanet.measurement.storage.parquetValue

@RunWith(JUnit4::class)
class RawImpressionSourceTest {
  @Rule @JvmField val tempDir = TemporaryFolder()

  /** Fake metadata service whose listed files are set per-test via [blobUris]. */
  private class FakeRawImpressionUploadFileService :
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase() {
    var blobUris: List<String> = emptyList()

    /** File-list mode: resource name -> blob_uri, set per-test via [filesByName]. */
    var filesByName: Map<String, String> = emptyMap()

    override suspend fun listRawImpressionUploadFiles(
      request: ListRawImpressionUploadFilesRequest
    ): ListRawImpressionUploadFilesResponse = listRawImpressionUploadFilesResponse {
      blobUris.forEach { rawImpressionUploadFiles.add(rawImpressionUploadFile { blobUri = it }) }
    }

    override suspend fun getRawImpressionUploadFile(
      request: GetRawImpressionUploadFileRequest
    ): RawImpressionUploadFile = rawImpressionUploadFile {
      name = request.name
      blobUri = filesByName.getValue(request.name)
    }
  }

  private val filesService = FakeRawImpressionUploadFileService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(filesService) }

  private val filesStub:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub by lazy {
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
      grpcTestServerRule.channel
    )
  }

  private lateinit var metricReader: InMemoryMetricReader
  private lateinit var testMetrics: RawImpressionSourceMetrics

  @Before
  fun setUpMetrics() {
    metricReader = InMemoryMetricReader.create()
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    testMetrics = RawImpressionSourceMetrics(meterProvider.get("test"))
  }

  /** Cumulative value of the long-sum counter named [name], or 0 if not recorded. */
  private fun counterValue(name: String): Long {
    val metric = metricReader.collectAllMetrics().find { it.name == name } ?: return 0L
    return (metric.longSumData.points.first() as LongPointData).value
  }

  /** Number of values recorded into the histogram named [name], or 0 if not recorded. */
  private fun histogramCount(name: String): Long {
    val metric = metricReader.collectAllMetrics().find { it.name == name } ?: return 0L
    return metric.histogramData.points.first().count
  }

  private fun newClient(): ParquetStorageClient =
    ParquetStorageClient(Configuration(), Path(tempDir.root.absolutePath))

  /** Writes [rows] to [key] and registers [key] as a file of the upload. */
  private suspend fun writeFile(client: ParquetStorageClient, key: String, rows: List<ParquetRow>) {
    client.writeBlob(key, flow { rows.forEach { emit(it.toByteString()) } })
    filesService.blobUris = filesService.blobUris + key
  }

  private fun row(eventId: String): ParquetRow = parquetRow {
    columns["event_id"] = parquetValue { stringValue = eventId }
  }

  /** A row whose `event_id` is a raw `BINARY` column (decoded as [ByteString]). */
  private fun rowWithBytesId(eventId: String): ParquetRow = parquetRow {
    columns["event_id"] = parquetValue { bytesValue = ByteString.copyFromUtf8(eventId) }
  }

  private fun newSource(
    client: ParquetStorageClient,
    shardIndex: Int = 0,
    totalShards: Int = 1,
    extractor: EventIdDigestExtractor = EventIdDigestExtractor(),
  ): RawImpressionSource =
    RawImpressionSource(
      parquetStorageClient = client,
      rawImpressionUploadFilesStub = filesStub,
      rawImpressionUpload = UPLOAD,
      eventIdColumn = "event_id",
      shardIndex = shardIndex,
      totalShards = totalShards,
      eventIdDigestExtractor = extractor,
      metrics = testMetrics,
    )

  /** Reads the decoded `event_id` (STRING or BINARY) from a [DigestedEvent]. */
  private fun eventId(event: ParquetDigestedEvent): String {
    val v = event.row.getValue("event_id")
    return when (v.kindCase) {
      ParquetValue.KindCase.STRING_VALUE -> v.stringValue
      ParquetValue.KindCase.BYTES_VALUE -> v.bytesValue.toStringUtf8()
      else -> error("unexpected event_id kind ${v.kindCase}")
    }
  }

  /**
   * Builds an `openSink` lambda whose per-blob [RawImpressionSource.BlobSink] records every event
   * id into [sink] (and, optionally, open/close calls into [opened]/[closed]).
   */
  private fun collectingSink(
    sink: ConcurrentLinkedQueue<String>,
    opened: ConcurrentLinkedQueue<String>? = null,
    committed: ConcurrentLinkedQueue<String>? = null,
    closed: ConcurrentLinkedQueue<String>? = null,
  ): suspend (String) -> RawImpressionSource.BlobSink = { blobUri ->
    opened?.add(blobUri)
    object : RawImpressionSource.BlobSink {
      override suspend fun processBatch(events: List<ParquetDigestedEvent>) {
        events.forEach { sink.add(eventId(it)) }
      }

      override suspend fun commit() {
        committed?.add(blobUri)
      }

      override suspend fun close() {
        closed?.add(blobUri)
      }
    }
  }

  @Test
  fun `streamBlobs emits every row of the shard`(): Unit = runBlocking {
    val client = newClient()
    writeFile(client, "upload/a.parquet", listOf(row("e1"), row("e2")))
    writeFile(client, "upload/b.parquet", listOf(row("e3")))

    val subject = newSource(client)
    val sink = ConcurrentLinkedQueue<String>()
    subject.streamBlobs(collectingSink(sink))

    assertThat(sink.toList()).containsExactly("e1", "e2", "e3")
    assertThat(counterValue(READ)).isEqualTo(3)
    assertThat(counterValue(DROPPED)).isEqualTo(0)
    assertThat(counterValue(EMITTED)).isEqualTo(3)
    // One per-file processing-time sample per input file.
    assertThat(histogramCount(FILE_DURATION)).isEqualTo(2)
  }

  @Test
  fun `streamBlobs in file-list mode reads exactly the given files`(): Unit = runBlocking {
    val client = newClient()
    // x and y are on this WorkItem's file list; z is also in the upload but NOT on the list.
    writeFile(client, "fl/x.parquet", listOf(row("e1"), row("e2")))
    writeFile(client, "fl/y.parquet", listOf(row("e3")))
    writeFile(client, "fl/z.parquet", listOf(row("e4")))
    filesService.filesByName =
      mapOf("$UPLOAD/files/x" to "fl/x.parquet", "$UPLOAD/files/y" to "fl/y.parquet")

    val subject =
      RawImpressionSource(
        parquetStorageClient = client,
        rawImpressionUploadFilesStub = filesStub,
        rawImpressionUpload = UPLOAD,
        eventIdColumn = "event_id",
        eventIdDigestExtractor = EventIdDigestExtractor(),
        metrics = testMetrics,
        inputFiles = listOf("$UPLOAD/files/x", "$UPLOAD/files/y"),
      )
    val sink = ConcurrentLinkedQueue<String>()
    subject.streamBlobs(collectingSink(sink))

    // Only x + y are read (every row, no shard filter); z is excluded.
    assertThat(sink.toList()).containsExactly("e1", "e2", "e3")
    assertThat(counterValue(EMITTED)).isEqualTo(3)
    assertThat(counterValue(DROPPED)).isEqualTo(0)
  }

  @Test
  fun `streamBlobs processes every event across many files (concurrent-safe sink)`(): Unit =
    runBlocking {
      val client = newClient()
      // More files than the default open-file concurrency would admit at once, so
      // the per-file coroutines genuinely overlap and a blob's batches are
      // processed by multiple workers; the shared sink must be concurrency-safe.
      val fileCount = 12
      val perFile = 50
      val expected = mutableListOf<String>()
      for (f in 0 until fileCount) {
        val rows =
          (0 until perFile).map { i ->
            val id = "f$f-e$i"
            expected.add(id)
            row(id)
          }
        writeFile(client, "many/file-$f.parquet", rows)
      }
      val subject = newSource(client)

      val sink = ConcurrentLinkedQueue<String>()
      subject.streamBlobs(collectingSink(sink))

      assertThat(sink.toList()).containsExactlyElementsIn(expected)
      assertThat(counterValue(READ)).isEqualTo((fileCount * perFile).toLong())
      assertThat(counterValue(EMITTED)).isEqualTo((fileCount * perFile).toLong())
      assertThat(histogramCount(FILE_DURATION)).isEqualTo(fileCount.toLong())
    }

  @Test
  fun `streamBlobs opens, commits, and closes one sink per input file`(): Unit = runBlocking {
    val client = newClient()
    writeFile(client, "lc/a.parquet", listOf(row("a1")))
    writeFile(client, "lc/b.parquet", listOf(row("b1")))
    writeFile(client, "lc/c.parquet", listOf(row("c1")))
    val subject = newSource(client)

    val opened = ConcurrentLinkedQueue<String>()
    val committed = ConcurrentLinkedQueue<String>()
    val closed = ConcurrentLinkedQueue<String>()
    subject.streamBlobs(
      collectingSink(
        ConcurrentLinkedQueue(),
        opened = opened,
        committed = committed,
        closed = closed,
      )
    )

    // openSink receives each file's blob URI; on success commit() and close() each run once per
    // file.
    assertThat(opened.toList()).containsExactly("lc/a.parquet", "lc/b.parquet", "lc/c.parquet")
    assertThat(committed.toList()).containsExactly("lc/a.parquet", "lc/b.parquet", "lc/c.parquet")
    assertThat(closed.toList()).containsExactly("lc/a.parquet", "lc/b.parquet", "lc/c.parquet")
  }

  @Test
  fun `streamBlobs does not commit but still closes a sink when a file fails`(): Unit =
    runBlocking {
      val client = newClient()
      // event_id written as INT64 → readEventIdBytes throws mid-file (unwrapper).
      writeFile(
        client,
        "fail/a.parquet",
        listOf(parquetRow { columns["event_id"] = parquetValue { int64Value = 7L } }),
      )
      val subject = newSource(client)

      val committed = ConcurrentLinkedQueue<String>()
      val closed = ConcurrentLinkedQueue<String>()
      assertFailsWith<IllegalStateException> {
        subject.streamBlobs(
          collectingSink(ConcurrentLinkedQueue(), committed = committed, closed = closed)
        )
      }
      // Failure ⇒ commit() is skipped (no partial output published) but close() still
      // runs (resources released — no leak).
      assertThat(committed).isEmpty()
      assertThat(closed.toList()).containsExactly("fail/a.parquet")
    }

  @Test
  fun `streamBlobs drops rows that do not belong to this shard`(): Unit = runBlocking {
    val client = newClient()
    val ids = (1..40).map { "event-$it" }
    writeFile(client, "s/a.parquet", ids.map { row(it) })

    val extractor = EventIdDigestExtractor()
    val totalShards = 4
    val shardIndex = 1
    val expected =
      ids.filter {
        val digest = extractor.extract(ByteString.copyFromUtf8(it))
        Math.floorMod(digest.high, totalShards.toLong()).toInt() == shardIndex
      }

    val subject = newSource(client, shardIndex, totalShards, extractor)
    val sink = ConcurrentLinkedQueue<String>()
    subject.streamBlobs(collectingSink(sink))

    assertThat(sink.toList()).containsExactlyElementsIn(expected)
    assertThat(counterValue(READ)).isEqualTo(40)
    assertThat(counterValue(DROPPED).toInt()).isEqualTo(40 - expected.size)
    assertThat(counterValue(EMITTED).toInt()).isEqualTo(expected.size)
  }

  @Test
  fun `streamBlobs reads a raw BINARY (ByteString) event-id column`(): Unit = runBlocking {
    val client = newClient()
    writeFile(client, "b/a.parquet", listOf(rowWithBytesId("e1"), rowWithBytesId("e2")))
    val subject = newSource(client)

    val sink = ConcurrentLinkedQueue<String>()
    subject.streamBlobs(collectingSink(sink))

    assertThat(sink.toList()).containsExactly("e1", "e2")
    assertThat(counterValue(EMITTED)).isEqualTo(2)
  }

  @Test
  fun `streamBlobs fails on an unexpected event-id column type`(): Unit = runBlocking {
    val client = newClient()
    // event_id written as INT64 → decoded as Long → neither String nor ByteString.
    writeFile(
      client,
      "bad/a.parquet",
      listOf(parquetRow { columns["event_id"] = parquetValue { int64Value = 7L } }),
    )
    val subject = newSource(client)

    assertFailsWith<IllegalStateException> {
      subject.streamBlobs(collectingSink(ConcurrentLinkedQueue()))
    }
  }

  @Test
  fun `constructor rejects invalid arguments`() {
    // totalShards must be positive.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionSource(
        newClient(),
        filesStub,
        UPLOAD,
        "event_id",
        0,
        0,
        EventIdDigestExtractor(),
      )
    }
    // shardIndex out of range.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionSource(
        newClient(),
        filesStub,
        UPLOAD,
        "event_id",
        5,
        2,
        EventIdDigestExtractor(),
      )
    }
    // maxOpenFiles must be positive.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionSource(
        newClient(),
        filesStub,
        UPLOAD,
        "event_id",
        0,
        1,
        EventIdDigestExtractor(),
        maxOpenFiles = 0,
      )
    }
    // rawImpressionUpload must be non-blank.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionSource(newClient(), filesStub, "  ", "event_id", 0, 1, EventIdDigestExtractor())
    }
    // eventIdColumn must be non-blank.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionSource(newClient(), filesStub, UPLOAD, "  ", 0, 1, EventIdDigestExtractor())
    }
  }

  companion object {
    private const val UPLOAD = "dataProviders/dp/rawImpressionUploads/u1"
    private const val READ = "edpa.raw_impression_source.rows_read"
    private const val DROPPED = "edpa.raw_impression_source.rows_dropped_other_shard"
    private const val EMITTED = "edpa.raw_impression_source.rows_emitted"
    private const val FILE_DURATION = "edpa.raw_impression_source.file_processing_duration"
  }
}
