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
import com.google.protobuf.Timestamp
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.epochMicrosToInstant
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.instantToEpochMicros
import org.wfanet.measurement.storage.ParquetRow
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.ParquetValue

@RunWith(JUnit4::class)
class RawImpressionStreamTest {
  @Rule @JvmField val tempDir = TemporaryFolder()

  private fun newClient(): ParquetStorageClient =
    ParquetStorageClient(Configuration(), Path(tempDir.root.absolutePath))

  private suspend fun writeParquet(
    client: ParquetStorageClient,
    key: String,
    rows: List<ParquetRow>,
  ) {
    client.writeBlob(key, flow { rows.forEach { emit(it.toByteString()) } })
  }

  private fun row(eventId: String, eventTimeMicros: Long): ParquetRow =
    ParquetRow.newBuilder()
      .putColumns("event_id", ParquetValue.newBuilder().setStringValue(eventId).build())
      .putColumns("event_time", ParquetValue.newBuilder().setInt64Value(eventTimeMicros).build())
      .build()

  /**
   * Builds a row whose event-time column is written as a `TIMESTAMP`-annotated
   * `INT64` (via [ParquetValue.setTimestampValue]); `ParquetStorageClient`
   * decodes such a column back to an [Instant] rather than a [Long].
   */
  private fun rowWithTimestamp(eventId: String, eventTime: Instant): ParquetRow =
    ParquetRow.newBuilder()
      .putColumns("event_id", ParquetValue.newBuilder().setStringValue(eventId).build())
      .putColumns(
        "event_time",
        ParquetValue.newBuilder()
          .setTimestampValue(
            Timestamp.newBuilder()
              .setSeconds(eventTime.epochSecond)
              .setNanos(eventTime.nano)
              .build()
          )
          .build(),
      )
      .build()

  /** A row whose `event_id` is a raw `BINARY` column (decoded as [ByteString]). */
  private fun rowWithBytesId(eventId: String, eventTimeMicros: Long): ParquetRow =
    ParquetRow.newBuilder()
      .putColumns(
        "event_id",
        ParquetValue.newBuilder().setBytesValue(ByteString.copyFromUtf8(eventId)).build(),
      )
      .putColumns("event_time", ParquetValue.newBuilder().setInt64Value(eventTimeMicros).build())
      .build()

  private fun newStream(
    client: ParquetStorageClient,
    doneBlobKey: String,
    shardIndex: Int = 0,
    totalShards: Int = 1,
    extractor: FingerprintExtractor = FingerprintExtractor(),
  ): RawImpressionStream =
    RawImpressionStream(
      parquetStorageClient = client,
      doneBlobKey = doneBlobKey,
      labelerInputFieldMapping = MAPPING,
      shardIndex = shardIndex,
      totalShards = totalShards,
      fingerprintExtractor = extractor,
    )

  /**
   * Builds a [RawImpressionStream.ModelLineConsumerFactory] that opens an
   * anonymous per-file consumer running [onEvent] per event and [onClose] on
   * file completion.
   */
  private fun consumerFactory(
    activeWindow: ActiveWindow = ActiveWindow(0L, ActiveWindow.OPEN_ENDED),
    onClose: suspend (String) -> Unit = {},
    onEvent: suspend (FingerprintedEvent) -> Unit,
  ): RawImpressionStream.ModelLineConsumerFactory =
    RawImpressionStream.ModelLineConsumerFactory { blobKey ->
      object : RawImpressionStream.ModelLineConsumer {
        override val window = activeWindow

        override suspend fun process(event: FingerprintedEvent) = onEvent(event)

        override suspend fun close() = onClose(blobKey)
      }
    }

  /** A factory whose per-file consumers collect event ids (open window) into [sink]. */
  private fun collectingFactory(
    sink: ConcurrentLinkedQueue<String>
  ): RawImpressionStream.ModelLineConsumerFactory =
    consumerFactory { sink.add(it.row["event_id"] as String) }

  @Test
  fun `stream emits every row of the shard and ignores the done marker`(): Unit = runBlocking {
    val client = newClient()
    writeParquet(client, "upload/a.parquet", listOf(row("e1", 1_000L), row("e2", 2_000L)))
    writeParquet(client, "upload/b.parquet", listOf(row("e3", 3_000L)))
    // The done marker is excluded by name; its contents are never read.
    writeParquet(client, "upload/done", listOf(row("ignored", 0L)))

    val subject = newStream(client, doneBlobKey = "upload/done")
    val sink = ConcurrentLinkedQueue<String>()
    subject.stream(listOf(collectingFactory(sink)))

    assertThat(sink.toList()).containsExactly("e1", "e2", "e3")
    assertThat(subject.stats.read.get()).isEqualTo(3)
    assertThat(subject.stats.droppedOtherShard.get()).isEqualTo(0)
    assertThat(subject.stats.emitted.get()).isEqualTo(3)
  }

  @Test
  fun `stream routes events to model lines by window in a single pass`(): Unit = runBlocking {
    val client = newClient()
    writeParquet(
      client,
      "u/a.parquet",
      listOf(
        row("e1", 10_000_000L), // t = 10s
        row("e2", 20_000_000L), // t = 20s
        row("e3", 30_000_000L), // t = 30s
      ),
    )
    val subject = newStream(client, doneBlobKey = "u/done")

    val modelLineA = ConcurrentLinkedQueue<String>()
    val modelLineB = ConcurrentLinkedQueue<String>()
    subject.stream(
      listOf(
        // [0s, 25s): e1, e2
        consumerFactory(ActiveWindow.of(Instant.ofEpochSecond(0), Instant.ofEpochSecond(25))) {
          modelLineA.add(it.row["event_id"] as String)
        },
        // [15s, +inf): e2, e3
        consumerFactory(ActiveWindow.of(Instant.ofEpochSecond(15), null)) {
          modelLineB.add(it.row["event_id"] as String)
        },
      )
    )

    assertThat(modelLineA.toList()).containsExactly("e1", "e2")
    assertThat(modelLineB.toList()).containsExactly("e2", "e3")
    // One shared pass: each row decoded + fingerprinted exactly once.
    assertThat(subject.stats.read.get()).isEqualTo(3)
    assertThat(subject.stats.emitted.get()).isEqualTo(3)
  }

  @Test
  fun `stream handles a TIMESTAMP-annotated event-time column`(): Unit = runBlocking {
    val client = newClient()
    writeParquet(
      client,
      "ts/a.parquet",
      listOf(
        rowWithTimestamp("e1", Instant.ofEpochSecond(10)),
        rowWithTimestamp("e2", Instant.ofEpochSecond(30)),
      ),
    )
    val subject = newStream(client, doneBlobKey = "ts/done")

    val got = ConcurrentLinkedQueue<String>()
    subject.stream(
      listOf(
        // [0s, 20s): e1 only — e2 at 30s is out of window.
        consumerFactory(ActiveWindow.of(Instant.ofEpochSecond(0), Instant.ofEpochSecond(20))) {
          got.add(it.row["event_id"] as String)
        }
      )
    )

    assertThat(got.toList()).containsExactly("e1")
  }

  @Test
  fun `stream labels every event across many files (concurrent-safe sink)`(): Unit = runBlocking {
    val client = newClient()
    // More files than the default reader concurrency would admit at once, so the
    // per-file coroutines genuinely overlap; the shared sink must be concurrency-safe.
    val fileCount = 12
    val perFile = 50
    val expected = mutableListOf<String>()
    for (f in 0 until fileCount) {
      val rows =
        (0 until perFile).map { i ->
          val id = "f$f-e$i"
          expected.add(id)
          row(id, 1_000L)
        }
      writeParquet(client, "many/file-$f.parquet", rows)
    }
    val subject = newStream(client, doneBlobKey = "many/done")

    val sink = ConcurrentLinkedQueue<String>()
    subject.stream(listOf(collectingFactory(sink)))

    assertThat(sink.toList()).containsExactlyElementsIn(expected)
    assertThat(subject.stats.read.get()).isEqualTo((fileCount * perFile).toLong())
    assertThat(subject.stats.emitted.get()).isEqualTo((fileCount * perFile).toLong())
  }

  @Test
  fun `stream opens and closes one consumer per input file`(): Unit = runBlocking {
    val client = newClient()
    writeParquet(client, "lc/a.parquet", listOf(row("a1", 1_000L)))
    writeParquet(client, "lc/b.parquet", listOf(row("b1", 1_000L)))
    writeParquet(client, "lc/c.parquet", listOf(row("c1", 1_000L)))
    val subject = newStream(client, doneBlobKey = "lc/done")

    val opened = ConcurrentLinkedQueue<String>()
    val closed = ConcurrentLinkedQueue<String>()
    val factory =
      RawImpressionStream.ModelLineConsumerFactory { blobKey ->
        opened.add(blobKey)
        object : RawImpressionStream.ModelLineConsumer {
          override val window = ActiveWindow(0L, ActiveWindow.OPEN_ENDED)

          override suspend fun process(event: FingerprintedEvent) {}

          override suspend fun close() {
            closed.add(blobKey)
          }
        }
      }
    subject.stream(listOf(factory))

    // open() receives each file's key; close() runs once per file.
    assertThat(opened.toList())
      .containsExactly("lc/a.parquet", "lc/b.parquet", "lc/c.parquet")
    assertThat(closed.toList())
      .containsExactly("lc/a.parquet", "lc/b.parquet", "lc/c.parquet")
  }

  @Test
  fun `stream does not close consumers when a file fails`(): Unit = runBlocking {
    val client = newClient()
    // event_id written as INT64 → readEventIdBytes throws mid-file.
    writeParquet(
      client,
      "fail/a.parquet",
      listOf(
        ParquetRow.newBuilder()
          .putColumns("event_id", ParquetValue.newBuilder().setInt64Value(7L).build())
          .putColumns("event_time", ParquetValue.newBuilder().setInt64Value(1_000L).build())
          .build()
      ),
    )
    val subject = newStream(client, doneBlobKey = "fail/done")

    val closed = ConcurrentLinkedQueue<String>()
    val factory =
      consumerFactory(onClose = { closed.add(it) }) {
        /* never reached */
      }

    assertFailsWith<IllegalStateException> { subject.stream(listOf(factory)) }
    // Failure ⇒ close() is not called ⇒ no partial output uploaded.
    assertThat(closed).isEmpty()
  }

  @Test
  fun `stream drops rows that do not belong to this shard`(): Unit = runBlocking {
    val client = newClient()
    val ids = (1..40).map { "event-$it" }
    writeParquet(client, "s/a.parquet", ids.map { row(it, 1_000L) })

    val extractor = FingerprintExtractor()
    val totalShards = 4
    val shardIndex = 1
    val expected =
      ids.filter {
        val fp = extractor.extract(ByteString.copyFromUtf8(it))
        Math.floorMod(fp.high, totalShards.toLong()).toInt() == shardIndex
      }

    val subject = newStream(client, "s/done", shardIndex, totalShards, extractor)
    val sink = ConcurrentLinkedQueue<String>()
    subject.stream(listOf(collectingFactory(sink)))

    assertThat(sink.toList()).containsExactlyElementsIn(expected)
    assertThat(subject.stats.read.get()).isEqualTo(40)
    assertThat(subject.stats.droppedOtherShard.get().toInt()).isEqualTo(40 - expected.size)
    assertThat(subject.stats.emitted.get().toInt()).isEqualTo(expected.size)
  }

  @Test
  fun `stream reads a raw BINARY (ByteString) event-id column`(): Unit = runBlocking {
    val client = newClient()
    writeParquet(
      client,
      "b/a.parquet",
      listOf(rowWithBytesId("e1", 1_000L), rowWithBytesId("e2", 2_000L)),
    )
    val subject = newStream(client, doneBlobKey = "b/done")

    val sink = ConcurrentLinkedQueue<String>()
    subject.stream(
      listOf(consumerFactory { sink.add((it.row["event_id"] as ByteString).toStringUtf8()) })
    )

    assertThat(sink.toList()).containsExactly("e1", "e2")
    assertThat(subject.stats.emitted.get()).isEqualTo(2)
  }

  @Test
  fun `stream rejects empty consumer factories`(): Unit = runBlocking {
    val subject = newStream(newClient(), doneBlobKey = "x/done")
    assertFailsWith<IllegalArgumentException> { subject.stream(emptyList()) }
  }

  @Test
  fun `constructor rejects mapping missing required entries`() {
    // Missing timestamp_usec.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionStream(
        newClient(),
        "x/done",
        mapOf("event_id.id" to "event_id"),
        0,
        1,
        FingerprintExtractor(),
      )
    }
    // Missing event_id.id.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionStream(
        newClient(),
        "x/done",
        mapOf("timestamp_usec" to "event_time"),
        0,
        1,
        FingerprintExtractor(),
      )
    }
  }

  @Test
  fun `constructor rejects invalid arguments`() {
    // totalShards must be positive.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionStream(newClient(), "x/done", MAPPING, 0, 0, FingerprintExtractor())
    }
    // shardIndex out of range.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionStream(newClient(), "x/done", MAPPING, 5, 2, FingerprintExtractor())
    }
    // maxConcurrentReaders must be positive.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionStream(
        newClient(),
        "x/done",
        MAPPING,
        0,
        1,
        FingerprintExtractor(),
        maxConcurrentReaders = 0,
      )
    }
    // doneBlobKey must be non-blank.
    assertFailsWith<IllegalArgumentException> {
      RawImpressionStream(newClient(), "  ", MAPPING, 0, 1, FingerprintExtractor())
    }
  }

  @Test
  fun `stream fails on an unexpected event-id column type`(): Unit = runBlocking {
    val client = newClient()
    // event_id written as INT64 → decoded as Long → neither String nor ByteString.
    writeParquet(
      client,
      "bad/a.parquet",
      listOf(
        ParquetRow.newBuilder()
          .putColumns("event_id", ParquetValue.newBuilder().setInt64Value(7L).build())
          .putColumns("event_time", ParquetValue.newBuilder().setInt64Value(1_000L).build())
          .build()
      ),
    )
    val subject = newStream(client, doneBlobKey = "bad/done")

    assertFailsWith<IllegalStateException> {
      subject.stream(listOf(collectingFactory(ConcurrentLinkedQueue())))
    }
  }

  @Test
  fun `stream fails on an unexpected event-time column type`(): Unit = runBlocking {
    val client = newClient()
    // event_time written as STRING → decoded as String → neither Long nor Instant.
    writeParquet(
      client,
      "badt/a.parquet",
      listOf(
        ParquetRow.newBuilder()
          .putColumns("event_id", ParquetValue.newBuilder().setStringValue("e1").build())
          .putColumns("event_time", ParquetValue.newBuilder().setStringValue("nope").build())
          .build()
      ),
    )
    val subject = newStream(client, doneBlobKey = "badt/done")

    assertFailsWith<IllegalStateException> {
      subject.stream(listOf(collectingFactory(ConcurrentLinkedQueue())))
    }
  }

  @Test
  fun `ActiveWindow rejects non-positive width`() {
    assertFailsWith<IllegalArgumentException> { ActiveWindow(100L, 100L) } // equal bounds
    assertFailsWith<IllegalArgumentException> { ActiveWindow(100L, 50L) } // inverted bounds
    assertFailsWith<IllegalArgumentException> {
      ActiveWindow.of(Instant.ofEpochSecond(10), Instant.ofEpochSecond(10))
    }
  }

  @Test
  fun `ActiveWindow contains is half-open`() {
    val window = ActiveWindow(10_000_000L, 20_000_000L)
    assertThat(window.contains(9_999_999L)).isFalse()
    assertThat(window.contains(10_000_000L)).isTrue() // inclusive start
    assertThat(window.contains(19_999_999L)).isTrue()
    assertThat(window.contains(20_000_000L)).isFalse() // exclusive end
  }

  @Test
  fun `ActiveWindow of with null end is open-ended`() {
    val window = ActiveWindow.of(Instant.ofEpochSecond(10), null)
    assertThat(window.endMicros).isEqualTo(ActiveWindow.OPEN_ENDED)
    assertThat(window.contains(9_999_999L)).isFalse()
    assertThat(window.contains(Long.MAX_VALUE - 1)).isTrue()
  }

  @Test
  fun `instant micros conversions round-trip`() {
    assertThat(instantToEpochMicros(Instant.ofEpochSecond(1, 500_000_000))).isEqualTo(1_500_000L)
    // Sub-microsecond nanos are truncated.
    assertThat(instantToEpochMicros(Instant.ofEpochSecond(0, 1_999))).isEqualTo(1L)
    assertThat(epochMicrosToInstant(1_500_000L)).isEqualTo(Instant.ofEpochSecond(1, 500_000_000))
  }

  companion object {
    private val MAPPING =
      mapOf("timestamp_usec" to "event_time", "event_id.id" to "event_id")
  }
}
