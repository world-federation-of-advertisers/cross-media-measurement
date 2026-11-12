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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient

@RunWith(JUnit4::class)
class RawImpressionInputMonitorTest {
  @Test
  fun `scan reports only quiet directories without done`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/old/data.parquet", OLD, size = 1),
          blob("raw/recent/data.parquet", RECENT, size = 1),
        )

      val result = createMonitor(storage).scan()

      assertThat(result.missingDoneDirectories).isEqualTo(1)
      assertThat(result.findings.map { it.type })
        .containsExactly(RawImpressionInputMonitor.FindingType.MISSING_DONE)
      assertThat(storage.contentReadCalls.get()).isEqualTo(0)
    }

  @Test
  fun `scan does not duplicate nested missing-done directories`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/upload/data.parquet", OLD, size = 1),
          blob("raw/upload/partition/data.parquet", OLD, size = 1),
        )

      val result = createMonitor(storage).scan()

      assertThat(result.missingDoneDirectories).isEqualTo(1)
    }

  @Test
  fun `scan reports quiet done marker without nonempty data`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/empty/done", OLD, generation = 1),
          blob("raw/empty/control", OLD, size = 0),
        )

      val result = createMonitor(storage).scan()

      assertThat(result.doneWithoutDataDirectories).isEqualTo(1)
      assertThat(result.findings.map { it.type })
        .containsExactly(RawImpressionInputMonitor.FindingType.DONE_WITHOUT_DATA)
    }

  @Test
  fun `scan suppresses registered empty correction revision`() =
    runBlocking<Unit> {
      val storage = FakeStorageClient(blob("raw/removed/done", OLD, generation = 7))

      val result =
        createMonitor(storage)
          .scan(
            ignoredEmptyDoneObjects =
              setOf(
                RawImpressionInputMonitor.DoneObjectIdentity(
                  blobUri = "gs://bucket/raw/removed/done",
                  generation = 7,
                )
              )
          )

      assertThat(result.doneWithoutDataDirectories).isEqualTo(0)
    }

  @Test
  fun `scan reports quiet files uploaded after done`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/late/done", OLD.minusSeconds(60), generation = 1),
          blob("raw/late/data.parquet", OLD, size = 1),
          blob("raw/recent-late/done", OLD, generation = 2),
          blob("raw/recent-late/data.parquet", RECENT, size = 1),
        )

      val result = createMonitor(storage).scan()

      assertThat(result.dataFilesAfterDone).isEqualTo(1)
    }

  @Test
  fun `scan keeps an empty marker empty when another directory has finalized data`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/empty-late/done", OLD.minusSeconds(120), generation = 1),
          blob("raw/empty-late/data.parquet", OLD.minusSeconds(60), size = 1),
          blob("raw/populated/data.parquet", OLD.minusSeconds(120), size = 1),
          blob("raw/populated/done", OLD.minusSeconds(60), generation = 2),
        )

      val result = createMonitor(storage).scan()

      assertThat(result.doneWithoutDataDirectories).isEqualTo(1)
      assertThat(result.unregisteredDoneDirectories).isEqualTo(1)
      assertThat(result.dataFilesAfterDone).isEqualTo(1)
      assertThat(result.findings.map { it.type })
        .containsExactly(
          RawImpressionInputMonitor.FindingType.DONE_WITHOUT_DATA,
          RawImpressionInputMonitor.FindingType.UNREGISTERED_DONE,
          RawImpressionInputMonitor.FindingType.DATA_AFTER_DONE,
        )
    }

  @Test
  fun `scan accepts later child backfill marker`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/date/done", OLD.minusSeconds(120), generation = 1),
          blob("raw/date/backfill/data.parquet", OLD.minusSeconds(60), size = 1),
          blob("raw/date/backfill/done", OLD, generation = 2),
        )

      val result = createMonitor(storage).scan()

      assertThat(result.ambiguousDoneLayouts).isEqualTo(0)
      assertThat(result.dataFilesAfterDone).isEqualTo(0)
      assertThat(result.missingDoneDirectories).isEqualTo(0)
    }

  @Test
  fun `scan reports parent marker that overlaps child upload`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/date/backfill/data.parquet", OLD.minusSeconds(120), size = 1),
          blob("raw/date/backfill/done", OLD.minusSeconds(60), generation = 1),
          blob("raw/date/done", OLD, generation = 2),
        )

      val result = createMonitor(storage).scan()

      assertThat(result.ambiguousDoneLayouts).isEqualTo(1)
      assertThat(result.findings.map { it.type })
        .contains(RawImpressionInputMonitor.FindingType.AMBIGUOUS_DONE_LAYOUT)
    }

  @Test
  fun `scan ignores blobs outside configured prefix`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/healthy/data.parquet", OLD, size = 1),
          blob("raw/healthy/done", OLD.plusSeconds(1), generation = 1),
          blob("other/orphan/data.parquet", OLD, size = 1),
        )

      val result = createMonitor(storage).scan()

      assertThat(result.objectsScanned).isEqualTo(2)
      assertThat(result.missingDoneDirectories).isEqualTo(0)
    }

  @Test
  fun `scan ignores configured non-raw descendant`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/upload/data.parquet", OLD, size = 1),
          blob("raw/upload/done", OLD.plusSeconds(1), generation = 1),
          blob("raw/labeled-output/model-line/data.parquet", OLD, size = 1),
        )

      val result = createMonitor(storage, setOf("raw/labeled-output")).scan()

      assertThat(result.objectsScanned).isEqualTo(2)
      assertThat(result.missingDoneDirectories).isEqualTo(0)
    }

  @Test
  fun `scan reports registered files absent from storage`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/upload/present.parquet", OLD, size = 1),
          blob("raw/upload/done", OLD.plusSeconds(1), generation = 1),
        )

      val result =
        createMonitor(storage)
          .scan(
            missingRegisteredBlobKeys =
              mutableSetOf("raw/upload/present.parquet", "raw/upload/missing.parquet")
          )

      assertThat(result.missingRegisteredFiles).isEqualTo(1)
    }

  @Test
  fun `scan clears unregistered done after exact generation is registered`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/upload/data.parquet", OLD, size = 1),
          blob("raw/upload/done", OLD.plusSeconds(1), generation = 9),
        )
      val monitor = createMonitor(storage)

      assertThat(monitor.scan().unregisteredDoneDirectories).isEqualTo(1)

      val registered =
        monitor.scan(
          registeredDoneObjects =
            setOf(RawImpressionInputMonitor.DoneObjectIdentity("gs://bucket/raw/upload/done", 9))
        )
      assertThat(registered.unregisteredDoneDirectories).isEqualTo(0)
    }

  @Test
  fun `scan recognizes finalized raw data for every done marker spelling and size`() =
    runBlocking<Unit> {
      for (markerName in listOf("done", "Done", "DONE", "dOnE")) {
        for (markerSize in listOf(0L, 3L)) {
          val storage =
            FakeStorageClient(
              blob("raw/upload/data.parquet", OLD, size = 1),
              blob("raw/upload/partition/data.parquet", OLD, size = 1),
              blob("raw/upload/$markerName", OLD.plusSeconds(1), size = markerSize, generation = 42),
            )

          val result = createMonitor(storage).scan()

          assertThat(result.missingDoneDirectories).isEqualTo(0)
          assertThat(result.doneWithoutDataDirectories).isEqualTo(0)
          assertThat(result.unregisteredDoneDirectories).isEqualTo(1)
          assertThat(result.dataFilesAfterDone).isEqualTo(0)
          assertThat(result.ambiguousDoneLayouts).isEqualTo(0)
          assertThat(result.objectsScanned).isEqualTo(3)
          assertThat(result.findings.map { it.type })
            .containsExactly(RawImpressionInputMonitor.FindingType.UNREGISTERED_DONE)
          assertThat(storage.contentReadCalls.get()).isEqualTo(0)
        }
      }
    }

  @Test
  fun `scan never counts done marker contents as raw data regardless of spelling`() =
    runBlocking<Unit> {
      for (markerName in listOf("done", "Done", "DONE", "dOnE")) {
        for (markerSize in listOf(0L, 3L)) {
          val storage =
            FakeStorageClient(
              blob("raw/empty/$markerName", OLD, size = markerSize, generation = 42),
              blob("raw/empty/control", OLD, size = 0),
            )

          val result = createMonitor(storage).scan()

          assertThat(result.doneWithoutDataDirectories).isEqualTo(1)
          assertThat(result.missingDoneDirectories).isEqualTo(0)
          assertThat(result.unregisteredDoneDirectories).isEqualTo(0)
          assertThat(result.dataFilesAfterDone).isEqualTo(0)
          assertThat(result.findings.map { it.type })
            .containsExactly(RawImpressionInputMonitor.FindingType.DONE_WITHOUT_DATA)
          assertThat(storage.contentReadCalls.get()).isEqualTo(0)
        }
      }
    }

  @Test
  fun `scan preserves exact done URI and generation matching for every marker spelling`() =
    runBlocking<Unit> {
      for (markerName in listOf("done", "Done", "DONE", "dOnE")) {
        val storage =
          FakeStorageClient(
            blob("raw/upload/data.parquet", OLD, size = 1),
            blob("raw/upload/$markerName", OLD.plusSeconds(1), generation = 42),
          )
        val monitor = createMonitor(storage)
        val markerUri = "gs://bucket/raw/upload/$markerName"

        val registered =
          monitor.scan(
            registeredDoneObjects =
              setOf(RawImpressionInputMonitor.DoneObjectIdentity(markerUri, 42))
          )
        assertThat(registered.findings).isEmpty()
        assertThat(registered.missingDoneDirectories).isEqualTo(0)
        assertThat(registered.unregisteredDoneDirectories).isEqualTo(0)

        val differentGeneration =
          monitor.scan(
            registeredDoneObjects =
              setOf(RawImpressionInputMonitor.DoneObjectIdentity(markerUri, 41))
          )
        assertThat(differentGeneration.unregisteredDoneDirectories).isEqualTo(1)

        val differentSpelling = if (markerName == "done") "DONE" else "done"
        val differentUri =
          monitor.scan(
            registeredDoneObjects =
              setOf(
                RawImpressionInputMonitor.DoneObjectIdentity(
                  "gs://bucket/raw/upload/$differentSpelling",
                  42,
                )
              )
          )
        assertThat(differentUri.unregisteredDoneDirectories).isEqualTo(1)
      }
    }

  @Test
  fun `scan suppresses exact empty correction markers for every spelling and size`() =
    runBlocking<Unit> {
      for (markerName in listOf("done", "Done", "DONE", "dOnE")) {
        for (markerSize in listOf(0L, 3L)) {
          val storage =
            FakeStorageClient(
              blob("raw/empty/$markerName", OLD, size = markerSize, generation = 42)
            )

          val result =
            createMonitor(storage)
              .scan(
                ignoredEmptyDoneObjects =
                  setOf(
                    RawImpressionInputMonitor.DoneObjectIdentity(
                      "gs://bucket/raw/empty/$markerName",
                      42,
                    )
                  )
              )

          assertThat(result.findings).isEmpty()
          assertThat(result.doneWithoutDataDirectories).isEqualTo(0)
          assertThat(result.missingDoneDirectories).isEqualTo(0)
        }
      }
    }

  @Test
  fun `scan reports late data after registered done markers for every spelling`() =
    runBlocking<Unit> {
      for (markerName in listOf("done", "Done", "DONE", "dOnE")) {
        val storage =
          FakeStorageClient(
            blob("raw/upload/data.parquet", OLD.minusSeconds(1), size = 1),
            blob("raw/upload/$markerName", OLD, generation = 42),
            blob("raw/upload/partition/late.parquet", OLD.plusSeconds(1), size = 1),
          )

        val result =
          createMonitor(storage)
            .scan(
              registeredDoneObjects =
                setOf(
                  RawImpressionInputMonitor.DoneObjectIdentity(
                    "gs://bucket/raw/upload/$markerName",
                    42,
                  )
                )
            )

        assertThat(result.dataFilesAfterDone).isEqualTo(1)
        assertThat(result.missingDoneDirectories).isEqualTo(0)
        assertThat(result.doneWithoutDataDirectories).isEqualTo(0)
        assertThat(result.unregisteredDoneDirectories).isEqualTo(0)
        assertThat(result.findings.map { it.type })
          .containsExactly(RawImpressionInputMonitor.FindingType.DATA_AFTER_DONE)
      }
    }

  @Test
  fun `scan treats marker-like basenames and done directory names as data`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/upload/done.parquet", OLD, size = 1),
          blob("raw/upload/DONE.txt", OLD, size = 1),
          blob("raw/upload/undone", OLD, size = 1),
          blob("raw/upload/done-1", OLD, size = 1),
          blob("raw/upload/DONE/data.parquet", OLD, size = 1),
        )

      val result = createMonitor(storage).scan()

      assertThat(result.missingDoneDirectories).isEqualTo(1)
      assertThat(result.doneWithoutDataDirectories).isEqualTo(0)
      assertThat(result.unregisteredDoneDirectories).isEqualTo(0)
      assertThat(result.objectsScanned).isEqualTo(5)
      assertThat(result.findings.map { it.type })
        .containsExactly(RawImpressionInputMonitor.FindingType.MISSING_DONE)
      assertThat(storage.contentReadCalls.get()).isEqualTo(0)
    }

  @Test
  fun `scan selects newest marker by creation time when case variants coexist`() =
    runBlocking<Unit> {
      for (newerName in listOf("done", "Done", "DONE", "dOnE")) {
        val olderName = if (newerName == "done") "DONE" else "done"
        val storage =
          FakeStorageClient(
            blob("raw/upload/$olderName", OLD.minusSeconds(2), generation = 99),
            blob("raw/upload/data.parquet", OLD.minusSeconds(1), size = 1),
            blob("raw/upload/$newerName", OLD, generation = 42),
          )
        val monitor = createMonitor(storage)

        val registeredNewest =
          monitor.scan(
            registeredDoneObjects =
              setOf(
                RawImpressionInputMonitor.DoneObjectIdentity(
                  "gs://bucket/raw/upload/$newerName",
                  42,
                )
              )
          )
        assertThat(registeredNewest.findings).isEmpty()
        assertThat(registeredNewest.missingDoneDirectories).isEqualTo(0)
        assertThat(registeredNewest.doneWithoutDataDirectories).isEqualTo(0)
        assertThat(registeredNewest.dataFilesAfterDone).isEqualTo(0)

        val registeredOlderOnly =
          monitor.scan(
            registeredDoneObjects =
              setOf(
                RawImpressionInputMonitor.DoneObjectIdentity(
                  "gs://bucket/raw/upload/$olderName",
                  99,
                )
              )
          )
        assertThat(registeredOlderOnly.unregisteredDoneDirectories).isEqualTo(1)
        assertThat(registeredOlderOnly.doneWithoutDataDirectories).isEqualTo(0)
        assertThat(registeredOlderOnly.dataFilesAfterDone).isEqualTo(0)
      }
    }

  @Test
  fun `scan retains listing order for markers with equal creation times`() =
    runBlocking<Unit> {
      val storage =
        FakeStorageClient(
          blob("raw/upload/data.parquet", OLD.minusSeconds(1), size = 1),
          blob("raw/upload/DONE", OLD, generation = 99),
          blob("raw/upload/done", OLD, generation = 42),
        )

      val result =
        createMonitor(storage)
          .scan(
            registeredDoneObjects =
              setOf(RawImpressionInputMonitor.DoneObjectIdentity("gs://bucket/raw/upload/done", 42))
          )

      assertThat(result.findings).isEmpty()
      assertThat(result.unregisteredDoneDirectories).isEqualTo(0)
      assertThat(result.dataFilesAfterDone).isEqualTo(0)
    }

  private fun createMonitor(
    storageClient: StorageClient,
    excludedBlobPrefixes: Set<String> = emptySet(),
  ): RawImpressionInputMonitor =
    RawImpressionInputMonitor(
      storageClient = storageClient,
      storageRootUri = "gs://bucket",
      blobPrefix = "raw",
      quietPeriod = QUIET_PERIOD,
      excludedBlobPrefixes = excludedBlobPrefixes,
      clock = Clock.fixed(NOW, ZoneOffset.UTC),
    )

  private fun blob(
    key: String,
    createTime: Instant,
    size: Long = 0L,
    generation: Long = 1L,
  ): FakeBlob = FakeBlob(key, size, createTime, generation)

  private class FakeStorageClient(vararg blobs: FakeBlob) : StorageClient {
    private val blobs = blobs.sortedBy { it.blobKey }
    val contentReadCalls = AtomicLong()

    init {
      for (blob in this.blobs) {
        blob.storageClient = this
        blob.contentReadCalls = contentReadCalls
      }
    }

    override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob =
      error("unexpected write")

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? =
      blobs.firstOrNull { it.blobKey == blobKey }

    override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
      flowOf(*blobs.filter { prefix == null || it.blobKey.startsWith(prefix) }.toTypedArray())
  }

  private class FakeBlob(
    override val blobKey: String,
    override val size: Long,
    override val createTime: Instant,
    generation: Long,
  ) : ConditionalOperationStorageClient.Blob {
    override lateinit var storageClient: StorageClient
    lateinit var contentReadCalls: AtomicLong

    override val updateTime: Instant = createTime
    override val metadata: Map<String, String> = emptyMap()
    override val freshnessToken: String = generation.toString()

    override fun read(): Flow<ByteString> {
      contentReadCalls.incrementAndGet()
      return emptyFlow()
    }

    override suspend fun delete() = error("unexpected delete")
  }

  companion object {
    private val NOW = Instant.parse("2026-10-06T12:00:00Z")
    private val QUIET_PERIOD = Duration.ofHours(1)
    private val OLD = NOW.minus(Duration.ofHours(2))
    private val RECENT = NOW.minus(Duration.ofMinutes(30))
  }
}
