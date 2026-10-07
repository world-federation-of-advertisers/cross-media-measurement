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
                  blobKey = "raw/removed/done",
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
            setOf(RawImpressionInputMonitor.DoneObjectIdentity("raw/upload/done", 9))
        )
      assertThat(registered.unregisteredDoneDirectories).isEqualTo(0)
    }

  private fun createMonitor(
    storageClient: StorageClient,
    excludedBlobPrefixes: Set<String> = emptySet(),
  ): RawImpressionInputMonitor =
    RawImpressionInputMonitor(
      storageClient = storageClient,
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
