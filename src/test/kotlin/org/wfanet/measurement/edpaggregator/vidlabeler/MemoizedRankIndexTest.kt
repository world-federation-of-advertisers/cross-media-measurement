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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigest
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexMap
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

private const val DP = "dataProviders/dp"
private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml1"

@RunWith(JUnit4::class)
class MemoizedRankIndexTest {
  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
  private lateinit var kmsClient: FakeKmsClient
  private lateinit var storageClient: InMemoryStorageClient
  private lateinit var rankStore: RankIndexStore
  private lateinit var dek: EncryptedDek

  @Before
  fun setUp() {
    AeadConfig.register()
    kmsClient =
      FakeKmsClient().apply {
        setAead(
          kekUri,
          KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM")).getPrimitive(Aead::class.java),
        )
      }
    storageClient = InMemoryStorageClient()
    rankStore = RankIndexStore(storageClient, kmsClient)
    dek = rankStore.generateDek(kekUri)
  }

  /**
   * Writes a SNAPSHOT blob holding one `(fingerprint, rank)` and returns its `RankIndexBlob` row.
   */
  private fun seedSnapshot(
    blobKey: String,
    poolOffset: Long,
    fingerprint: ByteArray,
    rank: Int,
    createTimeSeconds: Long,
  ): RankIndexBlob = runBlocking {
    val record: RankIndexMap = rankIndexMap {
      this.poolOffset = poolOffset
      rankedSize = 1000
      fingerprints = ByteString.copyFrom(fingerprint)
      ranks += rank
    }
    val checksum = rankStore.writeBlob(blobKey, dek, flowOf(record))
    rankIndexBlob {
      this.poolOffset = poolOffset
      blobType = RankIndexBlob.BlobType.SNAPSHOT
      cmmsModelLine = MODEL_LINE
      blobUri = blobKey
      encryptedDek = dek
      blobChecksum = checksum
      createTime = timestamp { seconds = createTimeSeconds }
    }
  }

  private fun stubReturning(rows: List<RankIndexBlob>): RankIndexBlobServiceCoroutineStub = mock {
    onBlocking { listRankIndexBlobs(any(), any()) } doReturn
      listRankIndexBlobsResponse { rankIndexBlobs += rows }
  }

  @Test
  fun `load builds one map per subpool and looks up ranks`() {
    val fpA = fp(0x11)
    val fpB = fp(0x22)
    val rows =
      listOf(
        seedSnapshot("ranks/pool10/up1", poolOffset = 10L, fpA, rank = 5, createTimeSeconds = 1L),
        seedSnapshot("ranks/pool20/up1", poolOffset = 20L, fpB, rank = 7, createTimeSeconds = 1L),
      )

    val index = runBlocking {
      MemoizedRankIndex.load(stubReturning(rows), rankStore, DP, MODEL_LINE)
    }

    assertThat(index.subpoolCount).isEqualTo(2)
    assertThat(index.lookup(digestOf(fpA)).map { it.poolOffset to it.localRank })
      .containsExactly(10L to 5L)
    assertThat(index.lookup(digestOf(fpB)).map { it.poolOffset to it.localRank })
      .containsExactly(20L to 7L)
  }

  @Test
  fun `lookup returns all subpool ranks for a multi-subpool fingerprint`() {
    val fpX = fp(0x33)
    // The same fingerprint is ranked in two different subpools (it routed to each across uploads).
    val rows =
      listOf(
        seedSnapshot("ranks/pool10/up1", poolOffset = 10L, fpX, rank = 3, createTimeSeconds = 1L),
        seedSnapshot("ranks/pool20/up1", poolOffset = 20L, fpX, rank = 8, createTimeSeconds = 1L),
      )

    val index = runBlocking {
      MemoizedRankIndex.load(stubReturning(rows), rankStore, DP, MODEL_LINE)
    }

    assertThat(index.lookup(digestOf(fpX)).map { it.poolOffset to it.localRank })
      .containsExactly(10L to 3L, 20L to 8L)
  }

  @Test
  fun `lookup returns empty for an unseen fingerprint`() {
    val rows =
      listOf(seedSnapshot("ranks/pool10/up1", 10L, fp(0x11), rank = 5, createTimeSeconds = 1L))
    val index = runBlocking {
      MemoizedRankIndex.load(stubReturning(rows), rankStore, DP, MODEL_LINE)
    }

    assertThat(index.lookup(digestOf(fp(0x99)))).isEmpty()
  }

  @Test
  fun `load picks the newest snapshot per subpool by create_time`() {
    val fpA = fp(0x11)
    // Two SNAPSHOTs for the same subpool from different uploads: the newer create_time wins.
    val older = seedSnapshot("ranks/pool10/up0", 10L, fpA, rank = 99, createTimeSeconds = 1L)
    val newer = seedSnapshot("ranks/pool10/up1", 10L, fpA, rank = 5, createTimeSeconds = 2L)

    val index = runBlocking {
      MemoizedRankIndex.load(stubReturning(listOf(older, newer)), rankStore, DP, MODEL_LINE)
    }

    assertThat(index.subpoolCount).isEqualTo(1)
    assertThat(index.lookup(digestOf(fpA)).map { it.poolOffset to it.localRank })
      .containsExactly(10L to 5L)
  }

  @Test
  fun `load merges snapshots across pages`() {
    val fpA = fp(0x11)
    val fpB = fp(0x22)
    val page1 = listRankIndexBlobsResponse {
      rankIndexBlobs +=
        seedSnapshot("ranks/pool10/up1", poolOffset = 10L, fpA, rank = 5, createTimeSeconds = 1L)
      nextPageToken = "page-2"
    }
    val page2 = listRankIndexBlobsResponse {
      rankIndexBlobs +=
        seedSnapshot("ranks/pool20/up1", poolOffset = 20L, fpB, rank = 7, createTimeSeconds = 1L)
    }
    val stub: RankIndexBlobServiceCoroutineStub = mock {
      onBlocking { listRankIndexBlobs(any(), any()) }.doReturn(page1, page2)
    }

    val index = runBlocking { MemoizedRankIndex.load(stub, rankStore, DP, MODEL_LINE) }

    assertThat(index.subpoolCount).isEqualTo(2)
    assertThat(index.lookup(digestOf(fpA)).map { it.poolOffset to it.localRank })
      .containsExactly(10L to 5L)
    assertThat(index.lookup(digestOf(fpB)).map { it.poolOffset to it.localRank })
      .containsExactly(20L to 7L)
  }

  @Test
  fun `load throws when a memoized model line has no snapshots`() {
    val exception =
      assertFailsWith<IllegalStateException> {
        runBlocking {
          MemoizedRankIndex.load(stubReturning(emptyList()), rankStore, DP, MODEL_LINE)
        }
      }

    assertThat(exception).hasMessageThat().contains("No SNAPSHOT rank-index blobs")
  }

  @Test
  fun `load breaks create_time ties deterministically by blob name`() {
    // Two SNAPSHOTs for one subpool with an identical create_time: the greater resource name wins
    // regardless of listing order (guards against a listing-order-dependent winner).
    val b1 =
      seedSnapshot("ranks/pool10/b1", poolOffset = 10L, fp(0x11), rank = 5, createTimeSeconds = 1L)
        .copy { name = "blob-1" }
    val b2 =
      seedSnapshot("ranks/pool10/b2", poolOffset = 10L, fp(0x22), rank = 9, createTimeSeconds = 1L)
        .copy { name = "blob-2" }

    for (ordering in listOf(listOf(b1, b2), listOf(b2, b1))) {
      val index = runBlocking {
        MemoizedRankIndex.load(stubReturning(ordering), rankStore, DP, MODEL_LINE)
      }
      assertThat(index.lookup(digestOf(fp(0x22))).map { it.poolOffset to it.localRank })
        .containsExactly(10L to 9L)
      assertThat(index.lookup(digestOf(fp(0x11)))).isEmpty()
    }
  }

  @Test
  fun `load throws when fingerprints length does not match ranks`() {
    val blobKey = "ranks/pool10/up1"
    // One rank but two fingerprints' worth of bytes (24 = 2 * 12): violates the length invariant.
    val record = rankIndexMap {
      poolOffset = 10L
      rankedSize = 1000
      fingerprints = ByteString.copyFrom(fp(0x11) + fp(0x22))
      ranks += 5
    }
    val checksum = runBlocking { rankStore.writeBlob(blobKey, dek, flowOf(record)) }
    val row = rankIndexBlob {
      poolOffset = 10L
      blobType = RankIndexBlob.BlobType.SNAPSHOT
      cmmsModelLine = MODEL_LINE
      blobUri = blobKey
      encryptedDek = dek
      blobChecksum = checksum
      createTime = timestamp { seconds = 1L }
    }

    val exception =
      assertFailsWith<IllegalStateException> {
        runBlocking {
          MemoizedRankIndex.load(stubReturning(listOf(row)), rankStore, DP, MODEL_LINE)
        }
      }
    assertThat(exception).hasMessageThat().contains("Malformed rank index blob")
  }

  @Test
  fun `load throws when a blob checksum does not match`() {
    val corrupted =
      seedSnapshot("ranks/pool10/up1", 10L, fp(0x11), rank = 5, createTimeSeconds = 1L).copy {
        blobChecksum = ByteString.copyFromUtf8("not-the-real-checksum")
      }

    assertFailsWith<IllegalStateException> {
      runBlocking {
        MemoizedRankIndex.load(stubReturning(listOf(corrupted)), rankStore, DP, MODEL_LINE)
      }
    }
  }

  @Test
  fun `load records cold-start metrics keyed by subpool`() {
    val reader = InMemoryMetricReader.create()
    val meter = SdkMeterProvider.builder().registerMetricReader(reader).build().get("test")
    val metrics = MemoizedRankIndexMetrics(meter)
    val rows =
      listOf(
        seedSnapshot(
          "ranks/pool10/up1",
          poolOffset = 10L,
          fp(0x11),
          rank = 5,
          createTimeSeconds = 1L,
        ),
        seedSnapshot(
          "ranks/pool20/up1",
          poolOffset = 20L,
          fp(0x22),
          rank = 7,
          createTimeSeconds = 1L,
        ),
      )

    runBlocking {
      MemoizedRankIndex.load(stubReturning(rows), rankStore, DP, MODEL_LINE, metrics = metrics)
    }

    val collected = reader.collectAllMetrics().associateBy { it.name }
    assertThat(
        collected
          .getValue("edpa.vid_labeler.memoized_index.subpool_count")
          .longGaugeData
          .points
          .map { it.value }
      )
      .containsExactly(2L)
    assertThat(
        collected
          .getValue("edpa.vid_labeler.memoized_index.cold_start_duration")
          .histogramData
          .points
          .sumOf { it.count }
      )
      .isEqualTo(1L)
    assertThat(
        collected.getValue("edpa.vid_labeler.memoized_index.entry_count").longGaugeData.points.map {
          it.value
        }
      )
      .containsExactly(1L, 1L)
    assertThat(
        collected.getValue("edpa.vid_labeler.memoized_index.ranked_size").longGaugeData.points.map {
          it.value
        }
      )
      .containsExactly(1000L, 1000L)
  }

  @Test
  fun `load throws when records in a blob disagree on ranked_size`() {
    val blobKey = "ranks/pool10/up1"
    val record1 = rankIndexMap {
      poolOffset = 10L
      rankedSize = 1000
      fingerprints = ByteString.copyFrom(fp(0x11))
      ranks += 5
    }
    val record2 =
      record1.copy {
        rankedSize = 2000
        fingerprints = ByteString.copyFrom(fp(0x22))
      }
    val checksum = runBlocking { rankStore.writeBlob(blobKey, dek, flowOf(record1, record2)) }
    val row = rankIndexBlob {
      poolOffset = 10L
      blobType = RankIndexBlob.BlobType.SNAPSHOT
      cmmsModelLine = MODEL_LINE
      blobUri = blobKey
      encryptedDek = dek
      blobChecksum = checksum
      createTime = timestamp { seconds = 1L }
    }

    assertFailsWith<IllegalStateException> {
      runBlocking { MemoizedRankIndex.load(stubReturning(listOf(row)), rankStore, DP, MODEL_LINE) }
    }
  }

  @Test
  fun `load throws when a blob has no records`() {
    val blobKey = "ranks/pool10/up1"
    val checksum = runBlocking { rankStore.writeBlob(blobKey, dek, emptyFlow()) }
    val row = rankIndexBlob {
      poolOffset = 10L
      blobType = RankIndexBlob.BlobType.SNAPSHOT
      cmmsModelLine = MODEL_LINE
      blobUri = blobKey
      encryptedDek = dek
      blobChecksum = checksum
      createTime = timestamp { seconds = 1L }
    }

    assertFailsWith<IllegalStateException> {
      runBlocking { MemoizedRankIndex.load(stubReturning(listOf(row)), rankStore, DP, MODEL_LINE) }
    }
  }

  companion object {
    /** A 12-byte fingerprint whose every byte is [tag]. */
    private fun fp(tag: Int): ByteArray = ByteArray(12) { tag.toByte() }

    /** The [EventIdDigest] of a 12-byte fingerprint (big-endian high 8 bytes, low 4 bytes). */
    private fun digestOf(bytes: ByteArray): EventIdDigest {
      var high = 0L
      for (i in 0 until 8) high = (high shl 8) or (bytes[i].toLong() and 0xFF)
      var low = 0
      for (i in 8 until 12) low = (low shl 8) or (bytes[i].toInt() and 0xFF)
      return EventIdDigest(high, low)
    }
  }
}
