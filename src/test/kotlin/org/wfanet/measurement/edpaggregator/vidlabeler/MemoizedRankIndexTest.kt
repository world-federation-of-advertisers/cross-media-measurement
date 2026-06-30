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
    index.lookup(digestOf(fpA))!!.let {
      assertThat(it.poolOffset).isEqualTo(10L)
      assertThat(it.localRank).isEqualTo(5L)
    }
    index.lookup(digestOf(fpB))!!.let {
      assertThat(it.poolOffset).isEqualTo(20L)
      assertThat(it.localRank).isEqualTo(7L)
    }
  }

  @Test
  fun `lookup returns null for an unseen fingerprint`() {
    val rows =
      listOf(seedSnapshot("ranks/pool10/up1", 10L, fp(0x11), rank = 5, createTimeSeconds = 1L))
    val index = runBlocking {
      MemoizedRankIndex.load(stubReturning(rows), rankStore, DP, MODEL_LINE)
    }

    assertThat(index.lookup(digestOf(fp(0x99)))).isNull()
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
    assertThat(index.lookup(digestOf(fpA))!!.localRank).isEqualTo(5L)
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
