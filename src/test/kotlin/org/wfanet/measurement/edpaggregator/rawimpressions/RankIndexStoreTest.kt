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
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexMap
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class RankIndexStoreTest {
  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
  private lateinit var kmsClient: FakeKmsClient
  private lateinit var storageClient: InMemoryStorageClient

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
  }

  private fun record(poolOffset: Long, rankedSize: Int, vararg ranks: Int): RankIndexMap =
    rankIndexMap {
      this.poolOffset = poolOffset
      this.rankedSize = rankedSize
      fingerprints = ByteString.copyFrom(ByteArray(ranks.size * 12) { (it % 251).toByte() })
      ranks.forEach { this.ranks += it }
    }

  @Test
  fun `writeBlob writes one record per chunk and readBlob round-trips`() = runBlocking {
    val store = RankIndexStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    val chunk1 = record(7L, 100, 0, 1)
    val chunk2 = record(7L, 100, 2)

    store.writeBlob("snapshot/subpool-7", dek, flowOf(chunk1, chunk2))

    val records = store.readBlob("snapshot/subpool-7", dek).toList()
    assertThat(records.map { it.ranksCount }).containsExactly(2, 1).inOrder()
    assertThat(records.all { it.poolOffset == 7L && it.rankedSize == 100 }).isTrue()
    assertThat(records.flatMap { it.ranksList }).containsExactly(0, 1, 2).inOrder()
  }

  @Test
  fun `readBlob validates a matching checksum`() = runBlocking {
    val store = RankIndexStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    val checksum = store.writeBlob("snapshot/subpool-7", dek, flowOf(record(7L, 100, 0, 1, 2)))

    val records = store.readBlob("snapshot/subpool-7", dek, expectedChecksum = checksum).toList()

    assertThat(records.flatMap { it.ranksList }).containsExactly(0, 1, 2).inOrder()
  }

  @Test
  fun `readBlob throws on a checksum mismatch`() = runBlocking {
    val store = RankIndexStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    store.writeBlob("snapshot/subpool-7", dek, flowOf(record(7L, 100, 0, 1, 2)))
    val wrongChecksum = ByteString.copyFromUtf8("not-the-real-checksum")

    assertFailsWith<IllegalStateException> {
      store.readBlob("snapshot/subpool-7", dek, expectedChecksum = wrongChecksum).toList()
    }
    Unit
  }

  @Test
  fun `openBlob returns the byte size and streams the same records as readBlob`() = runBlocking {
    val store = RankIndexStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    store.writeBlob("snapshot/subpool-7", dek, flowOf(record(7L, 100, 0, 1), record(7L, 100, 2)))

    val opened = store.openBlob("snapshot/subpool-7", dek)
    assertThat(opened).isNotNull()
    val (size, flow) = opened!!
    // Size matches the underlying (encrypted) blob's size — the same value blobSize returns.
    assertThat(size).isEqualTo(store.blobSize("snapshot/subpool-7", dek))
    assertThat(size).isGreaterThan(0L)
    // Records stream identically to readBlob.
    val viaOpen = flow.toList()
    val viaRead = store.readBlob("snapshot/subpool-7", dek).toList()
    assertThat(viaOpen).isEqualTo(viaRead)
    assertThat(viaOpen.flatMap { it.ranksList }).containsExactly(0, 1, 2).inOrder()
  }

  @Test
  fun `openBlob enforces the checksum on the returned flow`() = runBlocking {
    val store = RankIndexStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    store.writeBlob("snapshot/subpool-7", dek, flowOf(record(7L, 100, 0, 1, 2)))
    val wrongChecksum = ByteString.copyFromUtf8("not-the-real-checksum")

    val (_, flow) = store.openBlob("snapshot/subpool-7", dek, expectedChecksum = wrongChecksum)!!
    assertFailsWith<IllegalStateException> { flow.toList() }
    Unit
  }

  @Test
  fun `openBlob returns null for an absent blob`() = runBlocking {
    val store = RankIndexStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    assertThat(store.openBlob("snapshot/does-not-exist", dek)).isNull()
  }

  @Test
  fun `delete removes a blob`() = runBlocking {
    val store = RankIndexStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    store.writeBlob("snapshot/subpool-7", dek, flowOf(record(7L, 100, 0)))

    store.delete("snapshot/subpool-7")

    assertThat(store.readBlob("snapshot/subpool-7", dek).toList()).isEmpty()
  }

  @Test
  fun `blob keys are scoped by upload, model line, and attempt`() {
    val upload = "dataProviders/edp1/rawImpressionUploads/abc-123"
    val modelLine = "modelProviders/mp/modelSuites/ms/modelLines/ml1"

    assertThat(
        RankIndexStore.snapshotKey("maps", upload, modelLine, poolOffset = 7L, attemptId = "att1")
      )
      .isEqualTo("maps/upload/abc-123/modelLine/ml1/snapshot/subpoolOffset/7/attempt/att1")
    assertThat(
        RankIndexStore.dayOnlyKey("maps/", upload, modelLine, poolOffset = 7L, attemptId = "att2")
      )
      .isEqualTo("maps/upload/abc-123/modelLine/ml1/dayOnly/subpoolOffset/7/attempt/att2")
  }

  @Test
  fun `writeBlob of an empty flow yields an empty readable blob`() = runBlocking {
    val store = RankIndexStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)

    store.writeBlob("snapshot/empty", dek, emptyFlow())

    assertThat(store.readBlob("snapshot/empty", dek).toList()).isEmpty()
  }
}
