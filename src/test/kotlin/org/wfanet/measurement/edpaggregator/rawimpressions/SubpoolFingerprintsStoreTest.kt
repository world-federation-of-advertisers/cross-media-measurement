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
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class SubpoolFingerprintsStoreTest {
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

  private fun pack(fingerprints: List<ByteArray>): ByteString {
    val out = ByteString.newOutput()
    fingerprints.forEach {
      require(it.size == 12)
      out.write(it)
    }
    return out.toByteString()
  }

  private fun fp(fill: Int): ByteArray = ByteArray(12) { fill.toByte() }

  private suspend fun readAll(
    store: SubpoolFingerprintsStore,
    key: String,
    dek: EncryptedDek,
  ): ByteString {
    val out = ByteString.newOutput()
    store.readBlob(key, dek).toList().forEach { out.write(it.fingerprints.toByteArray()) }
    return out.toByteString()
  }

  @Test
  fun `writeBlob writes one record per chunk and readBlob round-trips`() = runBlocking {
    val store = SubpoolFingerprintsStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    val chunk1 = pack(listOf(fp(0x11), fp(0x22)))
    val chunk2 = pack(listOf(fp(0x33)))

    store.writeBlob(
      "shard-0/subpool-7",
      dek,
      poolOffset = 7L,
      fingerprintChunks = flowOf(chunk1, chunk2),
    )

    val records = store.readBlob("shard-0/subpool-7", dek).toList()
    assertThat(records.map { it.fingerprints.size() }).containsExactly(24, 12).inOrder()
    assertThat(records.all { it.poolOffset == 7L }).isTrue()
    assertThat(readAll(store, "shard-0/subpool-7", dek)).isEqualTo(chunk1.concat(chunk2))
  }

  @Test
  fun `mergeSubpool concatenates disjoint shard blobs`() = runBlocking {
    val store = SubpoolFingerprintsStore(storageClient, kmsClient)
    val dek0 = store.generateDek(kekUri)
    val dek1 = store.generateDek(kekUri)
    store.writeBlob("shard-0/subpool-7", dek0, 7L, flowOf(pack(listOf(fp(0x11), fp(0x22)))))
    store.writeBlob("shard-1/subpool-7", dek1, 7L, flowOf(pack(listOf(fp(0x33)))))

    val mergedDek = store.generateDek(kekUri)
    store.mergeSubpool(
      listOf(
        SubpoolFingerprintsStore.SubpoolBlob("shard-0/subpool-7", dek0),
        SubpoolFingerprintsStore.SubpoolBlob("shard-1/subpool-7", dek1),
        // A shard with no blob for this subpool is skipped.
        SubpoolFingerprintsStore.SubpoolBlob("shard-2/subpool-7", dek1),
      ),
      outputKey = "merged/subpool-7",
      outputEncryptedDek = mergedDek,
    )

    assertThat(readAll(store, "merged/subpool-7", mergedDek))
      .isEqualTo(pack(listOf(fp(0x11), fp(0x22), fp(0x33))))
  }

  @Test
  fun `delete removes a blob`() = runBlocking {
    val store = SubpoolFingerprintsStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    store.writeBlob("shard-0/subpool-7", dek, 7L, flowOf(pack(listOf(fp(0x11)))))

    store.delete("shard-0/subpool-7")

    assertThat(store.readBlob("shard-0/subpool-7", dek).toList()).isEmpty()
  }

  @Test
  fun `blob keys are scoped by upload and model line resource ids`() {
    val upload = "dataProviders/edp1/rawImpressionUploads/abc-123"
    val modelLine = "modelProviders/mp/modelSuites/ms/modelLines/ml1"

    assertThat(
        SubpoolFingerprintsStore.shardSubpoolKey(
          "maps",
          upload,
          modelLine,
          shardIndex = 2,
          poolOffset = 7L,
        )
      )
      .isEqualTo("maps/upload/abc-123/modelLine/ml1/shard/2/subpoolOffset/7")
    assertThat(
        SubpoolFingerprintsStore.mergedSubpoolKey("maps/", upload, modelLine, poolOffset = 7L)
      )
      .isEqualTo("maps/upload/abc-123/modelLine/ml1/merged/subpoolOffset/7")
  }
}
