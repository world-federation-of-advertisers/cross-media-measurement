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
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class SubpoolFingerprintsStoreTest {
  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
  private lateinit var kmsClient: FakeKmsClient
  private lateinit var storageClient: InMemoryStorageClient

  @Before
  fun setUp() {
    AeadConfig.register()
    // The per-writer DEK uses a StreamingAead key template (see EncryptedRecordIoStore).
    StreamingAeadConfig.register()
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

  private fun bs(bytes: ByteArray): ByteString = ByteString.copyFrom(bytes)

  /** Decrypts [key] and returns its 12-byte fingerprints as a SET (order-independent). */
  private suspend fun fingerprintSet(
    store: SubpoolFingerprintsStore,
    key: String,
    dek: EncryptedDek,
  ): Set<ByteString> {
    val set = mutableSetOf<ByteString>()
    store.readBlob(key, dek).toList().forEach { record ->
      val bytes = record.fingerprints
      var i = 0
      while (i < bytes.size()) {
        set.add(bytes.substring(i, i + 12))
        i += 12
      }
    }
    return set
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
  fun `mergeSubpool merges disjoint shard blobs and skips a missing input`() =
    runBlocking<Unit> {
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
        readSemaphore = Semaphore(16),
      )

      // Reads run concurrently, so records may interleave across shards; assert on the SET.
      assertThat(fingerprintSet(store, "merged/subpool-7", mergedDek))
        .containsExactly(bs(fp(0x11)), bs(fp(0x22)), bs(fp(0x33)))
    }

  @Test
  fun `mergeSubpool caches the encrypted client per DEK across ops and subpools`() = runBlocking {
    // FileSystemStorageClient exercises a real streaming backend; the counting KMS proves the
    // cache.
    val countingKms = CountingKmsClient(kmsClient)
    val tempDir = Files.createTempDirectory("subpool-fingerprints-store-test").toFile()
    val fsClient: ConditionalOperationStorageClient =
      asConditional(FileSystemStorageClient(tempDir))
    val store = SubpoolFingerprintsStore(fsClient, countingKms)

    // Two shards, each with its own DEK; two subpools (7, 8) with disjoint fingerprints.
    val dek0 = store.generateDek(kekUri)
    val dek1 = store.generateDek(kekUri)
    val mergedDek = store.generateDek(kekUri)
    // Count only the KEK->DEK unwraps done by the blob operations below (DEK generation above wraps
    // a keyset with its own KEK round-trip, which is not what the client cache is about).
    countingKms.unwrapCount.set(0)
    store.writeBlob("shard-0/subpool-7", dek0, 7L, flowOf(pack(listOf(fp(0x11), fp(0x22)))))
    store.writeBlob("shard-1/subpool-7", dek1, 7L, flowOf(pack(listOf(fp(0x33)))))
    store.writeBlob("shard-0/subpool-8", dek0, 8L, flowOf(pack(listOf(fp(0x44)))))
    store.writeBlob("shard-1/subpool-8", dek1, 8L, flowOf(pack(listOf(fp(0x55), fp(0x66)))))

    val readSemaphore = Semaphore(16)
    for (offset in listOf(7L, 8L)) {
      store.mergeSubpool(
        listOf(
          SubpoolFingerprintsStore.SubpoolBlob("shard-0/subpool-$offset", dek0),
          SubpoolFingerprintsStore.SubpoolBlob("shard-1/subpool-$offset", dek1),
          // A shard with no blob for this subpool is skipped.
          SubpoolFingerprintsStore.SubpoolBlob("shard-2/subpool-$offset", dek1),
        ),
        outputKey = "merged/subpool-$offset",
        outputEncryptedDek = mergedDek,
        readSemaphore = readSemaphore,
      )
    }

    // Each merged blob's fingerprint SET is the disjoint union of its shards, order-independent.
    assertThat(fingerprintSet(store, "merged/subpool-7", mergedDek))
      .containsExactly(bs(fp(0x11)), bs(fp(0x22)), bs(fp(0x33)))
    assertThat(fingerprintSet(store, "merged/subpool-8", mergedDek))
      .containsExactly(bs(fp(0x44)), bs(fp(0x55)), bs(fp(0x66)))

    // Cache proof: many blob ops (4 input writes + 2 merged writes + 4 shard reads + 2 readbacks,
    // spanning both subpools) ran, but the KEK->DEK unwrap happened exactly once per DISTINCT DEK
    // (dek0, dek1, mergedDek), NOT once per blob op — proving encryptedClient() is cached per DEK.
    val distinctDeks = setOf(dek0, dek1, mergedDek).size
    assertThat(distinctDeks).isEqualTo(3)
    assertThat(countingKms.unwrapCount.get()).isEqualTo(distinctDeks)
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
  fun `blobSize returns the blob byte size and 0 when the blob is absent`() = runBlocking {
    val store = SubpoolFingerprintsStore(storageClient, kmsClient)
    val dek = store.generateDek(kekUri)
    // Absent blob -> 0 so the forward rank build falls back to MIN_CAPACITY.
    assertThat(store.blobSize("shard-0/subpool-7", dek)).isEqualTo(0L)
    store.writeBlob("shard-0/subpool-7", dek, 7L, flowOf(pack(listOf(fp(0x11), fp(0x22)))))
    // Present blob -> a positive byte size the caller divides by EventIdDigestBytes.WIDTH to
    // estimate the fingerprint count.
    assertThat(store.blobSize("shard-0/subpool-7", dek)).isGreaterThan(0L)
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

  /**
   * Adapts a plain [StorageClient] to [ConditionalOperationStorageClient]. The store's writes are
   * unconditional, so the conditional-write method is never exercised.
   */
  private fun asConditional(delegate: StorageClient): ConditionalOperationStorageClient =
    object : ConditionalOperationStorageClient, StorageClient by delegate {
      override suspend fun getFreshnessToken(blobKey: String): String? =
        throw UnsupportedOperationException("unconditional writes only")

      override suspend fun writeBlobIfNotFound(
        blobKey: String,
        content: Flow<ByteString>,
      ): StorageClient.Blob = throw UnsupportedOperationException("unconditional writes only")

      override suspend fun writeBlobIfUnchanged(
        blob: StorageClient.Blob,
        content: Flow<ByteString>,
      ): StorageClient.Blob = throw UnsupportedOperationException("unconditional writes only")

      override suspend fun writeBlobIfUnchanged(
        blobKey: String,
        freshnessToken: String,
        content: Flow<ByteString>,
      ): StorageClient.Blob = throw UnsupportedOperationException("unconditional writes only")
    }

  /**
   * Wraps a [KmsClient] to count KEK->DEK unwraps, i.e. how many times a wrapped DEK is decrypted
   * under its KEK Aead. Building an encrypted client unwraps exactly once; DEK generation (wrap)
   * uses `encrypt`, so it is not counted.
   */
  private class CountingKmsClient(private val delegate: KmsClient) : KmsClient {
    val unwrapCount = AtomicInteger()

    override fun doesSupport(keyUri: String?): Boolean = delegate.doesSupport(keyUri)

    override fun withCredentials(credentialPath: String?): KmsClient = this

    override fun withDefaultCredentials(): KmsClient = this

    override fun getAead(keyUri: String?): Aead {
      val delegateAead = delegate.getAead(keyUri)
      return object : Aead {
        override fun encrypt(plaintext: ByteArray?, associatedData: ByteArray?): ByteArray =
          delegateAead.encrypt(plaintext, associatedData)

        override fun decrypt(ciphertext: ByteArray?, associatedData: ByteArray?): ByteArray {
          unwrapCount.incrementAndGet()
          return delegateAead.decrypt(ciphertext, associatedData)
        }
      }
    }
  }
}
