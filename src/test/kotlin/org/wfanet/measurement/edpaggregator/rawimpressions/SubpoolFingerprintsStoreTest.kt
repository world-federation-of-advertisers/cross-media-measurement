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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient
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
  fun `attempt input lookup reads a legacy shard with its persisted DEK`() =
    runBlocking<Unit> {
      val store = SubpoolFingerprintsStore(storageClient, kmsClient)
      val dek = store.generateDek(kekUri)
      val legacy =
        SubpoolFingerprintsStore.shardSubpoolKey("maps", "uploads/up", "modelLines/ml", 0, 7L)
      val attempt =
        SubpoolFingerprintsStore.shardSubpoolKey("maps", "uploads/up", "modelLines/ml", 0, 7L, dek)
      store.writeBlob(legacy, dek, 7L, flowOf(pack(listOf(fp(0x11)))))
      val mergedDek = store.generateDek(kekUri)
      store.mergeSubpool(
        listOf(SubpoolFingerprintsStore.SubpoolBlob(attempt, dek)),
        "merged",
        mergedDek,
        Semaphore(1),
      )
      assertThat(readAll(store, "merged", mergedDek)).isEqualTo(pack(listOf(fp(0x11))))
    }

  @Test
  fun `conditional shard write leaves winning ciphertext and DEK unchanged`() =
    runBlocking<Unit> {
      val store = SubpoolFingerprintsStore(storageClient, kmsClient)
      val winner = store.generateDek(kekUri)
      val loser = store.generateDek(kekUri)
      val expected = pack(listOf(fp(0x11)))
      store.writeBlob("shard", winner, 7L, flowOf(expected))
      val ciphertext = checkNotNull(storageClient.getBlob("shard")).read().toList()
      assertFailsWith<BlobChangedException> {
        store.writeBlob("shard", loser, 7L, flowOf(pack(listOf(fp(0x22)))))
      }
      assertThat(checkNotNull(storageClient.getBlob("shard")).read().toList()).isEqualTo(ciphertext)
      assertThat(readAll(store, "shard", winner)).isEqualTo(expected)
    }

  @Test
  fun `different wrapped DEKs isolate shard attempts while persisted bytes reconstruct the key`() =
    runBlocking<Unit> {
      val store = SubpoolFingerprintsStore(storageClient, kmsClient)
      val winner = store.generateDek(kekUri)
      val loser = store.generateDek(kekUri)
      val upload = "dataProviders/dp/rawImpressionUploads/up"
      val model = "modelProviders/mp/modelSuites/ms/modelLines/ml"
      val winnerKey = SubpoolFingerprintsStore.shardSubpoolKey("maps", upload, model, 0, 7L, winner)
      val loserKey = SubpoolFingerprintsStore.shardSubpoolKey("maps", upload, model, 0, 7L, loser)
      assertThat(loserKey).isNotEqualTo(winnerKey)
      assertThat(
          SubpoolFingerprintsStore.shardSubpoolKey(
            "maps",
            upload,
            model,
            0,
            7L,
            EncryptedDek.parseFrom(winner.toByteString()),
          )
        )
        .isEqualTo(winnerKey)
      store.writeBlob(winnerKey, winner, 7L, flowOf(pack(listOf(fp(0x11)))))
      store.writeBlob(loserKey, loser, 7L, flowOf(pack(listOf(fp(0x22)))))
      assertThat(readAll(store, winnerKey, winner)).isEqualTo(pack(listOf(fp(0x11))))
      assertThat(readAll(store, loserKey, loser)).isEqualTo(pack(listOf(fp(0x22))))
    }

  @Test
  fun `failed first merge publishes a clean prefix and retry replaces its generation`() =
    runBlocking<Unit> {
      val writer = SubpoolFingerprintsStore(storageClient, kmsClient)
      val shardDek = writer.generateDek(kekUri)
      val mergedDek = writer.generateDek(kekUri)
      val prefix = pack(listOf(fp(0x11)))
      val suffix = pack(listOf(fp(0x22)))
      writer.writeBlob("input", shardDek, 7L, flowOf(prefix, suffix))
      var failFirst = true
      var replacements = 0
      var expectedToken: String? = null
      val failingClient =
        object : ConditionalOperationStorageClient by storageClient {
          override suspend fun writeBlobIfNotFound(
            blobKey: String,
            content: Flow<ByteString>,
          ): StorageClient.Blob {
            if (blobKey == "merged" && failFirst) {
              failFirst = false
              // Simulate finalization of already processed records on input failure.
              writer.writeBlob("merged", mergedDek, 7L, flowOf(prefix))
              throw java.io.IOException("source failed after one complete record")
            }
            return storageClient.writeBlobIfNotFound(blobKey, content)
          }

          override suspend fun writeBlobIfUnchanged(
            blobKey: String,
            freshnessToken: String,
            content: Flow<ByteString>,
          ): StorageClient.Blob {
            replacements++
            assertThat(freshnessToken).isEqualTo(expectedToken)
            return storageClient.writeBlobIfUnchanged(blobKey, freshnessToken, content)
          }
        }
      val store = SubpoolFingerprintsStore(failingClient, kmsClient)
      val inputs = listOf(SubpoolFingerprintsStore.SubpoolBlob("input", shardDek))
      assertFailsWith<java.io.IOException> {
        store.mergeSubpool(inputs, "merged", mergedDek, Semaphore(1))
      }
      // This prefix is valid ciphertext and RecordIO, so scanning it accepts incomplete output.
      assertThat(readAll(writer, "merged", mergedDek)).isEqualTo(prefix)
      expectedToken = storageClient.getFreshnessToken("merged")
      store.mergeSubpool(inputs, "merged", mergedDek, Semaphore(1))
      assertThat(readAll(writer, "merged", mergedDek)).isEqualTo(prefix.concat(suffix))
      assertThat(replacements).isEqualTo(1)
      assertThat(storageClient.getFreshnessToken("merged")).isNotEqualTo(expectedToken)
    }

  @Test
  fun `ranking retry accepts existing output without reading or writing payload`() =
    runBlocking<Unit> {
      val writer = SubpoolFingerprintsStore(storageClient, kmsClient)
      val dek = writer.generateDek(kekUri)
      writer.writeBlob("merged", dek, 7L, flowOf(pack(listOf(fp(0x11)))))
      val metadataOnlyClient =
        object : ConditionalOperationStorageClient by storageClient {
          override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
            val blob = storageClient.getBlob(blobKey) ?: return null
            return object : StorageClient.Blob by blob {
              override fun read(): Flow<ByteString> = error("merged payload must not be read")
            }
          }

          override suspend fun writeBlobIfNotFound(
            blobKey: String,
            content: Flow<ByteString>,
          ): StorageClient.Blob = error("completed merge must not be rewritten")

          override suspend fun writeBlobIfUnchanged(
            blobKey: String,
            freshnessToken: String,
            content: Flow<ByteString>,
          ): StorageClient.Blob = error("completed merge must not be replaced")
        }
      SubpoolFingerprintsStore(metadataOnlyClient, kmsClient).mergeSubpool(
        emptyList(),
        "merged",
        dek,
        Semaphore(1),
      ) {
        false
      }
    }

  @Test
  fun `pool assigning replacement never reads the existing merged payload`() =
    runBlocking<Unit> {
      val writer = SubpoolFingerprintsStore(storageClient, kmsClient)
      val dek = writer.generateDek(kekUri)
      writer.writeBlob("merged", dek, 7L, flowOf(pack(listOf(fp(0x00)))))
      writer.writeBlob("input", dek, 7L, flowOf(pack(listOf(fp(0x11)))))
      val noOutputRead =
        object : ConditionalOperationStorageClient by storageClient {
          override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
            val blob = storageClient.getBlob(blobKey) ?: return null
            if (blobKey != "merged") return blob
            val conditional = blob as ConditionalOperationStorageClient.Blob
            return object : ConditionalOperationStorageClient.Blob by conditional {
              override fun read(): Flow<ByteString> =
                error("existing merged payload must not be read")
            }
          }
        }
      SubpoolFingerprintsStore(noOutputRead, kmsClient)
        .mergeSubpool(
          listOf(SubpoolFingerprintsStore.SubpoolBlob("input", dek)),
          "merged",
          dek,
          Semaphore(1),
        )
      assertThat(readAll(writer, "merged", dek)).isEqualTo(pack(listOf(fp(0x11))))
    }

  @Test
  fun `lost replacement generation is re-observed before retry`() =
    runBlocking<Unit> {
      val writer = SubpoolFingerprintsStore(storageClient, kmsClient)
      val dek = writer.generateDek(kekUri)
      writer.writeBlob("merged", dek, 7L, flowOf(pack(listOf(fp(0x00)))))
      writer.writeBlob("input", dek, 7L, flowOf(pack(listOf(fp(0x11)))))
      val tokens = mutableListOf<String>()
      val racingClient =
        object : ConditionalOperationStorageClient by storageClient {
          override suspend fun writeBlobIfUnchanged(
            blobKey: String,
            freshnessToken: String,
            content: Flow<ByteString>,
          ): StorageClient.Blob {
            tokens += freshnessToken
            if (tokens.size == 1) {
              storageClient.writeBlob(
                blobKey,
                flowOf(ByteString.copyFromUtf8("changed generation")),
              )
            }
            return storageClient.writeBlobIfUnchanged(blobKey, freshnessToken, content)
          }
        }
      SubpoolFingerprintsStore(racingClient, kmsClient)
        .mergeSubpool(
          listOf(SubpoolFingerprintsStore.SubpoolBlob("input", dek)),
          "merged",
          dek,
          Semaphore(1),
        )
      assertThat(tokens).hasSize(2)
      assertThat(tokens[1]).isNotEqualTo(tokens[0])
      assertThat(readAll(writer, "merged", dek)).isEqualTo(pack(listOf(fp(0x11))))
    }

  @Test
  fun `parent advancing during source reads leaves committed output unchanged`() =
    runBlocking<Unit> {
      val writer = SubpoolFingerprintsStore(storageClient, kmsClient)
      val dek = writer.generateDek(kekUri)
      writer.writeBlob("merged", dek, 7L, flowOf(pack(listOf(fp(0x11)))))
      writer.writeBlob("input", dek, 7L, flowOf(pack(listOf(fp(0x22)))))
      val token = storageClient.getFreshnessToken("merged")
      var stateReads = 0
      SubpoolFingerprintsStore(storageClient, kmsClient).mergeSubpool(
        listOf(SubpoolFingerprintsStore.SubpoolBlob("input", dek)),
        "merged",
        dek,
        Semaphore(1),
      ) {
        stateReads++ == 0
      }
      assertThat(stateReads).isEqualTo(2)
      assertThat(storageClient.getFreshnessToken("merged")).isEqualTo(token)
      assertThat(readAll(writer, "merged", dek)).isEqualTo(pack(listOf(fp(0x11))))
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
  fun `writeBlob builds its own client per writer and is safe under concurrent writers`() =
    runBlocking<Unit> {
      // Review #4235: per-subpool uploads run concurrently on one store (SubpoolAssigner's
      // uploadSemaphore). encryptedClient() must build a FRESH client per call — sharing one client
      // across concurrent writeBlob interleaves its mutable write state and mixes fingerprints
      // between subpool blobs. InMemoryStorageClient is a thread-safe backend, so any corruption
      // here is the client, not the storage layer.
      val countingKms = CountingKmsClient(kmsClient)
      val store = SubpoolFingerprintsStore(InMemoryStorageClient(), countingKms)
      val dek = store.generateDek(kekUri)
      countingKms.unwrapCount.set(0)

      val n = 16
      val chunks = (0 until n).associateWith { pack(listOf(fp(it))) }
      coroutineScope {
        (0 until n).map { i ->
          launch(Dispatchers.IO) {
            store.writeBlob("subpool-" + i, dek, i.toLong(), flowOf(chunks.getValue(i)))
          }
        }
      }

      // One client (one KEK->DEK unwrap) PER concurrent writer — no shared/cached client.
      assertThat(countingKms.unwrapCount.get()).isEqualTo(n)
      // ...and no cross-writer corruption: each blob round-trips to exactly its own bytes.
      for (i in 0 until n) {
        assertThat(readAll(store, "subpool-" + i, dek)).isEqualTo(chunks.getValue(i))
      }
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
