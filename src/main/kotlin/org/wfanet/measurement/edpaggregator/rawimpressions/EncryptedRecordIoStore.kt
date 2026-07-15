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

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.MesosRecordIoStorageClient

/**
 * Shared envelope-encryption plumbing for the memoized VID pipeline's **RecordIO** blob stores.
 *
 * Both [SubpoolFingerprintsStore] (Phase-0 fingerprint sets) and [RankIndexStore] (Phase-1
 * rank-index maps) persist a stream of protobuf records to a single blob, each blob envelope-
 * encrypted with a per-writer DEK wrapped under the EDP's KEK. The DEK generation and the encrypted
 * [MesosRecordIoStorageClient] construction are identical across both; only the record type and the
 * per-record framing differ, so those live in the concrete subclasses' typed `writeBlob` /
 * `readBlob`.
 *
 * DEK *persistence* is not this layer's concern — each subclass's caller records the [EncryptedDek]
 * on the appropriate metadata-storage row.
 *
 * @param storageClient the base (unencrypted) storage client for the bucket.
 * @param kmsClient KMS client able to wrap/unwrap the EDP's KEK.
 */
abstract class EncryptedRecordIoStore(
  protected val storageClient: ConditionalOperationStorageClient,
  protected val kmsClient: KmsClient,
) {
  /** Generates a fresh DEK wrapped under [kekUri], used to encrypt this VM's blobs. */
  fun generateDek(kekUri: String): EncryptedDek {
    val serialized =
      EncryptedStorage.generateSerializedEncryptionKey(kmsClient, kekUri, TINK_KEY_TEMPLATE)
    return encryptedDek {
      this.kekUri = kekUri
      ciphertext = serialized
      protobufFormat = EncryptedDek.ProtobufFormat.BINARY
      typeUrl = TYPE_URL_TINK_KEYSET
    }
  }

  /**
   * Caches one [MesosRecordIoStorageClient] per distinct [EncryptedDek] for the lifetime of this
   * store (one `WorkItem`). Building a client unwraps the DEK under the KEK (a KMS round-trip) and
   * allocates per-client encryption buffers, so reusing the cached instance across repeated and
   * concurrent `getBlob`/`writeBlob` calls avoids redundant KEK->DEK unwraps and buffer allocs. The
   * cached clients are stateless and thread-safe (`MesosRecordIoStorageClient` /
   * `StreamingAeadStorageClient` / `GcsStorageClient` hold no mutable instance state), so a single
   * instance is safe for concurrent reuse. [EncryptedDek] is an immutable proto, so it is a valid
   * map key.
   */
  private val clientCache =
    java.util.concurrent.ConcurrentHashMap<EncryptedDek, MesosRecordIoStorageClient>()

  /**
   * A cached [MesosRecordIoStorageClient] over [storageClient] that envelope-encrypts with
   * [encryptedDek].
   */
  protected fun encryptedClient(encryptedDek: EncryptedDek): MesosRecordIoStorageClient =
    clientCache.computeIfAbsent(encryptedDek) { dek ->
      EncryptedStorage.buildEncryptedMesosStorageClient(
        storageClient,
        kmsClient = kmsClient,
        kekUri = dek.kekUri,
        encryptedDek = dek,
      )
    }

  companion object {
    init {
      // The DEK template is a StreamingAead key, so the streaming key managers must be registered
      // before the DEK is generated; AeadConfig covers the KEK unwrap. Registering in the companion
      // init guarantees this runs on class-load, ahead of any DEK generation.
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    const val TINK_KEY_TEMPLATE = "AES256_GCM_HKDF_1MB" // a StreamingAeadKey
    private const val TYPE_URL_TINK_KEYSET = "type.googleapis.com/google.crypto.tink.Keyset"
  }
}
