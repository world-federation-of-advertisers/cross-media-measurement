// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator

import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.protobuf.ByteString
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.edpaggregator.resultsfulfiller.crypto.parseJsonEncryptedKey
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek.ProtobufFormat
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.StorageClient

/** Useful functions for interacting with encrypted storage. */
object EncryptedStorage {

  private const val TYPE_URL_ENCRYPTION_KEY =
    "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.EncryptionKey"

  private const val TYPE_URL_TINK_KEYSET = "type.googleapis.com/google.crypto.tink.Keyset"

  /** Generates a serialized encrypted keyset using a [KmsClient]. */
  fun generateSerializedEncryptionKey(
    kmsClient: KmsClient,
    kekUri: String,
    tinkKeyTemplateType: String,
    associatedData: ByteArray = byteArrayOf(),
  ): ByteString {
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    return ByteString.copyFrom(
      TinkProtoKeysetFormat.serializeEncryptedKeyset(
        keyEncryptionHandle,
        kmsClient.getAead(kekUri),
        associatedData,
      )
    )
  }

  /**
   * Wraps [storageClient] with envelope-encryption decryption using the
   * caller-supplied [encryptedDek]. The DEK is unwrapped via the [kmsClient]
   * (resolving the KEK at [kekUri]) and the returned [StorageClient]
   * decrypts blob bytes on read.
   *
   * Dispatches the unwrap based on `(encryptedDek.typeUrl,
   * encryptedDek.protobufFormat)` — currently supports two formats used
   * across the EDP-Aggregator pipeline:
   *  - `type.googleapis.com/google.crypto.tink.Keyset` + `BINARY` — standard
   *    Tink-encrypted keyset (default `parseEncryptedKeyset`).
   *  - `type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.EncryptionKey`
   *    + `JSON` — the v1alpha `EncryptionKey` proto in JSON, parsed via
   *    [parseJsonEncryptedKey].
   *
   * Throws [IllegalArgumentException] for any other combination so format
   * mistakes fail loudly instead of silently.
   *
   * This is the single source of truth for the DEK format dispatch — call
   * it from any code path that needs to read EDP-Aggregator encrypted blobs.
   */
  fun buildDecryptingStorageClient(
    storageClient: StorageClient,
    kmsClient: KmsClient,
    kekUri: String,
    encryptedDek: EncryptedDek,
  ): StorageClient =
    when (encryptedDek.typeUrl to encryptedDek.protobufFormat) {
      TYPE_URL_TINK_KEYSET to ProtobufFormat.BINARY ->
        storageClient.withEnvelopeEncryption(
          kmsClient = kmsClient,
          kekUri = kekUri,
          encryptedDek = encryptedDek.ciphertext,
        )
      TYPE_URL_ENCRYPTION_KEY to ProtobufFormat.JSON ->
        storageClient.withEnvelopeEncryption(
          kmsClient = kmsClient,
          kekUri = kekUri,
          encryptedDek = encryptedDek.ciphertext,
          parseEncryptedKeyset = ::parseJsonEncryptedKey,
        )
      else ->
        throw IllegalArgumentException(
          "Unsupported type_url=${encryptedDek.typeUrl} with format=${encryptedDek.protobufFormat}"
        )
    }

  /**
   * Builds an envelope-encryption storage client wrapped by
   * [MesosRecordIoStorageClient] — convenience for callers that need
   * record-oriented reads on top of Mesos RecordIO blobs (e.g. the
   * `ResultsFulfiller` labeled-impression pipeline).
   *
   * Delegates DEK format handling to [buildDecryptingStorageClient].
   */
  fun buildEncryptedMesosStorageClient(
    storageClient: StorageClient,
    kmsClient: KmsClient,
    kekUri: String,
    encryptedDek: EncryptedDek,
  ): MesosRecordIoStorageClient =
    MesosRecordIoStorageClient(
      buildDecryptingStorageClient(storageClient, kmsClient, kekUri, encryptedDek)
    )

  /** Writes a data encryption key to storage. */
  suspend fun writeDek(
    storageClient: StorageClient,
    kekUri: String,
    serializedEncryptionKey: ByteString,
    impressionsFileUri: String,
    dekBlobKey: String,
  ) {
    val blobDetails =
      encryptAndCreateBlobDetails(kekUri, serializedEncryptionKey, impressionsFileUri)

    storageClient.writeBlob(dekBlobKey, blobDetails.toByteString())
  }

  private fun encryptAndCreateBlobDetails(
    kekUri: String,
    serializedEncryptionKey: ByteString,
    blobUri: String,
  ): BlobDetails {
    val encryptedDek = encryptedDek {
      this.kekUri = kekUri
      ciphertext = serializedEncryptionKey
      protobufFormat = ProtobufFormat.BINARY
      typeUrl = TYPE_URL_TINK_KEYSET
    }

    return blobDetails {
      this.blobUri = blobUri
      this.encryptedDek = encryptedDek
    }
  }
}
