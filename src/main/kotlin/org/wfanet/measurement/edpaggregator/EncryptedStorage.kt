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

  /** Builds a envelope encryption storage client wrapped by Mesos Record IO Storage Client. */
  fun buildEncryptedMesosStorageClient(
    storageClient: StorageClient,
    kmsClient: KmsClient,
    kekUri: String,
    encryptedDek: EncryptedDek,
  ): MesosRecordIoStorageClient {

    val aeadStorageClient =
      when (encryptedDek.typeUrl to encryptedDek.protobufFormat) {
        TYPE_URL_TINK_KEYSET to ProtobufFormat.BINARY -> {
          storageClient.withEnvelopeEncryption(
            kmsClient = kmsClient,
            kekUri = kekUri,
            encryptedDek = encryptedDek.ciphertext,
          )
        }
        TYPE_URL_ENCRYPTION_KEY to ProtobufFormat.JSON -> {
          storageClient.withEnvelopeEncryption(
            kmsClient = kmsClient,
            kekUri = kekUri,
            encryptedDek = encryptedDek.ciphertext,
            parseEncryptedKeyset = ::parseJsonEncryptedKey,
          )
        }
        else ->
          throw IllegalArgumentException(
            "Unsupported type_url=${encryptedDek.typeUrl} with format=${encryptedDek.protobufFormat}"
          )
      }

    return MesosRecordIoStorageClient(aeadStorageClient)
  }

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
