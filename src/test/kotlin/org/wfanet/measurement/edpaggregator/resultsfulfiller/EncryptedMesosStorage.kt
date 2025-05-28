package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.protobuf.ByteString
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.StorageClient

object EncryptedMesosStorage {
  fun createEncryptedMesosStorage(
    storageClient: StorageClient,
    kmsClient: KmsClient,
    kekUri: String,
    serializedEncryptionKey: ByteString,
    tinkKeyTemplateType: String = "AES128_GCM_HKDF_1MB"
  ): MesosRecordIoStorageClient {
    val aeadStorageClient =
      storageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)

    return MesosRecordIoStorageClient(aeadStorageClient)
  }

  fun createKmsClient(kekUri: String, keyTemplate: String = "AES128_GCM"): KmsClient {
    val kmsClient = FakeKmsClient()
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get(keyTemplate))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
    return kmsClient
  }

  fun generateSerializedEnryptionKey(
    kmsClient: KmsClient,
    kekUri: String,
    tinkKeyTemplateType: String = "AES128_GCM_HKDF_1MB"
  ): ByteString {
    val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    return ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          keyEncryptionHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )
  }

  fun encryptAndCreateBlobDetails(
    kekUri: String,
    serializedEncryptionKey: ByteString,
    blobUri: String,
  ): BlobDetails {
    val encryptedDek = encryptedDek {
      this.kekUri = kekUri
      encryptedDek = serializedEncryptionKey
    }

    return blobDetails {
      this.blobUri = blobUri
      this.encryptedDek = encryptedDek
    }
  }
}
