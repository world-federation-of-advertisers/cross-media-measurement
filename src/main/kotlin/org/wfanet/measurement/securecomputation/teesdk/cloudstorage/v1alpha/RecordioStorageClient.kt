// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.teesdk.cloudstorage.v1alpha

import com.google.crypto.tink.Aead
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.transform
import java.util.Base64
import java.util.logging.Logger
import org.wfanet.measurement.storage.StorageClient
import com.google.crypto.tink.subtle.AesGcmJce
import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec

/**
 * A wrapper class for [StorageClient] interface that uses Tink AEAD encryption/decryption for
 * blob/object storage operations on RecordIO-based files.
 *
 * @param storageClient underlying client for accessing blob/object storage
 * @param dataKey a base64-encoded symmetric data key
 */
class RecordioStorageClient(
  private val storageClient: StorageClient,
  private val base64Dek: String,
) : StorageClient {

  private val aead: Aead

  init {
    aead = createAeadFromBase64(base64Dek)
  }

  private fun createAeadFromBase64(base64Key: String): AesGcmJce {
    val keyBytes: ByteArray = Base64.getDecoder().decode(base64Key)
    val secretKey: SecretKey = SecretKeySpec(keyBytes, 0, keyBytes.size, "AES")
    return AesGcmJce(secretKey.encoded)
  }

  /**
   * Creates a blob with the specified [blobKey] and [content] encrypted by [aead].
   *
   * @param blobKey the key for the blob.
   * @param content [Flow] producing the content be encrypted and stored in the blob
   * @return [StorageClient.Blob] with [content] encrypted by [jca]
   */
  override suspend fun writeBlob(blobKey: String, content: Flow<ByteString>): StorageClient.Blob {

    val encryptedContent = content.transform { byteString ->
      val encryptedBytes = aead.encrypt(byteString.toByteArray(), blobKey.encodeToByteArray())
      if (encryptedBytes != null) {
        val recordSize = encryptedBytes.size.toString()
        val record = recordSize + "\n" + ByteString.copyFrom(encryptedBytes).toStringUtf8()
        emit(ByteString.copyFromUtf8(record))
      }
    }

    val wrappedBlob: StorageClient.Blob = storageClient.writeBlob(blobKey, encryptedContent as Flow<ByteString>)
    logger.fine { "Wrote ciphertext to storage $blobKey" }
    return RecordioBlob(wrappedBlob, blobKey)
  }

  /**
   * Returns a [StorageClient.Blob] with specified blob key, or `null` if not found.
   *
   * Blob content is not decrypted until [RecordioBlob.read]
   */
  override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
    val blob = storageClient.getBlob(blobKey)
    return blob?.let { RecordioBlob(it, blobKey) }
  }

  /** A blob that will decrypt the content when read */
  private inner class RecordioBlob(private val blob: StorageClient.Blob, private val blobKey: String) :
    StorageClient.Blob {
    override val storageClient = this@RecordioStorageClient.storageClient

    override val size: Long
      get() = blob.size

    /**
     * Reads the RecordIO file and processes each row in a `Flow<ByteString>`.
     * Each row is decrypted using AES/GCM/NoPadding and then emitted as a `Flow<ByteString>`.
     *
     * This function handles:
     * - Reading the RecordIO format, where each row is prefixed by its length, followed by an encrypted record.
     * - Decrypting each row using Tink AEAD.
     * - Emitting each decrypted row as a `Flow<ByteString>`.
     *
     * @return A `Flow<ByteString>` that emits each decrypted row. The decrypted rows are emitted one at a time as they
     * are processed.
     *
     * @throws IllegalArgumentException If the length of a row cannot be parsed as an integer or if decryption fails.
     */
    override fun read(): Flow<ByteString> = flow {
      var buffer = ByteString.EMPTY

      blob.read().collect { chunk ->
        buffer = buffer.concat(chunk)

        while (true) {
          val lengthEndIndex = buffer.indexOf('\n'.code.toByte())
          if (lengthEndIndex == -1) break
          val lengthString = buffer.substring(0, lengthEndIndex).toStringUtf8()
          val length = lengthString.toIntOrNull()
          if (length == null) {
            throw IllegalArgumentException("Invalid length: $lengthString")
          }
          buffer = buffer.substring(lengthEndIndex + 1)
          if (buffer.size() < length) break
          val encryptedRow = buffer.substring(0, length)
          buffer = buffer.substring(length)
          val decryptedRow = aead.decrypt(encryptedRow.toByteArray(), blobKey.encodeToByteArray())
          emit(ByteString.copyFrom(decryptedRow))
        }
      }
    }

    override suspend fun delete() = blob.delete()

  }

  companion object {
    internal val logger = Logger.getLogger(this::class.java.name)
  }
}
