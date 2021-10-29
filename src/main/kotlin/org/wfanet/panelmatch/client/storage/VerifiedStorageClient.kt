// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.storage

import com.google.protobuf.ByteString
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.signFlow
import org.wfanet.measurement.common.crypto.verifySignedFlow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.measurement.storage.createBlob
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.protocol.NamedSignature
import org.wfanet.panelmatch.protocol.namedSignature

/** [StorageClient] that writes a signature when creating blobs and verifies signatures on reads. */
class VerifiedStorageClient(
  private val storageClient: StorageClient,
  private val context: ExchangeContext,
  private val ownerCertificateName: String?,
  private val certificateManager: CertificateManager,
) {

  private val defaultBufferSizeBytes: Int
    get() = storageClient.defaultBufferSizeBytes

  /** A helper function to get the implicit path for a input's signature. */
  private fun getSigPath(blobKey: String): String = "${blobKey}_signature"

  /**
   * Replacement for StorageClient.getBlob(). Intended to be used in combination with another
   * party's provided [X509Certificate], this creates a [VerifiedBlob] that will check that the data
   * in the blob has been generated (or at least signed as valid) by the other party.
   *
   * Notes:
   * - The validation happens is non-blocking, but will throw a terminal error if it fails.
   * - The final validation does not happen until the Flow the underlying Blob reads is collected.
   */
  @Throws(StorageNotFoundException::class)
  suspend fun getBlob(blobKey: String): VerifiedBlob {
    val sourceBlob: Blob = storageClient.getBlob(blobKey) ?: throw StorageNotFoundException(blobKey)
    val namedSignature = parseSignature(blobKey)

    return VerifiedBlob(
      sourceBlob,
      namedSignature.signature,
      certificateManager.getCertificate(
        context.exchangeDateKey,
        context.partnerName,
        namedSignature.certificateName
      )
    )
  }

  private suspend fun parseSignature(blobKey: String): NamedSignature {
    val signatureBlob: Blob =
      storageClient.getBlob(getSigPath(blobKey))
        ?: throw StorageNotFoundException(getSigPath(blobKey))
    val serializedSignature = signatureBlob.read(defaultBufferSizeBytes).flatten()

    @Suppress("BlockingMethodInNonBlockingContext")
    return NamedSignature.parseFrom(serializedSignature)
  }

  /**
   * Stub for verified write function. Intended to be used in combination with a provided
   * [PrivateKey] , this creates a signature in shared storage for the written blob that can be
   * verified by the other party using a pre-provided [X509Certificate].
   */
  @Suppress("EXPERIMENTAL_API_USAGE") // For Deferred.getCompleted.
  suspend fun createBlob(blobKey: String, content: Flow<ByteString>): VerifiedBlob {
    // Since StorageClient has no concept of "overwriting" a blob, we first delete existing blobs.
    // This is to ensure that transient failures after some blobs are written do not cause problems
    // when re-attempting to write.
    deleteExistingBlobs(blobKey)

    val privateKey = certificateManager.getExchangePrivateKey(context.exchangeDateKey)
    val ownerCertificate =
      certificateManager.getCertificate(
        context.exchangeDateKey,
        context.localName,
        requireNotNull(ownerCertificateName)
      )
    val (signedContent, deferredSig) = privateKey.signFlow(ownerCertificate, content)
    val sourceBlob = storageClient.createBlob(blobKey = blobKey, content = signedContent)

    val signatureVal = deferredSig.getCompleted()
    val namedSignature = namedSignature {
      certificateName = ownerCertificateName
      signature = signatureVal
    }
    storageClient.createBlob(blobKey = getSigPath(blobKey), content = namedSignature.toByteString())
    return VerifiedBlob(sourceBlob, signatureVal, ownerCertificate)
  }

  suspend fun createBlob(blobKey: String, content: ByteString): VerifiedBlob =
    createBlob(blobKey, content.asBufferedFlow(defaultBufferSizeBytes))

  private fun deleteExistingBlobs(blobKey: String) {
    storageClient.getBlob(blobKey)?.delete()
    storageClient.getBlob(getSigPath(blobKey))?.delete()
  }

  class VerifiedBlob(
    private val sourceBlob: Blob,
    private val signature: ByteString,
    private val cert: X509Certificate
  ) {
    val size: Long
      get() = sourceBlob.size

    /**
     * Stub for verified read function. Intended to be used in combination with a the other party's
     * provided [X509Certificate], this validates that the data in the blob has been generated (or
     * at least signed as valid) by the other party.
     *
     * Note that the validation happens in a separate thread and is non-blocking, but will throw a
     * terminal error if it fails.
     */
    private fun verifiedRead(bufferSize: Int): Flow<ByteString> {
      return cert.verifySignedFlow(sourceBlob.read(bufferSize), signature)
    }

    fun read(bufferSize: Int = sourceBlob.storageClient.defaultBufferSizeBytes): Flow<ByteString> {
      return verifiedRead(bufferSize)
    }

    suspend fun toByteString(): ByteString = this.read().flatten()

    suspend fun toStringUtf8(): String = toByteString().toStringUtf8()
  }
}
