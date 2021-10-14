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

import com.google.common.collect.ImmutableMap
import com.google.protobuf.ByteString
import java.lang.Exception
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.signFlow
import org.wfanet.measurement.common.crypto.verifySignedFlow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.measurement.storage.createBlob
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.protocol.NamedSignature
import org.wfanet.panelmatch.protocol.namedSignature

class StorageNotFoundException(inputKey: String) : Exception("$inputKey not found")

class VerifiedStorageClient(
  private val storageClient: StorageClient,
  private val exchangeKey: ExchangeKey,
  private val ownerName: String,
  private val partnerName: String,
  private val ownerCertificateName: String?,
  private val certificateManager: CertificateManager,
) {

  // TODO: This is just wildly a bad idea and I'm only keeping it here to reduce the number of files
  //   I'm changing. I will immediately rework this in a following PR once StorageSelector is
  //   implemented. - jmolle
  suspend fun getPrivateKey(): PrivateKey = certificateManager.getExchangePrivateKey(exchangeKey)

  val defaultBufferSizeBytes: Int = storageClient.defaultBufferSizeBytes

  /** A helper function to get the implicit path for a input's signature. */
  private fun getSigPath(blobKey: String): String = "${blobKey}_signature"

  /**
   * Transforms values of [inputLabels] into the underlying blobs.
   *
   * If any blob can't be found, it throws [StorageNotFoundException].
   *
   * All files read are verified against the appropriate [X509Certificate] to validate that the
   * files came from the expected source.
   */
  @Throws(StorageNotFoundException::class)
  suspend fun verifiedBatchRead(inputLabels: Map<String, String>): Map<String, VerifiedBlob> {
    return inputLabels.mapValues { entry -> getBlob(blobKey = entry.value) }
  }

  /**
   * Writes output [data] based on [outputLabels].
   *
   * All outputs written by this function are signed by the user's PrivateKey, which is also written
   * as a separate file.
   */
  suspend fun verifiedBatchWrite(
    outputLabels: Map<String, String>,
    data: Map<String, Flow<ByteString>>
  ) {
    // create an immutable copy of outputLabels to avoid race conditions if the underlying label map
    // is changed during execution.
    val immutableOutputs: ImmutableMap<String, String> = ImmutableMap.copyOf(outputLabels)
    require(immutableOutputs.values.toSet().size == immutableOutputs.size) {
      "Cannot write to the same output location twice"
    }
    for ((key, value) in immutableOutputs) {
      val payload = requireNotNull(data[key]) { "Key $key not found in ${data.keys}" }
      createBlob(blobKey = value, content = payload)
    }
  }

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
      blobKey,
      certificateManager.getCertificate(exchangeKey, partnerName, namedSignature.certificateName)
    )
  }

  private suspend fun parseSignature(blobKey: String): NamedSignature {
    val signatureBlob: Blob =
      storageClient.getBlob(getSigPath(blobKey))
        ?: throw StorageNotFoundException(getSigPath(blobKey))

    return NamedSignature.parseFrom(signatureBlob.read(defaultBufferSizeBytes).flatten())
  }

  /**
   * Stub for verified write function. Intended to be used in combination with a provided
   * [PrivateKey] , this creates a signature in shared storage for the written blob that can be
   * verified by the other party using a pre-provided [X509Certificate].
   */
  @Suppress("EXPERIMENTAL_API_USAGE")
  suspend fun createBlob(blobKey: String, content: Flow<ByteString>): VerifiedBlob {
    val privateKey = certificateManager.getExchangePrivateKey(exchangeKey)
    val ownerCertificate =
      certificateManager.getCertificate(
        exchangeKey,
        ownerName,
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
    return VerifiedBlob(sourceBlob, signatureVal, blobKey, ownerCertificate)
  }

  suspend fun createBlob(blobKey: String, content: ByteString): VerifiedBlob =
    createBlob(blobKey, content.asBufferedFlow(defaultBufferSizeBytes))

  class VerifiedBlob(
    private val sourceBlob: Blob,
    private val signature: ByteString,
    val blobKey: String,
    private val cert: X509Certificate
  ) {

    val size: Long
      get() = sourceBlob.size

    val defaultBufferSizeBytes: Int
      get() = sourceBlob.storageClient.defaultBufferSizeBytes

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

    fun read(bufferSize: Int = this.defaultBufferSizeBytes): Flow<ByteString> {
      return verifiedRead(bufferSize)
    }

    suspend fun toByteString(): ByteString = this.read().flatten()

    suspend fun toStringUtf8(): String = toByteString().toStringUtf8()
  }
}

/** Writes output [data] based on [outputLabels]. */
suspend fun StorageClient.batchWrite(
  outputLabels: Map<String, String>,
  data: Map<String, Flow<ByteString>>,
  exchangeKey: ExchangeKey
) {
  // create an immutable copy of outputLabels to avoid race conditions if the underlying label map
  // is changed during execution.
  val immutableOutputs: ImmutableMap<String, String> = ImmutableMap.copyOf(outputLabels)
  require(immutableOutputs.values.toSet().size == immutableOutputs.size) {
    "Cannot write to the same output location twice"
  }
  for ((key, value) in immutableOutputs) {
    val payload = requireNotNull(data[key]) { "Key $key not found in ${data.keys}" }
    createBlob(blobKey = prefixBlobKey(value, exchangeKey), content = payload)
  }
}

/**
 * Transforms values of [inputLabels] into the underlying blobs.
 *
 * If any blob can't be found, it throws [StorageNotFoundException].
 *
 * All files read are verified against the appropriate [X509Certificate] to validate that the files
 * came from the expected source.
 */
@Throws(StorageNotFoundException::class)
suspend fun StorageClient.batchRead(
  inputLabels: Map<String, String>,
  exchangeKey: ExchangeKey
): Map<String, StorageClient.Blob> {
  return inputLabels.mapValues { entry ->
    requireNotNull(getBlob(blobKey = prefixBlobKey(entry.value, exchangeKey)))
  }
}

/** Provides a unique per-exchange prefix to prevent collision between multiple workflows */
private fun prefixBlobKey(blobKey: String, exchangeKey: ExchangeKey): String =
  "${exchangeKey.toName()}/$blobKey"
