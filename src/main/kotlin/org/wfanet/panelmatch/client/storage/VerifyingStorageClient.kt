// Copyright 2022 The Cross-Media Measurement Authors
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
import java.io.Serializable
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.crypto.SignedBlob
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.protocol.NamedSignature

/**
 * Pseudo implementation of [StorageClient] that reads (blob, signature) pairs from shared storage,
 * and uses the signature to verify the authenticity of the blob.
 *
 * The given [x509] must be the certificate that signed the blob(s) being read.
 */
class VerifyingStorageClient(
  private val sharedStorageFactory: StorageFactory,
  private val x509: X509Certificate,
) : Serializable {

  /**
   * Reads [blobKey] from shared storage. Returns a [VerifiedBlob] which checks the blob's signature
   * using [x509] when it is read.
   *
   * Note that the final signature validation does not happen until the [Flow] underlying the
   * [VerifiedBlob] is collected.
   */
  @Throws(BlobNotFoundException::class)
  suspend fun getBlob(blobKey: String): VerifiedBlob {
    val sharedStorage: StorageClient = sharedStorageFactory.build()
    val sourceBlob: Blob = sharedStorage.getBlob(blobKey) ?: throw BlobNotFoundException(blobKey)
    val namedSignature: NamedSignature = sharedStorage.getBlobSignature(blobKey)
    return VerifiedBlob(SignedBlob(sourceBlob, namedSignature.signature), x509)
  }

  /** [Blob] wrapper that ensures the blob's signature is verified when read. */
  class VerifiedBlob(private val sourceBlob: SignedBlob, private val x509: X509Certificate) {
    val size: Long
      get() = sourceBlob.size

    val signature: ByteString
      get() = sourceBlob.signature

    /** Reads the underlying blob. Throws if the signature was invalid. */
    fun read(): Flow<ByteString> = sourceBlob.readVerifying(x509)

    /** @see [StorageClient::toByteString]. */
    suspend fun toByteString(): ByteString = read().flatten()

    /** Reads the blob into a UTF8 String. */
    suspend fun toStringUtf8(): String = toByteString().toStringUtf8()
  }
}
