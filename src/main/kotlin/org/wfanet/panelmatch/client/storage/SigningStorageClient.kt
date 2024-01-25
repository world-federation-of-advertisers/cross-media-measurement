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
import java.security.PrivateKey
import java.security.cert.X509Certificate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.apache.beam.sdk.options.PipelineOptions
import org.wfanet.measurement.common.crypto.createSignedBlob
import org.wfanet.measurement.common.crypto.newSigner
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.protocol.NamedSignature
import org.wfanet.panelmatch.protocol.copy

/**
 * Pseudo implementation of [StorageClient] that writes (blob, signature) pairs to shared storage.
 */
class SigningStorageClient(
  private val sharedStorageFactory: StorageFactory,
  private val x509: X509Certificate,
  private val privateKey: PrivateKey,
  private val signatureTemplate: NamedSignature,
) : Serializable {

  /**
   * Writes [content] to shared storage using the given [blobKey]. Also writes a serialized
   * [NamedSignature] for the blob.
   */
  suspend fun writeBlob(
    blobKey: String,
    content: Flow<ByteString>,
    pipelineOptions: PipelineOptions? = null,
  ) {
    val sharedStorage: StorageClient = sharedStorageFactory.build(pipelineOptions)

    // Since StorageClient has no concept of "overwriting" a blob, we first delete existing blobs.
    // This is to ensure that transient failures after some blobs are written do not cause problems
    // when re-attempting to write.
    sharedStorage.getBlob(blobKey)?.delete()
    sharedStorage.getBlob(signatureBlobKeyFor(blobKey))?.delete()

    val signedBlob = sharedStorage.createSignedBlob(blobKey, content) { privateKey.newSigner(x509) }
    val signature = signatureTemplate.copy { signature = signedBlob.signature }
    sharedStorage.writeBlob(signatureBlobKeyFor(blobKey), signature.toByteString())
  }

  suspend fun writeBlob(
    blobKey: String,
    content: ByteString,
    pipelineOptions: PipelineOptions? = null,
  ) = writeBlob(blobKey, flowOf(content), pipelineOptions)
}
