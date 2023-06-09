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

import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.protocol.NamedSignature

private const val SIGNATURE_SUFFIX = ".signature"

/** Returns [blobKey] with [SIGNATURE_SUFFIX] appended. */
fun signatureBlobKeyFor(blobKey: String): String = "$blobKey$SIGNATURE_SUFFIX"

/** Reads the signature blob for [blobKey] and returns its parsed [NamedSignature]. */
suspend fun StorageClient.getBlobSignature(blobKey: String): NamedSignature {
  val signatureBlob: Blob =
    getBlob(signatureBlobKeyFor(blobKey))
      ?: throw BlobNotFoundException(signatureBlobKeyFor(blobKey))
  val serializedSignature = signatureBlob.toByteString()

  @Suppress("BlockingMethodInNonBlockingContext") // This is in-memory.
  return NamedSignature.parseFrom(serializedSignature)
}
