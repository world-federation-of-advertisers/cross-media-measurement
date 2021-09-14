// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.storage

import java.util.UUID
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.storage.BlobKeyGenerator
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.Store

private const val BLOB_KEY_PREFIX = "/computations"

/** A [Store] instance for managing Blobs associated with computations in the Duchy. */
class ComputationStore
private constructor(
  storageClient: StorageClient,
  generateBlobKey: BlobKeyGenerator<ComputationBlobContext>
) : Store<ComputationBlobContext>(storageClient, generateBlobKey) {
  constructor(storageClient: StorageClient) : this(storageClient, ::generateBlobKey)

  override val blobKeyPrefix = BLOB_KEY_PREFIX

  companion object {
    fun forTesting(
      storageClient: StorageClient,
      generateBlobKey: BlobKeyGenerator<ComputationBlobContext>
    ): ComputationStore = ComputationStore(storageClient, generateBlobKey)
  }
}

/** The context used to generate blob key for the [ComputationStore]. */
data class ComputationBlobContext(
  val externalComputationId: String,
  val computationStage: ComputationStage
)

/** Generates a Blob key using the [ComputationBlobContext]. */
private fun generateBlobKey(context: ComputationBlobContext): String {
  return "/${context.externalComputationId}/${context.computationStage.name}/${UUID.randomUUID()}"
}
