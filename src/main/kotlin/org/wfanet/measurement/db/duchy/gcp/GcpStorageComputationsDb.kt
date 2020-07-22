// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import java.nio.file.Paths
import kotlin.random.Random
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ComputationsBlobDb

/**
 * Implementation of [ComputationsBlobDb] using Google Cloud Storage for interacting with a
 * single storage bucket.
 */
class GcpStorageComputationsDb<StageT : Enum<StageT>>(
  private val storage: Storage,
  private val bucket: String,
  private val random: Random = Random
) : ComputationsBlobDb<StageT> {
  override suspend fun read(reference: BlobRef): ByteArray =
    storage[blobId(reference.pathToBlob)]?.getContent() ?: error("No blob for $reference")

  override suspend fun blockingWrite(path: String, bytes: ByteArray) {
    storage.create(blobInfo(path), bytes)
  }

  override suspend fun delete(reference: BlobRef) {
    storage.delete(blobId(reference.pathToBlob))
  }

  /**
   * Returns a path to that can be used for writing a Blob of the form localId/stage/name/randomId.
   */
  override suspend fun newBlobPath(token: ComputationToken<StageT>, name: String): String {
    val hexValue = random.nextLong(until = Long.MAX_VALUE).toString(16)
    return Paths.get(token.localId.toString(), token.stage.name, name, hexValue).toString()
  }

  private fun blobId(path: String): BlobId = BlobId.of(bucket, path)
  private fun blobInfo(path: String): BlobInfo = BlobInfo.newBuilder(blobId(path)).build()
}
