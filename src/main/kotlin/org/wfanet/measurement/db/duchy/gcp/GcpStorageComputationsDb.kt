package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ComputationsBlobDb
import java.nio.file.Paths
import kotlin.random.Random

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
