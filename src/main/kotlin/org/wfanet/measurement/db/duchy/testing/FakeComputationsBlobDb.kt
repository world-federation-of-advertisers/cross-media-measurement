package org.wfanet.measurement.db.duchy.testing

import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ComputationsBlobDb

/** Testing fake of [ComputationsBlobDb] that is basically an in memory map. */
class FakeComputationsBlobDb<StageT : Enum<StageT>>(
  private val fakeComputations: MutableMap<String, ByteArray>
) : ComputationsBlobDb<StageT> {
  override suspend fun read(reference: BlobRef): ByteArray {
    return fakeComputations[reference.pathToBlob] ?: error("No blob found")
  }

  override suspend fun blockingWrite(path: String, bytes: ByteArray) {
    fakeComputations[path] = bytes
  }

  override suspend fun delete(reference: BlobRef) {
    fakeComputations.remove(reference.pathToBlob)
  }

  override suspend fun newBlobPath(
    token: ComputationToken<StageT>,
    name: String
  ): String = blobPath(token.localId, token.stage, name)

  companion object {
    /** A deterministic name for a blob useful for testing. */
    fun <StageT> blobPath(id: Long, stage: StageT, name: String): String = "$id-$stage-$name"
  }
}

