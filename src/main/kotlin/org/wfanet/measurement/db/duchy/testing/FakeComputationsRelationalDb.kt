package org.wfanet.measurement.db.duchy.testing

import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.AfterTransition
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.BlobId
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ComputationsRelationalDb
import org.wfanet.measurement.db.duchy.EndComputationReason
import org.wfanet.measurement.db.duchy.ProtocolStageDetails
import org.wfanet.measurement.db.duchy.ProtocolStageEnumHelper

/**
 * Testing fake of [ComputationsRelationalDb] which is an in memory map of global ids to tokens
 * and data about the blobs required by the current stage.
 */
class FakeComputationsRelationalDatabase<StageT : Enum<StageT>, StageDetailsT>(
  private val fakeComputations: FakeComputationStorage<StageT, StageDetailsT>,
  private val stageEnumHelper: ProtocolStageEnumHelper<StageT>,
  private val details: ProtocolStageDetails<StageT, StageDetailsT>
) : ComputationsRelationalDb<StageT, StageDetailsT> {

  override suspend fun insertComputation(
    globalId: Long,
    initialStage: StageT
  ): ComputationToken<StageT> {
    TODO("Not yet implemented")
  }

  override suspend fun getToken(globalId: Long): ComputationToken<StageT>? =
    fakeComputations[globalId]?.token

  override suspend fun updateComputationStage(
    token: ComputationToken<StageT>,
    to: StageT,
    inputBlobPaths: List<String>,
    outputBlobs: Int,
    afterTransition: AfterTransition
  ): ComputationToken<StageT> {
    require(stageEnumHelper.validTransition(token.stage, to))
    fakeComputations.changeTokenAndBlobs(token) {
      val newToken =
        when (afterTransition) {
          AfterTransition.CONTINUE_WORKING -> token.copy(stage = to)
          else -> token.copy(stage = to, owner = null)
        }
      val blobs = mutableMapOf<FakeBlobMetadata, String?>()
      blobs.putAll(
        inputBlobPaths.mapIndexed { idx, path ->
          FakeBlobMetadata(idx.toLong(), BlobDependencyType.INPUT) to path
        }
      )
      blobs.putAll(
        (0 until outputBlobs).map { idx ->
          FakeBlobMetadata(idx.toLong(), BlobDependencyType.OUTPUT) to null
        }
      )
      FakeComputation(newToken, blobs)
    }
    return fakeComputations[token.globalId]?.token ?: error("Missing token for ${token.globalId}")
  }

  override suspend fun endComputation(
    token: ComputationToken<StageT>,
    endingStage: StageT,
    endComputationReason: EndComputationReason
  ) {
    require(stageEnumHelper.validTerminalStage(endingStage))
    fakeComputations.changeToken(token) { token.copy(stage = endingStage, owner = null) }
  }

  override suspend fun readBlobReferences(
    token: ComputationToken<StageT>,
    dependencyType: BlobDependencyType
  ): Map<BlobId, String?> =
    fakeComputations[token.globalId]!!.blobs.filter {
      dependencyType == BlobDependencyType.ANY || it.key.dependencyType == dependencyType
    }
      .map { it.key.id to it.value }.toMap()

  override suspend fun writeOutputBlobReference(
    token: ComputationToken<StageT>,
    blobName: BlobRef
  ) {
    fakeComputations.changeToken(token) {
      val metadata = FakeBlobMetadata(blobName.name, BlobDependencyType.OUTPUT)
      check(metadata in fakeComputations[token.globalId]!!.blobs) {
        "$metadata not in ${fakeComputations[token.globalId]!!.blobs}"
      }
      fakeComputations[token.globalId]?.blobs?.put(metadata, blobName.pathToBlob)
      token
    }
  }

  override suspend fun enqueue(token: ComputationToken<StageT>) {
    fakeComputations.changeToken(token) { token.copy(owner = null) }
  }

  override suspend fun claimTask(ownerId: String): ComputationToken<StageT>? {
    TODO("Not yet implemented")
  }

  override suspend fun renewTask(token: ComputationToken<StageT>): ComputationToken<StageT> {
    TODO("Not yet implemented")
  }

  override suspend fun readStageSpecificDetails(token: ComputationToken<StageT>): StageDetailsT {
    return details.detailsFor(token.stage)
  }

  override suspend fun readGlobalComputationIds(stages: Set<StageT>): Set<Long> =
    fakeComputations.filter { it.value.token.stage in stages }.map { it.value.token.globalId }
      .toSet()
}

data class FakeBlobMetadata(val id: Long, val dependencyType: BlobDependencyType)
data class FakeComputation<StageT : Enum<StageT>>(
  var token: ComputationToken<StageT>,
  var blobs: MutableMap<FakeBlobMetadata, String?>
)

/** In memory mapping of computation ids to [FakeComputation]s. */
class FakeComputationStorage<StageT : Enum<StageT>, StageDetailsT> :
  MutableMap<Long, FakeComputation<StageT>> by mutableMapOf() {
  companion object {
    private const val NEXT_WORKER = "NEXT_WORKER"
  }

  /** Adds a fake computation to the fake computation storage. */
  fun addComputation(
    id: Long,
    stage: StageT,
    role: DuchyRole,
    blobs: MutableMap<FakeBlobMetadata, String?>
  ) {
    this[id] = FakeComputation(
      token = ComputationToken(
        globalId = id,
        // For the purpose of a fake it is fine to use the same id for both local and global ids
        localId = id,
        owner = null,
        stage = stage,
        // The last update time is a counter of updates.
        lastUpdateTime = 0,
        role = role,
        nextWorker = NEXT_WORKER,
        attempt = 0
      ),
      blobs = blobs
    )
  }

  /**
   * Changes the token for a computation to a new one and increments the lastUpdateTime.
   * Blob references are unchanged.
   */
  fun changeToken(
    token: ComputationToken<StageT>,
    block: () -> ComputationToken<StageT>
  ) {
    val currentComputation = requireTokenFromCurrent(token)
    val newToken = block().copy(lastUpdateTime = token.lastUpdateTime + 1)
    this[token.localId] = currentComputation.copy(token = newToken)
  }

  /** Changes the token and blobs for a computation. */
  fun changeTokenAndBlobs(
    token: ComputationToken<StageT>,
    block: () -> FakeComputation<StageT>
  ) {
    requireTokenFromCurrent(token)
    val newComputation = block()
    newComputation.token = newComputation.token.copy(lastUpdateTime = token.lastUpdateTime + 1)
    this[token.localId] = newComputation
  }

  private fun requireTokenFromCurrent(token: ComputationToken<StageT>): FakeComputation<StageT> {
    val current = this[token.localId]!!
    require(current.token == token) { "Token provided $token != current token ${current.token}" }
    return current
  }
}
