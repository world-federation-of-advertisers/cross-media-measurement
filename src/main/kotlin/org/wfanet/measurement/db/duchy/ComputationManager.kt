package org.wfanet.measurement.db.duchy

import java.io.IOException

/**
 * Manages status and stage transitions for ongoing computations.
 *
 * @param[StageT] enum of the stages of a computation.
 */
abstract class ComputationManager<StageT : Enum<StageT>, StageDetailT>(
  private val relationalDatabase: ComputationsRelationalDb<StageT, StageDetailT>,
  private val blobDatabase: ComputationsBlobDb<StageT>
) {

  /**
   * Creates a new computation.
   *
   * @throws [IOException] upon failure
   */
  suspend fun createComputation(globalId: Long, stage: StageT): ComputationToken<StageT> {
    return relationalDatabase.insertComputation(globalId, stage)
  }

  /**
   * Returns a [ComputationToken] for the most recent computation for a [globalId].
   */
  suspend fun getToken(globalId: Long): ComputationToken<StageT>? {
    return relationalDatabase.getToken(globalId)
  }

  /**
   * Transitions the stage of an ongoing computation.
   *
   * This can be thought of as an atomic transaction, at least on the underlying
   * [ComputationsRelationalDb]. BLOBs are written first via a [ComputationsBlobDb], and then the
   * stage and work queue is updated in a single transaction. The new stage for the computation is
   * in a clean stage and is thus ready for whatever processing is required, by a worker. This means
   * all input BLOBs are present at the end of this operation and all of those BLOBS' references are
   * known for the stage.
   *
   * Because each task knows the input blobs required to complete it, the set of BLOBs required by
   * the task are all referenced here. The BLOBs should be present before calling this function.
   *
   * @param[token] The task currently being worked
   * @param[stageAfter] The stage to transition the computation to
   * @param[inputBlobsPaths] List of pathes ot all input BLOBs for the new stage. They must exist.
   * @param[outputBlobCount] The number of BLOBs which should be written as part of this stage,
   *    this may be useful when a stage is waiting on inputs from multiple other workers.
   * @param[afterTransition] What to do with the work after transitioning the stage.
   *
   * @throws [IOException] when stage stage transition fails
   */
  suspend fun transitionStage(
    token: ComputationToken<StageT>,
    stageAfter: StageT,
    inputBlobsPaths: List<String> = listOf(),
    outputBlobCount: Int = 0,
    afterTransition: AfterTransition
  ): ComputationToken<StageT> {
    return relationalDatabase.updateComputationStage(
      token = token,
      to = stageAfter,
      inputBlobPaths = inputBlobsPaths,
      outputBlobs = outputBlobCount,
      afterTransition = afterTransition
    )
  }

  /**
   * Transitions the stage of an ongoing computation to an ending state.
   *
   * @param[token] The task currently being worked
   * @param[endingStage] The terminal stage to transition the computation to
   * @param [endComputationReason] The reason why the computation is ending
   */
  suspend fun endComputation(
    token: ComputationToken<StageT>,
    endingStage: StageT,
    endComputationReason: EndComputationReason
  ) {
    relationalDatabase.endComputation(token, endingStage, endComputationReason)
  }

  /**
   * Enqueues a computation into the work queue.
   *
   * @throws [IOException] upon failure
   */
  suspend fun enqueue(token: ComputationToken<StageT>) {
    relationalDatabase.enqueue(token)
  }

  /**
   * Pull for available work and claim a task for the worker.
   *
   * If the returned value is present, then the task has been claimed for the worker. When absent,
   * no task was claimed.
   */
  suspend fun claimWork(workerId: String): ComputationToken<StageT>? {
    return relationalDatabase.claimTask(workerId)
  }

  /**
   * Extend the lock time on a computation.
   *
   * @throws [IOException] upon failure
   */
  suspend fun renewWork(token: ComputationToken<StageT>): ComputationToken<StageT> {
    return relationalDatabase.renewTask(token)
  }

  /**
   * Reads all the input BLOBs required for a computation task.
   *
   * @throws [IOException] upon failure
   */
  suspend fun readInputBlobs(c: ComputationToken<StageT>): Map<BlobRef, ByteArray> {
    return readBlobReferences(
      c,
      BlobDependencyType.INPUT
    )
      .map {
        BlobRef(
          it.key,
          checkNotNull(it.value) { "INPUT BLOB $it missing a path." }
        )
      }
      // TODO: Read input blobs in parallel
      .map { it to blobDatabase.read(it) }
      .toMap()
  }

  /**
   * Reads BLOB names for a computation task
   *
   * @throws [IOException] upon failure
   */
  suspend fun readBlobReferences(
    token: ComputationToken<StageT>,
    dependencyType: BlobDependencyType
  ): Map<BlobId, String?> {
    return relationalDatabase.readBlobReferences(token, dependencyType = dependencyType)
  }

  /**
   * Write a BLOB to blob storage, and then update the reference to it in the relational data
   * base for a stage.
   *
   * @throws [IOException] upon failure
   */
  suspend fun writeAndRecordOutputBlob(
    token: ComputationToken<StageT>,
    blobName: BlobRef,
    blob: ByteArray
  ) {
    blobDatabase.blockingWrite(blobName, blob)
    relationalDatabase.writeOutputBlobReference(token, blobName)
  }

  /** Convenience function to get a path were a new blob may be written. */
  suspend fun newBlobPath(token: ComputationToken<StageT>, name: String): String =
    blobDatabase.newBlobPath(token, name)

  /**
   * Reads the specific stage details as a [M] protobuf message for the current stage of a
   * computation.
   *
   * @throws [IOException] upon failure
   */
  suspend fun readStageSpecificDetails(token: ComputationToken<StageT>): StageDetailT {
    return relationalDatabase.readStageSpecificDetails(token)
  }

  /**
   * Gets a collection of all the global computation ids for a computation in the database
   * which are in a one of the provided stages.
   *
   * @throws [IOException] upon failure
   */
  suspend fun readGlobalComputationIds(stages: Set<StageT>): Set<Long> {
    if (stages.isEmpty()) return setOf()
    return relationalDatabase.readGlobalComputationIds(stages)
  }
}
