package org.wfanet.measurement.db.duchy

import java.io.IOException

/**
 * Manages status and state transitions for ongoing computations.
 *
 * @param[T] enum of the stages of a computation.
 */
class ComputationManager<T : Enum<T>>(
  private val relationalDatabase: ComputationsRelationalDb<T>,
  private val blobDatabase: ComputationsBlobDb
) {

  /**
   * Creates a new computation.
   *
   * @throws [IOException] upon failure
   */
  fun createComputation(globalId: Long, stage: T): ComputationToken<T> {
    return relationalDatabase.insertComputation(globalId, stage)
  }

  /**
   * Returns a [ComputationToken] for the most recent computation for a
   * [globalId].
   */
  fun getToken(globalId: Long): ComputationToken<T>? {
    return relationalDatabase.getToken(globalId)
  }

  /**
   * Transitions the state of an ongoing computation.
   *
   * This can be thought of as an atomic transaction, at least on the underlying
   * [ComputationsRelationalDb]. BLOBs are written first via a
   * [ComputationsBlobDb], and then the state and work queue is updated in a
   * single transaction. The new state for the computation is in a clean state
   * and is thus ready for whatever processing is required, by a worker. This
   * means all input BLOBs are present at the end of this operation and all of
   * those BLOBS' references are known for the state. Upon failure, it may be
   * possible for BLOBs to be written. In such a case, it is up to the caller
   * to clean up BLOBs if desired.
   *
   * Because each task knows the input blobs required to complete it the set of
   * BLOBs required by the task are all referenced here. Not all of these BLOBs
   * must be rewritten, as some can be carried forward to the next computation.
   *
   * @param[c] The task currently being worked
   * @param[stateAfter] The state to transition the computation to
   * @param[blobsToWrite] The input BLOBs for the new state which must be
   *    written to the [BlobComputationsDb]
   * @param[blobToCarryForward] The input BLOBs for the new state which already
   *    exist in [BlobComputationsDb]
   * @param[blobsRequiredForOutput] The BLOBs which should be written as part of
   *    this stage, this may be useful when a stage is waiting on inputs from
   *    multiple other workers.
   * @param[afterTransition] What to do with the work after transitioning the
   *    state.
   *
   * @throws [IOException] when state state transition fails
   */
  fun transitionState(
    c: ComputationToken<T>,
    stateAfter: T,
    blobsToWrite: Map<BlobRef, ByteArray> = mapOf(),
    blobToCarryForward: Collection<BlobRef> = listOf(),
    blobsRequiredForOutput: Collection<BlobName> = listOf(),
    afterTransition: AfterTransition
  ): ComputationToken<T> {
    val refs = HashSet<BlobRef>(blobToCarryForward)

    for ((ref, bytes) in blobsToWrite) {
      // TODO: Investigate using co-routines to write in parallel.
      blobDatabase.blockingWrite(ref, bytes)
      refs.add(ref)
    }
    return relationalDatabase.updateComputationState(
      c, stateAfter, refs, blobsRequiredForOutput, afterTransition
    )
  }

  /**
   * Enqueues a computation into the work queue.
   *
   * @throws [IOException] upon failure
   */
  fun enqueue(token: ComputationToken<T>) {
    relationalDatabase.enqueue(token)
  }

  /**
   * Pull for available work and claim a task for the worker.
   *
   * If the returned value is present, then the task has been claimed for the
   * worker. When absent, no task was claimed.
   */
  fun claimWork(workerId: String): ComputationToken<T>? {
    return relationalDatabase.claimTask(workerId)
  }

  /**
   * Extend the lock time on a computation.
   *
   * @throws [IOException] upon failure
   */
  fun renewWork(token: ComputationToken<T>) {
    relationalDatabase.renewTask(token)
  }

  /**
   * Reads all the input BLOBs required for a computation task.
   *
   * @throws [IOException] upon failure
   */
  fun readInputBlobs(c: ComputationToken<T>): Map<BlobRef, ByteArray> {
    return readBlobReferenceNames(
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
  fun readBlobReferenceNames(
    token: ComputationToken<T>,
    dependencyType: BlobDependencyType
  ): Map<BlobName, String?> {
    return relationalDatabase.readBlobReferenceNames(token, dependencyType = dependencyType)
  }

  /**
   * Write a BLOB to blob storage, and then update the reference to it in the
   * relation data base for a stage.
   *
   * @throws [IOException] upon failure
   */
  fun writeAndRecordOutputBlob(
    token: ComputationToken<T>,
    blobName: BlobRef,
    blob: ByteArray
  ) {
    blobDatabase.blockingWrite(blobName, blob)
    relationalDatabase.writeOutputBlobReference(token, blobName)
  }
}
