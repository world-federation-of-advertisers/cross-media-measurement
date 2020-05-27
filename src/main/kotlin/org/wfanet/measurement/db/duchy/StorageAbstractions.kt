package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.common.DuchyRole

/**
 * Information about a computation.
 */
data class ComputationToken<T : Enum<T>>(
  /** The identifier for the computation used locally. */
  val localId: Long,
  /** The identifier for the computation used across all systems. */
  val globalId: Long,
  /** The state of the computation when the token was created. */
  val state: T,
  /** Name of knight that owns the lock on the computation. */
  val owner: String?,
  /** Identifier of the duchy that receives work for this computation. */
  val nextWorker: String,
  /** The role this worker is playing for this computation. */
  val role: DuchyRole,
  /** The number of the current attempt of this stage for this computation. */
  val attempt: Long,
  /** The last time the computation was updated in number of milliseconds since the epoch. */
  val lastUpdateTime: Long
)

/**
 * Specifies what to do with the lock on a computation after transitioning to a
 * new stage.
 *
 * @see[ComputationsRelationalDb.updateComputationState]
 */
enum class AfterTransition {
  /** Retain and extend the lock for the current owner. */
  CONTINUE_WORKING,

  /**
   * Add the computation to the work queue, but in an unclaimed state for some
   * worker to claim at a later time.
   */
  ADD_UNCLAIMED_TO_QUEUE,

  /**
   * Do not add to the work queue, and release any lock on the computation.
   * There is no work to be done on the computation at this time.
   * Examples for when to set this include the computation finished or
   * input from another source is required before continuing.
   */
  DO_NOT_ADD_TO_QUEUE
}

/**
 * Relational database for keeping track of stages of a Computation within an
 * MPC Node.
 *
 * The database must have strong consistency guarantees as it is used to
 * coordinate assignment of work by pulling jobs.
 */
interface ComputationsRelationalDb<T : Enum<T>> {

  /**
   * Inserts a new computation for the global identifier.
   *
   * A new local identifier is created and returned in the [ComputationToken].
   * The computation is not added to the queue.
   */
  fun insertComputation(globalId: Long, initialState: T): ComputationToken<T>

  /**
   * Returns a [ComputationToken] for the most recent computation for a
   * [globalId].
   */
  fun getToken(globalId: Long): ComputationToken<T>?

  /**
   * Adds a computation to the work queue, saying it can be worked on by a
   * worker job.
   *
   * This will release any ownership and locks associated with the computation.
   */
  fun enqueue(token: ComputationToken<T>)

  /**
   * Query for Computations with tasks ready for processing, and claim one for
   * an owner.
   *
   * @param[ownerId] The identifier of the worker process that will own the
   *    lock.
   * @return [ComputationToken] for the work that needs to be completed,
   *    when this value is null, no work was claimed.
   */
  fun claimTask(ownerId: String): ComputationToken<T>?

  /** Extends the time a computation is locked. */
  fun renewTask(token: ComputationToken<T>): ComputationToken<T>

  /**
   * Transitions a computation to a new state.
   *
   * @param[token] The token for the computation
   * @param[to] Stage this computation should transition to.
   * @param[blobInputRefs] References to BLOBs that are inputs to this
   *    computation stage, all inputs should be written on transition and should
   *    not change.
   * @param[blobOutputRefs] Names of BLOBs that are outputs to this computation.
   *    These are created as part of the computation so they do not have a
   *    reference to the real storage location.
   * @param[afterTransition] States what work to do with the computation after
   *    a successful transition.
   */
  fun updateComputationState(
    token: ComputationToken<T>,
    to: T,
    blobInputRefs: Collection<BlobRef>,
    blobOutputRefs: Collection<BlobName>,
    afterTransition: AfterTransition
  ): ComputationToken<T>

  /**
   * Reads mappings of blob names to paths in blob storage.
   */
  fun readBlobReferenceNames(
    current: ComputationToken<T>,
    dependencyType: BlobDependencyType = BlobDependencyType.INPUT
  ): Map<BlobName, String?>

  /**
   * Writes the reference to a BLOB needed for [BlobDependencyType.OUTPUT] from
   * a stage.
   */
  fun writeOutputBlobReference(c: ComputationToken<T>, blobName: BlobRef)
}

/**
 * Name used to identify a BLOB by its purpose across multiple processes.
 */
data class BlobName(val name: String)

/**
 * Reference to a named BLOB's storage location.
 */
data class BlobRef(val name: BlobName, val pathToBlob: String) {
  constructor(name: String, pathToBlob: String) : this(BlobName(name), pathToBlob)
}

/** BLOBs storage used by a computation. */
interface ComputationsBlobDb {

  /**
   * Reads and returns a BLOB from storage
   */
  fun read(reference: BlobRef): ByteArray

  /**
   * Write a BLOB and ensure it is fully written before returning.
   */
  fun blockingWrite(blob: BlobRef, bytes: ByteArray)

  /**
   * Deletes a BLOB
   */
  fun delete(reference: String) {}
}

/**
 * The way in which a stage depends upon a BLOB.
 */
enum class BlobDependencyType {
  /** BLOB is used as an input to the computation stage. */
  INPUT,

  /** BLOB is an output of the computation stage. */
  OUTPUT,

  /**
   * ONLY makes sense when reading blobs from the database. Read all blobs
   * no mater the type.
   */
  ANY;
}
