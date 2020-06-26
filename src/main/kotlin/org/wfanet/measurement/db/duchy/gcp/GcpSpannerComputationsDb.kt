package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.TransactionContext
import com.google.protobuf.Message
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.AfterTransition
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.BlobId
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ComputationsRelationalDb
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.gcpTimestamp
import org.wfanet.measurement.db.gcp.getBytesAsByteArray
import org.wfanet.measurement.db.gcp.getNullableString
import org.wfanet.measurement.db.gcp.getProtoEnum
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toMillis
import org.wfanet.measurement.internal.ComputationBlobDependency
import org.wfanet.measurement.internal.db.gcp.ComputationDetails
import java.time.Clock

/**
 * Implementation of [ComputationsRelationalDb] using GCP Spanner Database.
 */
class GcpSpannerComputationsDb<StageT : Enum<StageT>, StageDetailsT : Message>(
  private val spanner: Spanner,
  private val databaseId: DatabaseId,
  private val duchyName: String,
  private val duchyOrder: DuchyOrder,
  private val blobStorageBucket: String = "knight-computation-stage-storage",
  private val computationMutations: ComputationMutations<StageT, StageDetailsT>,
  private val clock: Clock = Clock.systemUTC()
) : ComputationsRelationalDb<StageT, StageDetailsT> {

  private val localComputationIdGenerator: LocalComputationIdGenerator =
    HalfOfGlobalBitsAndTimeStampIdGenerator(clock)

  private val databaseClient: DatabaseClient by lazy {
    spanner.getDatabaseClient(databaseId)
  }

  override fun insertComputation(globalId: Long, initialState: StageT): ComputationToken<StageT> {
    require(
      computationMutations.validInitialState(initialState)
    ) { "Invalid initial state $initialState" }

    val localId: Long = localComputationIdGenerator.localId(globalId)
    val computationAtThisDuchy = duchyOrder.positionFor(globalId, duchyName)

    val details = ComputationDetails.newBuilder().apply {
      role = when (computationAtThisDuchy.role) {
        DuchyRole.PRIMARY -> ComputationDetails.RoleInComputation.PRIMARY
        else -> ComputationDetails.RoleInComputation.SECONDARY
      }
      incomingNodeId = computationAtThisDuchy.prev
      outgoingNodeId = computationAtThisDuchy.next
      blobsStoragePrefix = "$blobStorageBucket/$localId"
    }.build()

    val writeTimestamp = clock.gcpTimestamp()
    val computationRow =
      computationMutations.insertComputation(
        localId,
        updateTime = writeTimestamp,
        globalId = globalId,
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = WRITE_NULL_TIMESTAMP,
        details = details,
        stage = initialState
      )

    val computationStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = initialState,
        creationTime = writeTimestamp,
        nextAttempt = 2,
        details = computationMutations.detailsFor(initialState)
      )

    val computationStageAttemptRow =
      computationMutations.insertComputationStageAttempt(
        localId = localId,
        stage = initialState,
        attempt = 1,
        beginTime = writeTimestamp
      )

    val blobRefRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = initialState,
        blobId = 0,
        dependencyType = ComputationBlobDependency.OUTPUT
      )

    databaseClient.write(
      listOf(
        computationRow,
        computationStageRow,
        computationStageAttemptRow,
        blobRefRow
      )
    )

    return ComputationToken(
      localId = localId,
      globalId = globalId,
      state = initialState,
      attempt = 1,
      owner = null,
      lastUpdateTime = writeTimestamp.toMillis(),
      role = computationAtThisDuchy.role,
      nextWorker = details.outgoingNodeId
    )
  }

  override fun getToken(globalId: Long): ComputationToken<StageT>? {
    val query = TokenQuery(computationMutations::longToEnum, globalId)
    val results = query.execute(databaseClient).singleOrNull() ?: return null

    return ComputationToken(
      globalId = globalId,
      // From ComputationsByGlobalId index
      localId = results.computationId,
      // From Computations
      state = results.computationStage,
      owner = results.lockOwner,
      lastUpdateTime = results.updateTime.toMillis(),
      role = results.details.role.toDuchyRole(),
      nextWorker = results.details.outgoingNodeId,
      // From ComputationStages
      attempt = results.nextAttempt - 1
    )
  }

  override fun enqueue(token: ComputationToken<StageT>) {
    runIfTokenFromLastUpdate(token) { ctx ->
      ctx.buffer(
        computationMutations.updateComputation(
          localId = token.localId,
          updateTime = clock.gcpTimestamp(),
          // Release any lock on this computation. The owner says who has the current
          // lock on the computation, and the expiration time states both if and when the
          // computation can be worked on. When LockOwner is null the computation is not being
          // worked on, but that is not enough to say a knight should pick up the computation
          // as its quest as there are stages which waiting for inputs from other nodes.
          // A non-null LockExpirationTime states when a computation can be be taken up
          // by a knight, and by using the commit timestamp we pretty much get the behaviour
          // of a FIFO queue by querying the ComputationsByLockExpirationTime secondary index.
          lockOwner = WRITE_NULL_STRING,
          lockExpirationTime = clock.gcpTimestamp()
        )
      )
    }
  }

  override fun claimTask(ownerId: String): ComputationToken<StageT>? {
    /** Claim a specific task represented by the results of running the above sql. */
    fun claimSpecificTask(result: UnclaimedTaskQueryResult<StageT>): Boolean =
      databaseClient.readWriteTransaction().run { ctx ->
        claim(
          ctx,
          result.computationId,
          result.computationStage,
          result.nextAttempt,
          result.updateTime,
          ownerId
        )
      } ?: error("claim for a specific computation ($result) returned a null value")
    UnclaimedTasksQuery(computationMutations::longToEnum, clock.gcpTimestamp())
      .execute(databaseClient)
      // First the possible tasks to claim are selected from the computations table, then for each
      // item in the list we try to claim the lock in a transaction which will only succeed if the
      // lock is still available. This pattern means only the item which is being updated
      // would need to be locked and not every possible computation that can be worked on.
      .forEach { if (claimSpecificTask(it)) return getToken(it.globalId) }
    // Did not acquire the lock on any computation.
    return null
  }

  /**
   * Tries to claim a specific computation for an owner, returning the result of the attempt.
   * If a lock is acquired a new row is written to the ComputationStageAttempts table.
   */
  private fun claim(
    ctx: TransactionContext,
    computationId: Long,
    stage: StageT,
    nextAttempt: Long,
    lastUpdate: Timestamp,
    ownerId: String
  ): Boolean {
    val currentLockOwnerStruct =
      ctx.readRow("Computations", Key.of(computationId), listOf("LockOwner", "UpdateTime"))
        ?: error("Failed to claim computation $computationId. It does not exist.")
    // Verify that the row hasn't been updated since the previous, non-transactional read.
    // If it has been updated since that time the lock should not be acquired.
    if (currentLockOwnerStruct.getTimestamp("UpdateTime") != lastUpdate) return false

    val writeTime = clock.gcpTimestamp()
    ctx.buffer(setLockMutation(computationId, ownerId))
    // Create a new attempt of the stage for the nextAttempt.
    ctx.buffer(
      computationMutations.insertComputationStageAttempt(
        computationId,
        stage,
        nextAttempt,
        beginTime = writeTime
      )
    )
    // And increment NextAttempt column of the computation stage.
    ctx.buffer(
      computationMutations.updateComputationStage(
        computationId,
        stage,
        nextAttempt = nextAttempt + 1
      )
    )

    if (currentLockOwnerStruct.getNullableString("LockOwner") != null) {
      // If the computation was locked, but that lock was expired we need to finish off the
      // current attempt of the stage.
      ctx.buffer(
        // TODO(fryej): Add a column that captures why an attempt ended.
        computationMutations.updateComputationStageAttempt(
          localId = computationId,
          stage = stage,
          attempt = nextAttempt - 1, // The current attempt is the one before the nextAttempt
          endTime = writeTime
        )
      )
    }
    // The lock was acquired.
    return true
  }

  private fun setLockMutation(computationId: Long, ownerId: String): Mutation {
    return computationMutations.updateComputation(
      computationId,
      clock.gcpTimestamp(),
      lockOwner = ownerId,
      lockExpirationTime = fiveMinutesInTheFuture()
    )
  }

  private fun fiveMinutesInTheFuture() = clock.instant().plusSeconds(300).toGcpTimestamp()

  override fun renewTask(token: ComputationToken<StageT>): ComputationToken<StageT> {
    val owner = checkNotNull(token.owner) { "Cannot renew lock for computation with no owner." }
    runIfTokenFromLastUpdate(token) { it.buffer(setLockMutation(token.localId, owner)) }
    return getToken(token.globalId)
      ?: error("Failed to renew lock on computation (${token.globalId}, it does not exist.")
  }

  override fun updateComputationState(
    token: ComputationToken<StageT>,
    to: StageT,
    inputBlobPaths: List<String>,
    outputBlobs: Int,
    afterTransition: AfterTransition
  ): ComputationToken<StageT> {
    require(
      computationMutations.validTransition(token.state, to)
    ) { "Invalid state transition ${token.state} -> $to" }

    runIfTokenFromLastUpdate(token) { ctx ->
      val unwrittenOutputs =
        blobIdToPathMap(ctx, token.localId, token.state, BlobDependencyType.OUTPUT)
          .filterValues { it == null }
      check(unwrittenOutputs.isEmpty()) {
        """
        Cannot transition computation for $token to stage $to, all outputs have not been written.
        Outputs not written for blob ids (${unwrittenOutputs.keys})
        """.trimIndent()
      }
      val writeTime = clock.gcpTimestamp()

      ctx.buffer(mutationsToChangeStages(token, to, writeTime, afterTransition))

      ctx.buffer(
        mutationsToMakeBlobRefsForNewStage(
          token.localId,
          to,
          inputBlobPaths,
          outputBlobs
        )
      )
    }
    return getToken(token.globalId) ?: error("Computation $token no longer exists.")
  }

  private fun mutationsToChangeStages(
    token: ComputationToken<StageT>,
    newStage: StageT,
    writeTime: Timestamp,
    afterTransition: AfterTransition
  ): List<Mutation> {
    val mutations = arrayListOf<Mutation>()

    mutations.add(
      computationMutations.updateComputation(
        token.localId,
        writeTime,
        stage = newStage,
        lockOwner = when (afterTransition) {
          // Write the NULL value to the lockOwner column to release the lock.
          AfterTransition.DO_NOT_ADD_TO_QUEUE,
          AfterTransition.ADD_UNCLAIMED_TO_QUEUE -> WRITE_NULL_STRING
          // Do not change the owner
          AfterTransition.CONTINUE_WORKING -> null
        },
        lockExpirationTime = when (afterTransition) {
          // Null LockExpirationTime values will not be claimed from the work queue.
          AfterTransition.DO_NOT_ADD_TO_QUEUE -> WRITE_NULL_TIMESTAMP
          // The computation is ready for processing by some worker right away.
          AfterTransition.ADD_UNCLAIMED_TO_QUEUE -> writeTime
          // The computation lock will expire sometime in the future.
          AfterTransition.CONTINUE_WORKING -> fiveMinutesInTheFuture()
        }
      )
    )

    mutations.add(
      computationMutations.updateComputationStage(
        token.localId,
        stage = token.state,
        followingStage = newStage,
        endTime = writeTime
      )
    )

    mutations.add(
      computationMutations.updateComputationStageAttempt(
        token.localId,
        token.state,
        token.attempt,
        endTime = writeTime
      )
    )

    // Mutation to insert the first attempt of the stage. When this value is null, no attempt
    // of the newly inserted ComputationStage will be added.
    val attemptOfNewStageMutation: Mutation? =
      when (afterTransition) {
        // Do not start an attempt of the stage
        AfterTransition.ADD_UNCLAIMED_TO_QUEUE -> null
        // Start an attempt of the new stage.
        AfterTransition.DO_NOT_ADD_TO_QUEUE, AfterTransition.CONTINUE_WORKING ->
          computationMutations.insertComputationStageAttempt(
            localId = token.localId,
            stage = newStage,
            attempt = 1,
            beginTime = writeTime
          )
      }

    mutations.add(
      computationMutations.insertComputationStage(
        localId = token.localId,
        stage = newStage,
        previousStage = token.state,
        creationTime = writeTime,
        details = computationMutations.detailsFor(newStage),
        // nextAttempt is the number of the current attempt of the stage plus one. Adding an Attempt
        // to the new stage while transitioning state means that an attempt of that new stage is
        // ongoing at the end of this transaction. Meaning if for some reason there needs to be
        // another attempt of that stage in the future the next attempt will be #2. Conversely, when
        // an attempt of the new stage is not added because, attemptOfNewStageMutation is null, then
        // there is not an ongoing attempt of the stage at the end of the transaction, the next
        // attempt of stage will be the first.
        nextAttempt = if (attemptOfNewStageMutation == null) 1L else 2L
      )
    )

    // Add attemptOfNewStageMutation to mutations if it is not null. This must be added after
    // the mutation to insert the computation stage because it creates the parent row.
    attemptOfNewStageMutation?.let { mutations.add(it) }

    return mutations
  }

  private fun mutationsToMakeBlobRefsForNewStage(
    localId: Long,
    stage: StageT,
    blobInputRefs: List<String>,
    outputBlobs: Int
  ): List<Mutation> {
    val mutations = ArrayList<Mutation>()
    blobInputRefs.mapIndexedTo(mutations) { index, path ->
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = stage,
        blobId = index.toLong(),
        pathToBlob = path,
        dependencyType = ComputationBlobDependency.INPUT
      )
    }

    (0 until outputBlobs).mapTo(mutations) { index ->
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = stage,
        blobId = index.toLong() + blobInputRefs.size,
        dependencyType = ComputationBlobDependency.OUTPUT
      )
    }
    return mutations
  }

  override fun readStageSpecificDetails(token: ComputationToken<StageT>): StageDetailsT {
    return computationMutations.parseDetails(
      databaseClient.singleUseReadOnlyTransaction()
        .readRow(
          "ComputationStages",
          Key.of(token.localId, computationMutations.enumToLong(token.state)),
          listOf("Details")
        )
        ?.getBytesAsByteArray("Details")
        ?: error("No ComputationStages row for ($token)")
    )
  }

  override fun readBlobReferences(
    token: ComputationToken<StageT>,
    dependencyType: BlobDependencyType
  ): Map<BlobId, String?> {
    return runIfTokenFromLastUpdate(token) { ctx ->
      blobIdToPathMap(ctx, token.localId, token.state, dependencyType)
    }!!
  }

  private fun blobIdToPathMap(
    ctx: TransactionContext,
    localId: Long,
    stage: StageT,
    dependencyType: BlobDependencyType
  ): Map<Long, String?> {
    return ctx.read(
      "ComputationBlobReferences",
      KeySet.prefixRange(Key.of(localId, computationMutations.enumToLong(stage))),
      listOf("BlobId", "PathToBlob", "DependencyType")
    )
      .asSequence()
      .filter {
        val dep = it.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber)
        when (dependencyType) {
          BlobDependencyType.ANY -> true
          BlobDependencyType.OUTPUT -> dep == ComputationBlobDependency.OUTPUT
          BlobDependencyType.INPUT -> dep == ComputationBlobDependency.INPUT
        }
      }
      .map { it.getLong("BlobId") to it.getNullableString("PathToBlob") }
      .toMap()
  }

  override fun writeOutputBlobReference(token: ComputationToken<StageT>, blobName: BlobRef) {
    require(blobName.pathToBlob.isNotBlank()) { "Cannot insert blank path to blob. $blobName" }
    runIfTokenFromLastUpdate(token) { ctx ->
      val type = ctx.readRow(
        "ComputationBlobReferences",
        Key.of(token.localId, computationMutations.enumToLong(token.state), blobName.name),
        listOf("DependencyType")
      )
        ?.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber)
        ?: error(
          "No ComputationBlobReferences row for " +
            "(${token.localId}, ${token.state}, ${blobName.name})"
        )
      require(type == ComputationBlobDependency.OUTPUT) { "Cannot write to $type blob" }
      ctx.buffer(
        computationMutations.updateComputationBlobReference(
          localId = token.localId,
          stage = token.state,
          blobId = blobName.name,
          pathToBlob = blobName.pathToBlob
        )
      )
    }
  }

  /**
   * Runs the readWriteTransactionFunction if the ComputationToken is from the most recent
   * update to a computation. This is done atomically with in read/write transaction.
   *
   * @return [R] which is the result of the readWriteTransactionBlock
   * @throws IllegalStateException if the token is not for the most recent update.
   */
  private fun <R> runIfTokenFromLastUpdate(
    token: ComputationToken<StageT>,
    readWriteTransactionBlock: (TransactionContext) -> R
  ): R? {
    return databaseClient.readWriteTransaction().run { ctx ->
      val current =
        ctx.readRow("Computations", Key.of(token.localId), listOf("UpdateTime"))
          ?: error("No row for computation (${token.localId})")
      if (current.getTimestamp("UpdateTime").toMillis() == token.lastUpdateTime) {
        readWriteTransactionBlock(ctx)
      } else {
        error("Failed to update, token is from older update time.")
      }
    }
  }
}

private fun ComputationDetails.RoleInComputation.toDuchyRole(): DuchyRole {
  return when (this) {
    ComputationDetails.RoleInComputation.PRIMARY -> DuchyRole.PRIMARY
    ComputationDetails.RoleInComputation.SECONDARY -> DuchyRole.SECONDARY
    else -> error("Unknown role $this")
  }
}
