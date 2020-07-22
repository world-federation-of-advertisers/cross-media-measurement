package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import com.google.protobuf.Message
import java.time.Clock
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.AfterTransition
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.BlobId
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ComputationsRelationalDb
import org.wfanet.measurement.db.duchy.EndComputationReason
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.gcpTimestamp
import org.wfanet.measurement.db.gcp.getBytesAsByteArray
import org.wfanet.measurement.db.gcp.getNullableString
import org.wfanet.measurement.db.gcp.getProtoEnum
import org.wfanet.measurement.db.gcp.getProtoMessage
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toMillis
import org.wfanet.measurement.internal.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

/**
 * Implementation of [ComputationsRelationalDb] using GCP Spanner Database.
 */
class GcpSpannerComputationsDb<StageT : Enum<StageT>, StageDetailsT : Message>(
  private val databaseClient: DatabaseClient,
  private val duchyName: String,
  private val duchyOrder: DuchyOrder,
  private val blobStorageBucket: String = "mill-computation-stage-storage",
  private val computationMutations: ComputationMutations<StageT, StageDetailsT>,
  private val clock: Clock = Clock.systemUTC()
) : ComputationsRelationalDb<StageT, StageDetailsT> {

  private val localComputationIdGenerator: LocalComputationIdGenerator =
    HalfOfGlobalBitsAndTimeStampIdGenerator(clock)

  override suspend fun insertComputation(
    globalId: Long,
    initialStage: StageT
  ): ComputationToken<StageT> {
    require(
      computationMutations.validInitialStage(initialStage)
    ) { "Invalid initial stage $initialStage" }

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
        stage = initialStage
      )

    val computationStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = initialStage,
        creationTime = writeTimestamp,
        nextAttempt = 2,
        details = computationMutations.detailsFor(initialStage)
      )

    val computationStageAttemptRow =
      computationMutations.insertComputationStageAttempt(
        localId = localId,
        stage = initialStage,
        attempt = 1,
        beginTime = writeTimestamp,
        details = ComputationStageAttemptDetails.getDefaultInstance()
      )

    val blobRefRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = initialStage,
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
      stage = initialStage,
      attempt = 1,
      owner = null,
      lastUpdateTime = writeTimestamp.toMillis(),
      role = computationAtThisDuchy.role,
      nextWorker = details.outgoingNodeId
    )
  }

  override suspend fun getToken(globalId: Long): ComputationToken<StageT>? {
    val query = TokenQuery(computationMutations::longToEnum, globalId)
    val results = query.execute(databaseClient).singleOrNull() ?: return null

    return ComputationToken(
      globalId = globalId,
      // From ComputationsByGlobalId index
      localId = results.computationId,
      // From Computations
      stage = results.computationStage,
      owner = results.lockOwner,
      lastUpdateTime = results.updateTime.toMillis(),
      role = results.details.role.toDuchyRole(),
      nextWorker = results.details.outgoingNodeId,
      // From ComputationStages
      attempt = results.nextAttempt - 1
    )
  }

  override suspend fun enqueue(token: ComputationToken<StageT>) {
    runIfTokenFromLastUpdate(token) { ctx ->
      ctx.buffer(
        computationMutations.updateComputation(
          localId = token.localId,
          updateTime = clock.gcpTimestamp(),
          // Release any lock on this computation. The owner says who has the current
          // lock on the computation, and the expiration time stages both if and when the
          // computation can be worked on. When LockOwner is null the computation is not being
          // worked on, but that is not enough to say a mill should pick up the computation
          // as its quest as there are stages which waiting for inputs from other nodes.
          // A non-null LockExpirationTime stages when a computation can be be taken up
          // by a mill, and by using the commit timestamp we pretty much get the behaviour
          // of a FIFO queue by querying the ComputationsByLockExpirationTime secondary index.
          lockOwner = WRITE_NULL_STRING,
          lockExpirationTime = clock.gcpTimestamp()
        )
      )
    }
  }

  override suspend fun claimTask(ownerId: String): ComputationToken<StageT>? {
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
    return UnclaimedTasksQuery(computationMutations::longToEnum, clock.gcpTimestamp())
      .execute(databaseClient)
      // First the possible tasks to claim are selected from the computations table, then for each
      // item in the list we try to claim the lock in a transaction which will only succeed if the
      // lock is still available. This pattern means only the item which is being updated
      // would need to be locked and not every possible computation that can be worked on.
      .filter { claimSpecificTask(it) }
      // If the value is null, no tasks were claimed so there is no token to retrieve.
      .firstOrNull()?.let { getToken(it.globalId) ?: error("No token for claimed task $it") }
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
        beginTime = writeTime,
        details = ComputationStageAttemptDetails.getDefaultInstance()
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
      // The current attempt is the one before the nextAttempt
      val currentAttempt = nextAttempt - 1
      val details =
        ctx.readRow(
          "ComputationStageAttempts",
          Key.of(computationId, computationMutations.enumToLong(stage), currentAttempt),
          listOf("Details")
        )
          ?.getProtoMessage("Details", ComputationStageAttemptDetails.parser())
          ?: error("Failed to claim computation $computationId. It does not exist.")
      // If the computation was locked, but that lock was expired we need to finish off the
      // current attempt of the stage.
      ctx.buffer(
        computationMutations.updateComputationStageAttempt(
          localId = computationId,
          stage = stage,
          attempt = currentAttempt,
          endTime = writeTime,
          details = details.toBuilder()
            .setReasonEnded(ComputationStageAttemptDetails.EndReason.LOCK_OVERWRITTEN)
            .build()
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

  override suspend fun renewTask(token: ComputationToken<StageT>): ComputationToken<StageT> {
    val owner = checkNotNull(token.owner) { "Cannot renew lock for computation with no owner." }
    runIfTokenFromLastUpdate(token) { it.buffer(setLockMutation(token.localId, owner)) }
    return getToken(token.globalId)
      ?: error("Failed to renew lock on computation (${token.globalId}, it does not exist.")
  }

  override suspend fun updateComputationStage(
    token: ComputationToken<StageT>,
    to: StageT,
    inputBlobPaths: List<String>,
    outputBlobs: Int,
    afterTransition: AfterTransition
  ): ComputationToken<StageT> {
    require(
      computationMutations.validTransition(token.stage, to)
    ) { "Invalid stage transition ${token.stage} -> $to" }

    runIfTokenFromLastUpdate(token) { ctx ->
      val unwrittenOutputs =
        blobIdToPathMap(ctx, token.localId, token.stage, BlobDependencyType.OUTPUT)
          .filterValues { it == null }
      check(unwrittenOutputs.isEmpty()) {
        """
        Cannot transition computation for $token to stage $to, all outputs have not been written.
        Outputs not written for blob ids (${unwrittenOutputs.keys})
        """.trimIndent()
      }
      val writeTime = clock.gcpTimestamp()

      ctx.buffer(mutationsToChangeStages(ctx, token, to, writeTime, afterTransition))

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

  override suspend fun endComputation(
    token: ComputationToken<StageT>,
    endingStage: StageT,
    endComputationReason: EndComputationReason
  ) {
    require(computationMutations.validTerminalStage(endingStage)) {
      "Invalid terminal stage of computation $endingStage"
    }
    runIfTokenFromLastUpdate(token) { ctx ->
      val writeTime = clock.gcpTimestamp()
      val details = ctx.readRow("Computations", Key.of(token.localId), listOf("ComputationDetails"))
        ?.getProtoMessage("ComputationDetails", ComputationDetails.parser())
        ?: error("Computation missing $token")

      ctx.buffer(
        computationMutations.updateComputation(
          localId = token.localId,
          updateTime = writeTime,
          stage = endingStage,
          lockOwner = WRITE_NULL_STRING,
          lockExpirationTime = WRITE_NULL_TIMESTAMP,
          // Add a reason why the computation ended to the details section.
          details = details.toBuilder().setEndingState(
            when (endComputationReason) {
              EndComputationReason.SUCCEEDED -> ComputationDetails.CompletedReason.SUCCEEDED
              EndComputationReason.FAILED -> ComputationDetails.CompletedReason.FAILED
              EndComputationReason.CANCELED -> ComputationDetails.CompletedReason.CANCELED
            }
          ).build()
        )
      )
      ctx.buffer(
        computationMutations.updateComputationStage(
          localId = token.localId,
          stage = token.stage,
          endTime = writeTime,
          followingStage = endingStage
        )
      )
      ctx.buffer(
        computationMutations.insertComputationStage(
          localId = token.localId,
          stage = endingStage,
          creationTime = writeTime,
          previousStage = token.stage,
          nextAttempt = 1,
          details = computationMutations.detailsFor(endingStage)
        )
      )
      UnfinishedAttemptQuery(computationMutations::longToEnum, token.localId)
        .execute(ctx)
        .collect { unfinished ->
          ctx.buffer(
            computationMutations.updateComputationStageAttempt(
              localId = unfinished.computationId,
              stage = unfinished.stage,
              attempt = unfinished.attempt,
              endTime = writeTime,
              details = unfinished.details.toBuilder()
                .setReasonEnded(ComputationStageAttemptDetails.EndReason.CANCELLED)
                .build()
            )
          )
        }
    }
  }

  private fun mutationsToChangeStages(
    ctx: TransactionContext,
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
        localId = token.localId,
        stage = token.stage,
        followingStage = newStage,
        endTime = writeTime
      )
    )

    val attemptDetails =
      ctx.readRow(
        "ComputationStageAttempts",
        Key.of(token.localId, computationMutations.enumToLong(token.stage), token.attempt),
        listOf("Details")
      )
        ?.getProtoMessage("Details", ComputationStageAttemptDetails.parser())
        ?: error("No ComputationStageAttempt (${token.localId}, $newStage, ${token.attempt})")
    mutations.add(
      computationMutations.updateComputationStageAttempt(
        localId = token.localId,
        stage = token.stage,
        attempt = token.attempt,
        endTime = writeTime,
        details = attemptDetails.toBuilder()
          .setReasonEnded(ComputationStageAttemptDetails.EndReason.SUCCEEDED)
          .build()
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
            beginTime = writeTime,
            details = ComputationStageAttemptDetails.getDefaultInstance()
          )
      }

    mutations.add(
      computationMutations.insertComputationStage(
        localId = token.localId,
        stage = newStage,
        previousStage = token.stage,
        creationTime = writeTime,
        details = computationMutations.detailsFor(newStage),
        // nextAttempt is the number of the current attempt of the stage plus one. Adding an Attempt
        // to the new stage while transitioning stage means that an attempt of that new stage is
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

  override suspend fun readStageSpecificDetails(token: ComputationToken<StageT>): StageDetailsT {
    return computationMutations.parseDetails(
      databaseClient.singleUseReadOnlyTransaction()
        .readRow(
          "ComputationStages",
          Key.of(token.localId, computationMutations.enumToLong(token.stage)),
          listOf("Details")
        )
        ?.getBytesAsByteArray("Details")
        ?: error("No ComputationStages row for ($token)")
    )
  }

  override suspend fun readBlobReferences(
    token: ComputationToken<StageT>,
    dependencyType: BlobDependencyType
  ): Map<BlobId, String?> {
    return runIfTokenFromLastUpdate(token) { ctx ->
      blobIdToPathMap(ctx, token.localId, token.stage, dependencyType)
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

  override suspend fun writeOutputBlobReference(
    token: ComputationToken<StageT>,
    blobName: BlobRef
  ) {
    require(blobName.pathToBlob.isNotBlank()) { "Cannot insert blank path to blob. $blobName" }
    runIfTokenFromLastUpdate(token) { ctx ->
      val type = ctx.readRow(
        "ComputationBlobReferences",
        Key.of(token.localId, computationMutations.enumToLong(token.stage), blobName.name),
        listOf("DependencyType")
      )
        ?.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber)
        ?: error(
          "No ComputationBlobReferences row for " +
            "(${token.localId}, ${token.stage}, ${blobName.name})"
        )
      require(type == ComputationBlobDependency.OUTPUT) { "Cannot write to $type blob" }
      ctx.buffer(
        computationMutations.updateComputationBlobReference(
          localId = token.localId,
          stage = token.stage,
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
  private suspend fun <R> runIfTokenFromLastUpdate(
    token: ComputationToken<StageT>,
    readWriteTransactionBlock: suspend (TransactionContext) -> R
  ): R? {
    return databaseClient.readWriteTransaction().run { ctx ->
      val current =
        ctx.readRow("Computations", Key.of(token.localId), listOf("UpdateTime"))
          ?: error("No row for computation (${token.localId})")
      if (current.getTimestamp("UpdateTime").toMillis() == token.lastUpdateTime) {
        runBlocking { readWriteTransactionBlock(ctx) }
      } else {
        error("Failed to update, token is from older update time.")
      }
    }
  }

  override suspend fun readGlobalComputationIds(stages: Set<StageT>): Set<Long> {
    return GlobalIdsQuery(computationMutations, stages)
      .execute(databaseClient)
      .toCollection(mutableSetOf())
  }
}

private fun ComputationDetails.RoleInComputation.toDuchyRole(): DuchyRole {
  return when (this) {
    ComputationDetails.RoleInComputation.PRIMARY -> DuchyRole.PRIMARY
    ComputationDetails.RoleInComputation.SECONDARY -> DuchyRole.SECONDARY
    else -> error("Unknown role $this")
  }
}
