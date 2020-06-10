package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TransactionContext
import java.time.Clock
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.db.duchy.AfterTransition
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.BlobId
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ComputationsRelationalDb
import org.wfanet.measurement.db.duchy.ProtocolStateEnumHelper
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.gcpTimestamp
import org.wfanet.measurement.db.gcp.getNullableString
import org.wfanet.measurement.db.gcp.getProtoBufMessage
import org.wfanet.measurement.db.gcp.singleOrNull
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toMillis
import org.wfanet.measurement.db.gcp.toProtobufMessage
import org.wfanet.measurement.internal.ComputationBlobDependency
import org.wfanet.measurement.internal.db.gcp.ComputationDetails
import org.wfanet.measurement.internal.db.gcp.ComputationStageDetails

/**
 * Implementation of [ComputationsRelationalDb] using GCP Spanner Database.
 */
class GcpSpannerComputationsDb<T : Enum<T>>(
  private val spanner: Spanner,
  private val databaseId: DatabaseId,
  private val duchyName: String,
  private val duchyOrder: DuchyOrder,
  private val blobStorageBucket: String = "knight-computation-stage-storage",
  private val stateEnumHelper: ProtocolStateEnumHelper<T>,
  private val clock: Clock = Clock.systemUTC()
) : ComputationsRelationalDb<T> {

  private val localComputationIdGenerator: LocalComputationIdGenerator =
    HalfOfGlobalBitsAndTimeStampIdGenerator(clock)

  private val databaseClient: DatabaseClient by lazy {
    spanner.getDatabaseClient(databaseId)
  }

  override fun insertComputation(globalId: Long, initialState: T): ComputationToken<T> {
    require(
      stateEnumHelper.validInitialState(initialState)
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
    val initialStateAsInt64 = stateEnumHelper.enumToLong(initialState)
    val computationRow = insertComputation(
      localId,
      updateTime = writeTimestamp,
      globalId = globalId,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = WRITE_NULL_TIMESTAMP,
      details = details,
      stage = initialStateAsInt64)

    // There are not any details for the initial stage when the record is being created.
    val stageDetails = ComputationStageDetails.getDefaultInstance()
    val computationStageRow = insertComputationStage(
      localId = localId,
      stage = initialStateAsInt64,
      creationTime = writeTimestamp,
      // The stage is being attempted right now.
      nextAttempt = 2,
      details = stageDetails
    )

    val computationStageAttemptRow = insertComputationStageAttempt(
      localId = localId,
      stage = initialStateAsInt64,
      attempt = 1,
      beginTime = writeTimestamp)

    databaseClient.write(
      listOf(computationRow, computationStageRow, computationStageAttemptRow)
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

  override fun getToken(globalId: Long): ComputationToken<T>? {
    val sql =
      """
      SELECT c.ComputationId, c.LockOwner, c.ComputationStage, c.ComputationDetails,
             c.UpdateTime, cs.NextAttempt
      FROM Computations AS c
      JOIN ComputationStages AS cs USING (ComputationId, ComputationStage)
      WHERE c.GlobalComputationId = @global_id
      """.trimIndent()

    val query: Statement =
      Statement.newBuilder(sql)
        .bind("global_id").to(globalId)
        .build()

    val struct = databaseClient.singleUse().executeQuery(query).singleOrNull() ?: return null

    val computationDetails =
      struct
        .getBytes("ComputationDetails")
        .toProtobufMessage(ComputationDetails.parser())

    return ComputationToken(
      globalId = globalId,
      // From ComputationsByGlobalId index
      localId = struct.getLong("ComputationId"),
      // From Computations
      state = stateEnumHelper.longToEnum(struct.getLong("ComputationStage")),
      owner = struct.getNullableString("LockOwner"),
      lastUpdateTime = struct.getTimestamp("UpdateTime").toMillis(),
      role = computationDetails.role.toDuchyRole(),
      nextWorker = computationDetails.outgoingNodeId,
      // From ComputationStages
      attempt = struct.getLong("NextAttempt") - 1
    )
  }

  override fun enqueue(token: ComputationToken<T>) {
    runIfTokenFromLastUpdate(token) { ctx ->
      ctx.buffer(
        updateComputation(
          token.localId, clock.gcpTimestamp(),
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

  override fun claimTask(ownerId: String): ComputationToken<T>? {
    // First the possible tasks to claim are selected from the computations table, then for each
    // item in the list we try to claim the lock in a transaction which will only succeed if the
    // lock is still available. This pattern means only the item which is being updated
    // would need to be locked and not every possible computation that can be worked on.
    val sql =
      """
      SELECT c.ComputationId,  c.GlobalComputationId, c.ComputationStage, c.UpdateTime,
             cs.NextAttempt
      FROM Computations@{FORCE_INDEX=ComputationsByLockExpirationTime} AS c
      JOIN ComputationStages cs USING(ComputationId, ComputationStage)
      WHERE c.LockExpirationTime <= @current_time
      ORDER BY c.LockExpirationTime ASC, c.UpdateTime ASC
      LIMIT 50
      """.trimIndent()
    /** Claim a specific task represented by the results of running the above sql. */
    fun claimSpecificTask(struct: Struct): Boolean =
      databaseClient.readWriteTransaction().run { ctx ->
        claim(
          ctx,
          struct.getLong("ComputationId"),
          struct.getLong("ComputationStage"),
          struct.getLong("NextAttempt"),
          struct.getTimestamp("UpdateTime"),
          ownerId
        )
      } ?: error("claim for a specific computation ($struct) returned a null value")

    databaseClient
      .singleUse()
      .executeQuery(Statement.newBuilder(sql).bind("current_time").to(clock.gcpTimestamp()).build())
      .asSequence()
      .forEach {
        if (claimSpecificTask(it)) {
          return getToken(it.getLong("GlobalComputationId"))
        }
      }

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
    stageAsInt64: Long,
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
      insertComputationStageAttempt(
        computationId, stageAsInt64, nextAttempt, beginTime = writeTime)
    )
    // And increment NextAttempt column of the computation stage.
    ctx.buffer(
      updateComputationStage(computationId, stageAsInt64, nextAttempt = nextAttempt + 1)
    )

    if (currentLockOwnerStruct.getNullableString("LockOwner") != null) {
      // If the computation was locked, but that lock was expired we need to finish off the
      // current attempt of the stage.
      ctx.buffer(
        // TODO(fryej): Add a column that captures why an attempt ended.
        updateComputationStageAttempt(
          localId = computationId,
          stage = stageAsInt64,
          // The current attempt is the one before the nextAttempt
          attempt = nextAttempt - 1,
          endTime = writeTime)
      )
    }
    // The lock was acquired.
    return true
  }

  private fun setLockMutation(computationId: Long, ownerId: String): Mutation {
    return updateComputation(
      computationId, clock.gcpTimestamp(),
      lockOwner = ownerId,
      lockExpirationTime = fiveMinutesInTheFuture()
    )
  }

  private fun fiveMinutesInTheFuture() = clock.instant().plusSeconds(300).toGcpTimestamp()

  override fun renewTask(token: ComputationToken<T>): ComputationToken<T> {
    val owner = checkNotNull(token.owner) { "Cannot renew lock for computation with no owner." }
    runIfTokenFromLastUpdate(token) { it.buffer(setLockMutation(token.localId, owner)) }
    return getToken(token.globalId)
      ?: error("Failed to renew lock on computation (${token.globalId}, it does not exist.")
  }

  override fun updateComputationState(
    token: ComputationToken<T>,
    to: T,
    blobInputRefs: Collection<BlobRef>,
    blobOutputRefs: Collection<BlobId>,
    afterTransition: AfterTransition
  ): ComputationToken<T> {
    require(
      stateEnumHelper.validTransition(token.state, to)
    ) { "Invalid state transition ${token.state} -> $to" }

    runIfTokenFromLastUpdate(token) { ctx ->
      val writeTime = clock.gcpTimestamp()
      val newStageAsInt64 = stateEnumHelper.enumToLong(to)

      ctx.buffer(
        mutationsToChangeStages(
          ctx,
          token,
          newStageAsInt64,
          writeTime,
          afterTransition
        )
      )

      ctx.buffer(
        mutationsToMakeBlobRefsForNewStage(
          token.localId,
          newStageAsInt64,
          blobInputRefs,
          blobOutputRefs
        )
      )
    }
    return getToken(token.globalId) ?: error("Computation $token no longer exists.")
  }

  private fun mutationsToChangeStages(
    ctx: TransactionContext,
    token: ComputationToken<T>,
    newStageAsInt64: Long,
    writeTime: Timestamp,
    afterTransition: AfterTransition
  ): List<Mutation> {
    val currentStageAsInt64 = stateEnumHelper.enumToLong(token.state)
    val mutations = arrayListOf<Mutation>()

    mutations.add(updateComputation(
      token.localId,
      writeTime,
      stage = newStageAsInt64,
      lockOwner = when (afterTransition) {
        // Write the NULL value to the lockOwner column to release the lock.
        AfterTransition.DO_NOT_ADD_TO_QUEUE, AfterTransition.ADD_UNCLAIMED_TO_QUEUE ->
          WRITE_NULL_STRING
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
      })
    )

    mutations.add(mutationToTransitionOutOfCurrentStage(ctx, token, newStageAsInt64, writeTime))

    mutations.add(
      updateComputationStageAttempt(
        token.localId, currentStageAsInt64, token.attempt, endTime = writeTime)
    )

    // Mutation to insert the first attempt of the stage. When this value is null, no attempt
    // of the newly inserted ComputationStage will be added.
    val attemptOfNewStageMutation: Mutation? =
      when (afterTransition) {
        // Do not start an attempt of the stage
        AfterTransition.ADD_UNCLAIMED_TO_QUEUE -> null
        // Start an attempt of the new stage.
        AfterTransition.DO_NOT_ADD_TO_QUEUE, AfterTransition.CONTINUE_WORKING ->
          insertComputationStageAttempt(token.localId, newStageAsInt64, 1, beginTime = writeTime)
      }

    val newStageDetails = ComputationStageDetails.newBuilder().apply {
      previousStageValue = currentStageAsInt64.toInt()
      // TODO(fryej): Set stage_specific field.
    }.build()
    mutations.add(insertComputationStage(
      token.localId,
      newStageAsInt64,
      creationTime = writeTime,
      details = newStageDetails,
      // nextAttempt is the number of the current attempt of the stage plus one.
      // Adding an Attempt to the new stage while transitioning state means that an attempt of that
      // new stage is ongoing at the end of this transaction. Meaning if for some reason there
      // needs to be another attempt of that stage in the future the next attempt will be #2.
      // Conversely, when an attempt of the new stage is not added because,
      // attemptOfNewStageMutation is null, then there is not an ongoing attempt of the stage at
      // the end of the transaction, the next attempt of stage will be the first.
      nextAttempt = if (attemptOfNewStageMutation == null) 1L else 2L
    ))

    // Add attemptOfNewStageMutation to mutations if it is not null. This must be added after
    // the mutation to insert the computation stage because it creates the parent row.
    attemptOfNewStageMutation?.let { mutations.add(it) }

    return mutations
  }

  private fun mutationToTransitionOutOfCurrentStage(
    ctx: TransactionContext,
    token: ComputationToken<T>,
    newStageAsInt64: Long,
    writeTime: Timestamp
  ): Mutation {
    val currentStageAsInt64 = stateEnumHelper.enumToLong(token.state)
    val currentStageDetails = ctx.readRow(
      "ComputationStages",
      Key.of(token.localId, currentStageAsInt64),
      listOf("Details")
    ) ?: error("No row for (${token.localId}, $currentStageAsInt64)")

    val existingStageDetails =
      currentStageDetails.getProtoBufMessage("Details", ComputationStageDetails.parser())
        .toBuilder()
        .setFollowingStageValue(newStageAsInt64.toInt())
        .build()

    return updateComputationStage(
      token.localId, currentStageAsInt64, endTime = writeTime, details = existingStageDetails
    )
  }

  private fun mutationsToMakeBlobRefsForNewStage(
    localId: Long,
    stageAsInt64: Long,
    blobInputRefs: Collection<BlobRef>,
    blobOutputRefs: Collection<BlobId>
  ): List<Mutation> {
    val mutations = ArrayList<Mutation>()
    blobInputRefs.mapIndexedTo(mutations) { index, blobRef ->
      insertComputationBlobReference(
        localId = localId,
        stage = stageAsInt64,
        blobId = index.toLong(),
        pathToBlob = blobRef.pathToBlob,
        dependencyType = ComputationBlobDependency.INPUT
      )
    }

    blobOutputRefs.mapIndexedTo(mutations) { index, _ ->
      insertComputationBlobReference(
        localId = localId,
        stage = stageAsInt64,
        blobId = index.toLong() + blobInputRefs.size,
        dependencyType = ComputationBlobDependency.OUTPUT
      )
    }
    return mutations
  }

  override fun readBlobReferences(
    token: ComputationToken<T>,
    dependencyType: BlobDependencyType
  ): Map<BlobId, String?> {
    val sql =
      """
      SELECT BlobId, PathToBlob, DependencyType 
      FROM ComputationBlobReferences
      WHERE ComputationId = @local_id AND ComputationStage = @stage_as_int_64
      """.trimMargin()
    val blobRefsForStageQuery =
      Statement.newBuilder(sql)
        .bind("local_id").to(token.localId)
        .bind("stage_as_int_64").to(stateEnumHelper.enumToLong(token.state))
        .build()

    return runIfTokenFromLastUpdate(token) { ctx ->
      ctx
        .executeQuery(blobRefsForStageQuery)
        .asSequence()
        .filter {
          val dep = ComputationBlobDependency.forNumber(it.getLong("DependencyType").toInt())
          when (dependencyType) {
            BlobDependencyType.ANY -> true
            BlobDependencyType.OUTPUT -> dep == ComputationBlobDependency.OUTPUT
            BlobDependencyType.INPUT -> dep == ComputationBlobDependency.INPUT
          }
        }
        .map { it.getLong("BlobId") to it.getNullableString("PathToBlob") }
        .toMap()
    }!!
  }

  override fun writeOutputBlobReference(token: ComputationToken<T>, blobName: BlobRef) {
    TODO("Not yet implemented")
  }

  /**
   * Runs the readWriteTransactionFunction if the ComputationToken is from the most recent
   * update to a computation. This is done atomically with in read/write transaction.
   *
   * @return [R] which is the result of the readWriteTransactionBlock
   * @throws IllegalStateException if the token is not for the most recent update.
   */
  private fun <R> runIfTokenFromLastUpdate(
    token: ComputationToken<T>,
    readWriteTransactionBlock: (TransactionContext) -> R
  ): R? {
    return databaseClient.readWriteTransaction().run { ctx ->
      val current = ctx.readRow("Computations", Key.of(token.localId), listOf("UpdateTime"))
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
