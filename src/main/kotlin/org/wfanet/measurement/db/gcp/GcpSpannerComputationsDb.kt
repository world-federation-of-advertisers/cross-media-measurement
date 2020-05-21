package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.TransactionContext
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.AfterTransition
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.BlobName
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ComputationsRelationalDb
import org.wfanet.measurement.db.duchy.ProtocolStateEnumHelper
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
  private val localComputationIdGenerator: LocalComputationIdGenerator,
  private val blobStorageBucket: String = "knight-computation-stage-storage",
  private val stateEnumHelper: ProtocolStateEnumHelper<T>
) : ComputationsRelationalDb<T> {

  override fun insertComputation(globalId: Long, initialState: T): ComputationToken<T> {
    // TODO: Invalidate any previously running computation for the global id.
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

    val initialStateAsInt64 = stateEnumHelper.enumToLong(initialState)
    val computationRow = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(localId)
      .set("ComputationStage").to(initialStateAsInt64)
      .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      .set("GlobalComputationId").to(globalId)
      .set("ComputationDetails").to(details.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(details.toJson())
      .build()

    // There are not any details for the initial stage when the record is being created.
    val stageDetails = ComputationStageDetails.getDefaultInstance()
    val computationStageRow = Mutation.newInsertBuilder("ComputationStages")
      .set("ComputationId").to(localId)
      .set("ComputationStage").to(initialStateAsInt64)
      .set("CreationTime").to(Value.COMMIT_TIMESTAMP)
      // The stage is being attempted right now.
      .set("NextAttempt").to(2)
      .set("Details").to(stageDetails.toSpannerByteArray())
      .set("DetailsJSON").to(stageDetails.toJson())
      .build()

    val computationStageAttemptRow = Mutation.newInsertBuilder("ComputationStageAttempts")
      .set("ComputationId").to(localId)
      .set("ComputationStage").to(initialStateAsInt64)
      // The stage is being attempted right now.
      .set("Attempt").to(1)
      .set("BeginTime").to(Value.COMMIT_TIMESTAMP)
      .build()

    val writeTime = spanner.getDatabaseClient(databaseId).write(
      listOf(computationRow, computationStageRow, computationStageAttemptRow)
    )

    return ComputationToken(
      localId = localId,
      globalId = globalId,
      state = initialState,
      attempt = 1,
      owner = null,
      lastUpdateTime = writeTime.toMillis(),
      role = computationAtThisDuchy.role,
      nextWorker = details.outgoingNodeId
    )
  }

  override fun getToken(globalId: Long): ComputationToken<T>? {
    val transaction = spanner.getDatabaseClient(databaseId).singleUseReadOnlyTransaction()
    try {
      val results = transaction.executeQuery(
        Statement.newBuilder(
          """
          SELECT c.ComputationId, c.LockOwner, c.ComputationStage, c.ComputationDetails,
                 c.UpdateTime, cs.NextAttempt,
          FROM Computations AS c
          JOIN ComputationStages AS cs ON
            (c.ComputationId = cs.ComputationId AND c.ComputationStage = cs.ComputationStage)
          WHERE c.GlobalComputationId = @global_id
          ORDER BY UpdateTime DESC
          LIMIT 1
        """.trimIndent()
      ).bind("global_id").to(globalId).build())
      val struct = results.getAtMostOne() ?: return null

      val computationDetails =
        struct.getBytes("ComputationDetails").toProtobufMessage(ComputationDetails.parser())

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
    } finally { transaction.close() }
  }

  override fun enqueue(token: ComputationToken<T>) {
    runIfTokenFromLastUpdate(token) { ctx ->
      ctx.buffer(
        Mutation.newUpdateBuilder("Computations")
          .set("ComputationId").to(token.localId)
          .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
          // Release any lock on this computation. The owner says who has the current
          // lock on the computation, and the expiration time states both if and when the
          // computation can be worked on. When LockOwner is null the computation is not being
          // worked on, but that is not enough to say a knight should pick up the computation
          // as its quest as there are stages which waiting for inputs from other nodes.
          // A non-null LockExpirationTime states when a computation can be be taken up
          // by a knight, and by using the commit timestamp we pretty much get the behaviour
          // of a FIFO queue by querying the ComputationsByLockExpirationTime secondary index.
          .set("LockOwner").to(null as String?)
          .set("LockExpirationTime").to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    }
  }

  override fun claimTask(ownerId: String): ComputationToken<T>? {
    TODO("Not yet implemented")
  }

  override fun renewTask(token: ComputationToken<T>) {
    TODO("Not yet implemented")
  }

  override fun updateComputationState(
    token: ComputationToken<T>,
    to: T,
    blobInputRefs: Collection<BlobRef>,
    blobOutputRefs: Collection<BlobName>,
    afterTransition: AfterTransition
  ): ComputationToken<T> {
    TODO("Not yet implemented")
  }

  override fun readBlobReferenceNames(
    current: ComputationToken<T>,
    dependencyType: BlobDependencyType
  ): Map<BlobName, String?> {
    TODO("Not yet implemented")
  }

  override fun writeOutputBlobReference(c: ComputationToken<T>, blobName: BlobRef) {
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
  ) : R? {
    return spanner.getDatabaseClient(databaseId).readWriteTransaction().run { ctx ->
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
