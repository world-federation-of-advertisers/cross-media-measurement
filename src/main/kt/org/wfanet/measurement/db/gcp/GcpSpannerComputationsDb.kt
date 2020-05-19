package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.Value
import org.wfa.measurement.internal.db.gcp.ComputationDetails
import org.wfa.measurement.internal.db.gcp.ComputationStageDetails
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.AfterTransition
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.BlobName
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ComputationsRelationalDb

/**
 * Implementation of [ComputationsRelationalDb] using GCP Spanner Database.
 */
class GcpSpannerComputationsDb<T : Enum<T>>(
  private val spanner: Spanner,
  private val databaseId: DatabaseId,
  private val duchyName: String,
  private val duchyOrder: DuchyOrder,
  private val localComputationIdGenerator: LocalComputationIdGenerator,
  private val blobStorageBucket: String = "knight-computation-stage-storage"
) : ComputationsRelationalDb<T> {

  override fun insertComputation(globalId: Long, initialState: T): ComputationToken<T> {
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

    val computationRow = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(localId)
      .set("ComputationStage").to(initialState.ordinal.toLong())
      .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      .set("GlobalComputationId").to(globalId)
      .set("ComputationDetails").to(details.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(details.toJson())
      .build()

    // There are not any details for the initial stage when the record is being created.
    val stageDetails = ComputationStageDetails.getDefaultInstance()
    val computationStageRow = Mutation.newInsertBuilder("ComputationStages")
      .set("ComputationId").to(localId)
      .set("ComputationStage").to(initialState.ordinal.toLong())
      .set("CreationTime").to(Value.COMMIT_TIMESTAMP)
      // The stage is being attempted right now.
      .set("NextAttempt").to(2)
      .set("Details").to(stageDetails.toSpannerByteArray())
      .set("DetailsJSON").to(stageDetails.toJson())
      .build()

    val computationStageAttemptRow = Mutation.newInsertBuilder("ComputationStageAttempts")
      .set("ComputationId").to(localId)
      .set("ComputationStage").to(initialState.ordinal.toLong())
      // The stage is being attempted right now.
      .set("Attempt").to(1)
      .set("BeginTime").to(Value.COMMIT_TIMESTAMP)
      .build()

    spanner.getDatabaseClient(databaseId).write(
      listOf(computationRow, computationStageRow, computationStageAttemptRow)
    )

    return ComputationToken(
      localId = localId,
      globalId = globalId,
      state = initialState,
      attempt = 1,
      owner = null,
      role = computationAtThisDuchy.role,
      nextWorker = details.outgoingNodeId
    )
  }

  override fun getToken(globalId: Long): ComputationToken<T>? {
    TODO("Not yet implemented")
  }

  override fun enqueue(token: ComputationToken<T>) {
    TODO("Not yet implemented")
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
}
