// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.db.duchy.computation.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.protobuf.Message
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.computation.AfterTransition
import org.wfanet.measurement.db.duchy.computation.BlobRef
import org.wfanet.measurement.db.duchy.computation.ComputationStorageEditToken
import org.wfanet.measurement.db.duchy.computation.ComputationsRelationalDb
import org.wfanet.measurement.db.duchy.computation.EndComputationReason
import org.wfanet.measurement.gcloud.common.gcloudTimestamp
import org.wfanet.measurement.gcloud.common.toEpochMilli
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.TransactionWork
import org.wfanet.measurement.gcloud.spanner.getNullableString
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

/**
 * Implementation of [ComputationsRelationalDb] using GCP Spanner Database.
 */
class GcpSpannerComputationsDb<StageT, StageDetailsT : Message>(
  private val databaseClient: AsyncDatabaseClient,
  private val duchyName: String,
  private val duchyOrder: DuchyOrder,
  private val blobStorageBucket: String = "mill-computation-stage-storage",
  private val computationMutations: ComputationMutations<StageT, StageDetailsT>,
  private val clock: Clock = Clock.systemUTC(),
  private val lockDuration: Duration = Duration.ofMinutes(5)
) : ComputationsRelationalDb<StageT, StageDetailsT> {

  private val localComputationIdGenerator: LocalComputationIdGenerator =
    GlobalBitsPlusTimeStampIdGenerator(clock)

  override suspend fun insertComputation(
    globalId: String,
    initialStage: StageT,
    stageDetails: StageDetailsT
  ) {
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
      primaryNodeId = computationAtThisDuchy.primary
      blobsStoragePrefix = "$blobStorageBucket/$localId"
    }.build()

    val writeTimestamp = clock.gcloudTimestamp()
    val computationRow =
      computationMutations.insertComputation(
        localId,
        updateTime = writeTimestamp,
        globalId = globalId,
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = clock.gcloudTimestamp(),
        details = details,
        stage = initialStage
      )

    val computationStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = initialStage,
        creationTime = writeTimestamp,
        nextAttempt = 2,
        details = stageDetails
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
      computationRow,
      computationStageRow,
      computationStageAttemptRow,
      blobRefRow
    )
  }

  override suspend fun enqueue(token: ComputationStorageEditToken<StageT>, delaySecond: Int) {
    runIfTokenFromLastUpdate(token) { ctx ->
      ctx.buffer(
        computationMutations.updateComputation(
          localId = token.localId,
          updateTime = clock.gcloudTimestamp(),
          // Release any lock on this computation. The owner says who has the current
          // lock on the computation, and the expiration time stages both if and when the
          // computation can be worked on. When LockOwner is null the computation is not being
          // worked on, but that is not enough to say a mill should pick up the computation
          // as its quest as there are stages which waiting for inputs from other nodes.
          // A non-null LockExpirationTime stages when a computation can be be taken up
          // by a mill, and by using the commit timestamp we pretty much get the behaviour
          // of a FIFO queue by querying the ComputationsByLockExpirationTime secondary index.
          lockOwner = WRITE_NULL_STRING,
          lockExpirationTime = clock.instant().plusSeconds(delaySecond.toLong()).toGcloudTimestamp()
        )
      )
    }
  }

  override suspend fun claimTask(ownerId: String): String? {
    /** Claim a specific task represented by the results of running the above sql. */
    suspend fun claimSpecificTask(result: UnclaimedTaskQueryResult<StageT>): Boolean =
      databaseClient.readWriteTransaction().execute { ctx ->
        claim(
          ctx,
          result.computationId,
          result.computationStage,
          result.nextAttempt,
          result.updateTime,
          ownerId
        )
      }
    return UnclaimedTasksQuery(computationMutations::longToEnum, clock.gcloudTimestamp())
      .execute(databaseClient)
      // First the possible tasks to claim are selected from the computations table, then for each
      // item in the list we try to claim the lock in a transaction which will only succeed if the
      // lock is still available. This pattern means only the item which is being updated
      // would need to be locked and not every possible computation that can be worked on.
      .filter { claimSpecificTask(it) }
      // If the value is null, no tasks were claimed.
      .firstOrNull()?.globalId
  }

  /**
   * Tries to claim a specific computation for an owner, returning the result of the attempt.
   * If a lock is acquired a new row is written to the ComputationStageAttempts table.
   */
  private suspend fun claim(
    ctx: AsyncDatabaseClient.TransactionContext,
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

    // TODO(sanjayvas): Determine whether we can use commit timestamp via
    // spanner.commit_timestamp() in mutation.
    val writeTime = clock.gcloudTimestamp()
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
      clock.gcloudTimestamp(),
      lockOwner = ownerId,
      lockExpirationTime = nextLockExpiration()
    )
  }

  private fun nextLockExpiration(): Timestamp =
    clock.instant().plus(lockDuration).toGcloudTimestamp()

  override suspend fun updateComputationStage(
    token: ComputationStorageEditToken<StageT>,
    nextStage: StageT,
    inputBlobPaths: List<String>,
    outputBlobs: Int,
    afterTransition: AfterTransition,
    nextStageDetails: StageDetailsT
  ) {
    require(
      computationMutations.validTransition(token.stage, nextStage)
    ) { "Invalid stage transition ${token.stage} -> $nextStage" }

    runIfTokenFromLastUpdate(token) { ctx ->
      val unwrittenOutputs =
        outputBlobIdToPathMap(ctx, token.localId, token.stage).filterValues { it == null }
      check(unwrittenOutputs.isEmpty()) {
        """
        Cannot transition computation for $token to stage $nextStage, all outputs have not been written.
        Outputs not written for blob ids (${unwrittenOutputs.keys})
        """.trimIndent()
      }
      val writeTime = clock.gcloudTimestamp()

      ctx.buffer(
        mutationsToChangeStages(ctx, token, nextStage, writeTime, afterTransition, nextStageDetails)
      )

      ctx.buffer(
        mutationsToMakeBlobRefsForNewStage(
          token.localId,
          nextStage,
          inputBlobPaths,
          outputBlobs
        )
      )
    }
  }

  override suspend fun endComputation(
    token: ComputationStorageEditToken<StageT>,
    endingStage: StageT,
    endComputationReason: EndComputationReason
  ) {
    require(computationMutations.validTerminalStage(endingStage)) {
      "Invalid terminal stage of computation $endingStage"
    }
    runIfTokenFromLastUpdate(token) { ctx ->
      val writeTime = clock.gcloudTimestamp()
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

  private suspend fun mutationsToChangeStages(
    ctx: AsyncDatabaseClient.TransactionContext,
    token: ComputationStorageEditToken<StageT>,
    newStage: StageT,
    writeTime: Timestamp,
    afterTransition: AfterTransition,
    nextStageDetails: StageDetailsT
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
          AfterTransition.CONTINUE_WORKING -> nextLockExpiration()
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
        attempt = token.attempt.toLong(),
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
        details = nextStageDetails,
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

  private suspend fun outputBlobIdToPathMap(
    ctx: AsyncDatabaseClient.TransactionContext,
    localId: Long,
    stage: StageT
  ): Map<Long, String?> {
    return ctx.read(
      "ComputationBlobReferences",
      KeySet.prefixRange(Key.of(localId, computationMutations.enumToLong(stage))),
      listOf("BlobId", "PathToBlob", "DependencyType")
    )
      .filter {
        val dep = it.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber)
        dep == ComputationBlobDependency.OUTPUT
      }
      .toList()
      .associate { it.getLong("BlobId") to it.getNullableString("PathToBlob") }
  }

  override suspend fun writeOutputBlobReference(
    token: ComputationStorageEditToken<StageT>,
    blobRef: BlobRef
  ) {
    require(blobRef.key.isNotBlank()) { "Cannot insert blank path to blob. $blobRef" }
    runIfTokenFromLastUpdate(token) { ctx ->
      val type = ctx.readRow(
        "ComputationBlobReferences",
        Key.of(
          token.localId,
          computationMutations.enumToLong(token.stage),
          blobRef.idInRelationalDatabase
        ),
        listOf("DependencyType")
      )
        ?.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber)
        ?: error(
          "No ComputationBlobReferences row for " +
            "(${token.localId}, ${token.stage}, ${blobRef.idInRelationalDatabase})"
        )
      require(type == ComputationBlobDependency.OUTPUT) { "Cannot write to $type blob" }
      ctx.buffer(
        listOf(
          computationMutations.updateComputation(
            localId = token.localId,
            updateTime = clock.gcloudTimestamp()
          ),
          computationMutations.updateComputationBlobReference(
            localId = token.localId,
            stage = token.stage,
            blobId = blobRef.idInRelationalDatabase,
            pathToBlob = blobRef.key
          )
        )
      )
    }
  }

  /**
   * Runs [readWriteTransactionBlock] if the ComputationToken is from the most
   * recent update to a computation. This is done atomically with in read/write
   * transaction.
   *
   * @return [R] which is the result of the [readWriteTransactionBlock]
   * @throws IllegalStateException if the token is not for the most recent
   *     update
   */
  private suspend fun <R> runIfTokenFromLastUpdate(
    token: ComputationStorageEditToken<StageT>,
    readWriteTransactionBlock: TransactionWork<R>
  ): R {
    return databaseClient.readWriteTransaction().execute { ctx ->
      val current =
        ctx.readRow("Computations", Key.of(token.localId), listOf("UpdateTime"))
          ?: error("No row for computation (${token.localId})")
      if (current.getTimestamp("UpdateTime").toEpochMilli() == token.editVersion) {
        readWriteTransactionBlock(ctx)
      } else {
        error("Failed to update, token is from older update time.")
      }
    }
  }
}
