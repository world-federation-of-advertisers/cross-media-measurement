// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.protobuf.Message
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationStatMetric
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseTransactor
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseTransactor.ComputationEditToken
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.gcloud.common.gcloudTimestamp
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.TransactionWork
import org.wfanet.measurement.gcloud.spanner.getBytesAsByteArray
import org.wfanet.measurement.gcloud.spanner.getNullableString
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.RequisitionEntry
import org.wfanet.measurement.internal.duchy.copy

/** Implementation of [ComputationsDatabaseTransactor] using GCP Spanner Database. */
class GcpSpannerComputationsDatabaseTransactor<
  ProtocolT, StageT, StageDT : Message, ComputationDT : Message>(
  private val databaseClient: AsyncDatabaseClient,
  private val computationMutations: ComputationMutations<ProtocolT, StageT, StageDT, ComputationDT>,
  private val clock: Clock = Clock.systemUTC()
) : ComputationsDatabaseTransactor<ProtocolT, StageT, StageDT, ComputationDT> {

  private val localComputationIdGenerator: LocalComputationIdGenerator =
    GlobalBitsPlusTimeStampIdGenerator(clock)

  override suspend fun insertComputation(
    globalId: String,
    protocol: ProtocolT,
    initialStage: StageT,
    stageDetails: StageDT,
    computationDetails: ComputationDT,
    requisitions: List<RequisitionEntry>
  ) {
    require(computationMutations.validInitialStage(protocol, initialStage)) {
      "Invalid initial stage $initialStage"
    }

    val localId: Long = localComputationIdGenerator.localId(globalId)

    val writeTimestamp = clock.gcloudTimestamp()
    val computationRow =
      computationMutations.insertComputation(
        localId,
        creationTime = writeTimestamp,
        updateTime = writeTimestamp,
        globalId = globalId,
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = writeTimestamp,
        details = computationDetails,
        protocol = protocol,
        stage = initialStage
      )

    val computationStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = initialStage,
        creationTime = writeTimestamp,
        nextAttempt = 1,
        details = stageDetails
      )

    val requisitionRows =
      requisitions.map {
        computationMutations.insertRequisition(
          localComputationId = localId,
          requisitionId = requisitions.indexOf(it).toLong(),
          externalRequisitionId = it.key.externalRequisitionId,
          requisitionFingerprint = it.key.requisitionFingerprint,
          requisitionDetails = it.value
        )
      }

    databaseClient.write(listOf(computationRow, computationStageRow) + requisitionRows)
  }

  override suspend fun enqueue(token: ComputationEditToken<ProtocolT, StageT>, delaySecond: Int) {
    runIfTokenFromLastUpdate(token) { txn ->
      txn.buffer(
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

  override suspend fun claimTask(
    protocol: ProtocolT,
    ownerId: String,
    lockDuration: Duration
  ): String? {
    /** Claim a specific task represented by the results of running the above sql. */
    suspend fun claimSpecificTask(result: UnclaimedTaskQueryResult<StageT>): Boolean =
      databaseClient.readWriteTransaction().execute { txn ->
        claim(
          txn,
          result.computationId,
          result.computationStage,
          result.nextAttempt,
          result.updateTime,
          ownerId,
          lockDuration
        )
      }
    return UnclaimedTasksQuery(
        computationMutations.protocolEnumToLong(protocol),
        computationMutations::longValuesToComputationStageEnum,
        clock.gcloudTimestamp()
      )
      .execute(databaseClient)
      // First the possible tasks to claim are selected from the computations table, then for each
      // item in the list we try to claim the lock in a transaction which will only succeed if the
      // lock is still available. This pattern means only the item which is being updated
      // would need to be locked and not every possible computation that can be worked on.
      .filter { claimSpecificTask(it) }
      // If the value is null, no tasks were claimed.
      .firstOrNull()
      ?.globalId
  }

  /**
   * Tries to claim a specific computation for an owner, returning the result of the attempt. If a
   * lock is acquired a new row is written to the ComputationStageAttempts table.
   */
  private suspend fun claim(
    txn: AsyncDatabaseClient.TransactionContext,
    computationId: Long,
    stage: StageT,
    nextAttempt: Long,
    lastUpdate: Timestamp,
    ownerId: String,
    lockDuration: Duration
  ): Boolean {
    val currentLockOwnerStruct =
      txn.readRow("Computations", Key.of(computationId), listOf("LockOwner", "UpdateTime"))
        ?: error("Failed to claim computation $computationId. It does not exist.")
    // Verify that the row hasn't been updated since the previous, non-transactional read.
    // If it has been updated since that time the lock should not be acquired.
    if (currentLockOwnerStruct.getTimestamp("UpdateTime") != lastUpdate) return false

    // TODO(sanjayvas): Determine whether we can use commit timestamp via
    // spanner.commit_timestamp() in mutation.
    val writeTime: Instant = clock.instant()
    val writeTimestamp = writeTime.toGcloudTimestamp()
    txn.buffer(setLockMutation(computationId, ownerId, writeTime, lockDuration))
    // Create a new attempt of the stage for the nextAttempt.
    txn.buffer(
      computationMutations.insertComputationStageAttempt(
        computationId,
        stage,
        nextAttempt,
        beginTime = writeTimestamp,
        details = ComputationStageAttemptDetails.getDefaultInstance()
      )
    )
    // And increment NextAttempt column of the computation stage.
    txn.buffer(
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
        txn
          .readRow(
            "ComputationStageAttempts",
            Key.of(computationId, stage.toLongStage(), currentAttempt),
            listOf("Details")
          )
          ?.getProtoMessage("Details", ComputationStageAttemptDetails.parser())
          ?: error("Failed to claim computation $computationId. It does not exist.")
      // If the computation was locked, but that lock was expired we need to finish off the
      // current attempt of the stage.
      txn.buffer(
        computationMutations.updateComputationStageAttempt(
          localId = computationId,
          stage = stage,
          attempt = currentAttempt,
          endTime = writeTimestamp,
          details =
            details.copy { reasonEnded = ComputationStageAttemptDetails.EndReason.LOCK_OVERWRITTEN }
        )
      )
    }
    // The lock was acquired.
    return true
  }

  private fun setLockMutation(
    computationId: Long,
    ownerId: String,
    writeTime: Instant,
    lockDuration: Duration
  ): Mutation {
    return computationMutations.updateComputation(
      computationId,
      writeTime.toGcloudTimestamp(),
      lockOwner = ownerId,
      lockExpirationTime = writeTime.plus(lockDuration).toGcloudTimestamp()
    )
  }

  override suspend fun updateComputationStage(
    token: ComputationEditToken<ProtocolT, StageT>,
    nextStage: StageT,
    inputBlobPaths: List<String>,
    passThroughBlobPaths: List<String>,
    outputBlobs: Int,
    afterTransition: AfterTransition,
    nextStageDetails: StageDT,
    lockExtension: Duration?
  ) {
    require(computationMutations.validTransition(token.stage, nextStage)) {
      "Invalid stage transition ${token.stage} -> $nextStage"
    }

    runIfTokenFromLastUpdate(token) { txn ->
      val unwrittenOutputs =
        outputBlobIdToPathMap(txn, token.localId, token.stage).filterValues { it == null }
      check(unwrittenOutputs.isEmpty()) {
        """
        Cannot transition computation for $token to stage $nextStage, all outputs have not been written.
        Outputs not written for blob ids (${unwrittenOutputs.keys})
        """
          .trimIndent()
      }
      val writeTime = clock.instant()

      txn.buffer(
        mutationsToChangeStages(
          txn,
          token,
          nextStage,
          writeTime,
          afterTransition,
          nextStageDetails,
          lockExtension
        )
      )

      txn.buffer(
        mutationsToMakeBlobRefsForNewStage(
          token.localId,
          nextStage,
          inputBlobPaths,
          passThroughBlobPaths,
          outputBlobs
        )
      )
    }
  }

  override suspend fun endComputation(
    token: ComputationEditToken<ProtocolT, StageT>,
    endingStage: StageT,
    endComputationReason: EndComputationReason,
    computationDetails: ComputationDT
  ) {
    require(computationMutations.validTerminalStage(token.protocol, endingStage)) {
      "Invalid terminal stage of computation $endingStage"
    }
    runIfTokenFromLastUpdate(token) { txn ->
      val writeTime = clock.gcloudTimestamp()
      val detailsBytes =
        txn
          .readRow("Computations", Key.of(token.localId), listOf("ComputationDetails"))
          ?.getBytesAsByteArray("ComputationDetails")
          ?: error("Computation missing $token")
      val details = computationMutations.parseComputationDetails(detailsBytes)
      txn.buffer(
        computationMutations.updateComputation(
          localId = token.localId,
          updateTime = writeTime,
          stage = endingStage,
          lockOwner = WRITE_NULL_STRING,
          lockExpirationTime = WRITE_NULL_TIMESTAMP,
          // Add a reason why the computation ended to the details section.
          details = computationMutations.setEndingState(details, endComputationReason)
        )
      )
      txn.buffer(
        computationMutations.updateComputationStage(
          localId = token.localId,
          stage = token.stage,
          endTime = writeTime,
          followingStage = endingStage
        )
      )
      txn.buffer(
        computationMutations.insertComputationStage(
          localId = token.localId,
          stage = endingStage,
          creationTime = writeTime,
          previousStage = token.stage,
          nextAttempt = 1,
          details = computationMutations.detailsFor(endingStage, computationDetails)
        )
      )
      UnfinishedAttemptQuery(computationMutations::longValuesToComputationStageEnum, token.localId)
        .execute(txn)
        .collect { unfinished ->
          // Determine the reason the unfinished computation stage attempt is ending.
          val reason =
            if (unfinished.stage == token.stage && unfinished.attempt == token.attempt.toLong()) {
              // The unfinished attempt is the current attempt of the of the current stage.
              // Set its ending reason based on the ending status of the computation as a whole. { {
              when (endComputationReason) {
                EndComputationReason.SUCCEEDED -> ComputationStageAttemptDetails.EndReason.SUCCEEDED
                EndComputationReason.FAILED -> ComputationStageAttemptDetails.EndReason.ERROR
                EndComputationReason.CANCELED -> ComputationStageAttemptDetails.EndReason.CANCELLED
              }
            } else {
              logger.warning(
                "Stage attempt with primary key " +
                  "(${unfinished.computationId}, ${unfinished.stage}, ${unfinished.attempt}) " +
                  "did not have an ending reason set when ending computation, " +
                  "setting it to 'CANCELLED'."
              )
              ComputationStageAttemptDetails.EndReason.CANCELLED
            }
          txn.buffer(
            computationMutations.updateComputationStageAttempt(
              localId = unfinished.computationId,
              stage = unfinished.stage,
              attempt = unfinished.attempt,
              endTime = writeTime,
              details = unfinished.details.toBuilder().setReasonEnded(reason).build()
            )
          )
        }
    }
  }

  override suspend fun updateComputationDetails(
    token: ComputationEditToken<ProtocolT, StageT>,
    computationDetails: ComputationDT,
    requisitions: List<RequisitionEntry>
  ) {
    runIfTokenFromLastUpdate(token) { txn ->
      requisitions.forEach {
        val row =
          txn.readRowUsingIndex(
            "Requisitions",
            "RequisitionsByExternalId",
            Key.of(it.key.externalRequisitionId, it.key.requisitionFingerprint.toGcloudByteArray()),
            listOf("ComputationId", "RequisitionId")
          )
            ?: error("No Computation found row for this requisition: ${it.key}")
        txn.buffer(
          computationMutations.updateRequisition(
            localComputationId = row.getLong("ComputationId"),
            requisitionId = row.getLong("RequisitionId"),
            externalRequisitionId = it.key.externalRequisitionId,
            requisitionFingerprint = it.key.requisitionFingerprint,
            requisitionDetails = it.value
          )
        )
      }
      val writeTime = clock.gcloudTimestamp()
      txn.buffer(
        computationMutations.updateComputation(
          localId = token.localId,
          updateTime = writeTime,
          details = computationDetails
        )
      )
    }
  }

  private suspend fun mutationsToChangeStages(
    ctx: AsyncDatabaseClient.TransactionContext,
    token: ComputationEditToken<ProtocolT, StageT>,
    newStage: StageT,
    writeTime: Instant,
    afterTransition: AfterTransition,
    nextStageDetails: StageDT,
    lockExtension: Duration?
  ): List<Mutation> {
    val writeTimestamp = writeTime.toGcloudTimestamp()
    val mutations = arrayListOf<Mutation>()

    mutations.add(
      computationMutations.updateComputation(
        token.localId,
        writeTimestamp,
        stage = newStage,
        lockOwner =
          when (afterTransition) {
            // Write the NULL value to the lockOwner column to release the lock.
            AfterTransition.DO_NOT_ADD_TO_QUEUE,
            AfterTransition.ADD_UNCLAIMED_TO_QUEUE -> WRITE_NULL_STRING
            // Do not change the owner
            AfterTransition.CONTINUE_WORKING -> null
          },
        lockExpirationTime =
          when (afterTransition) {
            // Null LockExpirationTime values will not be claimed from the work queue.
            AfterTransition.DO_NOT_ADD_TO_QUEUE -> WRITE_NULL_TIMESTAMP
            // The computation is ready for processing by some worker right away.
            AfterTransition.ADD_UNCLAIMED_TO_QUEUE -> writeTimestamp
            // The computation lock will expire sometime in the future.
            AfterTransition.CONTINUE_WORKING ->
              writeTime.plus(requireNotNull(lockExtension)).toGcloudTimestamp()
          }
      )
    )

    mutations.add(
      computationMutations.updateComputationStage(
        localId = token.localId,
        stage = token.stage,
        followingStage = newStage,
        endTime = writeTimestamp
      )
    )

    val attemptDetails =
      ctx
        .readRow(
          "ComputationStageAttempts",
          Key.of(token.localId, token.stage.toLongStage(), token.attempt),
          listOf("Details")
        )
        ?.getProtoMessage("Details", ComputationStageAttemptDetails.parser())
        ?: error("No ComputationStageAttempt (${token.localId}, $newStage, ${token.attempt})")
    mutations.add(
      computationMutations.updateComputationStageAttempt(
        localId = token.localId,
        stage = token.stage,
        attempt = token.attempt.toLong(),
        endTime = writeTimestamp,
        details =
          attemptDetails
            .toBuilder()
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
        AfterTransition.DO_NOT_ADD_TO_QUEUE,
        AfterTransition.CONTINUE_WORKING ->
          computationMutations.insertComputationStageAttempt(
            localId = token.localId,
            stage = newStage,
            attempt = 1,
            beginTime = writeTimestamp,
            details = ComputationStageAttemptDetails.getDefaultInstance()
          )
      }

    mutations.add(
      computationMutations.insertComputationStage(
        localId = token.localId,
        stage = newStage,
        previousStage = token.stage,
        creationTime = writeTimestamp,
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
    passThroughBlobRefs: List<String>,
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

    passThroughBlobRefs.mapIndexedTo(mutations) { index, path ->
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = stage,
        blobId = index.toLong() + blobInputRefs.size,
        pathToBlob = path,
        dependencyType = ComputationBlobDependency.PASS_THROUGH
      )
    }

    (0 until outputBlobs).mapTo(mutations) { index ->
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = stage,
        blobId = index.toLong() + blobInputRefs.size + passThroughBlobRefs.size,
        dependencyType = ComputationBlobDependency.OUTPUT
      )
    }
    return mutations
  }

  private suspend fun outputBlobIdToPathMap(
    txn: AsyncDatabaseClient.TransactionContext,
    localId: Long,
    stage: StageT
  ): Map<Long, String?> {
    return txn
      .read(
        "ComputationBlobReferences",
        KeySet.prefixRange(Key.of(localId, stage.toLongStage())),
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
    token: ComputationEditToken<ProtocolT, StageT>,
    blobRef: BlobRef
  ) {
    require(blobRef.key.isNotBlank()) { "Cannot insert blank path to blob. $blobRef" }
    runIfTokenFromLastUpdate(token) { txn ->
      val type =
        txn
          .readRow(
            "ComputationBlobReferences",
            Key.of(token.localId, token.stage.toLongStage(), blobRef.idInRelationalDatabase),
            listOf("DependencyType")
          )
          ?.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber)
          ?: error(
            "No ComputationBlobReferences row for " +
              "(${token.localId}, ${token.stage}, ${blobRef.idInRelationalDatabase})"
          )
      require(type == ComputationBlobDependency.OUTPUT) { "Cannot write to $type blob" }
      txn.buffer(
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

  override suspend fun writeRequisitionBlobPath(
    token: ComputationEditToken<ProtocolT, StageT>,
    externalRequisitionKey: ExternalRequisitionKey,
    pathToBlob: String
  ) {
    require(pathToBlob.isNotBlank()) { "Cannot insert blank path to blob. $externalRequisitionKey" }
    databaseClient.readWriteTransaction().execute { txn ->
      val row =
        txn.readRowUsingIndex(
          "Requisitions",
          "RequisitionsByExternalId",
          Key.of(
            externalRequisitionKey.externalRequisitionId,
            externalRequisitionKey.requisitionFingerprint.toGcloudByteArray()
          ),
          listOf("ComputationId", "RequisitionId")
        )
          ?: error("No Computation found row for this requisition: $externalRequisitionKey")
      val localComputationId = row.getLong("ComputationId")
      val requisitionId = row.getLong("RequisitionId")
      require(localComputationId == token.localId) {
        "The token doesn't match the computation owns the requisition."
      }
      txn.buffer(
        listOf(
          computationMutations.updateComputation(
            localId = localComputationId,
            updateTime = clock.gcloudTimestamp()
          ),
          computationMutations.updateRequisition(
            localComputationId = localComputationId,
            requisitionId = requisitionId,
            externalRequisitionId = externalRequisitionKey.externalRequisitionId,
            requisitionFingerprint = externalRequisitionKey.requisitionFingerprint,
            pathToBlob = pathToBlob
          )
        )
      )
    }
  }

  override suspend fun insertComputationStat(
    localId: Long,
    stage: StageT,
    attempt: Long,
    metric: ComputationStatMetric
  ) {
    databaseClient.write(
      computationMutations.insertComputationStat(
        localId = localId,
        stage = stage,
        attempt = attempt,
        metricName = metric.name,
        metricValue = metric.value
      )
    )
  }

  override suspend fun deleteComputation(localId: Long) {
    val mutation = Mutation.delete("Computations", Key.of(localId))
    databaseClient.write(mutation)
  }

  /**
   * Runs [readWriteTransactionBlock] if the ComputationToken is from the most recent update to a
   * computation. This is done atomically with in read/write transaction.
   *
   * @return [R] which is the result of the [readWriteTransactionBlock]
   * @throws IllegalStateException if the token is not for the most recent update
   */
  private suspend fun <R> runIfTokenFromLastUpdate(
    token: ComputationEditToken<ProtocolT, StageT>,
    readWriteTransactionBlock: TransactionWork<R>
  ): R {
    return databaseClient.readWriteTransaction().execute { txn ->
      val current =
        txn.readRow("Computations", Key.of(token.localId), listOf("UpdateTime"))
          ?: error("No row for computation (${token.localId})")
      val updateTime = current.getTimestamp("UpdateTime").toInstant()
      val updateTimeMillis = updateTime.toEpochMilli()
      val tokenTimeMillis = token.editVersion
      if (updateTimeMillis == tokenTimeMillis) {
        readWriteTransactionBlock(txn)
      } else {
        val tokenTime = Instant.ofEpochMilli(tokenTimeMillis)
        error(
          """
          Failed to update because of editVersion mismatch.
            Token's editVersion: $tokenTimeMillis ($tokenTime)
            Computations table's UpdateTime: $updateTimeMillis ($updateTime)
            Difference: ${Duration.between(tokenTime, updateTime)}
          """
            .trimIndent()
        )
      }
    }
  }

  // Converts the [StageT] to a [Long] value used for the stage column in spanner.
  private fun StageT.toLongStage(): Long =
    computationMutations.computationStageEnumToLongValues(this).stage

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
