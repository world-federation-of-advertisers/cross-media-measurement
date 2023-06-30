// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.postgres.writers

import com.google.protobuf.Message
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

class AdvanceComputationStage<ProtocolT, StageT, StageDT : Message>(
  private val clock: Clock,
  private val localComputationId: Long,
  private val currentStage: StageT,
  private val attempt: Long,
  private val editVersion: Long,
  private val nextStage: StageT,
  private val inputBlobPaths: List<String>,
  private val passThroughBlobPaths: List<String>,
  private val outputBlobs: Int,
  private val afterTransition: AfterTransition,
  private val nextStageDetails: StageDT,
  private val lockExtension: Duration,
  private val protocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
) : PostgresWriter<Unit>() {
  override suspend fun TransactionScope.runTransaction() {
    require(protocolStagesEnumHelper.validTransition(currentStage, nextStage)) {
      "Invalid stage transition $currentStage -> $nextStage"
    }

    checkComputationUnmodified(localComputationId, editVersion)
    val unwrittenOutputs =
      outputBlobIdToPathMap(
          localComputationId,
          protocolStagesEnumHelper.computationStageEnumToLongValues(currentStage).stage
        )
        .filterValues { it == null }
    check(unwrittenOutputs.isEmpty()) {
      """
        Cannot transition computation for $localComputationId to stage $nextStage, all outputs have not been written.
        Outputs not written for blob ids (${unwrittenOutputs.keys})
        """
        .trimIndent()
    }

    val writeTime = clock.instant()
    val nextStageLong = protocolStagesEnumHelper.computationStageEnumToLongValues(nextStage).stage
    val currentStageLong =
      protocolStagesEnumHelper.computationStageEnumToLongValues(currentStage).stage
    updateComputation(
      localId = localComputationId,
      updateTime = writeTime,
      stage = nextStageLong,
    )

    when (afterTransition) {
      // Write the NULL value to the lockOwner column to release the lock.
      AfterTransition.DO_NOT_ADD_TO_QUEUE,
      AfterTransition.ADD_UNCLAIMED_TO_QUEUE ->
        releaseComputationLock(localComputationId, writeTime)
      // Do not change the owner
      AfterTransition.CONTINUE_WORKING ->
        extendComputationLock(localComputationId, writeTime, lockExtension)
    }

    updateComputationStage(
      localId = localComputationId,
      stage = currentStageLong,
      followingStage = nextStageLong,
      endTime = writeTime
    )

    val attemptDetails =
      readAttemptDetails(localComputationId, currentStageLong, attempt)
        ?: error("No ComputationStageAttempt ($localComputationId, $currentStage, $attempt)")

    updateComputationStageAttempt(
      localId = localComputationId,
      stage = currentStageLong,
      attempt = attempt,
      endTime = writeTime,
      details =
        attemptDetails
          .toBuilder()
          .setReasonEnded(ComputationStageAttemptDetails.EndReason.SUCCEEDED)
          .build()
    )

    val startAttempt: Boolean =
      when (afterTransition) {
        // Do not start an attempt of the stage
        AfterTransition.ADD_UNCLAIMED_TO_QUEUE -> false
        // Start an attempt of the new stage
        AfterTransition.DO_NOT_ADD_TO_QUEUE,
        AfterTransition.CONTINUE_WORKING -> true
      }

    insertComputationStage(
      localId = localComputationId,
      stage = nextStageLong,
      previousStage = currentStageLong,
      creationTime = writeTime,
      details = nextStageDetails,
      // nextAttempt is the number of the current attempt of the stage plus one. Adding an Attempt
      // to the new stage while transitioning stage means that an attempt of that new stage is
      // ongoing at the end of this transaction. Meaning if for some reason there needs to be
      // another attempt of that stage in the future the next attempt will be #2. Conversely, when
      // an attempt of the new stage is not added because, attemptOfNewStageMutation is null, then
      // there is not an ongoing attempt of the stage at the end of the transaction, the next
      // attempt of stage will be the first.
      nextAttempt = if (startAttempt) 2 else 1
    )

    if (startAttempt) {
      insertComputationStageAttempt(
        localComputationId = localComputationId,
        stage = nextStageLong,
        attempt = 1,
        beginTime = writeTime,
        details = ComputationStageAttemptDetails.getDefaultInstance()
      )
    }

    inputBlobPaths.forEachIndexed { index, path ->
      insertComputationBlobReference(
        localId = localComputationId,
        stage = nextStageLong,
        blobId = index.toLong(),
        pathToBlob = path,
        dependencyType = ComputationBlobDependency.INPUT
      )
    }

    passThroughBlobPaths.forEachIndexed { index, path ->
      insertComputationBlobReference(
        localId = localComputationId,
        stage = nextStageLong,
        blobId = index.toLong(),
        pathToBlob = path,
        dependencyType = ComputationBlobDependency.INPUT
      )
    }

    (0 until outputBlobs).forEach { index ->
      insertComputationBlobReference(
        localId = localComputationId,
        stage = nextStageLong,
        blobId = index.toLong() + inputBlobPaths.size + passThroughBlobPaths.size,
        pathToBlob = null,
        dependencyType = ComputationBlobDependency.OUTPUT
      )
    }
  }

  private suspend fun TransactionScope.outputBlobIdToPathMap(
    localComputationId: Long,
    stage: Long
  ): Map<Long, String?> {
    val sql =
      boundStatement(
        """
        SELECT BlobId, PathToBlob, DependencyType
        FROM ComputationBlobReferences
        WHERE
          ComputationId = $1
        AND
          ComputationStage = $2
      """
          .trimIndent()
      ) {
        bind("$1", localComputationId)
        bind("$2", stage)
      }

    return transactionContext
      .executeQuery(sql)
      .consume { row -> row }
      .filter {
        val dependencyType = it.getProtoEnum("DependencyType", ComputationBlobDependency::forNumber)
        dependencyType == ComputationBlobDependency.OUTPUT
      }
      .toList()
      .associate { it.get<Long>("BlobId") to it.get<String?>("PathToBlob") }
  }

  private suspend fun TransactionScope.readAttemptDetails(
    localId: Long,
    stage: Long,
    attempt: Long,
  ): ComputationStageAttemptDetails? {
    val sql =
      boundStatement(
        """
      SELECT Details
      FROM ComputationStageAttempts
      WHERE
        ComputationId = $1
      AND
        ComputationStage = $2
      AND
        Attempt = $3
      """
          .trimIndent()
      ) {
        bind("$1", localId)
        bind("$2", stage)
        bind("$3", attempt)
      }
    return transactionContext
      .executeQuery(sql)
      .consume { row -> row.getProtoMessage("Details", ComputationStageAttemptDetails.parser()) }
      .firstOrNull()
  }

  private suspend fun TransactionScope.insertComputationBlobReference(
    localId: Long,
    stage: Long,
    blobId: Long,
    pathToBlob: String?,
    dependencyType: ComputationBlobDependency
  ): ComputationStageAttemptDetails? {
    val sql =
      boundStatement(
        """
        INSERT INTO ComputationBlobReferences
        (
          ComputationId,
          ComputationStage,
          BlobId,
          PathToBlob,
          DependencyType
        )
        VALUES ($1, $2, $3, $4, $5)
      """
          .trimIndent()
      ) {
        bind("$1", localId)
        bind("$2", stage)
        bind("$3", blobId)
        bind("$4", pathToBlob)
        bind("$5", dependencyType.numberAsLong)
      }
    return transactionContext
      .executeQuery(sql)
      .consume { row -> row.getProtoMessage("Details", ComputationStageAttemptDetails.parser()) }
      .firstOrNull()
  }
}

suspend fun PostgresWriter.TransactionScope.insertComputationStageAttempt(
  localComputationId: Long,
  stage: Long,
  attempt: Long,
  beginTime: Instant,
  endTime: Instant? = null,
  details: ComputationStageAttemptDetails
) {
  val sql =
    boundStatement(
      """
      INSERT INTO ComputationStageAttempts
        (
          ComputationId,
          ComputationStage,
          Attempt,
          BeginTime,
          EndTime,
          Details,
          DetailsJson
        )
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb);
      """
    ) {
      bind("$1", localComputationId)
      bind("$2", stage)
      bind("$3", attempt)
      bind("$4", beginTime)
      bind("$5", endTime)
      bind("$6", details.toByteArray())
      bind("$7", details.toJson())
    }

  transactionContext.executeStatement(sql)
}
