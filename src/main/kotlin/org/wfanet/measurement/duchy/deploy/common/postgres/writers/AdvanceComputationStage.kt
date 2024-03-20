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

package org.wfanet.measurement.duchy.deploy.common.postgres.writers

import com.google.protobuf.Message
import java.time.Clock
import java.time.Duration
import java.time.temporal.ChronoUnit
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.ComputationEditToken
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationBlobReferenceReader
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationStageAttemptReader
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.copy

/**
 * [PostgresWriter] to advance a computation to next stage.
 *
 * @param token the [ComputationEditToken] of the target computation.
 * @param nextStage next stage enum.
 * @param nextStageDetails stageDetails of the next stage.
 * @param inputBlobPaths list of inputBlobPaths.
 * @param passThroughBlobPaths list of passThroughBlobPaths.
 * @param outputBlobs number of outputBlobs
 * @param afterTransition See [AfterTransition]
 * @param lockExtension the duration to extend the expiration time.
 * @param clock See [Clock].
 * @param protocolStagesEnumHelper See [ComputationProtocolStagesEnumHelper].
 */
class AdvanceComputationStage<ProtocolT, StageT, StageDT : Message>(
  private val token: ComputationEditToken<ProtocolT, StageT>,
  private val nextStage: StageT,
  private val nextStageDetails: StageDT,
  private val inputBlobPaths: List<String>,
  private val passThroughBlobPaths: List<String>,
  private val outputBlobs: Int,
  private val afterTransition: AfterTransition,
  private val lockExtension: Duration,
  private val clock: Clock,
  private val protocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
  private val computationReader: ComputationReader,
) : PostgresWriter<ComputationToken>() {
  override suspend fun TransactionScope.runTransaction(): ComputationToken {
    val currentStage = token.stage
    val localId = token.localId
    val editVersion = token.editVersion
    val attempt = token.attempt.toLong()

    require(protocolStagesEnumHelper.validTransition(currentStage, nextStage)) {
      "Invalid stage transition $currentStage -> $nextStage"
    }
    checkComputationUnmodified(localId, editVersion)

    val unwrittenOutputs =
      ComputationBlobReferenceReader()
        .readBlobIdToPathMap(
          transactionContext,
          localId,
          protocolStagesEnumHelper.computationStageEnumToLongValues(currentStage).stage,
          ComputationBlobDependency.OUTPUT.numberAsLong,
        )
        .filterValues { it == null }
    check(unwrittenOutputs.isEmpty()) {
      """
        Cannot transition computation for $localId to stage $nextStage, all outputs have not been written.
        Outputs not written for blob ids (${unwrittenOutputs.keys})
        """
        .trimIndent()
    }

    val writeTime = clock.instant().truncatedTo(ChronoUnit.MICROS)
    val nextStageLong = protocolStagesEnumHelper.computationStageEnumToLongValues(nextStage).stage
    val currentStageLong =
      protocolStagesEnumHelper.computationStageEnumToLongValues(currentStage).stage
    updateComputation(localId = localId, updateTime = writeTime, stage = nextStageLong)

    when (afterTransition) {
      AfterTransition.DO_NOT_ADD_TO_QUEUE -> releaseComputationLock(localId, writeTime)
      AfterTransition.ADD_UNCLAIMED_TO_QUEUE ->
        enqueueComputation(localId, writeTime, delaySeconds = 0)
      // Do not change the owner
      AfterTransition.CONTINUE_WORKING ->
        extendComputationLock(localId, writeTime, writeTime.plus(lockExtension))
    }

    updateComputationStage(
      localId = localId,
      stage = currentStageLong,
      followingStage = nextStageLong,
      endTime = writeTime,
    )

    val attemptDetails =
      ComputationStageAttemptReader()
        .readComputationStageDetails(transactionContext, localId, currentStageLong, attempt)
        ?: error("No ComputationStageAttempt ($localId, $currentStage, $attempt)")

    updateComputationStageAttempt(
      localId = localId,
      stage = currentStageLong,
      attempt = attempt,
      endTime = writeTime,
      details =
        attemptDetails.copy { reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED },
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
      localId = localId,
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
      nextAttempt = if (startAttempt) 2 else 1,
    )

    if (startAttempt) {
      insertComputationStageAttempt(
        localComputationId = localId,
        stage = nextStageLong,
        attempt = 1,
        beginTime = writeTime,
        details = ComputationStageAttemptDetails.getDefaultInstance(),
      )
    }

    inputBlobPaths.forEachIndexed { index, path ->
      insertComputationBlobReference(
        localId = localId,
        stage = nextStageLong,
        blobId = index.toLong(),
        pathToBlob = path,
        dependencyType = ComputationBlobDependency.INPUT,
      )
    }

    passThroughBlobPaths.forEachIndexed { index, path ->
      insertComputationBlobReference(
        localId = localId,
        stage = nextStageLong,
        blobId = index.toLong() + inputBlobPaths.size,
        pathToBlob = path,
        dependencyType = ComputationBlobDependency.PASS_THROUGH,
      )
    }

    (0 until outputBlobs).forEach { index ->
      insertComputationBlobReference(
        localId = localId,
        stage = nextStageLong,
        blobId = index.toLong() + inputBlobPaths.size + passThroughBlobPaths.size,
        pathToBlob = null,
        dependencyType = ComputationBlobDependency.OUTPUT,
      )
    }

    return checkNotNull(computationReader.readComputationToken(transactionContext, token.globalId))
  }
}
