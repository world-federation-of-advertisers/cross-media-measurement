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
import java.time.temporal.ChronoUnit
import java.util.logging.Logger
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.db.computation.ComputationEditToken
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationStageAttemptReader
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.copy

/**
 * [PostgresWriter] to finish a computation.
 *
 * @param token the [ComputationEditToken] of the target computation.
 * @param endingStage ending stage enum.
 * @param endComputationReason the reason to end this computation.
 * @param computationDetails the details of the computation.
 * @param clock See [Clock].
 * @param protocolStagesEnumHelper See [ComputationProtocolStagesEnumHelper].
 * @param protocolStageDetailsHelper See [ComputationProtocolStageDetailsHelper].
 */
class FinishComputation<ProtocolT, StageT, ComputationDT : Message, StageDT : Message>(
  private val token: ComputationEditToken<ProtocolT, StageT>,
  private val endingStage: StageT,
  private val endComputationReason: EndComputationReason,
  private val computationDetails: ComputationDT,
  private val clock: Clock,
  private val protocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
  private val protocolStageDetailsHelper:
    ComputationProtocolStageDetailsHelper<ProtocolT, StageT, StageDT, ComputationDT>,
  private val computationReader: ComputationReader,
) : PostgresWriter<ComputationToken>() {
  override suspend fun TransactionScope.runTransaction(): ComputationToken {
    val protocol = token.protocol
    val localId = token.localId
    val editVersion = token.editVersion
    val currentStage = token.stage
    val currentAttempt = token.attempt.toLong()

    require(protocolStagesEnumHelper.validTerminalStage(protocol, endingStage)) {
      "Invalid terminal stage of computation $endingStage"
    }

    checkComputationUnmodified(localId, editVersion)

    val writeTime = clock.instant().truncatedTo(ChronoUnit.MICROS)
    val endingStageLong =
      protocolStagesEnumHelper.computationStageEnumToLongValues(endingStage).stage
    val currentStageLong =
      protocolStagesEnumHelper.computationStageEnumToLongValues(currentStage).stage
    val endingComputationDetails =
      protocolStageDetailsHelper.setEndingState(computationDetails, endComputationReason)
    val endingStageDetails = protocolStageDetailsHelper.detailsFor(endingStage, computationDetails)

    updateComputation(
      localId = localId,
      updateTime = writeTime,
      stage = endingStageLong,
      details = endingComputationDetails,
    )

    releaseComputationLock(localComputationId = localId, updateTime = writeTime)

    if (currentStage == endingStage) {
      // TODO(world-federation-of-advertisers/cross-media-measurement#774): Determine whether this
      // actually works around this bug and come up with a better fix.
      logger.warning { "Computation $localId is already in ending stage" }

      updateComputationStage(
        localId = localId,
        stage = currentStageLong,
        endTime = writeTime,
        details = endingStageDetails,
        nextAttempt = 1,
      )
    } else {
      updateComputationStage(
        localId = localId,
        stage = currentStageLong,
        endTime = writeTime,
        followingStage = endingStageLong,
      )

      insertComputationStage(
        localId = localId,
        stage = endingStageLong,
        creationTime = writeTime,
        previousStage = currentStageLong,
        nextAttempt = 1,
        details = endingStageDetails,
      )
    }

    ComputationStageAttemptReader().readUnfinishedAttempts(transactionContext, localId).collect {
      unfinished ->
      // Determine the reason the unfinished computation stage attempt is ending.
      val reason =
        if (
          unfinished.computationStage == currentStageLong && unfinished.attempt == currentAttempt
        ) {
          // The unfinished attempt is the current attempt of the current stage.
          // Set its ending reason based on the ending status of the computation as a whole. { {
          when (endComputationReason) {
            EndComputationReason.SUCCEEDED -> ComputationStageAttemptDetails.EndReason.SUCCEEDED
            EndComputationReason.FAILED -> ComputationStageAttemptDetails.EndReason.ERROR
            EndComputationReason.CANCELED -> ComputationStageAttemptDetails.EndReason.CANCELLED
          }
        } else {
          logger.warning(
            "Stage attempt with primary key " +
              "(${unfinished.computationId}, ${unfinished.computationStage}, ${unfinished.attempt}) " +
              "did not have an ending reason set when ending computation, " +
              "setting it to 'CANCELLED'."
          )
          ComputationStageAttemptDetails.EndReason.CANCELLED
        }
      updateComputationStageAttempt(
        localId = unfinished.computationId,
        stage = unfinished.computationStage,
        attempt = unfinished.attempt,
        endTime = writeTime,
        details = unfinished.details.copy { reasonEnded = reason },
      )
    }

    return checkNotNull(computationReader.readComputationToken(transactionContext, token.globalId))
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
