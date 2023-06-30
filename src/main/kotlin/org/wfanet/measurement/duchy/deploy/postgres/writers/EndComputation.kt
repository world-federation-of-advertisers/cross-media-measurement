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
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

/**
 * [PostgresWriter] delete a computation by its localComputationId.
 *
 * @param localComputationId local identifier of a computation.
 */
class EndComputation<ProtocolT, StageT, ComputationDT : Message, StageDT : Message>(
  private val localComputationId: Long,
  private val editVersion: Long,
  private val protocol: ProtocolT,
  private val currentAttempt: Long,
  private val currentStage: StageT,
  private val endingStage: StageT,
  private val endComputationReason: EndComputationReason,
  private val computationDetails: ComputationDT,
  private val clock: Clock,
  private val protocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
  private val protocolStageDetailsHelper:
    ComputationProtocolStageDetailsHelper<ProtocolT, StageT, StageDT, ComputationDT>,
) : PostgresWriter<Unit>() {

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }

  override suspend fun TransactionScope.runTransaction() {
    require(protocolStagesEnumHelper.validTerminalStage(protocol, endingStage)) {
      "Invalid terminal stage of computation $endingStage"
    }

    checkComputationUnmodified(localComputationId, editVersion)

    //    val detailsBytes: ByteArray =
    //      readComputationDetails(localComputationId)
    //        ?: throw ComputationNotFoundException(localComputationId)

    val writeTime = clock.instant()
    val endingStageLong =
      protocolStagesEnumHelper.computationStageEnumToLongValues(endingStage).stage
    val currentStageLong =
      protocolStagesEnumHelper.computationStageEnumToLongValues(currentStage).stage
    //    val details = protocolStageDetailsHelper.parseComputationDetails(detailsBytes)
    val endingComputationDetails =
      protocolStageDetailsHelper.setEndingState(computationDetails, endComputationReason)
    val endingStageDetails = protocolStageDetailsHelper.detailsFor(endingStage, computationDetails)

    updateComputation(
      localId = localComputationId,
      updateTime = writeTime,
      stage = endingStageLong,
      details = endingComputationDetails
    )

    releaseComputationLock(
      localComputationId = localComputationId,
      updateTime = writeTime,
    )

    updateComputationStage(
      localId = localComputationId,
      stage = currentStageLong,
      endTime = writeTime,
      followingStage = endingStageLong
    )

    insertComputationStage(
      localId = localComputationId,
      stage = endingStageLong,
      creationTime = writeTime,
      previousStage = currentStageLong,
      nextAttempt = 1,
      details = endingStageDetails
    )

    readUnfinishedAttempts(localComputationId).collect { unfinished ->
      // Determine the reason the unfinished computation stage attempt is ending.
      val reason =
        if (
          unfinished.protocol == protocol &&
            unfinished.computationStage == currentStageLong &&
            unfinished.attempt == currentAttempt
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
        details = unfinished.details.toBuilder().setReasonEnded(reason).build()
      )
    }
  }

  data class UnfinishedAttempt(
    val computationId: Long,
    val protocol: Long,
    val computationStage: Long,
    val attempt: Long,
    val details: ComputationStageAttemptDetails
  ) {
    constructor(
      row: ResultRow
    ) : this(
      computationId = row["ComputationId"],
      protocol = row["Protocol"],
      computationStage = row["ComputationStage"],
      attempt = row["Attempt"],
      details = row.getProtoMessage("Details", ComputationStageAttemptDetails.parser())
    )
  }

  private suspend fun TransactionScope.readUnfinishedAttempts(
    localComputationId: Long
  ): Flow<UnfinishedAttempt> {
    val sql =
      boundStatement(
        """
      SELECT s.ComputationStage, s.Attempt, s.Details, c.Protocol
      FROM ComputationStageAttempts as s
      JOIN Computations AS c
        ON s.ComputationId = c.ComputationId
      WHERE s.ComputationId = $1
        AND EndTime IS NULL
      """
      ) {
        bind("$1", localComputationId)
      }
    return transactionContext.executeQuery(sql).consume(::UnfinishedAttempt)
  }

  private suspend fun TransactionScope.readComputationDetails(
    localComputationId: Long
  ): ByteArray? {
    val sql =
      boundStatement(
        """
      SELECT ComputationDetails
      FROM Computations
      WHERE ComputationId = $1
      """
      ) {
        bind("$1", localComputationId)
      }
    return transactionContext
      .executeQuery(sql)
      .consume { row -> row.get<ByteArray>("ComputationDetails") }
      .firstOrNull()
  }
}
