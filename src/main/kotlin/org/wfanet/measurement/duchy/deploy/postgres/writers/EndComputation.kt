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

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails.setEndingState
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.deploy.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.deploy.postgres.readers.ComputationReader.Computation
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

/**
 * [PostgresWriter] delete a computation by its localComputationId.
 *
 * @param localComputationId local identifier of a computation.
 */
class EndComputation(
  private val localComputationId: Long,
  private val editVersion: Long,
  private val protocol: Long,
  private val currentAttempt: Long,
  private val currentStage: Long,
  private val endingStage: Long,
  private val endComputationReason: EndComputationReason,
  private val endComputationDetails: ByteArray,
  private val endComputationDetailsJson: String,
  private val clock: Clock,
) : PostgresWriter<Unit>() {

  companion object {
    private val reader = ComputationReader(ComputationProtocolStages)
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }

  override suspend fun TransactionScope.runTransaction() {
    val computation: Computation =
      reader.readComputation(transactionContext, localComputationId)
        ?: throw ComputationNotFoundException(localComputationId)

    validateComputationEditVersion(computation.version, editVersion)

    val writeTime = clock.instant()
    val details = computation.computationDetails
    val endingComputationDetails = setEndingState(details, endComputationReason)

    updateComputation(
      localId = localComputationId,
      updateTime = writeTime,
      stage = endingStage,
      details = endingComputationDetails.toByteArray(),
      detailsJson = endingComputationDetails.toJson()
    )

    updateComputationStage(
      localId = localComputationId,
      stage = currentStage,
      endTime = writeTime,
      followingStage = endingStage
    )

    insertComputationStage(
      localId = localComputationId,
      stage = endingStage,
      creationTime = writeTime,
      previousStage = currentStage,
      nextAttempt = 1,
      details = endComputationDetails,
      detailsJson = endComputationDetailsJson
    )

    readUnfinishedAttempts(localComputationId).collect { unfinished ->
      // Determine the reason the unfinished computation stage attempt is ending.
      val reason =
        if (
          unfinished.protocol == protocol &&
            unfinished.computationStage == currentStage &&
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
        details =
          ComputationStageAttemptDetails.parser()
            .parseFrom(unfinished.details)
            .toBuilder()
            .setReasonEnded(reason)
            .build()
            .toByteArray()
      )
    }
  }

  data class UnfinishedAttempt(
    val computationId: Long,
    val protocol: Long,
    val computationStage: Long,
    val attempt: Long,
    val details: ByteArray
  ) {
    constructor(
      row: ResultRow
    ) : this(
      computationId = row["ComputationId"],
      protocol = row["Protocol"],
      computationStage = row["ComputationStage"],
      attempt = row["Attempt"],
      details = row["Details"]
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
        ON s.ComputationId = c.ComputationsId
      WHERE s.ComputationId = $1
        AND EndTime IS NULL
      """
      ) {
        bind("$1", localComputationId)
      }
    return transactionContext.executeQuery(sql).consume(::UnfinishedAttempt)
  }

  private fun validateComputationEditVersion(
    currentVersion: Long,
    editVersion: Long,
  ) {
    if (currentVersion != editVersion) {
      val currentVersionTime = Instant.ofEpochMilli(currentVersion)
      val editVersionTime = Instant.ofEpochMilli(editVersion)
      error(
        """
          Failed to update because of editVersion mismatch.
            editVersion: $editVersion ($editVersionTime)
            Computations table's version: $currentVersion ($currentVersionTime)
            Difference: ${Duration.between(editVersionTime, currentVersionTime)}
          """
          .trimIndent()
      )
    }
  }
}
