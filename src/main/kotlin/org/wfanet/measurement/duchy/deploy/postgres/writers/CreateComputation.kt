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

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import java.time.Instant
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.service.internal.ComputationInitialStageInvalidException
import org.wfanet.measurement.duchy.service.internal.DuchyInternalException
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionEntry

/**
 * [PostgresWriter] to inserts a new computation for the global identifier.
 *
 * @param globalId global identifier of this new computation.
 * @param protocol protocol of this new computation.
 * @param initialStage stage that this new computation is in.
 * @param stageDetails stage details of type [StageDT] e.g. [ComputationStageDetails].
 * @param computationDetails computation details of type [ComputationDT] e.g. [ComputationDetails].
 * @param requisitions list of [RequisitionEntry].
 * @param computationTypeEnumHelper helper class to work with computation enums
 * @param computationProtocolStagesEnumHelper helper class to work with computation stage enums
 * @param computationProtocolStageDetailsHelper helper class to work with computation details
 *
 * Throws a subclass of [DuchyInternalException] on [execute]:
 * * [ComputationInitialStageInvalidException] when the new token is malformed
 */
class CreateComputation<ProtocolT, ComputationDT : Message, StageT, StageDT : Message>(
  private val globalId: String,
  private val protocol: ProtocolT,
  private val initialStage: StageT,
  private val stageDetails: StageDT,
  private val computationDetails: ComputationDT,
  private val requisitions: List<RequisitionEntry>,
  computationTypeEnumHelper: ComputationTypeEnumHelper<ProtocolT>,
  computationProtocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
  computationProtocolStageDetailsHelper:
    ComputationProtocolStageDetailsHelper<ProtocolT, StageT, StageDT, ComputationDT>,
) :
  PostgresWriter<Unit>(),
  ComputationTypeEnumHelper<ProtocolT> by computationTypeEnumHelper,
  ComputationProtocolStagesEnumHelper<ProtocolT, StageT> by computationProtocolStagesEnumHelper,
  ComputationProtocolStageDetailsHelper<
    ProtocolT, StageT, StageDT, ComputationDT
  > by computationProtocolStageDetailsHelper {

  override suspend fun TransactionScope.runTransaction() {
    if (!validInitialStage(protocol, initialStage)) {
      throw ComputationInitialStageInvalidException(protocol.toString(), initialStage.toString())
    }

    val lockOwner: String? = null
    val localId = idGenerator.generateInternalId()
    val writeTimestamp = Instant.now()

    insertComputation(
      localId = localId.value,
      creationTime = writeTimestamp,
      updateTime = writeTimestamp,
      globalId = globalId,
      protocol = protocol,
      stage = initialStage,
      lockOwner = lockOwner,
      lockExpirationTime = writeTimestamp,
      details = computationDetails
    )

    insertComputationStage(
      localId = localId.value,
      stage = initialStage,
      nextAttempt = 1,
      creationTime = writeTimestamp,
      endTime = writeTimestamp,
      previousStage = null,
      followingStage = null,
      details = stageDetails
    )

    requisitions.map {
      insertRequisition(
        localComputationId = localId.value,
        requisitionId = requisitions.indexOf(it).toLong(),
        externalRequisitionId = it.key.externalRequisitionId,
        requisitionFingerprint = it.key.requisitionFingerprint,
        requisitionDetails = it.value
      )
    }
  }

  private suspend fun TransactionScope.insertComputation(
    localId: Long,
    creationTime: Instant?,
    updateTime: Instant,
    globalId: String? = null,
    protocol: ProtocolT? = null,
    stage: StageT? = null,
    lockOwner: String? = null,
    lockExpirationTime: Instant? = null,
    details: ComputationDT? = null
  ) {

    val insertComputationStatement =
      boundStatement(
        """
      INSERT INTO Computations
        (
          ComputationId,
          Protocol,
          ComputationStage,
          UpdateTime,
          GlobalComputationId,
          LockOwner,
          LockExpirationTime,
          ComputationDetails,
          ComputationDetailsJSON,
          CreationTime
        )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
      """
      ) {
        bind("$1", localId)
        bind("$2", protocol?.let { protocolEnumToLong(it) })
        bind("$3", stage?.let { computationStageEnumToLongValues(it).stage })
        bind("$4", updateTime)
        bind("$5", globalId)
        bind("$6", lockOwner)
        bind("$7", lockExpirationTime)
        bind("$8", details?.toByteArray())
        bind("$9", details?.toJson())
        bind("$10", creationTime)
      }

    transactionContext.executeStatement(insertComputationStatement)
  }

  private suspend fun TransactionScope.insertComputationStage(
    localId: Long,
    stage: StageT,
    nextAttempt: Long? = null,
    creationTime: Instant? = null,
    endTime: Instant? = null,
    previousStage: StageT? = null,
    followingStage: StageT? = null,
    details: StageDT? = null
  ) {
    val insertComputationStageStatement =
      boundStatement(
        """
      INSERT INTO ComputationStages
        (
          ComputationId,
          ComputationStage,
          CreationTime,
          NextAttempt,
          EndTime,
          PreviousStage,
          FollowingStage,
          Details,
          DetailsJSON
        )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
      """
      ) {
        bind("$1", localId)
        bind("$2", computationStageEnumToLongValues(stage).stage)
        bind("$3", creationTime)
        bind("$4", nextAttempt)
        bind("$5", endTime)
        bind("$6", previousStage?.let { computationStageEnumToLongValues(it).stage })
        bind("$7", followingStage?.let { computationStageEnumToLongValues(it).stage })
        bind("$8", details?.toByteArray())
        bind("$9", details?.toJson())
      }

    transactionContext.executeStatement(insertComputationStageStatement)
  }

  private suspend fun TransactionScope.insertRequisition(
    localComputationId: Long,
    requisitionId: Long,
    externalRequisitionId: String,
    requisitionFingerprint: ByteString,
    pathToBlob: String? = null,
    requisitionDetails: RequisitionDetails = RequisitionDetails.getDefaultInstance()
  ) {
    val insertRequisitionStatement =
      boundStatement(
        """
      INSERT INTO Requisitions
        (
          ComputationId,
          RequisitionId,
          ExternalRequisitionId,
          RequisitionFingerprint,
          PathToBlob,
          RequisitionDetails,
          RequisitionDetailsJSON
        )
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
      """
      ) {
        bind("$1", localComputationId)
        bind("$2", requisitionId)
        bind("$3", externalRequisitionId)
        bind("$4", requisitionFingerprint)
        bind("$5", pathToBlob)
        bind("$6", requisitionDetails.toByteArray())
        bind("$7", requisitionDetails.toJson())
      }

    transactionContext.executeStatement(insertRequisitionStatement)
  }
}
