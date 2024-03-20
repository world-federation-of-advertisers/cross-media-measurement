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
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.service.internal.ComputationAlreadyExistsException
import org.wfanet.measurement.duchy.service.internal.ComputationInitialStageInvalidException
import org.wfanet.measurement.duchy.service.internal.DuchyInternalException
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.RequisitionEntry

/**
 * [PostgresWriter] to insert a new computation for the global identifier.
 *
 * @param globalId global identifier of this new computation.
 * @param protocol protocol of this new computation.
 * @param initialStage stage that this new computation is in.
 * @param stageDetails stage details of type [StageDT] e.g. [ComputationStageDetails].
 * @param computationDetails computation details of type [ComputationDT] e.g. [ComputationDetails].
 * @param requisitions list of [RequisitionEntry].
 * @param clock [Clock] to get the write time to the database
 * @param computationTypeEnumHelper helper class to work with computation enums
 * @param computationProtocolStagesEnumHelper helper class to work with computation stage enums
 * @param computationProtocolStageDetailsHelper helper class to work with computation details
 *
 * Throws a subclass of [DuchyInternalException] on [execute]:
 * * [ComputationInitialStageInvalidException] when the new token is malformed
 * * [ComputationAlreadyExistsException] when there exists a computation with this
 *   globalComputationId
 */
class CreateComputation<ProtocolT : Any, ComputationDT : Message, StageT : Any, StageDT : Message>(
  private val globalId: String,
  private val protocol: ProtocolT,
  private val initialStage: StageT,
  private val stageDetails: StageDT,
  private val computationDetails: ComputationDT,
  private val requisitions: List<RequisitionEntry>,
  private val clock: Clock,
  private val computationTypeEnumHelper: ComputationTypeEnumHelper<ProtocolT>,
  private val computationProtocolStagesEnumHelper:
    ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
  private val computationProtocolStageDetailsHelper:
    ComputationProtocolStageDetailsHelper<ProtocolT, StageT, StageDT, ComputationDT>,
  private val computationReader: ComputationReader,
) : PostgresWriter<ComputationToken>() {

  override suspend fun TransactionScope.runTransaction(): ComputationToken {
    if (!computationProtocolStagesEnumHelper.validInitialStage(protocol, initialStage)) {
      throw ComputationInitialStageInvalidException(protocol.toString(), initialStage.toString())
    }

    val lockOwner: String? = null
    val localId = idGenerator.generateInternalId()
    val writeTimestamp = clock.instant().truncatedTo(ChronoUnit.MICROS)
    val initialStageLongValue =
      computationProtocolStagesEnumHelper.computationStageEnumToLongValues(initialStage).stage

    insertComputation(
      localId = localId.value,
      creationTime = writeTimestamp,
      updateTime = writeTimestamp,
      globalId = globalId,
      protocol = computationTypeEnumHelper.protocolEnumToLong(protocol),
      stage =
        computationProtocolStagesEnumHelper.computationStageEnumToLongValues(initialStage).stage,
      lockOwner = lockOwner,
      lockExpirationTime = writeTimestamp,
      details = computationDetails,
    )

    insertComputationStage(
      localId = localId.value,
      stage = initialStageLongValue,
      nextAttempt = 1,
      creationTime = writeTimestamp,
      endTime = writeTimestamp,
      previousStage = null,
      followingStage = null,
      details = stageDetails,
    )

    requisitions.map {
      insertRequisition(
        localComputationId = localId.value,
        requisitionId = requisitions.indexOf(it).toLong(),
        externalRequisitionId = it.key.externalRequisitionId,
        requisitionFingerprint = it.key.requisitionFingerprint,
        requisitionDetails = it.value,
        creationTime = writeTimestamp,
        updateTime = writeTimestamp,
      )
    }

    return checkNotNull(computationReader.readComputationToken(transactionContext, globalId))
  }
}
