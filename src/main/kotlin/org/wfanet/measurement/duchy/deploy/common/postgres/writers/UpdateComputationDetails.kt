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
import org.wfanet.measurement.duchy.db.computation.ComputationEditToken
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.RequisitionReader
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.RequisitionEntry

/**
 * [PostgresWriter] to update the computationDetails.
 *
 * @param localId local identifier of the computation.
 * @param editVersion the version of the computation.
 * @param computationDetails the details of the computation.
 * @param requisitionEntries list of [RequisitionEntry].
 * @param clock See [Clock].
 *
 * Throws following exceptions on [execute]:
 * * [IllegalStateException] when arguments does not meet requirement
 */
class UpdateComputationDetails<ProtocolT, StageT, ComputationDT : Message>(
  private val token: ComputationEditToken<ProtocolT, StageT>,
  private val computationDetails: ComputationDT,
  private val requisitionEntries: List<RequisitionEntry>,
  private val clock: Clock,
  private val computationReader: ComputationReader,
) : PostgresWriter<ComputationToken>() {
  override suspend fun TransactionScope.runTransaction(): ComputationToken {
    checkComputationUnmodified(token.localId, token.editVersion)

    val writeTime = clock.instant().truncatedTo(ChronoUnit.MICROS)
    requisitionEntries.forEach {
      val requisition =
        RequisitionReader().readRequisitionByExternalKey(transactionContext, it.key)
          ?: error("Requisition not found for external_key: $it.key")
      updateRequisition(
        localComputationId = requisition.computationId,
        requisitionId = requisition.requisitionId,
        externalRequisitionId = it.key.externalRequisitionId,
        requisitionFingerprint = it.key.requisitionFingerprint,
        requisitionDetails = it.value,
        updateTime = writeTime,
      )
    }
    updateComputation(localId = token.localId, updateTime = writeTime, details = computationDetails)

    return checkNotNull(computationReader.readComputationToken(transactionContext, token.globalId))
  }
}
