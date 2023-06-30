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
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.deploy.postgres.readers.RequisitionReader
import org.wfanet.measurement.internal.duchy.RequisitionEntry

class UpdateComputationDetails<ComputationDT : Message>(
  private val clock: Clock,
  private val localId: Long,
  private val editVersion: Long,
  private val computationDetails: ComputationDT,
  private val requisitionEntries: List<RequisitionEntry>
) : PostgresWriter<Unit>() {
  override suspend fun TransactionScope.runTransaction() {
    checkComputationUnmodified(localId, editVersion)

    val writeTime = clock.instant()
    requisitionEntries.forEach {
      val requisition =
        RequisitionReader().readRequisitionByExternalKey(transactionContext, it.key)
          ?: error("not found")
      updateRequisition(
        localComputationId = requisition.computationId,
        requisitionId = requisition.requisitionId,
        externalRequisitionId = it.key.externalRequisitionId,
        requisitionFingerprint = it.key.requisitionFingerprint,
        requisitionDetails = it.value,
        updateTime = writeTime
      )
    }
    updateComputation(localId = localId, updateTime = writeTime, details = computationDetails)
  }
}
