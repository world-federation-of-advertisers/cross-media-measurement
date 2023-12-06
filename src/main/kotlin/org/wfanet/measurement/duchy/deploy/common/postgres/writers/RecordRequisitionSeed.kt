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

import com.google.protobuf.ByteString
import java.time.Clock
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.RequisitionReader
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey

/**
 * [PostgresWriter] to record the path for a new requisition seed.
 *
 * @param localId local identifier of the computation.
 * @param externalRequisitionKey [ExternalRequisitionKey] of the computation.
 * @param seed requisition seed.
 * @param clock See [Clock].
 *
 * Throws following exceptions on [execute]:
 * * [IllegalStateException] when arguments does not meet requirement
 */
class RecordRequisitionSeed(
  private val localId: Long,
  private val externalRequisitionKey: ExternalRequisitionKey,
  private val seed: ByteString,
  private val clock: Clock,
  private val computationReader: ComputationReader,
) : PostgresWriter<ComputationToken>() {
  override suspend fun TransactionScope.runTransaction(): ComputationToken {
    require(!seed.isEmpty) { "Cannot insert empty seed. $externalRequisitionKey" }
    val requisition: RequisitionReader.RequisitionResult =
      RequisitionReader().readRequisitionByExternalKey(transactionContext, externalRequisitionKey)
        ?: error("Requisition not found for external_key: $externalRequisitionKey")
    require(localId == requisition.computationId) {
      "The token doesn't match the computation owns the requisition."
    }
    val writeTime = clock.instant()
    updateComputation(localId = localId, updateTime = writeTime)
    updateRequisition(
      localComputationId = requisition.computationId,
      requisitionId = requisition.requisitionId,
      externalRequisitionId = externalRequisitionKey.externalRequisitionId,
      requisitionFingerprint = externalRequisitionKey.requisitionFingerprint,
      randomSeed = seed,
      updateTime = writeTime
    )

    return checkNotNull(
      computationReader.readComputationToken(transactionContext, externalRequisitionKey)
    )
  }
}
