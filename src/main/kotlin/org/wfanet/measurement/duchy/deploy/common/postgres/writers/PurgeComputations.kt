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

import java.time.Instant
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationReader
import org.wfanet.measurement.internal.duchy.ComputationStage

class PurgeComputations(
  private val stages: List<ComputationStage>,
  private val updatedBefore: Instant,
  private val force: Boolean,
  private val computationReader: ComputationReader,
) : PostgresWriter<PurgeComputations.PurgeResult>() {
  data class PurgeResult(val purgeCount: Int, val purgeSamples: Set<String> = emptySet())

  override suspend fun TransactionScope.runTransaction(): PurgeResult {
    var deleted = 0
    val globalIds: Set<String> =
      computationReader.readGlobalComputationIds(transactionContext, stages, updatedBefore)
    if (!force) {
      return PurgeResult(globalIds.size, globalIds)
    }
    for (globalId in globalIds) {
      val numOfRowsDeleted = deleteComputationByGlobalId(globalId)
      deleted += numOfRowsDeleted.toInt()
    }
    return PurgeResult(deleted)
  }
}
