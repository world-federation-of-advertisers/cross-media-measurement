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
import java.time.temporal.ChronoUnit
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages.computationStageEnumToLongValues
import org.wfanet.measurement.internal.duchy.ComputationStage

/** [PostgresWriter] for insert computation stats. */
class InsertComputationStat(
  private val computationId: Long,
  private val computationStage: ComputationStage,
  private val attempt: Int,
  private val metricName: String,
  private val metricValue: Long,
) : PostgresWriter<Unit>() {

  override suspend fun TransactionScope.runTransaction() {
    val statement =
      boundStatement(
        """
      INSERT INTO ComputationStats
        (ComputationId, ComputationStage, Attempt, CreationTime, MetricName, MetricValue)
      VALUES
        ($1, $2, $3, $4, $5, $6)
      """
      ) {
        bind("$1", computationId)
        bind("$2", computationStageEnumToLongValues(computationStage).stage)
        bind("$3", attempt)
        bind("$4", Instant.now().truncatedTo(ChronoUnit.MICROS))
        bind("$5", metricName)
        bind("$6", metricValue)
      }
    transactionContext.executeStatement(statement)
  }
}
