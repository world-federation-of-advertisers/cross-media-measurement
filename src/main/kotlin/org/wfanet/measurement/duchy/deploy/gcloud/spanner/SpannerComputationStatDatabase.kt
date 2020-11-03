// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import org.wfanet.measurement.duchy.db.computationstat.ComputationStatDatabase
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

/** Metadata for `ComputationStats` table. */
private object ComputationStatsTable {
  const val TABLE_NAME = "ComputationStats"
  val columns = Columns

  object Columns {
    const val COMPUTATION_ID = "ComputationId"
    const val COMPUTATION_STAGE = "ComputationStage"
    const val ATTEMPT = "Attempt"
    const val METRIC_NAME = "MetricName"
    const val GLOBAL_COMPUTATION_ID = "GlobalComputationId"
    const val ROLE = "Role"
    const val IS_SUCCESSFUL_ATTEMPT = "IsSuccessfulAttempt"
    const val CREATE_TIME = "CreateTime"
    const val VALUE = "Value"
  }
}

/** Google Cloud Spanner implementation of [ComputationStatDatabase]. */
class SpannerComputationStatDatabase(
  private val databaseClient: AsyncDatabaseClient
) : ComputationStatDatabase {

  override suspend fun insertComputationStat(
    localId: Long,
    stage: Long,
    attempt: Long,
    metricName: String,
    globalId: String,
    role: String,
    isSuccessfulAttempt: Boolean,
    value: Long
  ) {
    val insertMutation = with(ComputationStatsTable) {
      Mutation.newInsertBuilder(TABLE_NAME)
        .set(columns.COMPUTATION_ID).to(localId)
        .set(columns.COMPUTATION_STAGE).to(stage)
        .set(columns.ATTEMPT).to(attempt)
        .set(columns.METRIC_NAME).to(metricName)
        .set(columns.GLOBAL_COMPUTATION_ID).to(globalId)
        .set(columns.ROLE).to(role)
        .set(columns.IS_SUCCESSFUL_ATTEMPT).to(isSuccessfulAttempt)
        .set(columns.CREATE_TIME).to(Value.COMMIT_TIMESTAMP)
        .set(columns.VALUE).to(value)
        .build()
    }
    databaseClient.write(insertMutation)
  }
}
