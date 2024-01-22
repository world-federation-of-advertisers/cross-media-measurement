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

package org.wfanet.measurement.duchy.deploy.common.postgres

import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.InsertComputationStat
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.CreateComputationStatRequest
import org.wfanet.measurement.internal.duchy.CreateComputationStatResponse

/** Implementation of the ComputationStats service for Postgres database. */
class PostgresComputationStatsService(
  private val client: DatabaseClient,
  private val idGenerator: IdGenerator,
) : ComputationStatsCoroutineImplBase() {

  override suspend fun createComputationStat(
    request: CreateComputationStatRequest
  ): CreateComputationStatResponse {
    val localComputationId = request.localComputationId
    val metricName = request.metricName
    grpcRequire(localComputationId != 0L) { "Missing local_computation_id" }
    grpcRequire(metricName.isNotEmpty()) { "Missing Metric name" }

    InsertComputationStat(
        localComputationId,
        request.computationStage,
        request.attempt,
        metricName,
        request.metricValue,
      )
      .execute(client, idGenerator)
    return CreateComputationStatResponse.getDefaultInstance()
  }
}
