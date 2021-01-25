// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.service.internal.computationstats

import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.duchy.db.computation.ComputationStatMetric
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase as ComputationStatsCoroutineService
import org.wfanet.measurement.internal.duchy.CreateComputationStatRequest
import org.wfanet.measurement.internal.duchy.CreateComputationStatResponse

/** Implementation of `wfa.measurement.internal.duchy.ComputationStats` gRPC service. */
class ComputationStatsService(
  private val computationsDatabase: ComputationsDatabase
) : ComputationStatsCoroutineService() {

  override suspend fun createComputationStat(
    request: CreateComputationStatRequest
  ): CreateComputationStatResponse {
    val localComputationId = request.localComputationId
    val metricName = request.metricName
    grpcRequire(localComputationId != 0L) {
      "Missing computation ID"
    }
    grpcRequire(metricName.isNotEmpty()) { "Missing Metric name" }
    computationsDatabase.insertComputationStat(
      localId = localComputationId,
      stage = request.computationStage,
      attempt = request.attempt.toLong(),
      metric = ComputationStatMetric(metricName, request.metricValue)
    )
    return CreateComputationStatResponse.getDefaultInstance()
  }
}
