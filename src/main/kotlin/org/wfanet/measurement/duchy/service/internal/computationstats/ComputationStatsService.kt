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

package org.wfanet.measurement.duchy.service.internal.computationstats

import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.duchy.db.computationstat.ComputationStatDatabase
import org.wfanet.measurement.internal.duchy.CreateComputationStatRequest
import org.wfanet.measurement.internal.duchy.CreateComputationStatResponse
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase as ComputationStatsCoroutineService

/** Implementation of `wfa.measurement.internal.duchy.ComputationStats` gRPC service. */
class ComputationStatsService(
  private val computationStatDb: ComputationStatDatabase
) : ComputationStatsCoroutineService() {

  override suspend fun createComputationStat(request: CreateComputationStatRequest): CreateComputationStatResponse {
    val globalComputationId = request.globalComputationId
    val localComputationId = request.localComputationId
    val metricName = request.metricName
    grpcRequire(globalComputationId.isNotEmpty() && localComputationId != 0L) { "Missing computation ID" }
    grpcRequire(metricName.isNotEmpty()) { "Missing Metric name" }
    computationStatDb.insertComputationStat(
      localId = localComputationId,
      stage = request.computationStage.toLong(),
      attempt = request.attempt.toLong(),
      metricName = metricName,
      globalId = globalComputationId,
      role = request.role.name,
      // TODO(yunyeng): Determine how to collect is_successful_attempt in Mill.
      isSuccessfulAttempt = true,
      value = request.metricValue
    )
    return CreateComputationStatResponse.getDefaultInstance()
  }
}
