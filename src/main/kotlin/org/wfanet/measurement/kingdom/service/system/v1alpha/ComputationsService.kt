// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.GetComputationRequest
import org.wfanet.measurement.system.v1alpha.SetComputationResultRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsResponse

class ComputationsService(
  private val measurementsClient: MeasurementsCoroutineStub,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext
) : ComputationsCoroutineImplBase() {
  override suspend fun getComputation(request: GetComputationRequest): Computation {
    val computationKey =
      grpcRequireNotNull(ComputationKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }
    val internalRequest =
      GetMeasurementByComputationIdRequest.newBuilder()
        .apply {
          externalComputationId = apiIdToExternalId(computationKey.computationId)
          measurementView = Measurement.View.COMPUTATION
        }
        .build()
    return measurementsClient.getMeasurementByComputationId(internalRequest).toSystemComputation()
  }

  override fun streamActiveComputations(
    request: StreamActiveComputationsRequest
  ): Flow<StreamActiveComputationsResponse> {
    TODO("Not implemented yet.")
  }

  override suspend fun setComputationResult(request: SetComputationResultRequest): Computation {
    TODO("Not implemented yet.")
  }
}
