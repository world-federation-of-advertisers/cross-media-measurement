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

package org.wfanet.measurement.service.internal.duchy.computation.storage.testing

import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.wfanet.measurement.db.duchy.testing.FakeComputationStorage
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.CreateComputationResponse
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationResponse
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toAdvanceComputationStageResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toClaimWorkResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toCreateComputationResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toFinishComputationResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toGetTokenRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage
import org.wfanet.measurement.service.internal.duchy.computation.storage.toRecordOutputBlobPathResponse

/**
 * Fake implementation of the Computation Storage Service used for testing.
 *
 * This fake is backed by a [FakeComputationStorage] which can be directly accessed to set up
 * test preconditions and check the results of running operations.
 *
 * At the moment this implementation only supports
 * [ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1] computations but can be expanded to other
 * protocols when needed.
 *
 * @param duchyNames list of all duchies in the computation with the first duchy being the name
 *   of the duchy running Computation Storage Service.
 */
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class FakeComputationStorageService(val storage: FakeComputationStorage) :
  ComputationStorageServiceCoroutineImplBase() {

  override suspend fun claimWork(request: ClaimWorkRequest): ClaimWorkResponse {
    require(request.computationType == ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1)
    val claimed = storage.claimTask()
    return claimed?.toClaimWorkResponse() ?: ClaimWorkResponse.getDefaultInstance()
  }

  override suspend fun createComputation(
    request: CreateComputationRequest
  ): CreateComputationResponse {
    require(request.computationType == ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1)
    val role =
      if ((request.globalComputationId % 2) == 0L) ComputationDetails.RoleInComputation.PRIMARY
      else ComputationDetails.RoleInComputation.SECONDARY
    storage.addComputation(
      request.globalComputationId,
      SketchAggregationStage.CREATED.toProtocolStage(),
      role = role,
      blobs = listOf()
    )
    return storage.getNonNull(request.globalComputationId).toCreateComputationResponse()
  }

  override suspend fun finishComputation(
    request: FinishComputationRequest
  ): FinishComputationResponse {
    storage.endComputation(request.token, request.endingComputationStage)
    return getComputationToken(request.token.globalComputationId.toGetTokenRequest())
      .token
      .toFinishComputationResponse()
  }

  override suspend fun getComputationToken(
    request: GetComputationTokenRequest
  ): GetComputationTokenResponse =
    GetComputationTokenResponse.newBuilder()
      .setToken(storage.getNonNull(request.globalComputationId))
      .build()

  override suspend fun recordOutputBlobPath(
    request: RecordOutputBlobPathRequest
  ): RecordOutputBlobPathResponse {
    storage.writeOutputBlobReference(request.token, request.outputBlobId, request.blobPath)
    return getComputationToken(request.token.globalComputationId.toGetTokenRequest())
      .token
      .toRecordOutputBlobPathResponse()
  }

  override suspend fun advanceComputationStage(
    request: AdvanceComputationStageRequest
  ): AdvanceComputationStageResponse {
    storage.advanceComputationStage(
      request.token,
      request.nextComputationStage,
      request.inputBlobsList,
      request.outputBlobs,
      request.afterTransition
    )
    return storage.getNonNull(request.token.globalComputationId).toAdvanceComputationStageResponse()
  }

  override suspend fun getComputationIds(
    request: GetComputationIdsRequest
  ): GetComputationIdsResponse {
    val ids = storage
      .filter { it.value.computationStage in request.stagesList.toSet() }
      .map { it.value.globalComputationId }.toSet()
    return GetComputationIdsResponse.newBuilder().addAllGlobalIds(ids).build()
  }

  override suspend fun enqueueComputation(
    request: EnqueueComputationRequest
  ): EnqueueComputationResponse {
    storage.enqueue(request.token.globalComputationId)
    return EnqueueComputationResponse.getDefaultInstance()
  }
}
