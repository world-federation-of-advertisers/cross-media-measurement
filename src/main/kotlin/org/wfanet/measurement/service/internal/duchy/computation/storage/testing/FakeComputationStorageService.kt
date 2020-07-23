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
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.AfterTransition
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.EndComputationReason
import org.wfanet.measurement.db.duchy.SketchAggregationStageDetails
import org.wfanet.measurement.db.duchy.SketchAggregationStages
import org.wfanet.measurement.db.duchy.testing.FakeComputationStorage
import org.wfanet.measurement.db.duchy.testing.FakeComputationsRelationalDatabase
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.ComputationType
import org.wfanet.measurement.internal.duchy.CreateComputationResponse
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationResponse
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toAdvanceComputationStageResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toClaimWorkResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toCreateComputationResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toFinishComputationResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage
import org.wfanet.measurement.service.internal.duchy.computation.storage.toGetTokenRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.toRecordOutputBlobPathResponse

typealias LegacyToken = org.wfanet.measurement.db.duchy.ComputationToken<SketchAggregationStage>

/**
 * Fake implementation of the Computation Storage Service used for testing.
 *
 * This fake is backed by a [FakeComputationStorage] which can be directly accessed to set up
 * test preconditions and check the results of running operations.
 *
 * At the moment this implementation only supports [ProtocolType.SKETCH_AGGREGATION] computations
 * but can be expanded to other protocols when needed.
 *
 * @param duchyNames list of all duchies in the computation with the first duchy being the name
 *   of the duchy running Computation Storage Service.
 */
@ExperimentalCoroutinesApi
class FakeComputationStorageService(
  duchyNames: List<String>,
  val storage: FakeComputationStorage<SketchAggregationStage, ComputationStageDetails> = FakeComputationStorage()
) :
  ComputationStorageServiceCoroutineImplBase() {

  private val fakeSketchAggregationComputationsDb = FakeComputationsRelationalDatabase(
    storage,
    SketchAggregationStages,
    SketchAggregationStageDetails(duchyNames.subList(1, duchyNames.size))
  )

  override suspend fun claimWork(request: ClaimWorkRequest): ClaimWorkResponse {
    require(request.computationType == ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1)
    val legacyToken = fakeSketchAggregationComputationsDb.claimTask(request.owner)
    return if(legacyToken != null) protoOf(legacyToken).toClaimWorkResponse()
      else ClaimWorkResponse.getDefaultInstance()
  }

  override suspend fun createComputation(request: CreateComputationRequest): CreateComputationResponse {
    require(request.computationType == ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1)
    return protoOf(
      fakeSketchAggregationComputationsDb.insertComputation(
        request.globalComputationId,
        SketchAggregationStage.CREATED
      )
    ).toCreateComputationResponse()
  }

  override suspend fun finishComputation(request: FinishComputationRequest): FinishComputationResponse {
    fakeSketchAggregationComputationsDb.endComputation(
      legacyTokenOf(request.token),
      request.endingComputationStage.liquidLegionsSketchAggregation,
      request.reason.toEndingReason()
    )
    return getComputationToken(request.token.globalComputationId.toGetTokenRequest())
      .token
      .toFinishComputationResponse()
  }

  override suspend fun getComputationToken(request: GetComputationTokenRequest): GetComputationTokenResponse =
    GetComputationTokenResponse.newBuilder().setToken(
      protoOf(
        fakeSketchAggregationComputationsDb.getToken(request.globalComputationId)
          ?: error("Token for computation $request not found")
      )).build()

  override suspend fun recordOutputBlobPath(
    request: RecordOutputBlobPathRequest
  ): RecordOutputBlobPathResponse {
    fakeSketchAggregationComputationsDb.writeOutputBlobReference(
      legacyTokenOf(request.token),
      BlobRef(request.outputBlobId, request.blobPath)
    )
    return getComputationToken(request.token.globalComputationId.toGetTokenRequest())
      .token
      .toRecordOutputBlobPathResponse()
  }

  override suspend fun advanceComputationStage(
    request: AdvanceComputationStageRequest
  ): AdvanceComputationStageResponse {
    return protoOf(
      fakeSketchAggregationComputationsDb.updateComputationStage(
        legacyTokenOf(request.token),
        request.nextComputationStage.liquidLegionsSketchAggregation,
        request.inputBlobsList,
        request.outputBlobs,
        request.afterTransition.toKotlinEnum()
      )
    ).toAdvanceComputationStageResponse()
  }

  override suspend fun getComputationIds(
    request: GetComputationIdsRequest
  ): GetComputationIdsResponse {
    val ids =
      fakeSketchAggregationComputationsDb.readGlobalComputationIds(
        request.stagesList.map { it.liquidLegionsSketchAggregation }.toSet()
      )
    return GetComputationIdsResponse.newBuilder().addAllGlobalIds(ids).build()
  }

  override suspend fun enqueueComputation(request: EnqueueComputationRequest): EnqueueComputationResponse {
    fakeSketchAggregationComputationsDb.enqueue(legacyTokenOf(request.token))
    return EnqueueComputationResponse.getDefaultInstance()
  }

  private suspend fun tokenProtoOrDefault(token: LegacyToken?): ComputationToken {
    return token?.let { protoOf(it) } ?: ComputationToken.getDefaultInstance()
  }

  private suspend fun protoOf(token: LegacyToken): ComputationToken {
    return ComputationToken.newBuilder().apply {
      localComputationId = token.localId
      globalComputationId = token.globalId
      computationStage = token.stage.toProtocolStage()
      attempt = token.attempt.toInt()
      nextDuchy = token.nextWorker
      role = token.role.toRoleInComputation()
      version = token.lastUpdateTime
      // Explicitly setting the default instance is not the same as implicitly reading it
      // Here the stageSpecificDetails is only set if it is not default.
      val details = fakeSketchAggregationComputationsDb.readStageSpecificDetails(token)
      if (details != ComputationStageDetails.getDefaultInstance())
        stageSpecificDetails = details
      storage[token.globalId]?.blobs?.map { (ref, p) ->
        ComputationStageBlobMetadata.newBuilder().apply {
          blobId = ref.id
          dependencyType = ref.dependencyType.toComputationStageBlobDependencyType()
          path = p.orEmpty()
        }.build()
      }?.let { addAllBlobs(it) }
    }.build()
  }

  private fun legacyTokenOf(token: ComputationToken): LegacyToken =
    LegacyToken(
      globalId = token.globalComputationId,
      localId = token.localComputationId,
      stage = token.computationStage.liquidLegionsSketchAggregation,
      role = token.role.toDuchyRole(),
      owner = null,
      attempt = token.attempt.toLong(),
      nextWorker = token.nextDuchy,
      lastUpdateTime = token.version
    )
}

private fun AdvanceComputationStageRequest.AfterTransition.toKotlinEnum(): AfterTransition =
  when (this) {
    AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE ->
      AfterTransition.ADD_UNCLAIMED_TO_QUEUE
    AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE ->
      AfterTransition.DO_NOT_ADD_TO_QUEUE
    AdvanceComputationStageRequest.AfterTransition.RETAIN_AND_EXTEND_LOCK ->
      AfterTransition.CONTINUE_WORKING
    else -> error("unknown $this")
  }

private fun ComputationDetails.CompletedReason.toEndingReason(): EndComputationReason =
  when (this) {
    ComputationDetails.CompletedReason.SUCCEEDED -> EndComputationReason.SUCCEEDED
    ComputationDetails.CompletedReason.FAILED -> EndComputationReason.FAILED
    ComputationDetails.CompletedReason.CANCELED -> EndComputationReason.CANCELED
    else -> error("unknown $this")
  }

// TODO: Put in common location and delete from gcpSpanner...
private fun ComputationDetails.RoleInComputation.toDuchyRole(): DuchyRole =
  when (this) {
    ComputationDetails.RoleInComputation.PRIMARY -> DuchyRole.PRIMARY
    else -> DuchyRole.SECONDARY
  }

private fun DuchyRole.toRoleInComputation(): ComputationDetails.RoleInComputation =
  when (this) {
    DuchyRole.PRIMARY -> ComputationDetails.RoleInComputation.PRIMARY
    DuchyRole.SECONDARY -> ComputationDetails.RoleInComputation.SECONDARY
  }

