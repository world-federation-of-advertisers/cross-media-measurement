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

package org.wfanet.measurement.service.internal.duchy.computation.storage

import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.CreateComputationResponse
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathResponse

fun LiquidLegionsSketchAggregationStage.toProtocolStage(): ComputationStage =
  ComputationStage.newBuilder().setLiquidLegionsSketchAggregation(this).build()

fun Long.toGetTokenRequest(
  computationType: ComputationType = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
): GetComputationTokenRequest =
  GetComputationTokenRequest.newBuilder()
    .setComputationType(computationType)
    .setGlobalComputationId(this)
    .build()

fun ComputationToken.toBlobPath(name: String) =
  "$localComputationId/${computationStage.toName()}_${attempt}_$name"

private fun ComputationStage.toName(): String =
  when (stageCase) {
    ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION ->
      liquidLegionsSketchAggregation.name
    else -> error("Unknown computation type $this")
  }

/** Wraps a [ComputationToken] in an [AdvanceComputationStageResponse]. */
fun ComputationToken.toAdvanceComputationStageResponse(): AdvanceComputationStageResponse =
  AdvanceComputationStageResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in an [CreateComputationResponse]. */
fun ComputationToken.toCreateComputationResponse(): CreateComputationResponse =
  CreateComputationResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in an [ClaimWorkResponse]. */
fun ComputationToken.toClaimWorkResponse(): ClaimWorkResponse =
  ClaimWorkResponse.newBuilder().addToken(this).build()!!

/** Wraps a [ComputationToken] in an [FinishComputationResponse]. */
fun ComputationToken.toFinishComputationResponse(): FinishComputationResponse =
  FinishComputationResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in an [GetComputationTokenResponse]. */
fun ComputationToken.toGetComputationTokenResponse(): GetComputationTokenResponse =
  GetComputationTokenResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in an [RecordOutputBlobPathResponse]. */
fun ComputationToken.toRecordOutputBlobPathResponse(): RecordOutputBlobPathResponse =
  RecordOutputBlobPathResponse.newBuilder().setToken(this).build()!!

/** Creates a [ComputationStageBlobMetadata] for an input blob. */
fun newInputBlobMetadata(id: Long, key: String): ComputationStageBlobMetadata =
  ComputationStageBlobMetadata.newBuilder().apply {
    blobId = id
    dependencyType = ComputationBlobDependency.INPUT
    path = key
  }.build()

/** Creates a [ComputationStageBlobMetadata] for an output blob. */
fun newOutputBlobMetadata(id: Long, key: String): ComputationStageBlobMetadata =
  ComputationStageBlobMetadata.newBuilder().apply {
    blobId = id
    dependencyType = ComputationBlobDependency.OUTPUT
    path = key
  }.build()

/** Creates a [ComputationStageBlobMetadata] for an output blob that doesn't have a key set. */
fun newEmptyOutputBlobMetadata(id: Long): ComputationStageBlobMetadata =
  ComputationStageBlobMetadata.newBuilder().apply {
    blobId = id
    dependencyType = ComputationBlobDependency.OUTPUT
  }.build()
