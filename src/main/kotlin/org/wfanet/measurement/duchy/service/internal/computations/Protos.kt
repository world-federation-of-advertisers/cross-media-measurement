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

package org.wfanet.measurement.duchy.service.internal.computations

import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency.INPUT
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency.OUTPUT
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency.PASS_THROUGH
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.CreateComputationResponse
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathResponse
import org.wfanet.measurement.internal.duchy.RecordRequisitionFulfillmentResponse
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsResponse
import org.wfanet.measurement.internal.duchy.config.RoleInComputation

fun String.toGetTokenRequest(): GetComputationTokenRequest =
  GetComputationTokenRequest.newBuilder().setGlobalComputationId(this).build()

/** Wraps a [ComputationToken] in an [AdvanceComputationStageResponse]. */
fun ComputationToken.toAdvanceComputationStageResponse(): AdvanceComputationStageResponse =
  AdvanceComputationStageResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in a [CreateComputationResponse]. */
fun ComputationToken.toCreateComputationResponse(): CreateComputationResponse =
  CreateComputationResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in a [ClaimWorkResponse]. */
fun ComputationToken.toClaimWorkResponse(): ClaimWorkResponse =
  ClaimWorkResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in an [UpdateComputationDetailsResponse]. */
fun ComputationToken.toUpdateComputationDetailsResponse(): UpdateComputationDetailsResponse =
  UpdateComputationDetailsResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in a [FinishComputationResponse]. */
fun ComputationToken.toFinishComputationResponse(): FinishComputationResponse =
  FinishComputationResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in a [GetComputationTokenResponse]. */
fun ComputationToken.toGetComputationTokenResponse(): GetComputationTokenResponse =
  GetComputationTokenResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in a [RecordOutputBlobPathResponse]. */
fun ComputationToken.toRecordOutputBlobPathResponse(): RecordOutputBlobPathResponse =
  RecordOutputBlobPathResponse.newBuilder().setToken(this).build()!!

/** Wraps a [ComputationToken] in a [RecordRequisitionBlobPathResponse]. */
fun ComputationToken.toRecordRequisitionBlobPathResponse(): RecordRequisitionFulfillmentResponse =
  RecordRequisitionFulfillmentResponse.newBuilder().setToken(this).build()!!

/** Extract the list of output blob paths from a [ComputationToken]. */
fun ComputationToken.outputPathList(): List<String> =
  this.blobsList
    .filter { it.dependencyType == OUTPUT || it.dependencyType == PASS_THROUGH }
    .map { it.path }

/** Extract the list of input blob paths from a [ComputationToken]. */
fun ComputationToken.inputPathList(): List<String> =
  this.blobsList.filter { it.dependencyType == INPUT }.map { it.path }

/** Extract the [RoleInComputation] from a [ComputationToken]. */
fun ComputationToken.role(): RoleInComputation {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (computationDetails.protocolCase) {
    ComputationDetails.ProtocolCase.LIQUID_LEGIONS_V2 -> computationDetails.liquidLegionsV2.role
    ComputationDetails.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 ->
      computationDetails.reachOnlyLiquidLegionsV2.role
    ComputationDetails.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE ->
      computationDetails.honestMajorityShareShuffle.role
    ComputationDetails.ProtocolCase.TRUS_TEE -> computationDetails.trusTee.role
    ComputationDetails.ProtocolCase.PROTOCOL_NOT_SET ->
      error("Invalid computation protocol to get role.")
  }
}

/** Creates a [ComputationStageBlobMetadata] for an input blob. */
fun newInputBlobMetadata(id: Long, key: String): ComputationStageBlobMetadata =
  ComputationStageBlobMetadata.newBuilder()
    .apply {
      blobId = id
      dependencyType = ComputationBlobDependency.INPUT
      path = key
    }
    .build()

/** Creates a [ComputationStageBlobMetadata] for a pass through blob. */
fun newPassThroughBlobMetadata(id: Long, key: String): ComputationStageBlobMetadata =
  ComputationStageBlobMetadata.newBuilder()
    .apply {
      blobId = id
      dependencyType = PASS_THROUGH
      path = key
    }
    .build()

/** Creates a [ComputationStageBlobMetadata] for an output blob. */
fun newOutputBlobMetadata(id: Long, key: String): ComputationStageBlobMetadata =
  ComputationStageBlobMetadata.newBuilder()
    .apply {
      blobId = id
      dependencyType = OUTPUT
      path = key
    }
    .build()

/** Creates a [ComputationStageBlobMetadata] for an output blob that doesn't have a key set. */
fun newEmptyOutputBlobMetadata(id: Long): ComputationStageBlobMetadata =
  ComputationStageBlobMetadata.newBuilder()
    .apply {
      blobId = id
      dependencyType = OUTPUT
    }
    .build()
