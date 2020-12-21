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

package org.wfanet.measurement.duchy.service.internal.computation

import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency.OUTPUT
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency.PASS_THROUGH
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.CreateComputationResponse
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathResponse
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsResponse

fun String.toGetTokenRequest(): GetComputationTokenRequest =
  GetComputationTokenRequest.newBuilder()
    .setGlobalComputationId(this)
    .build()

@Deprecated("Computation store should generate blob keys")
fun ComputationToken.toBlobPath(name: String) =
  "$localComputationId/${computationStage.name}_${attempt}_$name"

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

/** Extract the list of output blob paths from a [ComputationToken]. */
fun ComputationToken.outputPathList(): List<String> =
  this.blobsList.filter {
    it.dependencyType == OUTPUT || it.dependencyType == PASS_THROUGH
  }.map { it.path }

/** Creates a [ComputationStageBlobMetadata] for an input blob. */
fun newInputBlobMetadata(id: Long, key: String): ComputationStageBlobMetadata =
  ComputationStageBlobMetadata.newBuilder().apply {
    blobId = id
    dependencyType = ComputationBlobDependency.INPUT
    path = key
  }.build()

/** Creates a [ComputationStageBlobMetadata] for a pass through blob. */
fun newPassThroughBlobMetadata(id: Long, key: String): ComputationStageBlobMetadata =
  ComputationStageBlobMetadata.newBuilder().apply {
    blobId = id
    dependencyType = ComputationBlobDependency.PASS_THROUGH
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
