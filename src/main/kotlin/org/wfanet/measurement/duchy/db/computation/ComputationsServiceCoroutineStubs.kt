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

package org.wfanet.measurement.duchy.db.computation

import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub

/**
 * Calls AdvanceComputationStage to move to a new [ComputationStage] in a consistent way.
 *
 * The assumption is this will only be called by a job that is executing the stage of a computation,
 * which will have knowledge of all the data needed as input to the next stage. Most of the time
 * [inputsToNextStage] is the list of outputs of the currently running stage.
 */
suspend fun ComputationsCoroutineStub.advanceComputationStage(
  computationToken: ComputationToken,
  inputsToNextStage: List<String> = listOf(),
  passThroughBlobs: List<String> = listOf(),
  stage: ComputationStage,
): ComputationToken {
  require(
    ComputationProtocolStageDetails.validateRoleForStage(stage, computationToken.computationDetails)
  )
  val request: AdvanceComputationStageRequest =
    AdvanceComputationStageRequest.newBuilder()
      .apply {
        token = computationToken
        nextComputationStage = stage
        addAllInputBlobs(inputsToNextStage)
        addAllPassThroughBlobs(passThroughBlobs)
        stageDetails =
          ComputationProtocolStageDetails.detailsFor(stage, computationToken.computationDetails)
        afterTransition =
          ComputationProtocolStageDetails.afterTransitionForStage(stage).toRequestProtoEnum()
        outputBlobs =
          ComputationProtocolStageDetails.outputBlobNumbersForStage(
            stage,
            computationToken.computationDetails,
          )
      }
      .build()
  return this.advanceComputationStage(request).token
}

private fun AfterTransition.toRequestProtoEnum(): AdvanceComputationStageRequest.AfterTransition {
  return when (this) {
    AfterTransition.ADD_UNCLAIMED_TO_QUEUE ->
      AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE
    AfterTransition.DO_NOT_ADD_TO_QUEUE ->
      AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE
    AfterTransition.CONTINUE_WORKING ->
      AdvanceComputationStageRequest.AfterTransition.RETAIN_AND_EXTEND_LOCK
  }
}
