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

package org.wfanet.measurement.duchy.db.computation

/** Deals with stage specific details for MPC protocols. */
interface ComputationProtocolStageDetailsHelper<
  ProtocolT, StageT, StageDetailsT, ComputationDetailsT> {
  /** Creates the stage specific details for a given computation stage. */
  fun detailsFor(stage: StageT): StageDetailsT

  /** Converts bytes into a [StageDetailsT] based on [protocol] .*/
  fun parseDetails(protocol: ProtocolT, bytes: ByteArray): StageDetailsT

  /** True if a computation with [ComputationDetailsT] can be at [StageT]. */
  fun validateRoleForStage(stage: StageT, computationDetails: ComputationDetailsT): Boolean

  /** Returns the [AfterTransition] after a computation transits to [StageT]. */
  fun afterTransitionForStage(stage: StageT): AfterTransition

  /** Returns the expected number of output blobs of [StageT]. */
  fun outputBlobNumbersForStage(stage: StageT): Int

  /** Sets [EndComputationReason] to the [ComputationDetailsT]. */
  fun setEndingState(details: ComputationDetailsT, reason: EndComputationReason):
    ComputationDetailsT

  /** Parses the [ComputationDetailsT] proto from [ByteArray]. */
  fun parseComputationDetails(bytes: ByteArray): ComputationDetailsT
}
