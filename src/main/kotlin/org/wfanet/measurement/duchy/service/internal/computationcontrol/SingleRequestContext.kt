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

package org.wfanet.measurement.duchy.service.internal.computationcontrol

import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum

/**
 * Functions for controlling the flow of a computation in the context of a single
 * [AdvanceComputationRequest].
 */
interface SingleRequestContext {
  /** The request received by the AsyncComputationControl service. */
  val request: AdvanceComputationRequest

  /** The type of computation. */
  val computationType: ComputationTypeEnum.ComputationType

  /** Gets the [ComputationStageBlobMetadata] output from a token for the current context. */
  fun outputBlob(token: ComputationToken): ComputationStageBlobMetadata

  /** Gets the next stage for a computation or fail the RPC. */
  fun nextStage(computationDetails: ComputationDetails, stage: ComputationStage): ComputationStage
}
