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

package org.wfanet.measurement.duchy.service.system.v1alpha

import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest.Header.ProtocolCase
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV1
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2

/** True if the protocol specified in the header is asynchronous. */
fun AdvanceComputationRequest.Header.isForAsyncComputation(): Boolean =
  when (protocolCase) {
    ProtocolCase.LIQUID_LEGIONS_V1,
    ProtocolCase.LIQUID_LEGIONS_V2 -> true
    else -> failGrpc { "Unknown protocol $protocolCase" }
  }

/** Returns the [ComputationStage] which expects the input described in the header. */
fun AdvanceComputationRequest.Header.stageExpectingInput(): ComputationStage =
  when (protocolCase) {
    ProtocolCase.LIQUID_LEGIONS_V1 -> liquidLegionsV1.stageExpectingInput()
    ProtocolCase.LIQUID_LEGIONS_V2 -> liquidLegionsV2.stageExpectingInput()
    else -> failGrpc { "Unknown protocol $protocolCase" }
  }

private fun LiquidLegionsV1.stageExpectingInput(): ComputationStage =
  when (description) {
    LiquidLegionsV1.Description.NOISED_SKETCH ->
      LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES
    LiquidLegionsV1.Description.CONCATENATED_SKETCH ->
      LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED
    LiquidLegionsV1.Description.ENCRYPTED_FLAGS_AND_COUNTS ->
      LiquidLegionsSketchAggregationV1.Stage.WAIT_FLAG_COUNTS
    else -> failGrpc { "Unknown LiquidLegionsV1 payload description '$description'." }
  }.toProtocolStage()

private fun LiquidLegionsV2.stageExpectingInput(): ComputationStage =
  when (description) {
    LiquidLegionsV2.Description.SETUP_PHASE_INPUT ->
      LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS
    LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT ->
      LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
    LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT ->
      LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
    LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT ->
      LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS
    else -> failGrpc { "Unknown LiquidLegionsV2 payload description '$description'." }
  }.toProtocolStage()

/** Creates an [AdvanceComputationRequest.Header] for a liquid legions v1 computation. */
fun advanceComputationHeader(
  liquidLegionsV1ContentDescription: LiquidLegionsV1.Description,
  globalComputationId: String
): AdvanceComputationRequest.Header =
  AdvanceComputationRequest.Header.newBuilder().apply {
    keyBuilder.globalComputationId = globalComputationId
    liquidLegionsV1Builder.description = liquidLegionsV1ContentDescription
  }.build()

/** Creates an [AdvanceComputationRequest.Header] for a liquid legions v2 computation. */
fun advanceComputationHeader(
  liquidLegionsV2ContentDescription: LiquidLegionsV2.Description,
  globalComputationId: String
): AdvanceComputationRequest.Header =
  AdvanceComputationRequest.Header.newBuilder().apply {
    keyBuilder.globalComputationId = globalComputationId
    liquidLegionsV2Builder.description = liquidLegionsV2ContentDescription
  }.build()
