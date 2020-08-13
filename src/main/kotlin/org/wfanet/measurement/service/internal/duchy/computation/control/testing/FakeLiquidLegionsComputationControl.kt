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

package org.wfanet.measurement.service.internal.duchy.computation.control.testing

import com.google.protobuf.Message
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineImplBase
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchResponse
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsRequest
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsResponse
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchResponse

class FakeLiquidLegionsComputationControl : ComputationControlServiceCoroutineImplBase() {

  var lastRequestStream: List<Message> = listOf()

  override suspend fun handleConcatenatedSketch(
    requests: Flow<HandleConcatenatedSketchRequest>
  ): HandleConcatenatedSketchResponse {
    lastRequestStream = requests.toList()
    return HandleConcatenatedSketchResponse.getDefaultInstance()
  }

  override suspend fun handleEncryptedFlagsAndCounts(
    requests: Flow<HandleEncryptedFlagsAndCountsRequest>
  ): HandleEncryptedFlagsAndCountsResponse {
    lastRequestStream = requests.toList()
    return HandleEncryptedFlagsAndCountsResponse.getDefaultInstance()
  }

  override suspend fun handleNoisedSketch(
    requests: Flow<HandleNoisedSketchRequest>
  ): HandleNoisedSketchResponse {
    lastRequestStream = requests.toList()
    return HandleNoisedSketchResponse.getDefaultInstance()
  }
}
