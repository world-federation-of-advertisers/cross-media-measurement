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

package org.wfanet.measurement.duchy.mill.testing

import com.google.protobuf.ByteString
import org.wfanet.measurement.duchy.mill.LiquidLegionsCryptoWorker
import org.wfanet.measurement.internal.duchy.BlindLastLayerIndexThenJoinRegistersRequest
import org.wfanet.measurement.internal.duchy.BlindLastLayerIndexThenJoinRegistersResponse
import org.wfanet.measurement.internal.duchy.BlindOneLayerRegisterIndexRequest
import org.wfanet.measurement.internal.duchy.BlindOneLayerRegisterIndexResponse
import org.wfanet.measurement.internal.duchy.DecryptLastLayerFlagAndCountRequest
import org.wfanet.measurement.internal.duchy.DecryptLastLayerFlagAndCountResponse
import org.wfanet.measurement.internal.duchy.DecryptOneLayerFlagAndCountRequest
import org.wfanet.measurement.internal.duchy.DecryptOneLayerFlagAndCountResponse

class FakeLiquidLegionsCryptoWorker : LiquidLegionsCryptoWorker {

  override fun BlindOneLayerRegisterIndex(
    request: BlindOneLayerRegisterIndexRequest
  ): BlindOneLayerRegisterIndexResponse {
    val postFix = ByteString.copyFromUtf8("-BlindedOneLayerRegisterIndex")
    return BlindOneLayerRegisterIndexResponse
      .newBuilder().setSketch(request.sketch.concat(postFix)).build()
  }

  override fun BlindLastLayerIndexThenJoinRegisters(
    request: BlindLastLayerIndexThenJoinRegistersRequest
  ): BlindLastLayerIndexThenJoinRegistersResponse {
    val postFix = ByteString.copyFromUtf8("-BlindedLastLayerIndexThenJoinRegisters")
    return BlindLastLayerIndexThenJoinRegistersResponse
      .newBuilder().setFlagCounts(request.sketch.concat(postFix)).build()
  }

  override fun DecryptLastLayerFlagAndCount(
    request: DecryptLastLayerFlagAndCountRequest
  ): DecryptLastLayerFlagAndCountResponse {
    return DecryptLastLayerFlagAndCountResponse.getDefaultInstance()
  }

  override fun DecryptOneLayerFlagAndCount(
    request: DecryptOneLayerFlagAndCountRequest
  ): DecryptOneLayerFlagAndCountResponse {
    val postFix = ByteString.copyFromUtf8("-DecryptedOneLayerFlagAndCount")
    return DecryptOneLayerFlagAndCountResponse
      .newBuilder().setFlagCounts(request.flagCounts.concat(postFix)).build()
  }
}
