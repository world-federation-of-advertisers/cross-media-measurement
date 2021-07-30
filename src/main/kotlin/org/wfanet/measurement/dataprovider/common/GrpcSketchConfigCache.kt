// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.dataprovider.common

import org.wfanet.measurement.api.v2alpha.GetSketchConfigRequest
import org.wfanet.measurement.api.v2alpha.SketchConfig
import org.wfanet.measurement.api.v2alpha.SketchConfigsGrpcKt.SketchConfigsCoroutineStub

/**
 * Implementation of [SketchConfigStore] that caches [SketchConfig]s obtained via gRPC.
 */
class GrpcSketchConfigCache(
  val sketchConfigs: SketchConfigsCoroutineStub
) : SketchConfigStore {
  private val cache = mutableMapOf<SketchConfig.Key, SketchConfig>()

  override suspend fun get(key: SketchConfig.Key): SketchConfig {
    return cache.getOrPut(key) { getFromGrpc(key) }
  }

  private suspend fun getFromGrpc(key: SketchConfig.Key): SketchConfig {
    return sketchConfigs.getSketchConfig(
      GetSketchConfigRequest.newBuilder().also {
        it.key = key
      }.build()
    )
  }
}
