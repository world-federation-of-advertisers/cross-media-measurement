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

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.common.asBufferedFlow

/** [EncryptedSketchGenerator] that delegates to helpers. */
class DefaultEncryptedSketchGenerator : EncryptedSketchGenerator {

  private val MAX_COUNTER_VALUE = 5

  fun encryptSketch(sketch: Sketch, combinedPublicKey: ElGamalPublicKey): Flow<ByteString> {
    val request: EncryptSketchRequest =
      EncryptSketchRequest.newBuilder()
        .apply {
          this.sketch = sketch
          maximumValue = MAX_COUNTER_VALUE
          curveId = combinedPublicKey.ellipticCurveId.toLong()
          elGamalKeysBuilder.generator = combinedPublicKey.generator
          elGamalKeysBuilder.element = combinedPublicKey.element
          destroyedRegisterStrategy = FLAGGED_KEY // for LL_V2 protocol
        }
        .build()
    val response =
      EncryptSketchResponse.parseFrom(SketchEncrypterAdapter.EncryptSketch(request.toByteArray()))
    return response.encryptedSketch.asBufferedFlow(1024)
  }

  override suspend fun generate(
    sketch: Sketch,
    combinedPublicKey: ElGamalPublicKey
  ): Flow<ByteString> {
    return encryptSketch(sketch, combinedPublicKey)
  }
}
