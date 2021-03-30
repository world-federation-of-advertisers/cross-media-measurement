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
import java.nio.file.Paths
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.api.v1alpha.Sketch as LegacySketch
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.Sketch
import org.wfanet.measurement.common.loadLibrary

/**
 * Utility to encrypt [Sketch]es by wrapping JNIed C++ libraries.
 */
class JniSketchEncrypter(
  private val maximumValue: Int,
  private val destroyedRegisterStrategy: DestroyedRegisterStrategy
) {
  fun encrypt(
    sketch: Sketch,
    key: ElGamalPublicKey
  ): ByteString {
    val request = EncryptSketchRequest.newBuilder().also {
      // TODO: Update the underlying library to use v2alpha?
      it.sketch = LegacySketch.parseFrom(sketch.toByteString())
      it.curveId = key.ellipticCurveId.toLong()
      it.maximumValue = maximumValue
      it.elGamalKeysBuilder.apply {
        generator = key.generator
        element = key.element
      }
      it.destroyedRegisterStrategy = destroyedRegisterStrategy
    }.build()

    val response = EncryptSketchResponse.parseFrom(
      SketchEncrypterAdapter.EncryptSketch(request.toByteArray())
    )

    return response.encryptedSketch
  }

  companion object {
    init {
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath = Paths.get("any_sketch_java/src/main/java/org/wfanet/anysketch/crypto")
      )
    }
  }
}
