/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.ByteString
import java.nio.file.Paths
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.ElGamalPublicKey
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.anysketch.crypto.combineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.encryptSketchRequest
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.common.loadLibrary

/** An encrypter for [Sketch] instances. */
interface SketchEncrypter {
  /** Encrypts a sketch. */
  fun encrypt(
    sketch: Sketch,
    encryptionKey: ElGamalPublicKey,
    protocol: ProtocolConfig.Protocol,
  ): ByteString

  companion object {
    init {
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath =
          Paths.get(
            "any_sketch_java",
            "src",
            "main",
            "java",
            "org",
            "wfanet",
            "anysketch",
            "crypto"
          )
      )
    }

    /** Default instance of a [SketchEncrypter]. */
    val Default: SketchEncrypter = SketchEncrypterImpl()

    /** Combines [keys] into a single key. */
    fun combineElGamalPublicKeys(
      ellipticCurveId: Int,
      keys: Iterable<ElGamalPublicKey>
    ): ElGamalPublicKey {
      val request = combineElGamalPublicKeysRequest {
        curveId = ellipticCurveId.toLong()
        elGamalKeys += keys
      }
      val response =
        CombineElGamalPublicKeysResponse.parseFrom(
          SketchEncrypterAdapter.CombineElGamalPublicKeys(request.toByteArray())
        )
      return response.elGamalKeys
    }
  }
}

/**
 * The default implementation of [SketchEncrypter].
 *
 * Note that this does not currently support publisher noise.
 */
private class SketchEncrypterImpl : SketchEncrypter {
  override fun encrypt(
    sketch: Sketch,
    encryptionKey: ElGamalPublicKey,
    protocol: ProtocolConfig.Protocol,
  ): ByteString {
    val request = encryptSketchRequest {
      this.sketch = sketch
      elGamalKeys = encryptionKey
      when (protocol.protocolCase) {
        ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2 -> {
          curveId = protocol.liquidLegionsV2.ellipticCurveId.toLong()
          maximumValue = protocol.liquidLegionsV2.maximumFrequency
          destroyedRegisterStrategy = EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
        }
        ProtocolConfig.Protocol.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
          curveId = protocol.reachOnlyLiquidLegionsV2.ellipticCurveId.toLong()
        }
        else -> error("Protocol type not supported for encryptSketch.")
      }
    }
    val response =
      EncryptSketchResponse.parseFrom(SketchEncrypterAdapter.EncryptSketch(request.toByteArray()))
    return response.encryptedSketch
  }
}
