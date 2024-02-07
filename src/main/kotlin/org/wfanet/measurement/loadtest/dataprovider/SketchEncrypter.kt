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
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.ElGamalPublicKey
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.anysketch.crypto.combineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.encryptSketchRequest

/** An encrypter for [Sketch] instances. */
interface SketchEncrypter {
  /** Encrypts a sketch specifying maximumValue. */
  fun encrypt(
    sketch: Sketch,
    ellipticCurveId: Int,
    encryptionKey: ElGamalPublicKey,
    maximumValue: Int,
  ): ByteString

  /** Encrypts a sketch without maximumValue. */
  fun encrypt(sketch: Sketch, ellipticCurveId: Int, encryptionKey: ElGamalPublicKey): ByteString

  companion object {
    init {
      System.loadLibrary("sketch_encrypter_adapter")
    }

    /** Default instance of a [SketchEncrypter]. */
    val Default: SketchEncrypter = SketchEncrypterImpl()

    /** Combines [keys] into a single key. */
    fun combineElGamalPublicKeys(
      ellipticCurveId: Int,
      keys: Iterable<ElGamalPublicKey>,
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
    ellipticCurveId: Int,
    encryptionKey: ElGamalPublicKey,
    maximumValue: Int,
  ): ByteString {
    val request = encryptSketchRequest {
      this.sketch = sketch
      elGamalKeys = encryptionKey
      curveId = ellipticCurveId.toLong()
      this.maximumValue = maximumValue
      destroyedRegisterStrategy = EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
    }
    val response =
      EncryptSketchResponse.parseFrom(SketchEncrypterAdapter.EncryptSketch(request.toByteArray()))
    return response.encryptedSketch
  }

  override fun encrypt(
    sketch: Sketch,
    ellipticCurveId: Int,
    encryptionKey: ElGamalPublicKey,
  ): ByteString {
    val request = encryptSketchRequest {
      this.sketch = sketch
      elGamalKeys = encryptionKey
      curveId = ellipticCurveId.toLong()
    }
    val response =
      EncryptSketchResponse.parseFrom(SketchEncrypterAdapter.EncryptSketch(request.toByteArray()))
    return response.encryptedSketch
  }
}
