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
package org.wfanet.measurement.common.crypto.liquidlegionsv1

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy.CONFLICTING_KEYS
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig.ValueSpec.Aggregator
import org.wfanet.measurement.common.crypto.AddNoiseToSketchRequest
import org.wfanet.measurement.common.crypto.AddNoiseToSketchResponse
import org.wfanet.measurement.common.crypto.BlindLastLayerIndexThenJoinRegistersRequest
import org.wfanet.measurement.common.crypto.BlindLastLayerIndexThenJoinRegistersResponse
import org.wfanet.measurement.common.crypto.BlindOneLayerRegisterIndexRequest
import org.wfanet.measurement.common.crypto.BlindOneLayerRegisterIndexResponse
import org.wfanet.measurement.common.crypto.DecryptLastLayerFlagAndCountRequest
import org.wfanet.measurement.common.crypto.DecryptLastLayerFlagAndCountResponse
import org.wfanet.measurement.common.crypto.DecryptLastLayerFlagAndCountResponse.FlagCount
import org.wfanet.measurement.common.crypto.DecryptOneLayerFlagAndCountRequest
import org.wfanet.measurement.common.crypto.DecryptOneLayerFlagAndCountResponse
import org.wfanet.measurement.common.crypto.ElGamalKeyPair
import org.wfanet.measurement.common.crypto.ElGamalPublicKey
import org.wfanet.measurement.common.crypto.LiquidLegionsV1EncryptionUtility
import org.wfanet.measurement.common.hexAsByteString
import org.wfanet.measurement.common.loadLibrary

private const val CURVE_ID = 415L // NID_X9_62_prime256v1
private const val MAX_COUNTER_VALUE = 10
private const val DUCHY_1_PK_G =
  "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"
private const val DUCHY_1_PK_Y =
  "02d1432ca007a6c6d739fce2d21feb56d9a2c35cf968265f9093c4b691e11386b3"
private const val DUCHY_1_SK =
  "057b22ef9c4e9626c22c13daed1363a1e6a5b309a930409f8d131f96ea2fa888"
private const val DUCHY_2_PK_G =
  "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"
private const val DUCHY_2_PK_Y =
  "039ef370ff4d216225401781d88a03f5a670a5040e6333492cb4e0cd991abbd5a3"
private const val DUCHY_2_SK =
  "31cc32e7cd53ff24f2b64ae8c531099af9867ebf5d9a659f742459947caa29b0"
private const val DUCHY_3_PK_G =
  "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"
private const val DUCHY_3_PK_Y =
  "02d0f25ab445fc9c29e7e2509adc93308430f432522ffa93c2ae737ceb480b66d7"
private const val DUCHY_3_SK =
  "338cce0306416b70e901436cb9eca5ac758e8ff41d7b58dabadf8726608ca6cc"
private const val CLIENT_PK_G =
  "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"
private const val CLIENT_PK_Y =
  "02505d7b3ac4c3c387c74132ab677a3421e883b90d4c83dc766e400fe67acc1f04"
private val DUCHY_1_EL_GAMAL_KEYS = ElGamalKeyPair.newBuilder().apply {
  publicKeyBuilder.apply {
    generator = DUCHY_1_PK_G.hexAsByteString()
    element = DUCHY_1_PK_Y.hexAsByteString()
  }
  secretKey = DUCHY_1_SK.hexAsByteString()
}.build()
private val DUCHY_2_EL_GAMAL_KEYS = ElGamalKeyPair.newBuilder().apply {
  publicKeyBuilder.apply {
    generator = DUCHY_2_PK_G.hexAsByteString()
    element = DUCHY_2_PK_Y.hexAsByteString()
  }
  secretKey = DUCHY_2_SK.hexAsByteString()
}.build()
private val DUCHY_3_EL_GAMAL_KEYS = ElGamalKeyPair.newBuilder().apply {
  publicKeyBuilder.apply {
    generator = DUCHY_3_PK_G.hexAsByteString()
    element = DUCHY_3_PK_Y.hexAsByteString()
  }
  secretKey = DUCHY_3_SK.hexAsByteString()
}.build()
private val CLIENT_EL_GAMAL_KEYS = ElGamalPublicKey.newBuilder().apply {
  generator = CLIENT_PK_G.hexAsByteString()
  element = CLIENT_PK_Y.hexAsByteString()
}.build()
private val SKETCH_ENCRYPTER_KEY =
  org.wfanet.anysketch.crypto.ElGamalPublicKeys.newBuilder().apply {
    elGamalG = CLIENT_PK_G.hexAsByteString()
    elGamalY = CLIENT_PK_Y.hexAsByteString()
  }.build()

@RunWith(JUnit4::class)
class LiquidLegionsV1EncryptionUtilityTest {
  // Helper function to go through the entire MPC protocol using the input data.
  // The final (flag, count) lists are returned.
  private fun goThroughEntireMpcProtocol(
    encrypted_sketch: ByteString
  ): DecryptLastLayerFlagAndCountResponse {
    // Duchy 1 add noise to the sketch
    val addNoiseToSketchRequest =
      AddNoiseToSketchRequest.newBuilder().setSketch(encrypted_sketch).build()
    val addNoiseToSketchResponse = AddNoiseToSketchResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.addNoiseToSketch(addNoiseToSketchRequest.toByteArray())
    )

    // Blind register indexes at duchy 1
    val blindOneLayerRegisterIndexRequest1 = BlindOneLayerRegisterIndexRequest.newBuilder()
      .setCurveId(CURVE_ID)
      .setLocalElGamalKeyPair(DUCHY_1_EL_GAMAL_KEYS)
      .setCompositeElGamalPublicKey(CLIENT_EL_GAMAL_KEYS)
      .setSketch(addNoiseToSketchResponse.sketch)
      .build()
    val blindOneLayerRegisterIndexResponse1 = BlindOneLayerRegisterIndexResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.blindOneLayerRegisterIndex(
        blindOneLayerRegisterIndexRequest1.toByteArray()
      )
    )

    // Blind register indexes at duchy 2
    val blindOneLayerRegisterIndexRequest2 = BlindOneLayerRegisterIndexRequest.newBuilder()
      .setCurveId(CURVE_ID)
      .setLocalElGamalKeyPair(DUCHY_2_EL_GAMAL_KEYS)
      .setCompositeElGamalPublicKey(CLIENT_EL_GAMAL_KEYS)
      .setSketch(blindOneLayerRegisterIndexResponse1.sketch)
      .build()
    val blindOneLayerRegisterIndexResponse2 = BlindOneLayerRegisterIndexResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.blindOneLayerRegisterIndex(
        blindOneLayerRegisterIndexRequest2.toByteArray()
      )
    )

    // Blind register indexes and join registers at duchy 3 (primary duchy)
    val blindLastLayerIndexThenJoinRegistersRequest =
      BlindLastLayerIndexThenJoinRegistersRequest.newBuilder()
        .setCurveId(CURVE_ID)
        .setCompositeElGamalPublicKey(CLIENT_EL_GAMAL_KEYS)
        .setLocalElGamalKeyPair(DUCHY_3_EL_GAMAL_KEYS)
        .setSketch(blindOneLayerRegisterIndexResponse2.sketch)
        .build()
    val blindLastLayerIndexThenJoinRegistersResponse =
      BlindLastLayerIndexThenJoinRegistersResponse.parseFrom(
        LiquidLegionsV1EncryptionUtility.blindLastLayerIndexThenJoinRegisters(
          blindLastLayerIndexThenJoinRegistersRequest.toByteArray()
        )
      )

    // Decrypt flags and counts at duchy 1
    val decryptOneLayerFlagAndCountRequest1 = DecryptOneLayerFlagAndCountRequest.newBuilder()
      .setFlagCounts(blindLastLayerIndexThenJoinRegistersResponse.flagCounts)
      .setCurveId(CURVE_ID)
      .setLocalElGamalKeyPair(DUCHY_1_EL_GAMAL_KEYS)
      .build()
    val decryptOneLayerFlagAndCountResponse1 = DecryptOneLayerFlagAndCountResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.decryptOneLayerFlagAndCount(
        decryptOneLayerFlagAndCountRequest1.toByteArray()
      )
    )

    // Decrypt flags and counts at duchy 2
    val decryptOneLayerFlagAndCountRequest2 = DecryptOneLayerFlagAndCountRequest.newBuilder()
      .setFlagCounts(decryptOneLayerFlagAndCountResponse1.flagCounts)
      .setCurveId(CURVE_ID)
      .setLocalElGamalKeyPair(DUCHY_2_EL_GAMAL_KEYS)
      .build()
    val decryptOneLayerFlagAndCountResponse2 = DecryptOneLayerFlagAndCountResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.decryptOneLayerFlagAndCount(
        decryptOneLayerFlagAndCountRequest2.toByteArray()
      )
    )

    // Decrypt flags and counts at duchy 3 (primary duchy).
    val decryptLastLayerFlagAndCountRequest = DecryptLastLayerFlagAndCountRequest.newBuilder()
      .setFlagCounts(decryptOneLayerFlagAndCountResponse2.flagCounts)
      .setCurveId(CURVE_ID)
      .setLocalElGamalKeyPair(DUCHY_3_EL_GAMAL_KEYS)
      .setMaximumFrequency(MAX_COUNTER_VALUE)
      .build()
    return DecryptLastLayerFlagAndCountResponse.parseFrom(
      LiquidLegionsV1EncryptionUtility.decryptLastLayerFlagAndCount(
        decryptLastLayerFlagAndCountRequest.toByteArray()
      )
    )
  }

  private fun newFlagCount(notDestroyed: Boolean, frequency: Int): FlagCount {
    return FlagCount.newBuilder().setIsNotDestroyed(notDestroyed).setFrequency(frequency).build()
  }

  @Test
  fun endToEnd_basicBehavior() {
    val rawSketch = createEmptyClceSketch().apply {
      addRegister(index = 1L, key = 111L, count = 2L)
      addRegister(index = 1L, key = 111L, count = 3L)
      addRegister(index = 2L, key = 222L, count = 1L)
      addRegister(index = 2L, key = 333L, count = 3L)
      addRegister(index = 3L, key = 444L, count = 12L)
    }.build()
    val request = EncryptSketchRequest.newBuilder().apply {
      sketch = rawSketch
      curveId = CURVE_ID
      maximumValue = MAX_COUNTER_VALUE
      elGamalKeys = SKETCH_ENCRYPTER_KEY
      destroyedRegisterStrategy = CONFLICTING_KEYS
    }.build()
    val response = EncryptSketchResponse.parseFrom(
      SketchEncrypterAdapter.EncryptSketch(request.toByteArray())
    )
    val encryptedSketch = response.encryptedSketch
    val result = goThroughEntireMpcProtocol(encryptedSketch).flagCountsList
    assertThat(result)
      .containsExactly(
        newFlagCount(true, 5), // key 111, count 2+3=5.
        newFlagCount(true, 10), // key 444, capped by MAX_COUNTER_VALUE.
        newFlagCount(false, 10)
      ) // key 222 and key 333 collide.
  }

  @Test
  fun `blindOneLayerRegisterIndex fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV1EncryptionUtility.blindOneLayerRegisterIndex(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  @Test
  fun `blindLastLayerIndexThenJoinRegisters fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV1EncryptionUtility.blindLastLayerIndexThenJoinRegisters(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  @Test
  fun `decryptOneLayerFlagAndCount fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV1EncryptionUtility.decryptOneLayerFlagAndCount(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  @Test
  fun `decryptLastLayerFlagAndCount fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV1EncryptionUtility.decryptLastLayerFlagAndCount(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  companion object {
    init {
      loadLibrary(
        "liquid_legions_v1_encryption_utility",
        Paths.get("wfa_measurement_system/src/main/swig/common/crypto/liquidlegionsv1")
      )
      loadLibrary(
        "sketch_encrypter_adapter",
        Paths.get("any_sketch_java/src/main/java/org/wfanet/anysketch/crypto")
      )
    }
  }
}

private fun createEmptyClceSketch(): Sketch.Builder {
  return Sketch.newBuilder().apply {
    configBuilder.apply {
      addValuesBuilder().aggregator = Aggregator.UNIQUE
      addValuesBuilder().aggregator = Aggregator.SUM
    }
  }
}

private fun Sketch.Builder.addRegister(index: Long, key: Long, count: Long) {
  addRegistersBuilder().also {
    it.index = index
    it.addValues(key)
    it.addValues(count)
  }
}
