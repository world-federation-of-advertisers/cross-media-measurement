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
package org.wfanet.measurement.common.crypto.liquidlegionsv2

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig.ValueSpec.Aggregator
import org.wfanet.measurement.common.crypto.CompleteFilteringPhaseAtAggregatorRequest
import org.wfanet.measurement.common.crypto.CompleteFilteringPhaseAtAggregatorResponse
import org.wfanet.measurement.common.crypto.CompleteFilteringPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteFilteringPhaseResponse
import org.wfanet.measurement.common.crypto.CompleteFrequencyEstimationPhaseAtAggregatorRequest
import org.wfanet.measurement.common.crypto.CompleteFrequencyEstimationPhaseAtAggregatorResponse
import org.wfanet.measurement.common.crypto.CompleteFrequencyEstimationPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteFrequencyEstimationPhaseResponse
import org.wfanet.measurement.common.crypto.CompleteReachEstimationPhaseAtAggregatorRequest
import org.wfanet.measurement.common.crypto.CompleteReachEstimationPhaseAtAggregatorResponse
import org.wfanet.measurement.common.crypto.CompleteReachEstimationPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteReachEstimationPhaseResponse
import org.wfanet.measurement.common.crypto.CompleteSetupPhaseRequest
import org.wfanet.measurement.common.crypto.CompleteSetupPhaseResponse
import org.wfanet.measurement.common.crypto.ElGamalKeyPair
import org.wfanet.measurement.common.crypto.ElGamalPublicKey
import org.wfanet.measurement.common.crypto.LiquidLegionsV2EncryptionUtility
import org.wfanet.measurement.common.hexAsByteString
import org.wfanet.measurement.common.loadLibrary

@RunWith(JUnit4::class)
class LiquidLegionsV2EncryptionUtilityTest {

  private fun createEmptyLiquidLegionsSketch(): Sketch.Builder {
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

  //  Helper function to go through the entire Liquid Legions V2 protocol using the input data.
  //  The final relative_frequency_distribution map are returned.
  private fun goThroughEntireMpcProtocol(
    encrypted_sketch: ByteString
  ): CompleteFrequencyEstimationPhaseAtAggregatorResponse {
    // Setup phase at Duchy 1.
    // We assume all test data comes from duchy 1 in the test, so we ignore setup phase of Duchy 2
    // and 3.
    val completeSetupPhaseRequest =
      CompleteSetupPhaseRequest.newBuilder().apply {
        combinedRegisterVector = encrypted_sketch
      }.build()
    val completeSetupPhaseResponse = CompleteSetupPhaseResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeSetupPhase(completeSetupPhaseRequest.toByteArray())
    )

    // Reach estimation phase at duchy 1 (non-aggregator).
    val completeReachEstimationPhaseRequest1 =
      CompleteReachEstimationPhaseRequest.newBuilder().apply {
        localElGamalKeyPair = DUCHY_1_EL_GAMAL_KEYS
        compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
        curveId = CURVE_ID
        combinedRegisterVector = completeSetupPhaseResponse.combinedRegisterVector
      }.build()
    val completeReachEstimationPhaseResponse1 = CompleteReachEstimationPhaseResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeReachEstimationPhase(
        completeReachEstimationPhaseRequest1.toByteArray()
      )
    )

    // Reach estimation phase at duchy 2 (non-aggregator).
    val completeReachEstimationPhaseRequest2 =
      CompleteReachEstimationPhaseRequest.newBuilder().apply {
        localElGamalKeyPair = DUCHY_2_EL_GAMAL_KEYS
        compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
        curveId = CURVE_ID
        combinedRegisterVector = completeReachEstimationPhaseResponse1.combinedRegisterVector
      }.build()
    val completeReachEstimationPhaseResponse2 = CompleteReachEstimationPhaseResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeReachEstimationPhase(
        completeReachEstimationPhaseRequest2.toByteArray()
      )
    )

    // Reach estimation phase at duchy 3 (aggregator).
    val completeReachEstimationPhaseAtAggregatorRequest =
      CompleteReachEstimationPhaseAtAggregatorRequest.newBuilder().apply {
        localElGamalKeyPair = DUCHY_3_EL_GAMAL_KEYS
        compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
        curveId = CURVE_ID
        combinedRegisterVector = completeReachEstimationPhaseResponse2.combinedRegisterVector
        liquidLegionsParametersBuilder.apply {
          decayRate = DECAY_RATE
          size = LIQUID_LEGIONS_SIZE
        }
      }.build()
    val completeReachEstimationPhaseAtAggregatorResponse =
      CompleteReachEstimationPhaseAtAggregatorResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeReachEstimationPhaseAtAggregator(
          completeReachEstimationPhaseAtAggregatorRequest.toByteArray()
        )
      )

    // Filtering phase at duchy 1 (non-aggregator).
    val completeFilteringPhaseRequest1 =
      CompleteFilteringPhaseRequest.newBuilder().apply {
        localElGamalKeyPair = DUCHY_1_EL_GAMAL_KEYS
        compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
        curveId = CURVE_ID
        flagCountTuples = completeReachEstimationPhaseAtAggregatorResponse.flagCountTuples
      }.build()
    val CompleteFilteringPhaseResponse1 = CompleteFilteringPhaseResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeFilteringPhase(
        completeFilteringPhaseRequest1.toByteArray()
      )
    )

    // Filtering phase at duchy 2 (non-aggregator).
    val completeFilteringPhaseRequest2 =
      CompleteFilteringPhaseRequest.newBuilder().apply {
        localElGamalKeyPair = DUCHY_2_EL_GAMAL_KEYS
        compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
        curveId = CURVE_ID
        flagCountTuples = CompleteFilteringPhaseResponse1.flagCountTuples
      }.build()
    val completeFilteringPhaseResponse2 = CompleteFilteringPhaseResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeFilteringPhase(
        completeFilteringPhaseRequest2.toByteArray()
      )
    )

    // Filtering phase at duchy 3 (aggregator).
    val completeFilteringPhaseAtAggregatorRequest =
      CompleteFilteringPhaseAtAggregatorRequest.newBuilder().apply {
        localElGamalKeyPair = DUCHY_3_EL_GAMAL_KEYS
        compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
        curveId = CURVE_ID
        flagCountTuples = completeFilteringPhaseResponse2.flagCountTuples
        maximumFrequency = MAXIMUM_FREQUENCY
      }.build()
    val completeFilteringPhaseAtAggregatorResponse =
      CompleteFilteringPhaseAtAggregatorResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeFilteringPhaseAtAggregator(
          completeFilteringPhaseAtAggregatorRequest.toByteArray()
        )
      )

    // Frequency estimation phase at duchy 1 (non-aggregator).
    val completeFrequencyEstimationPhaseRequest1 =
      CompleteFrequencyEstimationPhaseRequest.newBuilder().apply {
        localElGamalKeyPair = DUCHY_1_EL_GAMAL_KEYS
        curveId = CURVE_ID
        sameKeyAggregatorMatrix = completeFilteringPhaseAtAggregatorResponse.sameKeyAggregatorMatrix
      }.build()
    val completeFrequencyEstimationPhaseResponse1 =
      CompleteFrequencyEstimationPhaseResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeFrequencyEstimationPhase(
          completeFrequencyEstimationPhaseRequest1.toByteArray()
        )
      )

    // Frequency estimation phase at duchy 2 (non-aggregator).
    val completeFrequencyEstimationPhaseRequest2 =
      CompleteFrequencyEstimationPhaseRequest.newBuilder().apply {
        localElGamalKeyPair = DUCHY_2_EL_GAMAL_KEYS
        curveId = CURVE_ID
        sameKeyAggregatorMatrix = completeFrequencyEstimationPhaseResponse1.sameKeyAggregatorMatrix
      }.build()
    val completeFrequencyEstimationPhaseResponse2 =
      CompleteFrequencyEstimationPhaseResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeFrequencyEstimationPhase(
          completeFrequencyEstimationPhaseRequest2.toByteArray()
        )
      )

    // Frequency estimation phase at duchy 3 (aggregator).
    val completeFrequencyEstimationPhaseAtAggregatorRequest =
      CompleteFrequencyEstimationPhaseAtAggregatorRequest.newBuilder().apply {
        localElGamalKeyPair = DUCHY_3_EL_GAMAL_KEYS
        curveId = CURVE_ID
        maximumFrequency = MAXIMUM_FREQUENCY
        sameKeyAggregatorMatrix = completeFrequencyEstimationPhaseResponse2.sameKeyAggregatorMatrix
      }.build()
    return CompleteFrequencyEstimationPhaseAtAggregatorResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeFrequencyEstimationPhaseAtAggregator(
        completeFrequencyEstimationPhaseAtAggregatorRequest.toByteArray()
      )
    )
  }

  @Test
  fun endToEnd_basicBehavior() {
    val rawSketch = createEmptyLiquidLegionsSketch().apply {
      addRegister(index = 1L, key = 111L, count = 2L)
      addRegister(index = 1L, key = 111L, count = 3L)
      addRegister(index = 2L, key = 222L, count = 1L)
      addRegister(index = 2L, key = 333L, count = 3L)
      addRegister(index = 3L, key = 444L, count = 12L)
      addRegister(index = 4L, key = -1, count = 1L)
      addRegister(index = 5L, key = 555, count = 5L)
    }.build()
    val request = EncryptSketchRequest.newBuilder().apply {
      sketch = rawSketch
      curveId = CURVE_ID
      maximumValue = MAX_COUNTER_VALUE
      elGamalKeys = SKETCH_ENCRYPTER_KEY
      destroyedRegisterStrategy = FLAGGED_KEY
    }.build()
    val response = EncryptSketchResponse.parseFrom(
      SketchEncrypterAdapter.EncryptSketch(request.toByteArray())
    )
    val encryptedSketch = response.encryptedSketch
    val result = goThroughEntireMpcProtocol(encryptedSketch).frequencyDistributionMap
    assertThat(result).containsExactly(
      5L, 2.0 / 3, // register 1 and 5 : 5
      11L, 1.0 / 3 // register 4  : MAXIMUM_FREQUENCY+1
    )
  }

  @Test
  fun `completeSetupPhase fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV2EncryptionUtility.completeSetupPhase(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  @Test
  fun `completeReachEstimationPhase fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV2EncryptionUtility.completeReachEstimationPhase(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  @Test
  fun `completeReachEstimationPhaseAtAggregator fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV2EncryptionUtility.completeReachEstimationPhaseAtAggregator(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  @Test
  fun `completeFilteringPhase fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV2EncryptionUtility.completeFilteringPhase(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  @Test
  fun `completeFilteringPhaseAtAggregator fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV2EncryptionUtility.completeFilteringPhaseAtAggregator(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  @Test
  fun `completeFrequencyEstimationPhase fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV2EncryptionUtility.completeFrequencyEstimationPhase(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  @Test
  fun `completeFrequencyEstimationPhaseAtAggregator fails with invalid request message`() {
    val exception = assertFailsWith(RuntimeException::class) {
      LiquidLegionsV2EncryptionUtility.completeFrequencyEstimationPhaseAtAggregator(
        "something not a proto".toByteArray()
      )
    }
    assertThat(exception).hasMessageThat().contains("failed to parse")
  }

  companion object {
    init {
      loadLibrary(
        "liquid_legions_v2_encryption_utility",
        Paths.get("wfa_measurement_system/src/main/swig/common/crypto/liquidlegionsv2")
      )
      loadLibrary(
        "sketch_encrypter_adapter",
        Paths.get("any_sketch_java/src/main/java/org/wfanet/anysketch/crypto")
      )
    }

    private const val DECAY_RATE = 12.0
    private const val LIQUID_LEGIONS_SIZE = 100_000L
    private const val MAXIMUM_FREQUENCY = 10

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
  }
}
