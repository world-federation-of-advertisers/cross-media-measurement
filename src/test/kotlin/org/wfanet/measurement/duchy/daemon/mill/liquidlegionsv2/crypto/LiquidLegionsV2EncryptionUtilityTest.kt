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
package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig.ValueSpec.Aggregator
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.duchy.daemon.utils.toAnySketchElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toCmmsElGamalPublicKey
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseOneResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseThreeResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteInitializationPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteSetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteSetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.liquidlegionsv2.LiquidLegionsV2EncryptionUtility

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
  ): CompleteExecutionPhaseThreeAtAggregatorResponse {
    // Setup phase at Duchy 1.
    // We assume all test data comes from duchy 1 in the test, so we ignore setup phase of Duchy 2
    // and 3.
    val completeSetupPhaseRequest =
      CompleteSetupPhaseRequest.newBuilder()
        .apply { combinedRegisterVector = encrypted_sketch }
        .build()
    val completeSetupPhaseResponse =
      CompleteSetupPhaseResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeSetupPhase(completeSetupPhaseRequest.toByteArray())
      )

    // Execution phase one at duchy 1 (non-aggregator).
    val completeExecutionPhaseOneRequest1 =
      CompleteExecutionPhaseOneRequest.newBuilder()
        .apply {
          localElGamalKeyPair = DUCHY_1_EL_GAMAL_KEYS
          compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
          curveId = CURVE_ID
          combinedRegisterVector = completeSetupPhaseResponse.combinedRegisterVector
        }
        .build()
    val completeExecutionPhaseOneResponse1 =
      CompleteExecutionPhaseOneResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseOne(
          completeExecutionPhaseOneRequest1.toByteArray()
        )
      )

    // Execution phase one at duchy 2 (non-aggregator).
    val completeExecutionPhaseOneRequest2 =
      CompleteExecutionPhaseOneRequest.newBuilder()
        .apply {
          localElGamalKeyPair = DUCHY_2_EL_GAMAL_KEYS
          compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
          curveId = CURVE_ID
          combinedRegisterVector = completeExecutionPhaseOneResponse1.combinedRegisterVector
        }
        .build()
    val completeExecutionPhaseOneResponse2 =
      CompleteExecutionPhaseOneResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseOne(
          completeExecutionPhaseOneRequest2.toByteArray()
        )
      )

    // Execution phase one at duchy 3 (aggregator).
    val completeExecutionPhaseOneAtAggregatorRequest =
      CompleteExecutionPhaseOneAtAggregatorRequest.newBuilder()
        .apply {
          localElGamalKeyPair = DUCHY_3_EL_GAMAL_KEYS
          compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
          curveId = CURVE_ID
          combinedRegisterVector = completeExecutionPhaseOneResponse2.combinedRegisterVector
          totalSketchesCount = 3
        }
        .build()
    val completeExecutionPhaseOneAtAggregatorResponse =
      CompleteExecutionPhaseOneAtAggregatorResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseOneAtAggregator(
          completeExecutionPhaseOneAtAggregatorRequest.toByteArray()
        )
      )

    // Execution phase two at duchy 1 (non-aggregator).
    val completeExecutionPhaseTwoRequest1 =
      CompleteExecutionPhaseTwoRequest.newBuilder()
        .apply {
          localElGamalKeyPair = DUCHY_1_EL_GAMAL_KEYS
          compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
          curveId = CURVE_ID
          flagCountTuples = completeExecutionPhaseOneAtAggregatorResponse.flagCountTuples
          partialCompositeElGamalPublicKey = DUCHY_2_3_COMBINED_EL_GAMAL_KEYS
        }
        .build()
    val completeExecutionPhaseTwoResponse1 =
      CompleteExecutionPhaseTwoResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseTwo(
          completeExecutionPhaseTwoRequest1.toByteArray()
        )
      )

    // Execution phase two at duchy 2 (non-aggregator).
    val completeExecutionPhaseTwoRequest2 =
      CompleteExecutionPhaseTwoRequest.newBuilder()
        .apply {
          localElGamalKeyPair = DUCHY_2_EL_GAMAL_KEYS
          compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
          curveId = CURVE_ID
          flagCountTuples = completeExecutionPhaseTwoResponse1.flagCountTuples
          partialCompositeElGamalPublicKey = DUCHY_3_EL_GAMAL_KEYS.publicKey
        }
        .build()
    val completeExecutionPhaseTwoResponse2 =
      CompleteExecutionPhaseTwoResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseTwo(
          completeExecutionPhaseTwoRequest2.toByteArray()
        )
      )

    // Execution phase two at duchy 3 (aggregator).
    val completeExecutionPhaseTwoAtAggregatorRequest =
      CompleteExecutionPhaseTwoAtAggregatorRequest.newBuilder()
        .apply {
          localElGamalKeyPair = DUCHY_3_EL_GAMAL_KEYS
          compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
          curveId = CURVE_ID
          flagCountTuples = completeExecutionPhaseTwoResponse2.flagCountTuples
          maximumFrequency = MAXIMUM_FREQUENCY
          liquidLegionsParametersBuilder.apply {
            decayRate = DECAY_RATE
            size = LIQUID_LEGIONS_SIZE
          }
          vidSamplingIntervalWidth = VID_SAMPLING_INTERVAL_WIDTH
        }
        .build()
    val completeExecutionPhaseTwoAtAggregatorResponse =
      CompleteExecutionPhaseTwoAtAggregatorResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseTwoAtAggregator(
          completeExecutionPhaseTwoAtAggregatorRequest.toByteArray()
        )
      )

    // Execution phase three at duchy 1 (non-aggregator).
    val completeExecutionPhaseThreeRequest1 =
      CompleteExecutionPhaseThreeRequest.newBuilder()
        .apply {
          localElGamalKeyPair = DUCHY_1_EL_GAMAL_KEYS
          curveId = CURVE_ID
          sameKeyAggregatorMatrix =
            completeExecutionPhaseTwoAtAggregatorResponse.sameKeyAggregatorMatrix
        }
        .build()
    val completeExecutionPhaseThreeResponse1 =
      CompleteExecutionPhaseThreeResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseThree(
          completeExecutionPhaseThreeRequest1.toByteArray()
        )
      )

    // Execution phase three at duchy 2 (non-aggregator).
    val completeExecutionPhaseThreeRequest2 =
      CompleteExecutionPhaseThreeRequest.newBuilder()
        .apply {
          localElGamalKeyPair = DUCHY_2_EL_GAMAL_KEYS
          curveId = CURVE_ID
          sameKeyAggregatorMatrix = completeExecutionPhaseThreeResponse1.sameKeyAggregatorMatrix
        }
        .build()
    val completeExecutionPhaseThreeResponse2 =
      CompleteExecutionPhaseThreeResponse.parseFrom(
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseThree(
          completeExecutionPhaseThreeRequest2.toByteArray()
        )
      )

    // Execution phase three at duchy 3 (aggregator).
    val completeExecutionPhaseThreeAtAggregatorRequest =
      CompleteExecutionPhaseThreeAtAggregatorRequest.newBuilder()
        .apply {
          localElGamalKeyPair = DUCHY_3_EL_GAMAL_KEYS
          curveId = CURVE_ID
          maximumFrequency = MAXIMUM_FREQUENCY
          sameKeyAggregatorMatrix = completeExecutionPhaseThreeResponse2.sameKeyAggregatorMatrix
        }
        .build()
    return CompleteExecutionPhaseThreeAtAggregatorResponse.parseFrom(
      LiquidLegionsV2EncryptionUtility.completeExecutionPhaseThreeAtAggregator(
        completeExecutionPhaseThreeAtAggregatorRequest.toByteArray()
      )
    )
  }

  @Test
  fun endToEnd_basicBehavior() {
    val rawSketch =
      createEmptyLiquidLegionsSketch()
        .apply {
          addRegister(index = 1L, key = 111L, count = 2L)
          addRegister(index = 1L, key = 111L, count = 3L)
          addRegister(index = 2L, key = 222L, count = 1L)
          addRegister(index = 2L, key = 333L, count = 3L)
          addRegister(index = 3L, key = 444L, count = 12L)
          addRegister(index = 4L, key = -1, count = 1L)
          addRegister(index = 5L, key = 555, count = 5L)
        }
        .build()
    val request =
      EncryptSketchRequest.newBuilder()
        .apply {
          sketch = rawSketch
          curveId = CURVE_ID
          maximumValue = MAX_COUNTER_VALUE
          elGamalKeys = CLIENT_EL_GAMAL_KEYS.toAnySketchElGamalPublicKey()
          destroyedRegisterStrategy = FLAGGED_KEY
        }
        .build()
    val response =
      EncryptSketchResponse.parseFrom(SketchEncrypterAdapter.EncryptSketch(request.toByteArray()))
    val encryptedSketch = response.encryptedSketch
    val result = goThroughEntireMpcProtocol(encryptedSketch).frequencyDistributionMap
    assertThat(result)
      .containsExactly(
        5L,
        2.0 / 3, // register 1 and 5 : 5
        10L,
        1.0 / 3 // register 4  : MAXIMUM_FREQUENCY
      )
  }

  @Test
  fun `completeSetupPhase fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        LiquidLegionsV2EncryptionUtility.completeSetupPhase("something not a proto".toByteArray())
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  @Test
  fun `completeExecutionPhaseOne fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseOne(
          "something not a proto".toByteArray()
        )
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  @Test
  fun `completeExecutionPhaseOneAtAggregator fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseOneAtAggregator(
          "something not a proto".toByteArray()
        )
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  @Test
  fun `completeExecutionPhaseTwo fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseTwo(
          "something not a proto".toByteArray()
        )
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  @Test
  fun `completeExecutionPhaseTwoAtAggregator fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseTwoAtAggregator(
          "something not a proto".toByteArray()
        )
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  @Test
  fun `completeExecutionPhaseThree fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseThree(
          "something not a proto".toByteArray()
        )
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  @Test
  fun `completeExecutionPhaseThreeAtAggregator fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        LiquidLegionsV2EncryptionUtility.completeExecutionPhaseThreeAtAggregator(
          "something not a proto".toByteArray()
        )
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  companion object {
    init {
      loadLibrary(
        "liquid_legions_v2_encryption_utility",
        Paths.get("wfa_measurement_system/src/main/swig/protocol/liquidlegionsv2")
      )
      loadLibrary(
        "sketch_encrypter_adapter",
        Paths.get("any_sketch_java/src/main/java/org/wfanet/anysketch/crypto")
      )
    }

    private const val DECAY_RATE = 12.0
    private const val LIQUID_LEGIONS_SIZE = 100_000L
    private const val MAXIMUM_FREQUENCY = 10
    private const val VID_SAMPLING_INTERVAL_WIDTH = 0.5f

    private const val CURVE_ID = 415L // NID_X9_62_prime256v1
    private const val MAX_COUNTER_VALUE = 10

    private val COMPLETE_INITIALIZATION_REQUEST =
      CompleteInitializationPhaseRequest.newBuilder().apply { curveId = CURVE_ID }.build()
    private val DUCHY_1_EL_GAMAL_KEYS =
      CompleteInitializationPhaseResponse.parseFrom(
          LiquidLegionsV2EncryptionUtility.completeInitializationPhase(
            COMPLETE_INITIALIZATION_REQUEST.toByteArray()
          )
        )
        .elGamalKeyPair
    private val DUCHY_2_EL_GAMAL_KEYS =
      CompleteInitializationPhaseResponse.parseFrom(
          LiquidLegionsV2EncryptionUtility.completeInitializationPhase(
            COMPLETE_INITIALIZATION_REQUEST.toByteArray()
          )
        )
        .elGamalKeyPair
    private val DUCHY_3_EL_GAMAL_KEYS =
      CompleteInitializationPhaseResponse.parseFrom(
          LiquidLegionsV2EncryptionUtility.completeInitializationPhase(
            COMPLETE_INITIALIZATION_REQUEST.toByteArray()
          )
        )
        .elGamalKeyPair

    private val CLIENT_EL_GAMAL_KEYS =
      CombineElGamalPublicKeysResponse.parseFrom(
          SketchEncrypterAdapter.CombineElGamalPublicKeys(
            CombineElGamalPublicKeysRequest.newBuilder()
              .apply {
                curveId = CURVE_ID
                addElGamalKeys(DUCHY_1_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey())
                addElGamalKeys(DUCHY_2_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey())
                addElGamalKeys(DUCHY_3_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey())
              }
              .build()
              .toByteArray()
          )
        )
        .elGamalKeys
        .toCmmsElGamalPublicKey()
    private val DUCHY_2_3_COMBINED_EL_GAMAL_KEYS =
      CombineElGamalPublicKeysResponse.parseFrom(
          SketchEncrypterAdapter.CombineElGamalPublicKeys(
            CombineElGamalPublicKeysRequest.newBuilder()
              .apply {
                curveId = CURVE_ID
                addElGamalKeys(DUCHY_2_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey())
                addElGamalKeys(DUCHY_3_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey())
              }
              .build()
              .toByteArray()
          )
        )
        .elGamalKeys
        .toCmmsElGamalPublicKey()
  }
}
