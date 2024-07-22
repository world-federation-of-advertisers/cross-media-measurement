// Copyright 2023 The Cross-Media Measurement Authors
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
package org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.anysketch.SketchKt
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.anysketch.crypto.combineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.encryptSketchRequest
import org.wfanet.anysketch.sketch
import org.wfanet.estimation.Estimators
import org.wfanet.measurement.duchy.utils.toAnySketchElGamalPublicKey
import org.wfanet.measurement.duchy.utils.toCmmsElGamalPublicKey
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyInitializationPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyExecutionPhaseAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyExecutionPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlySetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsSketchParameters
import org.wfanet.measurement.internal.duchy.protocol.reachonlyliquidlegionsv2.ReachOnlyLiquidLegionsV2EncryptionUtility

@RunWith(JUnit4::class)
class ReachOnlyLiquidLegionsV2EncryptionUtilityTest {

  //  Helper function to go through the entire Liquid Legions V2 protocol using the input data.
  //  The final relative_frequency_distribution map are returned.
  private fun goThroughEntireMpcProtocol(
    encrypted_sketch: ByteString
  ): CompleteReachOnlyExecutionPhaseAtAggregatorResponse {
    // Setup phase at Duchy 1 (NON_AGGREGATOR). Duchy 1 receives all the sketches.
    val completeReachOnlySetupPhaseRequest1 = completeReachOnlySetupPhaseRequest {
      combinedRegisterVector = encrypted_sketch
      curveId = CURVE_ID
      compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
      parallelism = PARALLELISM
    }
    val completeReachOnlySetupPhaseResponse1 =
      CompleteReachOnlySetupPhaseResponse.parseFrom(
        ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlySetupPhase(
          completeReachOnlySetupPhaseRequest1.toByteArray()
        )
      )
    // Setup phase at Duchy 2 (NON_AGGREGATOR). Duchy 2 does not receive any sketche.
    val completeReachOnlySetupPhaseRequest2 = completeReachOnlySetupPhaseRequest {
      curveId = CURVE_ID
      compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
      parallelism = PARALLELISM
    }
    val completeReachOnlySetupPhaseResponse2 =
      CompleteReachOnlySetupPhaseResponse.parseFrom(
        ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlySetupPhase(
          completeReachOnlySetupPhaseRequest2.toByteArray()
        )
      )
    // Setup phase at Duchy 3 (AGGREGATOR). Aggregator receives the combined register vector and
    // the concatenated excessive noise ciphertexts.
    val completeReachOnlySetupPhaseRequest3 = completeReachOnlySetupPhaseRequest {
      combinedRegisterVector =
        completeReachOnlySetupPhaseResponse1.combinedRegisterVector.concat(
          completeReachOnlySetupPhaseResponse2.combinedRegisterVector
        )
      curveId = CURVE_ID
      compositeElGamalPublicKey = CLIENT_EL_GAMAL_KEYS
      serializedExcessiveNoiseCiphertext =
        completeReachOnlySetupPhaseResponse1.serializedExcessiveNoiseCiphertext.concat(
          completeReachOnlySetupPhaseResponse2.serializedExcessiveNoiseCiphertext
        )
      parallelism = PARALLELISM
    }
    val completeReachOnlySetupPhaseResponse3 =
      CompleteReachOnlySetupPhaseResponse.parseFrom(
        ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlySetupPhase(
          completeReachOnlySetupPhaseRequest3.toByteArray()
        )
      )

    // Execution phase at duchy 1 (non-aggregator).
    val completeReachOnlyExecutionPhaseRequest1 = completeReachOnlyExecutionPhaseRequest {
      combinedRegisterVector = completeReachOnlySetupPhaseResponse3.combinedRegisterVector
      localElGamalKeyPair = DUCHY_1_EL_GAMAL_KEYS
      curveId = CURVE_ID
      serializedExcessiveNoiseCiphertext =
        completeReachOnlySetupPhaseResponse3.serializedExcessiveNoiseCiphertext
      parallelism = PARALLELISM
    }
    val completeReachOnlyExecutionPhaseResponse1 =
      CompleteReachOnlyExecutionPhaseResponse.parseFrom(
        ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyExecutionPhase(
          completeReachOnlyExecutionPhaseRequest1.toByteArray()
        )
      )

    // Execution phase at duchy 2 (non-aggregator).
    val completeReachOnlyExecutionPhaseRequest2 = completeReachOnlyExecutionPhaseRequest {
      combinedRegisterVector = completeReachOnlyExecutionPhaseResponse1.combinedRegisterVector
      localElGamalKeyPair = DUCHY_2_EL_GAMAL_KEYS
      curveId = CURVE_ID
      serializedExcessiveNoiseCiphertext =
        completeReachOnlyExecutionPhaseResponse1.serializedExcessiveNoiseCiphertext
      parallelism = PARALLELISM
    }
    val completeReachOnlyExecutionPhaseResponse2 =
      CompleteReachOnlyExecutionPhaseResponse.parseFrom(
        ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyExecutionPhase(
          completeReachOnlyExecutionPhaseRequest2.toByteArray()
        )
      )

    // Execution phase at duchy 3 (aggregator).
    val completeReachOnlyExecutionPhaseAtAggregatorRequest =
      completeReachOnlyExecutionPhaseAtAggregatorRequest {
        combinedRegisterVector = completeReachOnlyExecutionPhaseResponse2.combinedRegisterVector
        localElGamalKeyPair = DUCHY_3_EL_GAMAL_KEYS
        curveId = CURVE_ID
        serializedExcessiveNoiseCiphertext =
          completeReachOnlyExecutionPhaseResponse2.serializedExcessiveNoiseCiphertext
        parallelism = PARALLELISM
        sketchParameters = liquidLegionsSketchParameters {
          decayRate = DECAY_RATE
          size = LIQUID_LEGIONS_SIZE
        }
        vidSamplingIntervalWidth = VID_SAMPLING_INTERVAL_WIDTH
        parallelism = PARALLELISM
      }
    return CompleteReachOnlyExecutionPhaseAtAggregatorResponse.parseFrom(
      ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyExecutionPhaseAtAggregator(
        completeReachOnlyExecutionPhaseAtAggregatorRequest.toByteArray()
      )
    )
  }

  @Test
  fun endToEnd_basicBehavior() {
    val rawSketch = sketch {
      registers += SketchKt.register { index = 1L }
      registers += SketchKt.register { index = 2L }
      registers += SketchKt.register { index = 2L }
      registers += SketchKt.register { index = 3L }
      registers += SketchKt.register { index = 4L }
    }
    val request = encryptSketchRequest {
      sketch = rawSketch
      curveId = CURVE_ID
      maximumValue = MAX_COUNTER_VALUE
      elGamalKeys = CLIENT_EL_GAMAL_KEYS.toAnySketchElGamalPublicKey()
      destroyedRegisterStrategy = FLAGGED_KEY
    }
    val response =
      EncryptSketchResponse.parseFrom(SketchEncrypterAdapter.EncryptSketch(request.toByteArray()))
    val encryptedSketch = response.encryptedSketch
    val result = goThroughEntireMpcProtocol(encryptedSketch).reach
    val expectedResult =
      Estimators.EstimateCardinalityLiquidLegions(
        DECAY_RATE,
        LIQUID_LEGIONS_SIZE,
        4,
        VID_SAMPLING_INTERVAL_WIDTH.toDouble(),
      )
    assertEquals(expectedResult, result)
  }

  @Test
  fun `completeReachOnlySetupPhase fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlySetupPhase(
          "something not a proto".toByteArray()
        )
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  @Test
  fun `completeReachOnlyExecutionPhase fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyExecutionPhase(
          "something not a proto".toByteArray()
        )
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  @Test
  fun `completeReachOnlyExecutionPhaseAtAggregator fails with invalid request message`() {
    val exception =
      assertFailsWith(RuntimeException::class) {
        ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyExecutionPhaseAtAggregator(
          "something not a proto".toByteArray()
        )
      }
    assertThat(exception).hasMessageThat().contains("Failed to parse")
  }

  companion object {
    init {
      System.loadLibrary("reach_only_liquid_legions_v2_encryption_utility")
      System.loadLibrary("sketch_encrypter_adapter")
      System.loadLibrary("estimators")
    }

    private const val DECAY_RATE = 12.0
    private const val LIQUID_LEGIONS_SIZE = 100_000L
    private const val MAXIMUM_FREQUENCY = 10
    private const val VID_SAMPLING_INTERVAL_WIDTH = 0.5f

    private const val CURVE_ID = 415L // NID_X9_62_prime256v1
    private const val PARALLELISM = 3
    private const val MAX_COUNTER_VALUE = 10

    private val COMPLETE_INITIALIZATION_REQUEST = completeReachOnlyInitializationPhaseRequest {
      curveId = CURVE_ID
    }
    private val DUCHY_1_EL_GAMAL_KEYS =
      CompleteReachOnlyInitializationPhaseResponse.parseFrom(
          ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyInitializationPhase(
            COMPLETE_INITIALIZATION_REQUEST.toByteArray()
          )
        )
        .elGamalKeyPair
    private val DUCHY_2_EL_GAMAL_KEYS =
      CompleteReachOnlyInitializationPhaseResponse.parseFrom(
          ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyInitializationPhase(
            COMPLETE_INITIALIZATION_REQUEST.toByteArray()
          )
        )
        .elGamalKeyPair
    private val DUCHY_3_EL_GAMAL_KEYS =
      CompleteReachOnlyInitializationPhaseResponse.parseFrom(
          ReachOnlyLiquidLegionsV2EncryptionUtility.completeReachOnlyInitializationPhase(
            COMPLETE_INITIALIZATION_REQUEST.toByteArray()
          )
        )
        .elGamalKeyPair

    private val CLIENT_EL_GAMAL_KEYS =
      CombineElGamalPublicKeysResponse.parseFrom(
          SketchEncrypterAdapter.CombineElGamalPublicKeys(
            combineElGamalPublicKeysRequest {
                curveId = CURVE_ID
                elGamalKeys += DUCHY_1_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey()
                elGamalKeys += DUCHY_2_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey()
                elGamalKeys += DUCHY_3_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey()
              }
              .toByteArray()
          )
        )
        .elGamalKeys
        .toCmmsElGamalPublicKey()
    private val DUCHY_2_3_COMBINED_EL_GAMAL_KEYS =
      CombineElGamalPublicKeysResponse.parseFrom(
          SketchEncrypterAdapter.CombineElGamalPublicKeys(
            combineElGamalPublicKeysRequest {
                curveId = CURVE_ID
                elGamalKeys += DUCHY_2_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey()
                elGamalKeys += DUCHY_3_EL_GAMAL_KEYS.publicKey.toAnySketchElGamalPublicKey()
              }
              .toByteArray()
          )
        )
        .elGamalKeys
        .toCmmsElGamalPublicKey()
  }
}
