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

package org.wfanet.measurement.duchy.mill.liquidlegionsv2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.Base64
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.stub
import org.mockito.kotlin.whenever
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey as V2AlphaElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.MillBase
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto.LiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newInputBlobMetadata
import org.wfanet.measurement.duchy.storage.ComputationBlobContext
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionBlobContext
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.testing.TestRequisition
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.duchy.utils.toDuchyEncryptionPublicKey
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt.kingdomComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.ElGamalKeyPair
import org.wfanet.measurement.internal.duchy.ElGamalPublicKey
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.elGamalKeyPair
import org.wfanet.measurement.internal.duchy.elGamalPublicKey
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
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant as InternalComputationParticipant
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.COMPLETE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_TWO
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.parameters
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsV2NoiseConfig
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseOneAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseOneAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseOneRequest
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseThreeRequest
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseTwoAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseTwoAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseTwoRequest
import org.wfanet.measurement.internal.duchy.protocol.completeExecutionPhaseTwoResponse
import org.wfanet.measurement.internal.duchy.protocol.completeInitializationPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.completeSetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeSetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.copy
import org.wfanet.measurement.internal.duchy.protocol.flagCountTupleNoiseGenerationParameters
import org.wfanet.measurement.internal.duchy.protocol.globalReachDpNoiseBaseline
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsSketchParameters
import org.wfanet.measurement.internal.duchy.protocol.reachNoiseDifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.protocol.registerNoiseGenerationParameters
import org.wfanet.measurement.storage.Store.Blob
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationResponse
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase as SystemComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub as SystemComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntry
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as SystemComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase as SystemComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.FailComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2.Description.SETUP_PHASE_INPUT
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.SetComputationResultRequest
import org.wfanet.measurement.system.v1alpha.SetParticipantRequisitionParamsRequest
import org.wfanet.measurement.system.v1alpha.computationParticipant
import org.wfanet.measurement.system.v1alpha.computationStage
import org.wfanet.measurement.system.v1alpha.confirmComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.copy
import org.wfanet.measurement.system.v1alpha.liquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.setComputationResultRequest

private const val PUBLIC_API_VERSION = "v2alpha"

private const val WORKER_COUNT = 3
private const val MILL_ID = "a nice mill"
private const val DUCHY_ONE_NAME = "DUCHY_ONE"
private const val DUCHY_TWO_NAME = "DUCHY_TWO"
private const val DUCHY_THREE_NAME = "DUCHY_THREE"
private const val MAX_FREQUENCY = 15
private const val DECAY_RATE = 12.0
private const val SKETCH_SIZE = 100_000L
private const val CURVE_ID = 415L // NID_X9_62_prime256v1
private const val PARALLELISM = 2

private const val LOCAL_ID = 1234L
private const val GLOBAL_ID = LOCAL_ID.toString()

private val DUCHY_ONE_KEY_PAIR =
  ElGamalKeyPair.newBuilder()
    .apply {
      publicKeyBuilder.apply {
        generator = ByteString.copyFromUtf8("generator_1")
        element = ByteString.copyFromUtf8("element_1")
      }
      secretKey = ByteString.copyFromUtf8("secret_key_1")
    }
    .build()
private val DUCHY_TWO_PUBLIC_KEY =
  ElGamalPublicKey.newBuilder()
    .apply {
      generator = ByteString.copyFromUtf8("generator_2")
      element = ByteString.copyFromUtf8("element_2")
    }
    .build()
private val DUCHY_THREE_PUBLIC_KEY =
  ElGamalPublicKey.newBuilder()
    .apply {
      generator = ByteString.copyFromUtf8("generator_3")
      element = ByteString.copyFromUtf8("element_3")
    }
    .build()
private val COMBINED_PUBLIC_KEY =
  ElGamalPublicKey.newBuilder()
    .apply {
      generator = ByteString.copyFromUtf8("generator_1_generator_2_generator_3")
      element = ByteString.copyFromUtf8("element_1_element_2_element_3")
    }
    .build()
private val PARTIALLY_COMBINED_PUBLIC_KEY =
  ElGamalPublicKey.newBuilder()
    .apply {
      generator = ByteString.copyFromUtf8("generator_2_generator_3")
      element = ByteString.copyFromUtf8("element_2_element_3")
    }
    .build()

private val TEST_NOISE_CONFIG =
  LiquidLegionsV2NoiseConfig.newBuilder()
    .apply {
      reachNoiseConfigBuilder.apply {
        blindHistogramNoiseBuilder.apply {
          epsilon = 1.0
          delta = 2.0
        }
        noiseForPublisherNoiseBuilder.apply {
          epsilon = 3.0
          delta = 4.0
        }
        globalReachDpNoiseBuilder.apply {
          epsilon = 5.0
          delta = 6.0
        }
      }
      frequencyNoiseConfigBuilder.apply {
        epsilon = 7.0
        delta = 8.0
      }
    }
    .build()

private val LLV2_PARAMETERS = parameters {
  maximumFrequency = MAX_FREQUENCY
  sketchParameters = liquidLegionsSketchParameters {
    decayRate = DECAY_RATE
    size = SKETCH_SIZE
  }
  noise = TEST_NOISE_CONFIG
  ellipticCurveId = CURVE_ID.toInt()
}
private val LLV2_PARAMETERS_FREQUENCY_ONE = LLV2_PARAMETERS.copy { maximumFrequency = 1 }
private val LLV2_PARAMETERS_REACH =
  LLV2_PARAMETERS.copy {
    clearMaximumFrequency()
    noise = noise.copy { clearFrequencyNoiseConfig() }
  }

// In the test, use the same set of cert and encryption key for all parties.
private const val CONSENT_SIGNALING_CERT_NAME = "Just a name"
private val CONSENT_SIGNALING_CERT_DER =
  TestData.FIXED_SERVER_CERT_DER_FILE.readBytes().toByteString()
private val CONSENT_SIGNALING_PRIVATE_KEY_DER =
  TestData.FIXED_SERVER_KEY_DER_FILE.readBytes().toByteString()
private val CONSENT_SIGNALING_ROOT_CERT: X509Certificate =
  readCertificate(TestData.FIXED_CA_CERT_PEM_FILE)
private val ENCRYPTION_PRIVATE_KEY = TinkPrivateKeyHandle.generateEcies()
private val ENCRYPTION_PUBLIC_KEY: org.wfanet.measurement.api.v2alpha.EncryptionPublicKey =
  ENCRYPTION_PRIVATE_KEY.publicKey.toEncryptionPublicKey()

/** A public Key used for consent signaling check. */
private val CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY =
  V2AlphaElGamalPublicKey.newBuilder()
    .apply {
      generator = ByteString.copyFromUtf8("generator-foo")
      element = ByteString.copyFromUtf8("element-foo")
    }
    .build()
/** A pre-computed signature of the CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY. */
private val CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY_SINGATURE =
  ByteString.copyFrom(
    Base64.getDecoder()
      .decode(
        "MEUCIB2HWi/udHE9YlCH6n9lnGY12I9F1ra1SRWoJrIOXGiAAiEAm90wrJRqABFC5sjej+" +
          "zjSBOMHcmFOEpfW9tXaCla6Qs="
      )
  )

private val COMPUTATION_PARTICIPANT_1 =
  InternalComputationParticipant.newBuilder()
    .apply {
      duchyId = DUCHY_ONE_NAME
      publicKey = DUCHY_ONE_KEY_PAIR.publicKey
      elGamalPublicKey = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY.toByteString()
      elGamalPublicKeySignature = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY_SINGATURE
      duchyCertificateDer = CONSENT_SIGNALING_CERT_DER
    }
    .build()
private val COMPUTATION_PARTICIPANT_2 =
  InternalComputationParticipant.newBuilder()
    .apply {
      duchyId = DUCHY_TWO_NAME
      publicKey = DUCHY_TWO_PUBLIC_KEY
      elGamalPublicKey = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY.toByteString()
      elGamalPublicKeySignature = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY_SINGATURE
      duchyCertificateDer = CONSENT_SIGNALING_CERT_DER
    }
    .build()
private val COMPUTATION_PARTICIPANT_3 =
  InternalComputationParticipant.newBuilder()
    .apply {
      duchyId = DUCHY_THREE_NAME
      publicKey = DUCHY_THREE_PUBLIC_KEY
      elGamalPublicKey = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY.toByteString()
      elGamalPublicKeySignature = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY_SINGATURE
      duchyCertificateDer = CONSENT_SIGNALING_CERT_DER
    }
    .build()

private val TEST_REQUISITION_1 = TestRequisition("111") { SERIALIZED_MEASUREMENT_SPEC }
private val TEST_REQUISITION_2 = TestRequisition("222") { SERIALIZED_MEASUREMENT_SPEC }
private val TEST_REQUISITION_3 = TestRequisition("333") { SERIALIZED_MEASUREMENT_SPEC }

private val MEASUREMENT_SPEC = measurementSpec {
  nonceHashes += TEST_REQUISITION_1.nonceHash
  nonceHashes += TEST_REQUISITION_2.nonceHash
  nonceHashes += TEST_REQUISITION_3.nonceHash
}
private val SERIALIZED_MEASUREMENT_SPEC: ByteString = MEASUREMENT_SPEC.toByteString()

private val MEASUREMENT_SPEC_WITH_VID_SAMPLING_WIDTH = measurementSpec {
  nonceHashes += TEST_REQUISITION_1.nonceHash
  nonceHashes += TEST_REQUISITION_2.nonceHash
  nonceHashes += TEST_REQUISITION_3.nonceHash
  vidSamplingInterval = vidSamplingInterval { width = 0.5f }
}
private val SERIALIZED_MEASUREMENT_SPEC_WITH_VID_SAMPLING_WIDTH =
  MEASUREMENT_SPEC_WITH_VID_SAMPLING_WIDTH.toByteString()

private val REQUISITION_1 =
  TEST_REQUISITION_1.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_ONE_NAME).copy {
    path = RequisitionBlobContext(GLOBAL_ID, externalKey.externalRequisitionId).blobKey
  }
private val REQUISITION_2 =
  TEST_REQUISITION_2.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_TWO_NAME)
private val REQUISITION_3 =
  TEST_REQUISITION_3.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_THREE_NAME)
private val REQUISITIONS = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3)

private val AGGREGATOR_COMPUTATION_DETAILS = computationDetails {
  kingdomComputation = kingdomComputationDetails {
    publicApiVersion = PUBLIC_API_VERSION
    measurementPublicKey = ENCRYPTION_PUBLIC_KEY.toDuchyEncryptionPublicKey()
    measurementSpec = SERIALIZED_MEASUREMENT_SPEC
    participantCount = 3
  }
  liquidLegionsV2 =
    LiquidLegionsSketchAggregationV2Kt.computationDetails {
      role = RoleInComputation.AGGREGATOR
      parameters = LLV2_PARAMETERS
      participant +=
        listOf(COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3, COMPUTATION_PARTICIPANT_1)
      combinedPublicKey = COMBINED_PUBLIC_KEY
      // partiallyCombinedPublicKey and combinedPublicKey are the same at the aggregator.
      partiallyCombinedPublicKey = COMBINED_PUBLIC_KEY
      localElgamalKey = DUCHY_ONE_KEY_PAIR
    }
}

private val AGGREGATOR_COMPUTATION_DETAILS_FREQUENCY_ONE =
  AGGREGATOR_COMPUTATION_DETAILS.copy {
    liquidLegionsV2 = liquidLegionsV2.copy { parameters = LLV2_PARAMETERS_FREQUENCY_ONE }
  }

private val AGGREGATOR_COMPUTATION_DETAILS_REACH =
  AGGREGATOR_COMPUTATION_DETAILS.copy {
    liquidLegionsV2 = liquidLegionsV2.copy { parameters = LLV2_PARAMETERS_REACH }
  }

private val NON_AGGREGATOR_COMPUTATION_DETAILS =
  AGGREGATOR_COMPUTATION_DETAILS.copy {
    liquidLegionsV2 =
      liquidLegionsV2.copy {
        role = RoleInComputation.NON_AGGREGATOR
        participant.clear()
        participant +=
          listOf(COMPUTATION_PARTICIPANT_1, COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3)
        partiallyCombinedPublicKey = PARTIALLY_COMBINED_PUBLIC_KEY
      }
  }

private val NON_AGGREGATOR_COMPUTATION_DETAILS_FREQUENCY_ONE =
  NON_AGGREGATOR_COMPUTATION_DETAILS.copy {
    liquidLegionsV2 = liquidLegionsV2.copy { parameters = LLV2_PARAMETERS_FREQUENCY_ONE }
  }

private val NON_AGGREGATOR_COMPUTATION_DETAILS_REACH =
  NON_AGGREGATOR_COMPUTATION_DETAILS.copy {
    liquidLegionsV2 = liquidLegionsV2.copy { parameters = LLV2_PARAMETERS_REACH }
  }

@RunWith(JUnit4::class)
class ReachFrequencyLiquidLegionsV2MillTest {
  private val mockLiquidLegionsComputationControl: ComputationControlCoroutineImplBase =
    mockService {
      onBlocking { advanceComputation(any()) }
        .thenAnswer {
          val request: Flow<AdvanceComputationRequest> = it.getArgument(0)
          computationControlRequests = runBlocking { request.toList() }
          AdvanceComputationResponse.getDefaultInstance()
        }
    }
  private val mockSystemComputations: SystemComputationsCoroutineImplBase = mockService()
  private val mockComputationParticipants: SystemComputationParticipantsCoroutineImplBase =
    mockService()
  private val mockComputationLogEntries: SystemComputationLogEntriesCoroutineImplBase =
    mockService()
  private val mockComputationStats: ComputationStatsCoroutineImplBase = mockService()
  private val mockCryptoWorker: LiquidLegionsV2Encryption =
    mock(useConstructor = UseConstructor.parameterless()) {
      on { combineElGamalPublicKeys(any()) }
        .thenAnswer {
          val cryptoRequest: CombineElGamalPublicKeysRequest = it.getArgument(0)
          CombineElGamalPublicKeysResponse.newBuilder()
            .apply {
              elGamalKeysBuilder.apply {
                generator =
                  ByteString.copyFromUtf8(
                    cryptoRequest.elGamalKeysList
                      .sortedBy { key -> key.generator.toStringUtf8() }
                      .joinToString(separator = "_") { key -> key.generator.toStringUtf8() }
                  )
                element =
                  ByteString.copyFromUtf8(
                    cryptoRequest.elGamalKeysList
                      .sortedBy { key -> key.element.toStringUtf8() }
                      .joinToString(separator = "_") { key -> key.element.toStringUtf8() }
                  )
              }
            }
            .build()
        }
    }
  private val fakeComputationDb = FakeComputationsDatabase()

  private lateinit var computationDataClients: ComputationDataClients
  private lateinit var computationStore: ComputationStore
  private lateinit var requisitionStore: RequisitionStore

  private val tempDirectory = TemporaryFolder()

  private val grpcTestServerRule =
    GrpcTestServerRule(defaultServiceConfig = MillBase.SERVICE_CONFIG) {
      val storageClient = FileSystemStorageClient(tempDirectory.root)
      computationStore = ComputationStore(storageClient)
      requisitionStore = RequisitionStore(storageClient)
      computationDataClients =
        ComputationDataClients.forTesting(
          ComputationsCoroutineStub(channel),
          computationStore,
          requisitionStore,
        )
      addService(mockLiquidLegionsComputationControl)
      addService(mockSystemComputations)
      addService(mockComputationLogEntries)
      addService(mockComputationParticipants)
      addService(mockComputationStats)
      addService(
        ComputationsService(
          fakeComputationDb,
          systemComputationLogEntriesStub,
          computationStore,
          requisitionStore,
          DUCHY_THREE_NAME,
          clock = Clock.systemUTC(),
        )
      )
    }

  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  private val workerStub: ComputationControlCoroutineStub by lazy {
    ComputationControlCoroutineStub(grpcTestServerRule.channel)
  }

  private val systemComputationStub: SystemComputationsCoroutineStub by lazy {
    SystemComputationsCoroutineStub(grpcTestServerRule.channel)
  }

  private val systemComputationLogEntriesStub: SystemComputationLogEntriesCoroutineStub by lazy {
    SystemComputationLogEntriesCoroutineStub(grpcTestServerRule.channel)
  }

  private val systemComputationParticipantsStub:
    SystemComputationParticipantsCoroutineStub by lazy {
    SystemComputationParticipantsCoroutineStub(grpcTestServerRule.channel)
  }

  private val computationStatsStub: ComputationStatsCoroutineStub by lazy {
    ComputationStatsCoroutineStub(grpcTestServerRule.channel)
  }

  private var computationControlRequests: List<AdvanceComputationRequest> = emptyList()

  // Just use the same workerStub for all other duchies, since it is not relevant to this test.
  private val workerStubs = mapOf(DUCHY_TWO_NAME to workerStub, DUCHY_THREE_NAME to workerStub)

  private lateinit var aggregatorMill: ReachFrequencyLiquidLegionsV2Mill
  private lateinit var nonAggregatorMill: ReachFrequencyLiquidLegionsV2Mill

  private fun buildAdvanceComputationRequests(
    globalComputationId: String,
    description: LiquidLegionsV2.Description,
    vararg chunkContents: String,
  ): List<AdvanceComputationRequest> {
    val header =
      AdvanceComputationRequest.newBuilder()
        .apply {
          headerBuilder.apply {
            name = ComputationKey(globalComputationId).toName()
            liquidLegionsV2Builder.description = description
          }
        }
        .build()
    val body =
      chunkContents.asList().map {
        AdvanceComputationRequest.newBuilder()
          .apply { bodyChunkBuilder.apply { partialData = ByteString.copyFromUtf8(it) } }
          .build()
      }
    return listOf(header) + body
  }

  @Before
  fun initializeMill() = runBlocking {
    DuchyInfo.setForTest(setOf(DUCHY_ONE_NAME, DUCHY_TWO_NAME, DUCHY_THREE_NAME))
    val csX509Certificate = readCertificate(CONSENT_SIGNALING_CERT_DER)
    val csSigningKey =
      SigningKeyHandle(
        csX509Certificate,
        readPrivateKey(CONSENT_SIGNALING_PRIVATE_KEY_DER, csX509Certificate.publicKey.algorithm),
      )
    val csCertificate = Certificate(CONSENT_SIGNALING_CERT_NAME, csX509Certificate)
    val trustedCertificates =
      DuchyInfo.entries.values.associateBy(
        { it.rootCertificateSkid },
        { CONSENT_SIGNALING_ROOT_CERT },
      )

    aggregatorMill =
      ReachFrequencyLiquidLegionsV2Mill(
        millId = MILL_ID,
        duchyId = DUCHY_ONE_NAME,
        signingKey = csSigningKey,
        consentSignalCert = csCertificate,
        trustedCertificates = trustedCertificates,
        dataClients = computationDataClients,
        systemComputationParticipantsClient = systemComputationParticipantsStub,
        systemComputationsClient = systemComputationStub,
        systemComputationLogEntriesClient = systemComputationLogEntriesStub,
        computationStatsClient = computationStatsStub,
        workerStubs = workerStubs,
        cryptoWorker = mockCryptoWorker,
        workLockDuration = Duration.ofMinutes(5),
        requestChunkSizeBytes = 20,
        maximumAttempts = 2,
        parallelism = PARALLELISM,
      )
    nonAggregatorMill =
      ReachFrequencyLiquidLegionsV2Mill(
        millId = MILL_ID,
        duchyId = DUCHY_ONE_NAME,
        signingKey = csSigningKey,
        consentSignalCert = csCertificate,
        trustedCertificates = trustedCertificates,
        dataClients = computationDataClients,
        systemComputationParticipantsClient = systemComputationParticipantsStub,
        systemComputationsClient = systemComputationStub,
        systemComputationLogEntriesClient = systemComputationLogEntriesStub,
        computationStatsClient = computationStatsStub,
        workerStubs = workerStubs,
        cryptoWorker = mockCryptoWorker,
        workLockDuration = Duration.ofMinutes(5),
        requestChunkSizeBytes = 20,
        maximumAttempts = 2,
        parallelism = PARALLELISM,
      )
  }

  @Test
  fun `exceeding max attempt should fail the computation`() = runBlocking {
    // Stage 0. preparing the database and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = INITIALIZATION_PHASE.toProtocolStage(),
        )
        .build()
    val initialComputationDetails =
      NON_AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply {
          liquidLegionsV2Builder.apply {
            parametersBuilder.ellipticCurveId = CURVE_ID.toInt()
            clearPartiallyCombinedPublicKey()
            clearCombinedPublicKey()
            clearLocalElgamalKey()
          }
        }
        .build()
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = initialComputationDetails,
      requisitions = REQUISITIONS,
    )
    // Simulate multiple attempts.
    repeat(2) {
      fakeComputationDb.claimTask(
        ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
        MILL_ID,
        Duration.ZERO,
      )
      fakeComputationDb.claimedComputations.clear()
    }

    nonAggregatorMill.claimAndProcessWork()

    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 3
          computationStage = COMPLETE.toProtocolStage()
          version = 4
          computationDetails =
            initialComputationDetails.copy { endingState = CompletedReason.FAILED }
          requisitions += REQUISITIONS
        }
      )

    assertThat(fakeComputationDb.claimedComputations).isEmpty()
  }

  @Test
  fun `initialization phase has higher priority to be claimed`() = runBlocking {
    fakeComputationDb.addComputation(
      1L,
      EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
      blobs = listOf(newEmptyOutputBlobMetadata(1)),
    )
    fakeComputationDb.addComputation(
      2L,
      INITIALIZATION_PHASE.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
    )

    whenever(mockCryptoWorker.completeInitializationPhase(any())).thenAnswer {
      completeInitializationPhaseResponse {
        elGamalKeyPair = elGamalKeyPair {
          publicKey = elGamalPublicKey {
            generator = ByteString.copyFromUtf8("generator-foo")
            element = ByteString.copyFromUtf8("element-foo")
          }
          secretKey = ByteString.copyFromUtf8("secretKey-foo")
        }
      }
    }

    // Mill should claim computation1 of INITIALIZATION_PHASE.
    nonAggregatorMill.claimAndProcessWork()

    assertThat(fakeComputationDb[2]!!.computationStage)
      .isEqualTo(WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage())
    assertThat(fakeComputationDb[1]!!.computationStage)
      .isEqualTo(EXECUTION_PHASE_ONE.toProtocolStage())
  }

  @Test
  fun `initialization phase`() = runBlocking {
    // Stage 0. preparing the database and set up mock
    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.CREATED
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)

    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = INITIALIZATION_PHASE.toProtocolStage(),
        )
        .build()

    val initialComputationDetails =
      NON_AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply {
          liquidLegionsV2Builder.apply {
            parametersBuilder.ellipticCurveId = CURVE_ID.toInt()
            clearPartiallyCombinedPublicKey()
            clearCombinedPublicKey()
            clearLocalElgamalKey()
          }
        }
        .build()

    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = initialComputationDetails,
      requisitions = REQUISITIONS,
    )

    var cryptoRequest = CompleteInitializationPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeInitializationPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      CompleteInitializationPhaseResponse.newBuilder()
        .apply {
          elGamalKeyPairBuilder.apply {
            publicKeyBuilder.apply {
              generator = ByteString.copyFromUtf8("generator-foo")
              element = ByteString.copyFromUtf8("element-foo")
            }
            secretKey = ByteString.copyFromUtf8("secretKey-foo")
          }
        }
        .build()
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage()
            version = 3 // claimTask + updateComputationDetails + transitionStage
            computationDetails =
              initialComputationDetails
                .toBuilder()
                .apply {
                  liquidLegionsV2Builder.localElgamalKeyBuilder.apply {
                    publicKeyBuilder.apply {
                      generator = ByteString.copyFromUtf8("generator-foo")
                      element = ByteString.copyFromUtf8("element-foo")
                    }
                    secretKey = ByteString.copyFromUtf8("secretKey-foo")
                  }
                }
                .build()
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::setParticipantRequisitionParams,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        SetParticipantRequisitionParamsRequest.newBuilder()
          .apply {
            name = computationParticipant.name
            etag = computationParticipant.etag
            requisitionParamsBuilder.apply {
              duchyCertificate = CONSENT_SIGNALING_CERT_NAME
              liquidLegionsV2Builder.apply {
                elGamalPublicKey = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY.toByteString()
              }
            }
          }
          .build()
      )

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteInitializationPhaseRequest.newBuilder().apply { curveId = CURVE_ID }.build()
      )
  }

  @Test
  fun `initializationPhase retries sending requisition params when aborted`(): Unit = runTest {
    // Stage 0. preparing the database and set up mock
    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.CREATED
    }
    mockComputationParticipants.stub {
      onBlocking { getComputationParticipant(any()) }
        .thenReturn(computationParticipant)
        .thenReturn(
          computationParticipant.copy {
            etag = "entity tag 2"
            state = ComputationParticipant.State.REQUISITION_PARAMS_SET
            requisitionParams = ComputationParticipant.RequisitionParams.getDefaultInstance()
          }
        )
      onBlocking { setParticipantRequisitionParams(any()) }
        .thenThrow(Status.ABORTED.asRuntimeException())
    }

    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = INITIALIZATION_PHASE.toProtocolStage(),
        )
        .build()

    val initialComputationDetails =
      NON_AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply {
          liquidLegionsV2Builder.apply {
            parametersBuilder.ellipticCurveId = CURVE_ID.toInt()
            clearPartiallyCombinedPublicKey()
            clearCombinedPublicKey()
            clearLocalElgamalKey()
          }
        }
        .build()

    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = initialComputationDetails,
      requisitions = REQUISITIONS,
    )

    whenever(mockCryptoWorker.completeInitializationPhase(any())).thenAnswer {
      CompleteInitializationPhaseResponse.newBuilder()
        .apply {
          elGamalKeyPairBuilder.apply {
            publicKeyBuilder.apply {
              generator = ByteString.copyFromUtf8("generator-foo")
              element = ByteString.copyFromUtf8("element-foo")
            }
            secretKey = ByteString.copyFromUtf8("secretKey-foo")
          }
        }
        .build()
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.claimAndProcessWork()
    testScheduler.advanceUntilIdle()

    // Stage 3. Verify stage was advanced successfully.
    assertThat(fakeComputationDb[LOCAL_ID]?.computationStage)
      .isEqualTo(WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage())
  }

  @Test
  fun `confirmation phase, failed due to missing local requisition`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.REQUISITION_PARAMS_SET
      requisitionParams = ComputationParticipant.RequisitionParams.getDefaultInstance()
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)
    val requisition1 = REQUISITION_1
    // requisition2 is fulfilled at Duchy One, but doesn't have path set.
    val requisition2 =
      REQUISITION_2.copy { details = details.copy { externalFulfillingDuchyId = DUCHY_ONE_NAME } }
    val computationDetailsWithoutPublicKey =
      AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply { liquidLegionsV2Builder.clearCombinedPublicKey().clearPartiallyCombinedPublicKey() }
        .build()
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = CONFIRMATION_PHASE.toProtocolStage(),
      computationDetails = computationDetailsWithoutPublicKey,
      requisitions = listOf(requisition1, requisition2),
    )

    whenever(mockComputationLogEntries.createComputationLogEntry(any()))
      .thenReturn(ComputationLogEntry.getDefaultInstance())

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID]!!)
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = COMPLETE.toProtocolStage()
            version = 2 // claimTask + transitionStage
            computationDetails =
              computationDetailsWithoutPublicKey
                .toBuilder()
                .apply { endingState = CompletedReason.FAILED }
                .build()
            addAllRequisitions(listOf(requisition1, requisition2))
          }
          .build()
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::failComputationParticipant,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        FailComputationParticipantRequest.newBuilder()
          .apply {
            name = computationParticipant.name
            etag = computationParticipant.etag
            failureBuilder.apply {
              participantChildReferenceId = MILL_ID
              errorMessage =
                """
                @Mill a nice mill, Computation 1234 failed due to:
                Cannot verify participation of all DataProviders.
                Missing expected data for requisition 222.
                """
                  .trimIndent()
              stageAttemptBuilder.apply {
                stage = CONFIRMATION_PHASE.number
                stageName = CONFIRMATION_PHASE.name
                attemptNumber = 1
              }
            }
          }
          .build()
      )
  }

  @Test
  fun `confirmation phase, passed at non-aggregator`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val computationDetailsWithoutPublicKey =
      NON_AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply { liquidLegionsV2Builder.clearCombinedPublicKey().clearPartiallyCombinedPublicKey() }
        .build()
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = CONFIRMATION_PHASE.toProtocolStage(),
      computationDetails = computationDetailsWithoutPublicKey,
      requisitions = REQUISITIONS,
    )

    whenever(mockComputationLogEntries.createComputationLogEntry(any()))
      .thenReturn(ComputationLogEntry.getDefaultInstance())

    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.REQUISITION_PARAMS_SET
      requisitionParams = ComputationParticipant.RequisitionParams.getDefaultInstance()
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID]!!)
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_TO_START.toProtocolStage()
            version = 3 // claimTask + updateComputationDetail + transitionStage
            computationDetails =
              NON_AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
                .apply {
                  liquidLegionsV2Builder.apply {
                    combinedPublicKey = COMBINED_PUBLIC_KEY
                    partiallyCombinedPublicKey = PARTIALLY_COMBINED_PUBLIC_KEY
                  }
                }
                .build()
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::confirmComputationParticipant,
      )
      .isEqualTo(
        confirmComputationParticipantRequest {
          name = computationParticipant.name
          etag = computationParticipant.etag
        }
      )
  }

  @Test
  fun `confirmation phase, passed at aggregator`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.REQUISITION_PARAMS_SET
      requisitionParams = ComputationParticipant.RequisitionParams.getDefaultInstance()
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)

    val computationDetailsWithoutPublicKey =
      AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply { liquidLegionsV2Builder.clearCombinedPublicKey().clearPartiallyCombinedPublicKey() }
        .build()
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = CONFIRMATION_PHASE.toProtocolStage(),
      computationDetails = computationDetailsWithoutPublicKey,
      requisitions = REQUISITIONS,
    )

    whenever(mockComputationLogEntries.createComputationLogEntry(any()))
      .thenReturn(ComputationLogEntry.getDefaultInstance())

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID]!!)
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
            version = 3 // claimTask + updateComputationDetails + transitionStage
            addAllBlobs(listOf(newEmptyOutputBlobMetadata(0), newEmptyOutputBlobMetadata(1)))
            stageSpecificDetailsBuilder.apply {
              liquidLegionsV2Builder.waitSetupPhaseInputsDetailsBuilder.apply {
                putExternalDuchyLocalBlobId("DUCHY_TWO", 0L)
                putExternalDuchyLocalBlobId("DUCHY_THREE", 1L)
              }
            }
            computationDetails =
              AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
                .apply {
                  liquidLegionsV2Builder.apply {
                    combinedPublicKey = COMBINED_PUBLIC_KEY
                    partiallyCombinedPublicKey = COMBINED_PUBLIC_KEY
                  }
                }
                .build()
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::confirmComputationParticipant,
      )
      .isEqualTo(
        confirmComputationParticipantRequest {
          name = computationParticipant.name
          etag = computationParticipant.etag
        }
      )
  }

  @Test
  fun `confirmation phase, failed due to invalid nonce and ElGamal key signature`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.REQUISITION_PARAMS_SET
      requisitionParams = ComputationParticipant.RequisitionParams.getDefaultInstance()
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)

    val computationDetailsWithoutInvalidDuchySignature =
      AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply {
          liquidLegionsV2Builder.apply {
            participantBuilderList[0].apply {
              elGamalPublicKeySignature = ByteString.copyFromUtf8("An invalid signature")
            }
          }
        }
        .build()
    val requisitionWithInvalidNonce = REQUISITION_1.copy { details = details.copy { nonce = 404L } }
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = CONFIRMATION_PHASE.toProtocolStage(),
      computationDetails = computationDetailsWithoutInvalidDuchySignature,
      requisitions = listOf(requisitionWithInvalidNonce),
    )

    whenever(mockComputationLogEntries.createComputationLogEntry(any()))
      .thenReturn(ComputationLogEntry.getDefaultInstance())

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID]!!)
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = COMPLETE.toProtocolStage()
            version = 2 // claimTask + transitionStage
            computationDetails =
              computationDetailsWithoutInvalidDuchySignature
                .toBuilder()
                .apply { endingState = CompletedReason.FAILED }
                .build()
            addAllRequisitions(listOf(requisitionWithInvalidNonce))
          }
          .build()
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::failComputationParticipant,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        FailComputationParticipantRequest.newBuilder()
          .apply {
            name = computationParticipant.name
            etag = computationParticipant.etag
            failureBuilder.apply {
              participantChildReferenceId = MILL_ID
              errorMessage =
                """
                @Mill a nice mill, Computation 1234 failed due to:
                Cannot verify participation of all DataProviders.
                Invalid ElGamal public key signature for Duchy $DUCHY_TWO_NAME
                """
                  .trimIndent()
              stageAttemptBuilder.apply {
                stage = CONFIRMATION_PHASE.number
                stageName = CONFIRMATION_PHASE.name
                attemptNumber = 1
              }
            }
          }
          .build()
      )
  }

  @Test
  fun `setup phase at non-aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = SETUP_PHASE.toProtocolStage(),
        )
        .build()
    val requisitionBlobContext =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
    val calculatedBlobContext = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 1L)
    requisitionStore.writeString(requisitionBlobContext, "local_requisition")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
      blobs = listOf(newEmptyOutputBlobMetadata(calculatedBlobContext.blobId)),
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS
            }
          }
        )
    }

    var cryptoRequest = CompleteSetupPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeSetupPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeSetupPhase-done")
      CompleteSetupPhaseResponse.newBuilder()
        .apply {
          combinedRegisterVector =
            cryptoRequest.requisitionRegisterVector
              .concat(cryptoRequest.combinedRegisterVector)
              .concat(postFix)
        }
        .build()
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.INPUT
              blobId = 0L
              path = blobKey
            }
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1L
            }
            version = 3 // claimTask + writeOutputBlob + transitionStage
            computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS
            addAllRequisitions(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
          }
          .build()
      )

    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("local_requisition-completeSetupPhase-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          SETUP_PHASE_INPUT,
          "local_requisition-co",
          "mpleteSetupPhase-don",
          "e",
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeSetupPhaseRequest {
          requisitionRegisterVector = ByteString.copyFromUtf8("local_requisition")
          combinedRegisterVector = ByteString.EMPTY
          noiseParameters = registerNoiseGenerationParameters {
            compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
            curveId = CURVE_ID
            contributorsCount = WORKER_COUNT
            totalSketchesCount = REQUISITIONS.size
            dpParams = reachNoiseDifferentialPrivacyParams {
              blindHistogram = TEST_NOISE_CONFIG.reachNoiseConfig.blindHistogramNoise
              noiseForPublisherNoise = TEST_NOISE_CONFIG.reachNoiseConfig.noiseForPublisherNoise
              globalReachDpNoise = TEST_NOISE_CONFIG.reachNoiseConfig.globalReachDpNoise
            }
          }
          maximumFrequency = MAX_FREQUENCY
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `setup phase at aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = SETUP_PHASE.toProtocolStage(),
        )
        .build()
    val requisitionBlobContext =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
    requisitionStore.writeString(requisitionBlobContext, "local_requisition_")
    val inputBlob0Context = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 0L)
    computationStore.writeString(inputBlob0Context, "duchy_two_sketch_")
    val inputBlob1Context = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlob1Context, "duchy_three_sketch_")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(
          newInputBlobMetadata(0L, inputBlob0Context.blobKey),
          newInputBlobMetadata(1L, inputBlob1Context.blobKey),
          newEmptyOutputBlobMetadata(3L),
        ),
      requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
            }
          }
        )
    }

    var cryptoRequest = CompleteSetupPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeSetupPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeSetupPhase-done")
      CompleteSetupPhaseResponse.newBuilder()
        .apply {
          combinedRegisterVector =
            cryptoRequest.requisitionRegisterVector
              .concat(cryptoRequest.combinedRegisterVector)
              .concat(postFix)
        }
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 3L).blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.INPUT
              blobId = 0
              path = blobKey
            }
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1
            }
            version = 3 // claimTask + writeOutputBlob + transitionStage
            computationDetails = AGGREGATOR_COMPUTATION_DETAILS
            addAllRequisitions(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
          }
          .build()
      )

    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("local_requisition_duchy_two_sketch_duchy_three_sketch_-completeSetupPhase-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_ONE_INPUT,
          "local_requisition_du",
          "chy_two_sketch_duchy",
          "_three_sketch_-compl",
          "eteSetupPhase-done",
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeSetupPhaseRequest {
          requisitionRegisterVector = ByteString.copyFromUtf8("local_requisition_")
          combinedRegisterVector = ByteString.copyFromUtf8("duchy_two_sketch_duchy_three_sketch_")
          noiseParameters = registerNoiseGenerationParameters {
            compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
            curveId = CURVE_ID
            contributorsCount = WORKER_COUNT
            totalSketchesCount = REQUISITIONS.size
            dpParams = reachNoiseDifferentialPrivacyParams {
              blindHistogram = TEST_NOISE_CONFIG.reachNoiseConfig.blindHistogramNoise
              noiseForPublisherNoise = TEST_NOISE_CONFIG.reachNoiseConfig.noiseForPublisherNoise
              globalReachDpNoise = TEST_NOISE_CONFIG.reachNoiseConfig.globalReachDpNoise
            }
          }
          maximumFrequency = MAX_FREQUENCY
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `setup phase at aggregator using calculated result with fewer participants`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val computationDetails =
      AGGREGATOR_COMPUTATION_DETAILS.copy {
        kingdomComputation =
          kingdomComputation.copy {
            // Indicates that the Kingdom selected only 2 of the 3 Duchies.
            participantCount = 2
          }
      }
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = SETUP_PHASE.toProtocolStage(),
        )
        .build()
    val requisitionBlobContext =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
    requisitionStore.writeString(requisitionBlobContext, "local_requisition")
    val inputBlob0Context = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 0L)
    computationStore.writeString(inputBlob0Context, "-duchy_two_sketch")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = computationDetails,
      blobs =
        listOf(newInputBlobMetadata(0L, inputBlob0Context.blobKey), newEmptyOutputBlobMetadata(3L)),
      requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
            }
          }
        )
    }

    var cryptoRequest = CompleteSetupPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeSetupPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeSetupPhase_done")
      CompleteSetupPhaseResponse.newBuilder()
        .apply {
          combinedRegisterVector =
            cryptoRequest.requisitionRegisterVector
              .concat(cryptoRequest.combinedRegisterVector)
              .concat(postFix)
        }
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 3L).blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.INPUT
              blobId = 0
              path = blobKey
            }
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1
            }
            version = 3 // claimTask + writeOutputBlob + transitionStage
            this.computationDetails = computationDetails
            addAllRequisitions(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
          }
          .build()
      )

    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("local_requisition-duchy_two_sketch-completeSetupPhase_done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_ONE_INPUT,
          "local_requisition-du",
          "chy_two_sketch-compl",
          "eteSetupPhase_done",
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeSetupPhaseRequest {
          requisitionRegisterVector = ByteString.copyFromUtf8("local_requisition")
          combinedRegisterVector = ByteString.copyFromUtf8("-duchy_two_sketch")
          noiseParameters = registerNoiseGenerationParameters {
            compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
            curveId = CURVE_ID
            contributorsCount = 2
            totalSketchesCount = REQUISITIONS.size
            dpParams = reachNoiseDifferentialPrivacyParams {
              blindHistogram = TEST_NOISE_CONFIG.reachNoiseConfig.blindHistogramNoise
              noiseForPublisherNoise = TEST_NOISE_CONFIG.reachNoiseConfig.noiseForPublisherNoise
              globalReachDpNoise = TEST_NOISE_CONFIG.reachNoiseConfig.globalReachDpNoise
            }
          }
          maximumFrequency = MAX_FREQUENCY
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `execution phase one at non-aggregator using cached result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_ONE.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 0L)
    computationStore.writeString(inputBlobContext, "sketch")
    val cachedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 1L)
    computationStore.writeString(cachedBlobContext, "cached result")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          cachedBlobContext.toMetadata(ComputationBlobDependency.OUTPUT),
        ),
      requisitions = REQUISITIONS,
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
            }
          }
        )
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
          blobs += computationStageBlobMetadata {
            blobId = 0L
            dependencyType = ComputationBlobDependency.INPUT
            path = cachedBlobContext.blobKey
          }
          blobs += computationStageBlobMetadata {
            blobId = 1L
            dependencyType = ComputationBlobDependency.OUTPUT
          }
          version = 2 // claimTask + transitionStage
          computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS
          requisitions += REQUISITIONS
        }
      )

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(GLOBAL_ID, EXECUTION_PHASE_ONE_INPUT, "cached result")
      )
      .inOrder()
  }

  @Test
  fun `execution phase one at non-aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_ONE.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
            }
          }
        )
    }

    var cryptoRequest = CompleteExecutionPhaseOneRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseOne(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseOne-done")
      CompleteExecutionPhaseOneResponse.newBuilder()
        .apply { combinedRegisterVector = cryptoRequest.combinedRegisterVector.concat(postFix) }
        .build()
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.INPUT
              blobId = 0L
              path = blobKey
            }
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1L
            }
            version = 3 // claimTask + writeOutputBlob + transitionStage
            computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("data-completeExecutionPhaseOne-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_ONE_INPUT,
          "data-completeExecuti", // Chunk 1, size 20
          "onPhaseOne-done", // Chunk 2, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseOneRequest {
          combinedRegisterVector = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          curveId = CURVE_ID
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `execution phase one at aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_ONE.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
            }
          }
        )
    }

    var cryptoRequest = CompleteExecutionPhaseOneAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseOneAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseOneAtAggregator-done")
      CompleteExecutionPhaseOneAtAggregatorResponse.newBuilder()
        .apply { flagCountTuples = cryptoRequest.combinedRegisterVector.concat(postFix) }
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.INPUT
              blobId = 0
              path = blobKey
            }
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1
            }
            version = 3 // claimTask + writeOutputBlob + transitionStage
            computationDetails = AGGREGATOR_COMPUTATION_DETAILS
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("data-completeExecutionPhaseOneAtAggregator-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_TWO_INPUT,
          "data-completeExecuti", // Chunk 1, size 20
          "onPhaseOneAtAggregat", // Chunk 2, size 20
          "or-done", // Chunk 3, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseOneAtAggregatorRequest {
          combinedRegisterVector = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          curveId = CURVE_ID
          parallelism = PARALLELISM
          noiseParameters = flagCountTupleNoiseGenerationParameters {
            maximumFrequency = MAX_FREQUENCY
            contributorsCount = WORKER_COUNT
            dpParams = TEST_NOISE_CONFIG.frequencyNoiseConfig
          }
          totalSketchesCount = REQUISITIONS.size
        }
      )
  }

  @Test
  fun `execution phase one at aggregator with max frequency one`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_ONE.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS_FREQUENCY_ONE,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )

    var cryptoRequest = CompleteExecutionPhaseOneAtAggregatorRequest.getDefaultInstance()

    whenever(mockCryptoWorker.completeExecutionPhaseOneAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseOneAtAggregator-done")
      CompleteExecutionPhaseOneAtAggregatorResponse.newBuilder()
        .apply { flagCountTuples = cryptoRequest.combinedRegisterVector.concat(postFix) }
        .build()
    }

    // Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Check that the request sent to the crypto worker was correct.
    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseOneAtAggregatorRequest {
          combinedRegisterVector = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          curveId = CURVE_ID
          totalSketchesCount = REQUISITIONS.size
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `execution phase one at aggregator with reach measurement`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_ONE.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS_REACH,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )

    var cryptoRequest = CompleteExecutionPhaseOneAtAggregatorRequest.getDefaultInstance()

    whenever(mockCryptoWorker.completeExecutionPhaseOneAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseOneAtAggregator-done")
      completeExecutionPhaseOneAtAggregatorResponse {
        flagCountTuples = cryptoRequest.combinedRegisterVector.concat(postFix)
      }
    }

    // Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Check that the request sent to the crypto worker was correct.
    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseOneAtAggregatorRequest {
          combinedRegisterVector = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          curveId = CURVE_ID
          totalSketchesCount = REQUISITIONS.size
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `execution phase two at non-aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_TWO.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )

    var cryptoRequest = CompleteExecutionPhaseTwoRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseTwo(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseTwo-done")
      CompleteExecutionPhaseTwoResponse.newBuilder()
        .apply { flagCountTuples = cryptoRequest.flagCountTuples.concat(postFix) }
        .build()
    }
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
            }
          }
        )
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.INPUT
              blobId = 0
              path = blobKey
            }
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1
            }
            version = 3 // claimTask + writeOutputBlob + transitionStage
            computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("data-completeExecutionPhaseTwo-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_TWO_INPUT,
          "data-completeExecuti", // Chunk 1, size 20
          "onPhaseTwo-done", // Chunk 2, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseTwoRequest {
          flagCountTuples = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          partialCompositeElGamalPublicKey = PARTIALLY_COMBINED_PUBLIC_KEY
          curveId = CURVE_ID
          parallelism = PARALLELISM
          noiseParameters = flagCountTupleNoiseGenerationParameters {
            maximumFrequency = MAX_FREQUENCY
            contributorsCount = WORKER_COUNT
            dpParams = TEST_NOISE_CONFIG.frequencyNoiseConfig
          }
        }
      )
  }

  @Test
  fun `execution phase two at non-aggregator with frequency one`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_TWO.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS_FREQUENCY_ONE,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )

    var cryptoRequest = CompleteExecutionPhaseTwoRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseTwo(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseTwo-done")
      CompleteExecutionPhaseTwoResponse.newBuilder()
        .apply { flagCountTuples = cryptoRequest.flagCountTuples.concat(postFix) }
        .build()
    }

    // Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Check that the request sent to the crypto worker was correct.
    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseTwoRequest {
          flagCountTuples = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          partialCompositeElGamalPublicKey = PARTIALLY_COMBINED_PUBLIC_KEY
          curveId = CURVE_ID
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `execution phase two at non-aggregator with reach measurement`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_TWO.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS_REACH,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )

    var cryptoRequest = CompleteExecutionPhaseTwoRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseTwo(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseTwo-done")
      completeExecutionPhaseTwoResponse {
        flagCountTuples = cryptoRequest.flagCountTuples.concat(postFix)
      }
    }

    // Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Check that the request sent to the crypto worker was correct.
    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseTwoRequest {
          flagCountTuples = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          curveId = CURVE_ID
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `execution phase two at aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_TWO.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    val computationDetailsWithVidSamplingWidth =
      AGGREGATOR_COMPUTATION_DETAILS.copy {
        kingdomComputation =
          kingdomComputation.copy {
            measurementSpec = SERIALIZED_MEASUREMENT_SPEC_WITH_VID_SAMPLING_WIDTH
          }
      }
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = computationDetailsWithVidSamplingWidth,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS
            }
          }
        )
    }

    val testReach = 123L
    var cryptoRequest = CompleteExecutionPhaseTwoAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseTwoAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseTwoAtAggregator-done")
      CompleteExecutionPhaseTwoAtAggregatorResponse.newBuilder()
        .apply {
          sameKeyAggregatorMatrix = cryptoRequest.flagCountTuples.concat(postFix)
          reach = testReach
        }
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.INPUT
              blobId = 0
              path = blobKey
            }
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1
            }
            version = 4 // claimTask + writeOutputBlob + ComputationDetails + transitionStage
            computationDetails =
              computationDetailsWithVidSamplingWidth
                .toBuilder()
                .apply { liquidLegionsV2Builder.reachEstimateBuilder.reach = testReach }
                .build()
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("data-completeExecutionPhaseTwoAtAggregator-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_THREE_INPUT,
          "data-completeExecuti", // Chunk 1, size 20
          "onPhaseTwoAtAggregat", // Chunk 2, size 20
          "or-done", // Chunk 3, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseTwoAtAggregatorRequest.newBuilder()
          .apply {
            flagCountTuples = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
            compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
            curveId = CURVE_ID
            maximumFrequency = MAX_FREQUENCY
            vidSamplingIntervalWidth = 0.5f
            sketchParametersBuilder.apply {
              decayRate = DECAY_RATE
              size = SKETCH_SIZE
            }
            reachDpNoiseBaselineBuilder.apply {
              contributorsCount = WORKER_COUNT
              globalReachDpNoise = TEST_NOISE_CONFIG.reachNoiseConfig.globalReachDpNoise
            }
            frequencyNoiseParametersBuilder.apply {
              contributorsCount = WORKER_COUNT
              maximumFrequency = MAX_FREQUENCY
              dpParams = TEST_NOISE_CONFIG.frequencyNoiseConfig
            }
          }
          .build()
      )
  }

  @Test
  fun `execution phase two at aggregator using cached result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_TWO.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 0L)
    computationStore.writeString(inputBlobContext, "data")
    val cachedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 1L)
    computationStore.writeString(
      cachedBlobContext,
      "data-completeExecutionPhaseTwoAtAggregator-first-run-done",
    )
    val computationDetailsWithVidSamplingWidth =
      AGGREGATOR_COMPUTATION_DETAILS.copy {
        kingdomComputation =
          kingdomComputation.copy {
            measurementSpec = SERIALIZED_MEASUREMENT_SPEC_WITH_VID_SAMPLING_WIDTH
          }
      }
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = computationDetailsWithVidSamplingWidth,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          cachedBlobContext.toMetadata(ComputationBlobDependency.OUTPUT),
        ),
      requisitions = REQUISITIONS,
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS
            }
          }
        )
    }

    val testReach = 123L
    var cryptoRequest = CompleteExecutionPhaseTwoAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseTwoAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-second-run-done")
      CompleteExecutionPhaseTwoAtAggregatorResponse.newBuilder()
        .apply {
          sameKeyAggregatorMatrix = cryptoRequest.flagCountTuples.concat(postFix)
          reach = testReach
        }
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
            version = 3 // claimTask + ComputationDetails + transitionStage
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.INPUT
              blobId = 0
              path = cachedBlobContext.blobKey
            }
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1
            }
            computationDetails =
              computationDetailsWithVidSamplingWidth
                .toBuilder()
                .apply { liquidLegionsV2Builder.reachEstimateBuilder.reach = testReach }
                .build()
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )
    assertThat(computationStore.get(cachedBlobContext.blobKey)?.readToString())
      .isEqualTo("data-completeExecutionPhaseTwoAtAggregator-first-run-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_THREE_INPUT,
          "data-completeExecuti", // Chunk 1, size 20
          "onPhaseTwoAtAggregat", // Chunk 2, size 20
          "or-first-run-done", // Chunk 3, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseTwoAtAggregatorRequest.newBuilder()
          .apply {
            flagCountTuples = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
            compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
            curveId = CURVE_ID
            maximumFrequency = MAX_FREQUENCY
            vidSamplingIntervalWidth = 0.5f
            sketchParametersBuilder.apply {
              decayRate = DECAY_RATE
              size = SKETCH_SIZE
            }
            reachDpNoiseBaselineBuilder.apply {
              contributorsCount = WORKER_COUNT
              globalReachDpNoise = TEST_NOISE_CONFIG.reachNoiseConfig.globalReachDpNoise
            }
            frequencyNoiseParametersBuilder.apply {
              contributorsCount = WORKER_COUNT
              maximumFrequency = MAX_FREQUENCY
              dpParams = TEST_NOISE_CONFIG.frequencyNoiseConfig
            }
          }
          .build()
      )
  }

  @Test
  fun `execution phase two at aggregator using frequency one`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_TWO.toProtocolStage(),
        )
        .build()
    val computationDetailsWithReach =
      AGGREGATOR_COMPUTATION_DETAILS_FREQUENCY_ONE.toBuilder()
        .apply {
          liquidLegionsV2Builder.apply { reachEstimateBuilder.reach = 123 }
          kingdomComputation =
            kingdomComputation.copy { measurementSpec = SERIALIZED_MEASUREMENT_SPEC }
        }
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    val computationDetailsWithVidSamplingWidth =
      AGGREGATOR_COMPUTATION_DETAILS_FREQUENCY_ONE.copy {
        kingdomComputation =
          kingdomComputation.copy { measurementSpec = SERIALIZED_MEASUREMENT_SPEC }
      }
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = computationDetailsWithVidSamplingWidth,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )

    val testReach = 123L
    var cryptoRequest = CompleteExecutionPhaseTwoAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseTwoAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseTwoAtAggregator-done")
      completeExecutionPhaseTwoAtAggregatorResponse {
        sameKeyAggregatorMatrix = cryptoRequest.flagCountTuples.concat(postFix)
        reach = testReach
      }
    }

    var systemComputationResult = SetComputationResultRequest.getDefaultInstance()
    whenever(mockSystemComputations.setComputationResult(any())).thenAnswer {
      systemComputationResult = it.getArgument(0)
      Computation.getDefaultInstance()
    }

    // Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = COMPLETE.toProtocolStage()
          version = 4 // claimTask + writeOutputBlob + transitionStage
          computationDetails =
            computationDetailsWithReach.copy { endingState = CompletedReason.SUCCEEDED }
          requisitions += REQUISITIONS
        }
      )
    assertThat(computationStore.get(blobKey)?.readToString()).isNotEmpty()

    assertThat(systemComputationResult.name).isEqualTo("computations/$GLOBAL_ID")
    // The signature is non-deterministic, so we only verity the encryption is not empty.
    assertThat(systemComputationResult.encryptedResult).isNotEmpty()
    assertThat(systemComputationResult)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        setComputationResultRequest {
          name = "computations/$GLOBAL_ID"
          aggregatorCertificate = CONSENT_SIGNALING_CERT_NAME
          resultPublicKey = ENCRYPTION_PUBLIC_KEY.toByteString()
        }
      )

    // Check that the cryptoRequest is correct
    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseTwoAtAggregatorRequest {
          flagCountTuples = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          curveId = CURVE_ID
          maximumFrequency = 1
          vidSamplingIntervalWidth = 0.0f
          sketchParameters = liquidLegionsSketchParameters {
            decayRate = DECAY_RATE
            size = SKETCH_SIZE
          }
          reachDpNoiseBaseline = globalReachDpNoiseBaseline {
            contributorsCount = WORKER_COUNT
            globalReachDpNoise = TEST_NOISE_CONFIG.reachNoiseConfig.globalReachDpNoise
          }
        }
      )
  }

  @Test
  fun `execution phase two at aggregator using reach measurement`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_TWO.toProtocolStage(),
        )
        .build()
    val computationDetailsWithReach =
      AGGREGATOR_COMPUTATION_DETAILS_REACH.copy {
        liquidLegionsV2 =
          liquidLegionsV2.copy { reachEstimate = reachEstimate.copy { reach = 123 } }
        kingdomComputation =
          kingdomComputation.copy { measurementSpec = SERIALIZED_MEASUREMENT_SPEC }
      }
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_TWO.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    val computationDetailsWithVidSamplingWidth =
      AGGREGATOR_COMPUTATION_DETAILS_REACH.copy {
        kingdomComputation =
          kingdomComputation.copy { measurementSpec = SERIALIZED_MEASUREMENT_SPEC }
      }
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = computationDetailsWithVidSamplingWidth,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )

    val testReach = 123L
    var cryptoRequest = CompleteExecutionPhaseTwoAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseTwoAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseTwoAtAggregator-done")
      CompleteExecutionPhaseTwoAtAggregatorResponse.newBuilder()
        .apply {
          sameKeyAggregatorMatrix = cryptoRequest.flagCountTuples.concat(postFix)
          reach = testReach
        }
        .build()
    }

    var systemComputationResult = SetComputationResultRequest.getDefaultInstance()
    whenever(mockSystemComputations.setComputationResult(any())).thenAnswer {
      systemComputationResult = it.getArgument(0)
      Computation.getDefaultInstance()
    }

    // Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = COMPLETE.toProtocolStage()
            version = 4 // claimTask + writeOutputBlob + transitionStage
            computationDetails =
              computationDetailsWithReach
                .toBuilder()
                .apply { endingState = CompletedReason.SUCCEEDED }
                .build()
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString()).isNotEmpty()

    assertThat(systemComputationResult.name).isEqualTo("computations/$GLOBAL_ID")
    // The signature is non-deterministic, so we only verity the encryption is not empty.
    assertThat(systemComputationResult.encryptedResult).isNotEmpty()
    assertThat(systemComputationResult)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        setComputationResultRequest {
          name = "computations/$GLOBAL_ID"
          aggregatorCertificate = CONSENT_SIGNALING_CERT_NAME
          resultPublicKey = ENCRYPTION_PUBLIC_KEY.toByteString()
        }
      )

    // Check that the cryptoRequest is correct
    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseTwoAtAggregatorRequest.newBuilder()
          .apply {
            flagCountTuples = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
            compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
            curveId = CURVE_ID
            maximumFrequency = 1
            vidSamplingIntervalWidth = 0.0f
            sketchParametersBuilder.apply {
              decayRate = DECAY_RATE
              size = SKETCH_SIZE
            }
            reachDpNoiseBaselineBuilder.apply {
              contributorsCount = WORKER_COUNT
              globalReachDpNoise = TEST_NOISE_CONFIG.reachNoiseConfig.globalReachDpNoise
            }
          }
          .build()
      )
  }

  @Test
  fun `execution phase three at non-aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_THREE.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_THREE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_THREE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS
            }
          }
        )
    }

    var cryptoRequest = CompleteExecutionPhaseThreeRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseThree(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseThree-done")
      CompleteExecutionPhaseThreeResponse.newBuilder()
        .apply { sameKeyAggregatorMatrix = cryptoRequest.sameKeyAggregatorMatrix.concat(postFix) }
        .build()
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = COMPLETE.toProtocolStage()
            version = 3 // claimTask + writeOutputBlob + transitionStage
            computationDetails =
              NON_AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
                .apply { endingState = CompletedReason.SUCCEEDED }
                .build()
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("data-completeExecutionPhaseThree-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_THREE_INPUT,
          "data-completeExecuti", // Chunk 1, size 20
          "onPhaseThree-done", // Chunk 2, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseThreeRequest {
          sameKeyAggregatorMatrix = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          curveId = CURVE_ID
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `execution phase three at aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_THREE.toProtocolStage(),
        )
        .build()
    val computationDetailsWithReach =
      AGGREGATOR_COMPUTATION_DETAILS.toBuilder()
        .apply { liquidLegionsV2Builder.apply { reachEstimateBuilder.reach = 123 } }
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_THREE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_THREE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = computationDetailsWithReach,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )

    var cryptoRequest = CompleteExecutionPhaseThreeAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseThreeAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      CompleteExecutionPhaseThreeAtAggregatorResponse.newBuilder()
        .putAllFrequencyDistribution(mapOf(1L to 0.3, 2L to 0.7))
        .build()
    }

    var systemComputationResult = SetComputationResultRequest.getDefaultInstance()
    whenever(mockSystemComputations.setComputationResult(any())).thenAnswer {
      systemComputationResult = it.getArgument(0)
      Computation.getDefaultInstance()
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = COMPLETE.toProtocolStage()
            version = 3 // claimTask + writeOutputBlob + transitionStage
            computationDetails =
              computationDetailsWithReach
                .toBuilder()
                .apply { endingState = CompletedReason.SUCCEEDED }
                .build()
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString()).isNotEmpty()

    assertThat(systemComputationResult.name).isEqualTo("computations/$GLOBAL_ID")
    // The signature is non-deterministic, so we only verity the encryption is not empty.
    assertThat(systemComputationResult.encryptedResult).isNotEmpty()
    assertThat(systemComputationResult)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        setComputationResultRequest {
          name = "computations/$GLOBAL_ID"
          aggregatorCertificate = CONSENT_SIGNALING_CERT_NAME
          resultPublicKey = ENCRYPTION_PUBLIC_KEY.toByteString()
        }
      )

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseThreeAtAggregatorRequest.newBuilder()
          .apply {
            sameKeyAggregatorMatrix = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
            curveId = CURVE_ID
            maximumFrequency = MAX_FREQUENCY
            globalFrequencyDpNoisePerBucketBuilder.apply {
              contributorsCount = WORKER_COUNT
              dpParams = TEST_NOISE_CONFIG.frequencyNoiseConfig
            }
          }
          .build()
      )
  }

  @Test
  fun `skip advancing when the next duchy is in a future phase`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_ONE.toProtocolStage(),
        )
        .build()
    val inputBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE_ONE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          newEmptyOutputBlobMetadata(calculatedBlobContext.blobId),
        ),
      requisitions = REQUISITIONS,
    )
    mockLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            liquidLegionsV2Stage = liquidLegionsV2Stage {
              // A future stage
              stage = LiquidLegionsV2Stage.Stage.EXECUTION_PHASE_TWO
            }
          }
        )
    }

    var cryptoRequest = CompleteExecutionPhaseOneAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseOneAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseOneAtAggregator-done")
      CompleteExecutionPhaseOneAtAggregatorResponse.newBuilder()
        .apply { flagCountTuples = cryptoRequest.combinedRegisterVector.concat(postFix) }
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .apply {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.INPUT
              blobId = 0
              path = blobKey
            }
            addBlobsBuilder().apply {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1
            }
            version = 3 // claimTask + writeOutputBlob + transitionStage
            computationDetails = AGGREGATOR_COMPUTATION_DETAILS
            addAllRequisitions(REQUISITIONS)
          }
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("data-completeExecutionPhaseOneAtAggregator-done")

    assertThat(computationControlRequests).isEmpty()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeExecutionPhaseOneAtAggregatorRequest {
          combinedRegisterVector = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          curveId = CURVE_ID
          parallelism = PARALLELISM
          noiseParameters = flagCountTupleNoiseGenerationParameters {
            maximumFrequency = MAX_FREQUENCY
            contributorsCount = WORKER_COUNT
            dpParams = TEST_NOISE_CONFIG.frequencyNoiseConfig
          }
          totalSketchesCount = REQUISITIONS.size
        }
      )
  }

  @Test
  fun `handleException enqueues computation when control service returns retryable error`() =
    runBlocking {
      val partialToken =
        FakeComputationsDatabase.newPartialToken(
            localId = LOCAL_ID,
            stage = SETUP_PHASE.toProtocolStage(),
          )
          .build()
      val requisitionBlobContext =
        RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
      val calculatedBlobContext =
        ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 1L)
      requisitionStore.writeString(requisitionBlobContext, "local_requisition")
      fakeComputationDb.addComputation(
        partialToken.localComputationId,
        partialToken.computationStage,
        computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
        requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
        blobs = listOf(newEmptyOutputBlobMetadata(calculatedBlobContext.blobId)),
      )
      mockLiquidLegionsComputationControl.stub {
        onBlocking { getComputationStage(any()) }
          .thenReturn(
            computationStage {
              liquidLegionsV2Stage = liquidLegionsV2Stage {
                stage = LiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS
              }
            }
          )
        onBlocking { advanceComputation(any()) }.thenThrow(Status.UNAVAILABLE.asRuntimeException())
      }

      var cryptoRequest = CompleteSetupPhaseRequest.getDefaultInstance()
      whenever(mockCryptoWorker.completeSetupPhase(any())).thenAnswer {
        cryptoRequest = it.getArgument(0)
        val postFix = ByteString.copyFromUtf8("-completeSetupPhase-done")
        completeSetupPhaseResponse {
          combinedRegisterVector =
            cryptoRequest.requisitionRegisterVector
              .concat(cryptoRequest.combinedRegisterVector)
              .concat(postFix)
        }
      }

      nonAggregatorMill.claimAndProcessWork()

      val blobKey = calculatedBlobContext.blobKey
      assertThat(fakeComputationDb[LOCAL_ID])
        .isEqualTo(
          computationToken {
            globalComputationId = GLOBAL_ID
            localComputationId = LOCAL_ID
            attempt = 1
            computationStage = SETUP_PHASE.toProtocolStage()
            blobs += computationStageBlobMetadata {
              dependencyType = ComputationBlobDependency.OUTPUT
              blobId = 1L
              path = blobKey
            }
            version = 3 // claimTask + writeOutputBlob + enqueue
            computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS
            requisitions += listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3)
          }
        )

      assertThat(computationStore.get(blobKey)?.readToString())
        .isEqualTo("local_requisition-completeSetupPhase-done")

      assertThat(fakeComputationDb.claimedComputations).isEmpty()
    }
}

private fun ComputationBlobContext.toMetadata(dependencyType: ComputationBlobDependency) =
  computationStageBlobMetadata {
    blobId = this@toMetadata.blobId
    path = blobKey
    this.dependencyType = dependencyType
  }

private suspend fun Blob.readToString(): String = read().flatten().toStringUtf8()

private suspend fun ComputationStore.writeString(
  context: ComputationBlobContext,
  content: String,
): Blob = write(context, ByteString.copyFromUtf8(content))

private suspend fun RequisitionStore.writeString(
  context: RequisitionBlobContext,
  content: String,
): Blob = write(context, ByteString.copyFromUtf8(content))
