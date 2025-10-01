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
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.Base64
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
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
import org.wfanet.anysketch.crypto.combineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.elGamalPublicKey as AnySketchElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey as V2AlphaElGamalPublicKey
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
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto.ReachOnlyLiquidLegionsV2Encryption
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
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.computationStageDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.differentialPrivacyParams
import org.wfanet.measurement.internal.duchy.elGamalKeyPair
import org.wfanet.measurement.internal.duchy.elGamalPublicKey
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyExecutionPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlyInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteReachOnlySetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt as InternalComputationDetailsKt
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsV2NoiseConfigKt
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage.COMPLETE
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.parameters
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt.stageDetails
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyExecutionPhaseAtAggregatorRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyExecutionPhaseAtAggregatorResponse
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyExecutionPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyExecutionPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyInitializationPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlyInitializationPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlySetupPhaseRequest
import org.wfanet.measurement.internal.duchy.protocol.completeReachOnlySetupPhaseResponse
import org.wfanet.measurement.internal.duchy.protocol.globalReachDpNoiseBaseline
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsSketchParameters
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsV2NoiseConfig
import org.wfanet.measurement.internal.duchy.protocol.reachNoiseDifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.protocol.registerNoiseGenerationParameters
import org.wfanet.measurement.storage.Store.Blob
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequestKt
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
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt.RequisitionParamsKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt.requisitionParams
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as SystemComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase as SystemComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ReachOnlyLiquidLegionsV2
import org.wfanet.measurement.system.v1alpha.ReachOnlyLiquidLegionsV2.Description.EXECUTION_PHASE_INPUT
import org.wfanet.measurement.system.v1alpha.ReachOnlyLiquidLegionsV2.Description.SETUP_PHASE_INPUT
import org.wfanet.measurement.system.v1alpha.ReachOnlyLiquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.SetComputationResultRequest
import org.wfanet.measurement.system.v1alpha.advanceComputationRequest
import org.wfanet.measurement.system.v1alpha.computationParticipant
import org.wfanet.measurement.system.v1alpha.computationStage
import org.wfanet.measurement.system.v1alpha.confirmComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.failComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.reachOnlyLiquidLegionsV2
import org.wfanet.measurement.system.v1alpha.reachOnlyLiquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.setComputationResultRequest
import org.wfanet.measurement.system.v1alpha.setParticipantRequisitionParamsRequest
import org.wfanet.measurement.system.v1alpha.stageAttempt

private const val PUBLIC_API_VERSION = "v2alpha"

private const val WORKER_COUNT = 3
private const val MILL_ID = "a nice mill"
private const val DUCHY_ONE_NAME = "DUCHY_ONE"
private const val DUCHY_TWO_NAME = "DUCHY_TWO"
private const val DUCHY_THREE_NAME = "DUCHY_THREE"
private const val DECAY_RATE = 12.0
private const val SKETCH_SIZE = 100_000L
private const val CURVE_ID = 415L // NID_X9_62_prime256v1
private const val PARALLELISM = 2

private const val LOCAL_ID = 1234L
private const val GLOBAL_ID = LOCAL_ID.toString()

private const val CIPHERTEXT_SIZE = 66
private const val NOISE_CIPHERTEXT =
  "abcdefghijklmnopqrstuvwxyz0123456abcdefghijklmnopqrstuvwxyz0123456"
private val SERIALIZED_NOISE_CIPHERTEXT = ByteString.copyFromUtf8(NOISE_CIPHERTEXT)

private val DUCHY_ONE_KEY_PAIR = elGamalKeyPair {
  publicKey = elGamalPublicKey {
    generator = ByteString.copyFromUtf8("generator_1")
    element = ByteString.copyFromUtf8("element_1")
  }
  secretKey = ByteString.copyFromUtf8("secret_key_1")
}
private val DUCHY_TWO_PUBLIC_KEY = elGamalPublicKey {
  generator = ByteString.copyFromUtf8("generator_2")
  element = ByteString.copyFromUtf8("element_2")
}
private val DUCHY_THREE_PUBLIC_KEY = elGamalPublicKey {
  generator = ByteString.copyFromUtf8("generator_3")
  element = ByteString.copyFromUtf8("element_3")
}
private val COMBINED_PUBLIC_KEY = elGamalPublicKey {
  generator = ByteString.copyFromUtf8("generator_1_generator_2_generator_3")
  element = ByteString.copyFromUtf8("element_1_element_2_element_3")
}
private val PARTIALLY_COMBINED_PUBLIC_KEY = elGamalPublicKey {
  generator = ByteString.copyFromUtf8("generator_2_generator_3")
  element = ByteString.copyFromUtf8("element_2_element_3")
}

private val TEST_NOISE_CONFIG = liquidLegionsV2NoiseConfig {
  reachNoiseConfig =
    LiquidLegionsV2NoiseConfigKt.reachNoiseConfig {
      blindHistogramNoise = differentialPrivacyParams {
        epsilon = 1.0
        delta = 2.0
      }
      noiseForPublisherNoise = differentialPrivacyParams {
        epsilon = 3.0
        delta = 4.0
      }
      globalReachDpNoise = differentialPrivacyParams {
        epsilon = 5.0
        delta = 6.0
      }
    }
}

private val ROLLV2_PARAMETERS = parameters {
  sketchParameters = liquidLegionsSketchParameters {
    decayRate = DECAY_RATE
    size = SKETCH_SIZE
  }
  noise = TEST_NOISE_CONFIG
  ellipticCurveId = CURVE_ID.toInt()
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
private val CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY = V2AlphaElGamalPublicKey {
  generator = ByteString.copyFromUtf8("generator-foo")
  element = ByteString.copyFromUtf8("element-foo")
}
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
  InternalComputationDetailsKt.computationParticipant {
    duchyId = DUCHY_ONE_NAME
    publicKey = DUCHY_ONE_KEY_PAIR.publicKey
    elGamalPublicKey = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY.toByteString()
    elGamalPublicKeySignature = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY_SINGATURE
    duchyCertificateDer = CONSENT_SIGNALING_CERT_DER
  }
private val COMPUTATION_PARTICIPANT_2 =
  InternalComputationDetailsKt.computationParticipant {
    duchyId = DUCHY_TWO_NAME
    publicKey = DUCHY_TWO_PUBLIC_KEY
    elGamalPublicKey = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY.toByteString()
    elGamalPublicKeySignature = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY_SINGATURE
    duchyCertificateDer = CONSENT_SIGNALING_CERT_DER
  }
private val COMPUTATION_PARTICIPANT_3 =
  InternalComputationDetailsKt.computationParticipant {
    duchyId = DUCHY_THREE_NAME
    publicKey = DUCHY_THREE_PUBLIC_KEY
    elGamalPublicKey = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY.toByteString()
    elGamalPublicKeySignature = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY_SINGATURE
    duchyCertificateDer = CONSENT_SIGNALING_CERT_DER
  }

private val TEST_REQUISITION_1 = TestRequisition("111") { SERIALIZED_MEASUREMENT_SPEC }
private val TEST_REQUISITION_2 = TestRequisition("222") { SERIALIZED_MEASUREMENT_SPEC }
private val TEST_REQUISITION_3 = TestRequisition("333") { SERIALIZED_MEASUREMENT_SPEC }

private val MEASUREMENT_SPEC = measurementSpec {
  nonceHashes += TEST_REQUISITION_1.nonceHash
  nonceHashes += TEST_REQUISITION_2.nonceHash
  nonceHashes += TEST_REQUISITION_3.nonceHash
  reach = reach {}
}
private val SERIALIZED_MEASUREMENT_SPEC: ByteString = MEASUREMENT_SPEC.toByteString()

private val MEASUREMENT_SPEC_WITH_VID_SAMPLING_WIDTH = measurementSpec {
  nonceHashes += TEST_REQUISITION_1.nonceHash
  nonceHashes += TEST_REQUISITION_2.nonceHash
  nonceHashes += TEST_REQUISITION_3.nonceHash
  reach = reach {}
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
  reachOnlyLiquidLegionsV2 =
    ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
      role = RoleInComputation.AGGREGATOR
      parameters = ROLLV2_PARAMETERS
      participant +=
        listOf(COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3, COMPUTATION_PARTICIPANT_1)
      combinedPublicKey = COMBINED_PUBLIC_KEY
      // partiallyCombinedPublicKey and combinedPublicKey are the same at the aggregator.
      partiallyCombinedPublicKey = COMBINED_PUBLIC_KEY
      localElgamalKey = DUCHY_ONE_KEY_PAIR
    }
}

private val NON_AGGREGATOR_COMPUTATION_DETAILS = computationDetails {
  kingdomComputation = kingdomComputationDetails {
    publicApiVersion = PUBLIC_API_VERSION
    measurementPublicKey = ENCRYPTION_PUBLIC_KEY.toDuchyEncryptionPublicKey()
    measurementSpec = SERIALIZED_MEASUREMENT_SPEC
    participantCount = 3
  }
  reachOnlyLiquidLegionsV2 =
    ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
      role = RoleInComputation.NON_AGGREGATOR
      parameters = ROLLV2_PARAMETERS
      participant +=
        listOf(COMPUTATION_PARTICIPANT_1, COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3)
      combinedPublicKey = COMBINED_PUBLIC_KEY
      partiallyCombinedPublicKey = PARTIALLY_COMBINED_PUBLIC_KEY
      localElgamalKey = DUCHY_ONE_KEY_PAIR
    }
}

@RunWith(JUnit4::class)
class ReachOnlyLiquidLegionsV2MillTest {
  private val mockReachOnlyLiquidLegionsComputationControl: ComputationControlCoroutineImplBase =
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
  private val mockCryptoWorker: ReachOnlyLiquidLegionsV2Encryption =
    mock(useConstructor = UseConstructor.parameterless()) {
      on { combineElGamalPublicKeys(any()) }
        .thenAnswer {
          val cryptoRequest: CombineElGamalPublicKeysRequest = it.getArgument(0)
          combineElGamalPublicKeysResponse {
            elGamalKeys = AnySketchElGamalPublicKey {
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
        }
    }
  private val fakeComputationDb = FakeComputationsDatabase()

  private lateinit var computationDataClients: ComputationDataClients
  private lateinit var computationStore: ComputationStore
  private lateinit var requisitionStore: RequisitionStore

  private val tempDirectory = TemporaryFolder()

  private val grpcTestServerRule = GrpcTestServerRule {
    val storageClient = FileSystemStorageClient(tempDirectory.root)
    computationStore = ComputationStore(storageClient)
    requisitionStore = RequisitionStore(storageClient)
    computationDataClients =
      ComputationDataClients.forTesting(
        ComputationsCoroutineStub(channel),
        computationStore,
        requisitionStore,
      )
    addService(mockReachOnlyLiquidLegionsComputationControl)
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

  private lateinit var aggregatorMill: ReachOnlyLiquidLegionsV2Mill
  private lateinit var nonAggregatorMill: ReachOnlyLiquidLegionsV2Mill

  private fun buildAdvanceComputationRequests(
    globalComputationId: String,
    description: ReachOnlyLiquidLegionsV2.Description,
    vararg chunkContents: String,
  ): List<AdvanceComputationRequest> {
    val header = advanceComputationRequest {
      header =
        AdvanceComputationRequestKt.header {
          name = ComputationKey(globalComputationId).toName()
          this.reachOnlyLiquidLegionsV2 = reachOnlyLiquidLegionsV2 {
            this.description = description
          }
        }
    }
    val body =
      chunkContents.asList().map {
        advanceComputationRequest {
          bodyChunk =
            AdvanceComputationRequestKt.bodyChunk { partialData = ByteString.copyFromUtf8(it) }
        }
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
      ReachOnlyLiquidLegionsV2Mill(
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
      ReachOnlyLiquidLegionsV2Mill(
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
    val initialComputationDetails = computationDetails {
      kingdomComputation = NON_AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
      reachOnlyLiquidLegionsV2 =
        ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = RoleInComputation.NON_AGGREGATOR
          parameters = ROLLV2_PARAMETERS
          participant +=
            listOf(COMPUTATION_PARTICIPANT_1, COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3)
        }
    }
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = initialComputationDetails,
      requisitions = REQUISITIONS,
    )
    repeat(2) {
      fakeComputationDb.claimTask(
        ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
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
          this.computationDetails = computationDetails {
            kingdomComputation = initialComputationDetails.kingdomComputation
            endingState = CompletedReason.FAILED
            reachOnlyLiquidLegionsV2 =
              ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
                role = RoleInComputation.NON_AGGREGATOR
                parameters = ROLLV2_PARAMETERS
                participant +=
                  listOf(
                    COMPUTATION_PARTICIPANT_1,
                    COMPUTATION_PARTICIPANT_2,
                    COMPUTATION_PARTICIPANT_3,
                  )
              }
          }
          requisitions.addAll(REQUISITIONS)
        }
      )
  }

  @Test
  fun `initialization phase has higher priority to be claimed`() = runBlocking {
    fakeComputationDb.addComputation(
      1L,
      EXECUTION_PHASE.toProtocolStage(),
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

    whenever(mockCryptoWorker.completeReachOnlyInitializationPhase(any())).thenAnswer {
      completeReachOnlyInitializationPhaseResponse {
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
    assertThat(fakeComputationDb[1]!!.computationStage).isEqualTo(EXECUTION_PHASE.toProtocolStage())
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
    val initialComputationDetails = computationDetails {
      kingdomComputation = NON_AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
      reachOnlyLiquidLegionsV2 =
        ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = RoleInComputation.NON_AGGREGATOR
          parameters = ROLLV2_PARAMETERS
          participant +=
            listOf(COMPUTATION_PARTICIPANT_1, COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3)
        }
    }

    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = initialComputationDetails,
      requisitions = REQUISITIONS,
    )

    var cryptoRequest = CompleteReachOnlyInitializationPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeReachOnlyInitializationPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      completeReachOnlyInitializationPhaseResponse {
        this.elGamalKeyPair = elGamalKeyPair {
          publicKey = elGamalPublicKey {
            generator = ByteString.copyFromUtf8("generator-foo")
            element = ByteString.copyFromUtf8("element-foo")
          }
          secretKey = ByteString.copyFromUtf8("secretKey-foo")
        }
      }
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
          computationStage = WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage()
          version = 3 // claimTask + updateComputationDetails + enqueueComputation
          this.computationDetails = computationDetails {
            kingdomComputation = initialComputationDetails.kingdomComputation
            reachOnlyLiquidLegionsV2 =
              ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
                role = RoleInComputation.NON_AGGREGATOR
                parameters = ROLLV2_PARAMETERS
                participant +=
                  listOf(
                    COMPUTATION_PARTICIPANT_1,
                    COMPUTATION_PARTICIPANT_2,
                    COMPUTATION_PARTICIPANT_3,
                  )
                this.localElgamalKey = elGamalKeyPair {
                  publicKey = elGamalPublicKey {
                    generator = ByteString.copyFromUtf8("generator-foo")
                    element = ByteString.copyFromUtf8("element-foo")
                  }
                  secretKey = ByteString.copyFromUtf8("secretKey-foo")
                }
              }
          }
          requisitions.addAll(REQUISITIONS)
        }
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::setParticipantRequisitionParams,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        setParticipantRequisitionParamsRequest {
          name = computationParticipant.name
          etag = computationParticipant.etag
          this.requisitionParams = requisitionParams {
            duchyCertificate = CONSENT_SIGNALING_CERT_NAME
            reachOnlyLiquidLegionsV2 =
              RequisitionParamsKt.liquidLegionsV2 {
                elGamalPublicKey = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY.toByteString()
              }
          }
        }
      )

    assertThat(cryptoRequest)
      .isEqualTo(completeReachOnlyInitializationPhaseRequest { curveId = CURVE_ID })
  }

  @Test
  fun `confirmation phase, failed due to missing local requisition`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val requisition1 = REQUISITION_1
    // requisition2 is fulfilled at Duchy One, but doesn't have path set.
    val requisition2 =
      REQUISITION_2.copy { details = details.copy { externalFulfillingDuchyId = DUCHY_ONE_NAME } }
    val computationDetailsWithoutPublicKey = computationDetails {
      kingdomComputation = AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
      reachOnlyLiquidLegionsV2 =
        ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = RoleInComputation.AGGREGATOR
          parameters = ROLLV2_PARAMETERS
          participant +=
            listOf(COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3, COMPUTATION_PARTICIPANT_1)
          // partiallyCombinedPublicKey and combinedPublicKey are the same at the aggregator.
          partiallyCombinedPublicKey = COMBINED_PUBLIC_KEY
          localElgamalKey = DUCHY_ONE_KEY_PAIR
        }
    }
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = CONFIRMATION_PHASE.toProtocolStage(),
      computationDetails = computationDetailsWithoutPublicKey,
      requisitions = listOf(requisition1, requisition2),
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
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = COMPLETE.toProtocolStage()
          version = 2 // claimTask + transitionStage
          this.computationDetails = computationDetails {
            kingdomComputation = computationDetailsWithoutPublicKey.kingdomComputation
            reachOnlyLiquidLegionsV2 = computationDetailsWithoutPublicKey.reachOnlyLiquidLegionsV2
            endingState = CompletedReason.FAILED
          }
          requisitions.addAll(listOf(requisition1, requisition2))
        }
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::failComputationParticipant,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        failComputationParticipantRequest {
          name = computationParticipant.name
          etag = computationParticipant.etag
          failure =
            ComputationParticipantKt.failure {
              participantChildReferenceId = MILL_ID
              errorMessage =
                """
                @Mill a nice mill, Computation 1234 failed due to:
                Cannot verify participation of all DataProviders.
                Missing expected data for requisition 222.
                """
                  .trimIndent()
              this.stageAttempt = stageAttempt {
                stage = CONFIRMATION_PHASE.number
                stageName = CONFIRMATION_PHASE.name
                attemptNumber = 1
              }
            }
        }
      )
  }

  @Test
  fun `confirmation phase, passed at non-aggregator`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val computationDetailsWithoutPublicKey = computationDetails {
      kingdomComputation = NON_AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
      reachOnlyLiquidLegionsV2 =
        ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = RoleInComputation.NON_AGGREGATOR
          parameters = ROLLV2_PARAMETERS
          participant +=
            listOf(COMPUTATION_PARTICIPANT_1, COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3)
          partiallyCombinedPublicKey = PARTIALLY_COMBINED_PUBLIC_KEY
          localElgamalKey = DUCHY_ONE_KEY_PAIR
        }
    }
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
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = WAIT_TO_START.toProtocolStage()
          version = 3 // claimTask + updateComputationDetail + transitionStage
          computationDetails = computationDetails {
            kingdomComputation = NON_AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
            reachOnlyLiquidLegionsV2 = NON_AGGREGATOR_COMPUTATION_DETAILS.reachOnlyLiquidLegionsV2
          }
          requisitions.addAll(REQUISITIONS)
        }
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
    val computationDetailsWithoutPublicKey = computationDetails {
      kingdomComputation = AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
      reachOnlyLiquidLegionsV2 =
        ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = RoleInComputation.AGGREGATOR
          parameters = ROLLV2_PARAMETERS
          participant +=
            listOf(COMPUTATION_PARTICIPANT_2, COMPUTATION_PARTICIPANT_3, COMPUTATION_PARTICIPANT_1)
          localElgamalKey = DUCHY_ONE_KEY_PAIR
        }
    }
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
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          version = 3 // claimTask + updateComputationDetails + transitionStage
          blobs.addAll(listOf(newEmptyOutputBlobMetadata(0), newEmptyOutputBlobMetadata(1)))
          stageSpecificDetails = computationStageDetails {
            reachOnlyLiquidLegionsV2 = stageDetails {
              waitSetupPhaseInputsDetails =
                ReachOnlyLiquidLegionsSketchAggregationV2Kt.waitSetupPhaseInputsDetails {
                  externalDuchyLocalBlobId.put("DUCHY_TWO", 0L)
                  externalDuchyLocalBlobId.put("DUCHY_THREE", 1L)
                }
            }
          }
          computationDetails = computationDetails {
            kingdomComputation = AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
            reachOnlyLiquidLegionsV2 = AGGREGATOR_COMPUTATION_DETAILS.reachOnlyLiquidLegionsV2
          }
          requisitions.addAll(REQUISITIONS)
        }
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
    val COMPUTATION_PARTICIPANT_2_WITH_INVALID_SIGNATURE =
      InternalComputationDetailsKt.computationParticipant {
        duchyId = DUCHY_TWO_NAME
        publicKey = DUCHY_TWO_PUBLIC_KEY
        elGamalPublicKey = CONSENT_SIGNALING_EL_GAMAL_PUBLIC_KEY.toByteString()
        elGamalPublicKeySignature = ByteString.copyFromUtf8("An invalid signature")
        duchyCertificateDer = CONSENT_SIGNALING_CERT_DER
      }
    // Stage 0. preparing the storage and set up mock
    val computationDetailsWithoutInvalidDuchySignature = computationDetails {
      kingdomComputation = AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
      reachOnlyLiquidLegionsV2 =
        ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = RoleInComputation.AGGREGATOR
          parameters = ROLLV2_PARAMETERS
          participant +=
            listOf(
              COMPUTATION_PARTICIPANT_2_WITH_INVALID_SIGNATURE,
              COMPUTATION_PARTICIPANT_3,
              COMPUTATION_PARTICIPANT_1,
            )
          combinedPublicKey = COMBINED_PUBLIC_KEY
          // partiallyCombinedPublicKey and combinedPublicKey are the same at the aggregator.
          partiallyCombinedPublicKey = COMBINED_PUBLIC_KEY
          localElgamalKey = DUCHY_ONE_KEY_PAIR
        }
    }
    val requisitionWithInvalidNonce = REQUISITION_1.copy { details = details.copy { nonce = 404L } }
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = CONFIRMATION_PHASE.toProtocolStage(),
      computationDetails = computationDetailsWithoutInvalidDuchySignature,
      requisitions = listOf(requisitionWithInvalidNonce),
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
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = COMPLETE.toProtocolStage()
          version = 2 // claimTask + transitionStage
          computationDetails = computationDetails {
            kingdomComputation = computationDetailsWithoutInvalidDuchySignature.kingdomComputation
            reachOnlyLiquidLegionsV2 =
              computationDetailsWithoutInvalidDuchySignature.reachOnlyLiquidLegionsV2
            endingState = CompletedReason.FAILED
          }
          requisitions.addAll(listOf(requisitionWithInvalidNonce))
        }
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::failComputationParticipant,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        failComputationParticipantRequest {
          name = computationParticipant.name
          etag = computationParticipant.etag
          failure =
            ComputationParticipantKt.failure {
              participantChildReferenceId = MILL_ID
              errorMessage =
                """
                @Mill a nice mill, Computation 1234 failed due to:
                Cannot verify participation of all DataProviders.
                Invalid ElGamal public key signature for Duchy $DUCHY_TWO_NAME
                """
                  .trimIndent()
              this.stageAttempt = stageAttempt {
                stage = CONFIRMATION_PHASE.number
                stageName = CONFIRMATION_PHASE.name
                attemptNumber = 1
              }
            }
        }
      )
  }

  @Test
  fun `setup phase at non-aggregator using cached result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = SETUP_PHASE.toProtocolStage(),
        )
        .build()
    val requisitionBlobContext =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
    requisitionStore.writeString(requisitionBlobContext, "local_requisition")
    val cachedBlobContext = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 1L)
    computationStore.writeString(cachedBlobContext, "cached result")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
      blobs = listOf(cachedBlobContext.toMetadata(ComputationBlobDependency.OUTPUT)),
    )
    mockReachOnlyLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            reachOnlyLiquidLegionsStage = reachOnlyLiquidLegionsV2Stage {
              stage = ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS
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
          computationStage = WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
          blobs.addAll(
            listOf(
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.INPUT
                blobId = 0L
                path = cachedBlobContext.blobKey
              },
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.OUTPUT
                blobId = 1L
              },
            )
          )
          version = 2 // claimTask + transitionStage
          computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS
          requisitions.addAll(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
        }
      )

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(GLOBAL_ID, SETUP_PHASE_INPUT, "cached result")
      )
      .inOrder()
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

    var cryptoRequest = CompleteReachOnlySetupPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeReachOnlySetupPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeReachOnlySetupPhase")
      completeReachOnlySetupPhaseResponse {
        combinedRegisterVector = cryptoRequest.combinedRegisterVector.concat(postFix)
        serializedExcessiveNoiseCiphertext = ByteString.copyFromUtf8("-encryptedNoise")
      }
    }
    mockReachOnlyLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            reachOnlyLiquidLegionsStage = reachOnlyLiquidLegionsV2Stage {
              stage = ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS
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
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
          blobs.addAll(
            listOf(
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.INPUT
                blobId = 0L
                path = blobKey
              },
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.OUTPUT
                blobId = 1L
              },
            )
          )
          version = 3 // claimTask + writeOutputBlob + transitionStage
          computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS
          requisitions.addAll(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
        }
      )

    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("local_requisition-completeReachOnlySetupPhase-encryptedNoise")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          SETUP_PHASE_INPUT,
          "local_requisition-co",
          "mpleteReachOnlySetup",
          "Phase-encryptedNoise",
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeReachOnlySetupPhaseRequest {
          combinedRegisterVector = ByteString.copyFromUtf8("local_requisition")
          curveId = CURVE_ID
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
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `setup phase at aggregator using cached result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = SETUP_PHASE.toProtocolStage(),
        )
        .build()
    val requisitionBlobContext =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
    requisitionStore.writeString(requisitionBlobContext, "local_requisition")
    val cachedBlobContext = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 1L)
    computationStore.writeString(cachedBlobContext, "cached result")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
      blobs = listOf(cachedBlobContext.toMetadata(ComputationBlobDependency.OUTPUT)),
    )
    mockReachOnlyLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            reachOnlyLiquidLegionsStage = reachOnlyLiquidLegionsV2Stage {
              stage = ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_INPUTS
            }
          }
        )
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
          blobs.addAll(
            listOf(
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.INPUT
                blobId = 0L
                path = cachedBlobContext.blobKey
              },
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.OUTPUT
                blobId = 1L
              },
            )
          )
          version = 2 // claimTask + transitionStage
          computationDetails = AGGREGATOR_COMPUTATION_DETAILS
          requisitions.addAll(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
        }
      )

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(GLOBAL_ID, EXECUTION_PHASE_INPUT, "cached result")
      )
      .inOrder()
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
    computationStore.writeString(inputBlob0Context, "duchy_2_sketch_" + NOISE_CIPHERTEXT)
    val inputBlob1Context = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlob1Context, "duchy_3_sketch_" + NOISE_CIPHERTEXT)
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
    mockReachOnlyLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            reachOnlyLiquidLegionsStage = reachOnlyLiquidLegionsV2Stage {
              stage = ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_SETUP_PHASE_INPUTS
            }
          }
        )
    }

    var cryptoRequest = CompleteReachOnlySetupPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeReachOnlySetupPhaseAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeReachOnlySetupPhase")
      completeReachOnlySetupPhaseResponse {
        combinedRegisterVector = cryptoRequest.combinedRegisterVector.concat(postFix)
        serializedExcessiveNoiseCiphertext = ByteString.copyFromUtf8("-encryptedNoise")
      }
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 3L).blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
          blobs.addAll(
            listOf(
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.INPUT
                blobId = 0
                path = blobKey
              },
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.OUTPUT
                blobId = 1
              },
            )
          )
          version = 3 // claimTask + writeOutputBlob + transitionStage
          computationDetails = AGGREGATOR_COMPUTATION_DETAILS
          requisitions.addAll(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
        }
      )

    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo(
        "local_requisition_duchy_2_sketch_duchy_3_sketch_-completeReachOnlySetupPhase-encryptedNoise"
      )

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_INPUT,
          "local_requisition_du",
          "chy_2_sketch_duchy_3",
          "_sketch_-completeRea",
          "chOnlySetupPhase-enc",
          "ryptedNoise",
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeReachOnlySetupPhaseRequest {
          combinedRegisterVector =
            ByteString.copyFromUtf8("local_requisition_duchy_2_sketch_duchy_3_sketch_")
          curveId = CURVE_ID
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
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          serializedExcessiveNoiseCiphertext =
            SERIALIZED_NOISE_CIPHERTEXT.concat(SERIALIZED_NOISE_CIPHERTEXT)
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
    computationStore.writeString(inputBlob0Context, "-duchy_2_sketch$NOISE_CIPHERTEXT")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = computationDetails,
      blobs =
        listOf(newInputBlobMetadata(0L, inputBlob0Context.blobKey), newEmptyOutputBlobMetadata(3L)),
      requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
    )
    mockReachOnlyLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            reachOnlyLiquidLegionsStage = reachOnlyLiquidLegionsV2Stage {
              stage = ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_INPUTS
            }
          }
        )
    }

    var cryptoRequest = CompleteReachOnlySetupPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeReachOnlySetupPhaseAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeReachOnlySetupPhase")
      completeReachOnlySetupPhaseResponse {
        combinedRegisterVector = cryptoRequest.combinedRegisterVector.concat(postFix)
        serializedExcessiveNoiseCiphertext = ByteString.copyFromUtf8("-encryptedNoise")
      }
    }

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 3L).blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
          blobs.addAll(
            listOf(
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.INPUT
                blobId = 0
                path = blobKey
              },
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.OUTPUT
                blobId = 1
              },
            )
          )
          version = 3 // claimTask + writeOutputBlob + transitionStage
          this.computationDetails = computationDetails
          requisitions.addAll(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
        }
      )

    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("local_requisition-duchy_2_sketch-completeReachOnlySetupPhase-encryptedNoise")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_INPUT,
          "local_requisition-du",
          "chy_2_sketch-complet",
          "eReachOnlySetupPhase",
          "-encryptedNoise",
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        completeReachOnlySetupPhaseRequest {
          combinedRegisterVector = ByteString.copyFromUtf8("local_requisition-duchy_2_sketch")
          curveId = CURVE_ID
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
          compositeElGamalPublicKey = COMBINED_PUBLIC_KEY
          serializedExcessiveNoiseCiphertext = SERIALIZED_NOISE_CIPHERTEXT
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `setup phase at aggregator, failed due to invalid input blob size`() = runBlocking {
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
    computationStore.writeString(inputBlob0Context, "duchy_2_sketch_")
    val inputBlob1Context = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlob1Context, "duchy_3_sketch_" + NOISE_CIPHERTEXT)
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

    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.READY
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID]!!)
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = COMPLETE.toProtocolStage()
          version = 2 // claimTask + transitionStage
          computationDetails = computationDetails {
            kingdomComputation = AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
            reachOnlyLiquidLegionsV2 = AGGREGATOR_COMPUTATION_DETAILS.reachOnlyLiquidLegionsV2
            endingState = CompletedReason.FAILED
          }
          requisitions.addAll(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
        }
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::failComputationParticipant,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        failComputationParticipantRequest {
          name = computationParticipant.name
          etag = computationParticipant.etag
          failure =
            ComputationParticipantKt.failure {
              participantChildReferenceId = MILL_ID
              errorMessage =
                "Invalid input blob size. Input blob duchy_2_sketch_ has size 15 which is less " +
                  "than (66)."
              this.stageAttempt = stageAttempt {
                stage = SETUP_PHASE.number
                stageName = SETUP_PHASE.name
                attemptNumber = 1
              }
            }
        }
      )
  }

  @Test
  fun `execution phase at non-aggregator using cached result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE.toProtocolStage(),
        )
        .build()
    val inputBlobContext = ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 0L)
    computationStore.writeString(inputBlobContext, "sketch" + NOISE_CIPHERTEXT)
    val cachedBlobContext = ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 1L)
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
    mockReachOnlyLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            reachOnlyLiquidLegionsStage = reachOnlyLiquidLegionsV2Stage {
              stage = ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_INPUTS
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
          computationStage = COMPLETE.toProtocolStage()
          version = 2 // claimTask + transitionStage
          computationDetails = computationDetails {
            kingdomComputation = NON_AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
            reachOnlyLiquidLegionsV2 = NON_AGGREGATOR_COMPUTATION_DETAILS.reachOnlyLiquidLegionsV2
            endingState = CompletedReason.SUCCEEDED
          }
          requisitions += REQUISITIONS
        }
      )

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(GLOBAL_ID, EXECUTION_PHASE_INPUT, "cached result")
      )
      .inOrder()
  }

  @Test
  fun `execution phase at non-aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE.toProtocolStage(),
        )
        .build()
    val inputBlobContext = ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data" + NOISE_CIPHERTEXT)
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
    mockReachOnlyLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            reachOnlyLiquidLegionsStage = reachOnlyLiquidLegionsV2Stage {
              stage = ReachOnlyLiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_INPUTS
            }
          }
        )
    }

    var cryptoRequest = CompleteReachOnlyExecutionPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeReachOnlyExecutionPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeReachOnlyExecutionPhase")
      completeReachOnlyExecutionPhaseResponse {
        combinedRegisterVector = cryptoRequest.combinedRegisterVector.concat(postFix)
        serializedExcessiveNoiseCiphertext = ByteString.copyFromUtf8("-partiallyDecryptedNoise")
      }
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    val blobKey = calculatedBlobContext.blobKey
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = COMPLETE.toProtocolStage()
          version = 3 // claimTask + writeOutputBlob + transitionStage
          computationDetails = computationDetails {
            kingdomComputation = NON_AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
            reachOnlyLiquidLegionsV2 = NON_AGGREGATOR_COMPUTATION_DETAILS.reachOnlyLiquidLegionsV2
            endingState = CompletedReason.SUCCEEDED
          }
          requisitions.addAll(REQUISITIONS)
        }
      )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("data-completeReachOnlyExecutionPhase-partiallyDecryptedNoise")

    assertThat(cryptoRequest)
      .isEqualTo(
        completeReachOnlyExecutionPhaseRequest {
          combinedRegisterVector = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          curveId = CURVE_ID
          serializedExcessiveNoiseCiphertext = SERIALIZED_NOISE_CIPHERTEXT
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `execution phase at non-aggregator, failed due to invalid input blob size`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE.toProtocolStage(),
        )
        .build()
    val inputBlobContext = ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 1L)
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
    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.REQUISITION_PARAMS_SET
      requisitionParams = ComputationParticipant.RequisitionParams.getDefaultInstance()
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)

    // Stage 1. Process the above computation
    nonAggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID]!!)
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = COMPLETE.toProtocolStage()
          version = 2 // claimTask + transitionStage
          computationDetails = computationDetails {
            kingdomComputation = NON_AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
            reachOnlyLiquidLegionsV2 = NON_AGGREGATOR_COMPUTATION_DETAILS.reachOnlyLiquidLegionsV2
            endingState = CompletedReason.FAILED
          }
          requisitions.addAll(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
        }
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::failComputationParticipant,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        failComputationParticipantRequest {
          name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
          failure =
            ComputationParticipantKt.failure {
              participantChildReferenceId = MILL_ID
              errorMessage =
                "Invalid input blob size. Input blob data has size 4 which is less than (66)."
              this.stageAttempt = stageAttempt {
                stage = EXECUTION_PHASE.number
                stageName = EXECUTION_PHASE.name
                attemptNumber = 1
              }
            }
        }
      )
  }

  @Test
  fun `execution phase at aggregator using cached result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE.toProtocolStage(),
        )
        .build()
    val inputBlobContext = ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 0L)
    computationStore.writeString(inputBlobContext, "sketch" + NOISE_CIPHERTEXT)
    val cachedBlobContext = ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 1L)
    val testReach = 123L
    val cryptoResult = completeReachOnlyExecutionPhaseAtAggregatorResponse { reach = testReach }
    computationStore.writeString(cachedBlobContext, cryptoResult.toByteString().toStringUtf8())
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(
          inputBlobContext.toMetadata(ComputationBlobDependency.INPUT),
          cachedBlobContext.toMetadata(ComputationBlobDependency.OUTPUT),
        ),
      requisitions = REQUISITIONS,
    )

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = COMPLETE.toProtocolStage()
          version = 2 // claimTask + transitionStage
          computationDetails = computationDetails {
            kingdomComputation = AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
            reachOnlyLiquidLegionsV2 = AGGREGATOR_COMPUTATION_DETAILS.reachOnlyLiquidLegionsV2
            endingState = CompletedReason.SUCCEEDED
          }
          requisitions += REQUISITIONS
        }
      )
  }

  @Test
  fun `execution phase at aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE.toProtocolStage(),
        )
        .build()
    val computationDetailsWithVidSamplingWidth =
      AGGREGATOR_COMPUTATION_DETAILS.copy {
        kingdomComputation =
          kingdomComputation.copy {
            measurementSpec = SERIALIZED_MEASUREMENT_SPEC_WITH_VID_SAMPLING_WIDTH
          }
      }
    val inputBlobContext = ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 1L)
    computationStore.writeString(inputBlobContext, "data" + NOISE_CIPHERTEXT)
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
    var cryptoRequest = CompleteReachOnlyExecutionPhaseAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeReachOnlyExecutionPhaseAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      completeReachOnlyExecutionPhaseAtAggregatorResponse { reach = testReach }
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
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = COMPLETE.toProtocolStage()
          version = 3 // claimTask + writeOutputBlob + transitionStage
          computationDetails =
            computationDetailsWithVidSamplingWidth.copy {
              reachOnlyLiquidLegionsV2 =
                computationDetailsWithVidSamplingWidth.reachOnlyLiquidLegionsV2
              endingState = CompletedReason.SUCCEEDED
            }
          requisitions.addAll(REQUISITIONS)
        }
      )
    assertThat(computationStore.get(blobKey)?.readToString()).isEmpty()

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
        completeReachOnlyExecutionPhaseAtAggregatorRequest {
          combinedRegisterVector = ByteString.copyFromUtf8("data")
          localElGamalKeyPair = DUCHY_ONE_KEY_PAIR
          curveId = CURVE_ID
          serializedExcessiveNoiseCiphertext = SERIALIZED_NOISE_CIPHERTEXT
          sketchParameters = liquidLegionsSketchParameters {
            decayRate = DECAY_RATE
            size = SKETCH_SIZE
          }
          reachDpNoiseBaseline = globalReachDpNoiseBaseline {
            contributorsCount = WORKER_COUNT
            globalReachDpNoise = TEST_NOISE_CONFIG.reachNoiseConfig.globalReachDpNoise
          }
          vidSamplingIntervalWidth = 0.5f
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
          parallelism = PARALLELISM
        }
      )
  }

  @Test
  fun `execution phase at aggregator, failed due to invalid input blob size`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE.toProtocolStage(),
        )
        .build()
    val inputBlobContext = ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 0L)
    val calculatedBlobContext =
      ComputationBlobContext(GLOBAL_ID, EXECUTION_PHASE.toProtocolStage(), 1L)
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

    val computationParticipant = computationParticipant {
      name = ComputationParticipantKey(GLOBAL_ID, DUCHY_ONE_NAME).toName()
      etag = "entity tag"
      state = ComputationParticipant.State.READY
    }
    whenever(mockComputationParticipants.getComputationParticipant(any()))
      .thenReturn(computationParticipant)

    // Stage 1. Process the above computation
    aggregatorMill.claimAndProcessWork()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID]!!)
      .isEqualTo(
        computationToken {
          globalComputationId = GLOBAL_ID
          localComputationId = LOCAL_ID
          attempt = 1
          computationStage = COMPLETE.toProtocolStage()
          version = 2 // claimTask + transitionStage
          computationDetails = computationDetails {
            kingdomComputation = AGGREGATOR_COMPUTATION_DETAILS.kingdomComputation
            reachOnlyLiquidLegionsV2 = AGGREGATOR_COMPUTATION_DETAILS.reachOnlyLiquidLegionsV2
            endingState = CompletedReason.FAILED
          }
          requisitions.addAll(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
        }
      )

    verifyProtoArgument(
        mockComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::failComputationParticipant,
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        failComputationParticipantRequest {
          name = computationParticipant.name
          etag = computationParticipant.etag
          failure =
            ComputationParticipantKt.failure {
              participantChildReferenceId = MILL_ID
              errorMessage =
                "Invalid input blob size. Input blob data has size 4 which is less than (66)."
              this.stageAttempt = stageAttempt {
                stage = EXECUTION_PHASE.number
                stageName = EXECUTION_PHASE.name
                attemptNumber = 1
              }
            }
        }
      )
  }

  @Test
  fun `skip advancing when the next duchy is in a future phase`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = SETUP_PHASE.toProtocolStage(),
        )
        .build()
    val requisitionBlobContext =
      RequisitionBlobContext(GLOBAL_ID, REQUISITION_1.externalKey.externalRequisitionId)
    requisitionStore.writeString(requisitionBlobContext, "local_requisition")
    val cachedBlobContext = ComputationBlobContext(GLOBAL_ID, SETUP_PHASE.toProtocolStage(), 1L)
    computationStore.writeString(cachedBlobContext, "cached result")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      requisitions = listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3),
      blobs = listOf(cachedBlobContext.toMetadata(ComputationBlobDependency.OUTPUT)),
    )
    mockReachOnlyLiquidLegionsComputationControl.stub {
      onBlocking { getComputationStage(any()) }
        .thenReturn(
          computationStage {
            reachOnlyLiquidLegionsStage = reachOnlyLiquidLegionsV2Stage {
              stage = ReachOnlyLiquidLegionsV2Stage.Stage.SETUP_PHASE
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
          computationStage = WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
          blobs.addAll(
            listOf(
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.INPUT
                blobId = 0L
                path = cachedBlobContext.blobKey
              },
              computationStageBlobMetadata {
                dependencyType = ComputationBlobDependency.OUTPUT
                blobId = 1L
              },
            )
          )
          version = 2 // claimTask + transitionStage
          computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS
          requisitions.addAll(listOf(REQUISITION_1, REQUISITION_2, REQUISITION_3))
        }
      )

    assertThat(computationControlRequests).isEmpty()
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
