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

package org.wfanet.measurement.duchy.daemon.herald

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Empty
import com.google.protobuf.kotlin.toByteStringUtf8
import io.grpc.Status
import java.time.Clock
import kotlin.test.assertNotNull
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.eq
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams as cmmsDifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.daemon.testing.TestRequisition
import org.wfanet.measurement.duchy.daemon.utils.key
import org.wfanet.measurement.duchy.daemon.utils.toDuchyEncryptionPublicKey
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newPassThroughBlobMetadata
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetailsKt.kingdomComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase as InternalComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub as InternalComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub
import org.wfanet.measurement.internal.duchy.DeleteComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation
import org.wfanet.measurement.internal.duchy.config.liquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.config.protocolsSetupConfig
import org.wfanet.measurement.internal.duchy.deleteComputationRequest
import org.wfanet.measurement.internal.duchy.differentialPrivacyParams as duchyDifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.elGamalPublicKey as internalElgamalPublicKey
import org.wfanet.measurement.internal.duchy.getComputationTokenResponse
import org.wfanet.measurement.internal.duchy.getContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsV2NoiseConfig
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsV2NoiseConfigKt.reachNoiseConfig
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsSketchParameters
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsV2NoiseConfig
import org.wfanet.measurement.internal.duchy.setContinuationTokenRequest
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.Computation.MpcProtocolConfig
import org.wfanet.measurement.system.v1alpha.Computation.MpcProtocolConfig.NoiseMechanism as SystemNoiseMechanism
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.LiquidLegionsV2Kt.liquidLegionsSketchParams
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.LiquidLegionsV2Kt.mpcNoise
import org.wfanet.measurement.system.v1alpha.ComputationKt.MpcProtocolConfigKt.liquidLegionsV2
import org.wfanet.measurement.system.v1alpha.ComputationKt.mpcProtocolConfig
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub as SystemComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntry
import org.wfanet.measurement.system.v1alpha.ComputationParticipant as SystemComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt.RequisitionParamsKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt.requisitionParams
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as SystemComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase as SystemComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.FailComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsResponse
import org.wfanet.measurement.system.v1alpha.computation
import org.wfanet.measurement.system.v1alpha.computationParticipant as systemComputationParticipant
import org.wfanet.measurement.system.v1alpha.computationParticipant
import org.wfanet.measurement.system.v1alpha.copy
import org.wfanet.measurement.system.v1alpha.differentialPrivacyParams as systemDifferentialPrivacyParams
import org.wfanet.measurement.system.v1alpha.failComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.streamActiveComputationsResponse

private const val PUBLIC_API_VERSION = "v2alpha"
private const val DUCHY_ONE = "BOHEMIA"
private const val DUCHY_TWO = "SALZBURG"
private const val DUCHY_THREE = "AUSTRIA"

private val REQUISITION_1 = TestRequisition("1") { SERIALIZED_MEASUREMENT_SPEC }
private val REQUISITION_2 = TestRequisition("2") { SERIALIZED_MEASUREMENT_SPEC }

private val REACH_ONLY_REQUISITION_1 =
  TestRequisition("1") { SERIALIZED_REACH_ONLY_MEASUREMENT_SPEC }
private val REACH_ONLY_REQUISITION_2 =
  TestRequisition("2") { SERIALIZED_REACH_ONLY_MEASUREMENT_SPEC }

private val PUBLIC_API_ENCRYPTION_PUBLIC_KEY = encryptionPublicKey {
  format = EncryptionPublicKey.Format.TINK_KEYSET
  data = ByteString.copyFromUtf8("A nice encryption public key.")
}

private val PUBLIC_API_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.toByteString()
  reachAndFrequency = reachAndFrequency {
    reachPrivacyParams = cmmsDifferentialPrivacyParams {
      epsilon = 1.1
      delta = 1.2
    }
    frequencyPrivacyParams = cmmsDifferentialPrivacyParams {
      epsilon = 2.1
      delta = 2.2
    }
    maximumFrequency = 10
  }
  nonceHashes += REACH_ONLY_REQUISITION_1.nonceHash
  nonceHashes += REACH_ONLY_REQUISITION_2.nonceHash
}
private val SERIALIZED_MEASUREMENT_SPEC: ByteString = PUBLIC_API_MEASUREMENT_SPEC.toByteString()

private val PUBLIC_API_REACH_ONLY_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.toByteString()
  reach = reach {
    privacyParams = cmmsDifferentialPrivacyParams {
      epsilon = 1.1
      delta = 1.2
    }
  }
  nonceHashes += REACH_ONLY_REQUISITION_1.nonceHash
  nonceHashes += REACH_ONLY_REQUISITION_2.nonceHash
}

private val SERIALIZED_REACH_ONLY_MEASUREMENT_SPEC: ByteString =
  PUBLIC_API_REACH_ONLY_MEASUREMENT_SPEC.toByteString()

private val LLV2_MPC_PROTOCOL_CONFIG = mpcProtocolConfig {
  liquidLegionsV2 = liquidLegionsV2 {
    sketchParams = liquidLegionsSketchParams {
      decayRate = 12.0
      maxSize = 100_000
    }
    mpcNoise = mpcNoise {
      blindedHistogramNoise = systemDifferentialPrivacyParams {
        epsilon = 3.1
        delta = 3.2
      }
      publisherNoise = systemDifferentialPrivacyParams {
        epsilon = 4.1
        delta = 4.2
      }
    }
    ellipticCurveId = 415
    noiseMechanism = SystemNoiseMechanism.GEOMETRIC
  }
}

private val RO_LLV2_MPC_PROTOCOL_CONFIG = mpcProtocolConfig {
  reachOnlyLiquidLegionsV2 = liquidLegionsV2 {
    sketchParams = liquidLegionsSketchParams {
      decayRate = 12.0
      maxSize = 100_000
    }
    mpcNoise = mpcNoise {
      blindedHistogramNoise = systemDifferentialPrivacyParams {
        epsilon = 3.1
        delta = 3.2
      }
      publisherNoise = systemDifferentialPrivacyParams {
        epsilon = 4.1
        delta = 4.2
      }
    }
    ellipticCurveId = 415
    noiseMechanism = SystemNoiseMechanism.GEOMETRIC
  }
}

private const val AGGREGATOR_DUCHY_ID = "aggregator_duchy"
private const val AGGREGATOR_HERALD_ID = "aggregator_herald"
private const val NON_AGGREGATOR_DUCHY_ID = "worker_duchy"
private const val NON_AGGREGATOR_HERALD_ID = "worker_herald"

private val AGGREGATOR_PROTOCOLS_SETUP_CONFIG = protocolsSetupConfig {
  liquidLegionsV2 = liquidLegionsV2SetupConfig {
    role = RoleInComputation.AGGREGATOR
    externalAggregatorDuchyId = DUCHY_ONE
  }
  reachOnlyLiquidLegionsV2 = liquidLegionsV2SetupConfig {
    role = RoleInComputation.AGGREGATOR
    externalAggregatorDuchyId = DUCHY_ONE
  }
}

private val NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG = protocolsSetupConfig {
  liquidLegionsV2 = liquidLegionsV2SetupConfig {
    role = RoleInComputation.NON_AGGREGATOR
    externalAggregatorDuchyId = DUCHY_ONE
  }
  reachOnlyLiquidLegionsV2 = liquidLegionsV2SetupConfig {
    role = RoleInComputation.NON_AGGREGATOR
    externalAggregatorDuchyId = DUCHY_ONE
  }
}

private val LLV2_AGGREGATOR_COMPUTATION_DETAILS = computationDetails {
  liquidLegionsV2 =
    LiquidLegionsSketchAggregationV2Kt.computationDetails { role = RoleInComputation.AGGREGATOR }
}

private val LLV2_NON_AGGREGATOR_COMPUTATION_DETAILS = computationDetails {
  liquidLegionsV2 =
    LiquidLegionsSketchAggregationV2Kt.computationDetails {
      role = RoleInComputation.NON_AGGREGATOR
    }
}

private val RO_LLV2_AGGREGATOR_COMPUTATION_DETAILS = computationDetails {
  reachOnlyLiquidLegionsV2 =
    ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
      role = RoleInComputation.AGGREGATOR
    }
}

private val RO_LLV2_NON_AGGREGATOR_COMPUTATION_DETAILS = computationDetails {
  reachOnlyLiquidLegionsV2 =
    ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
      role = RoleInComputation.NON_AGGREGATOR
    }
}

private const val COMPUTATION_GLOBAL_ID = "123"

private val FAIL_COMPUTATION_PARTICIPANT_RESPONSE = computationParticipant {
  state = SystemComputationParticipant.State.FAILED
}

@RunWith(JUnit4::class)
@OptIn(ExperimentalCoroutinesApi::class) // For `runTest`.
class HeraldTest {

  private val systemComputations: SystemComputationsCoroutineImplBase = mockService()

  private val systemComputationParticipants: SystemComputationParticipantsCoroutineImplBase =
    mockService() {
      onBlocking { failComputationParticipant(any()) }
        .thenReturn(FAIL_COMPUTATION_PARTICIPANT_RESPONSE)
    }

  private val computationLogEntries: ComputationLogEntriesCoroutineImplBase =
    mockService() {
      onBlocking { createComputationLogEntry(any()) }
        .thenReturn(ComputationLogEntry.getDefaultInstance())
    }

  private val fakeComputationDatabase = FakeComputationsDatabase()

  private lateinit var storageClient: StorageClient
  private lateinit var computationStore: ComputationStore
  private lateinit var requisitionStore: RequisitionStore

  private var continuationTokensService: ContinuationTokensCoroutineImplBase = mockService()

  val grpcTestServerRule = GrpcTestServerRule {
    storageClient = InMemoryStorageClient()
    computationStore = ComputationStore(storageClient)
    requisitionStore = RequisitionStore(storageClient)
    addService(
      ComputationsService(
        fakeComputationDatabase,
        computationLogEntriesCoroutineStub,
        computationStore,
        requisitionStore,
        DUCHY_ONE,
        Clock.systemUTC()
      )
    )
    addService(systemComputations)
    addService(systemComputationParticipants)
    addService(computationLogEntries)
    addService(continuationTokensService)
  }

  private val internalComputationsStub: InternalComputationsCoroutineStub by lazy {
    InternalComputationsCoroutineStub(grpcTestServerRule.channel)
  }

  private val computationLogEntriesCoroutineStub: SystemComputationLogEntriesCoroutineStub by lazy {
    SystemComputationLogEntriesCoroutineStub(grpcTestServerRule.channel)
  }

  private val systemComputationsStub: SystemComputationsCoroutineStub by lazy {
    SystemComputationsCoroutineStub(grpcTestServerRule.channel)
  }

  private val systemComputationParticipantsStub:
    SystemComputationParticipantsCoroutineStub by lazy {
    SystemComputationParticipantsCoroutineStub(grpcTestServerRule.channel)
  }

  private val continuationTokensStub: ContinuationTokensCoroutineStub by lazy {
    ContinuationTokensCoroutineStub(grpcTestServerRule.channel)
  }

  private val internalComputationsMock: InternalComputationsCoroutineImplBase = mockService {}
  private val mockBasedInternalComputationsServerRule = GrpcTestServerRule {
    addService(internalComputationsMock)
  }
  private val mockBasedInternalComputationsStub =
    InternalComputationsCoroutineStub(mockBasedInternalComputationsServerRule.channel)

  private lateinit var aggregatorHerald: Herald
  private lateinit var nonAggregatorHerald: Herald

  @get:Rule
  val ruleChain =
    chainRulesSequentially(grpcTestServerRule, mockBasedInternalComputationsServerRule)

  @Before
  fun initHerald() {
    aggregatorHerald =
      Herald(
        AGGREGATOR_HERALD_ID,
        AGGREGATOR_DUCHY_ID,
        internalComputationsStub,
        systemComputationsStub,
        systemComputationParticipantsStub,
        ContinuationTokenManager(continuationTokensStub),
        AGGREGATOR_PROTOCOLS_SETUP_CONFIG,
        Clock.systemUTC(),
      )
    nonAggregatorHerald =
      Herald(
        NON_AGGREGATOR_HERALD_ID,
        NON_AGGREGATOR_DUCHY_ID,
        internalComputationsStub,
        systemComputationsStub,
        systemComputationParticipantsStub,
        ContinuationTokenManager(continuationTokensStub),
        NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG,
        Clock.systemUTC(),
      )
  }

  @Test
  fun `syncStatuses on empty stream retains same computation token`() = runTest {
    mockStreamActiveComputationsToReturn() // No items in stream.
    continuationTokensService.stub {
      onBlocking { getContinuationToken(any()) }
        .thenReturn(getContinuationTokenResponse { token = "123" })
    }

    nonAggregatorHerald.syncStatuses()

    verifyBlocking(continuationTokensService, never()) { setContinuationToken(any()) }
  }

  @Test
  fun `syncStatuses creates new llv2 computations`() = runTest {
    val confirmingKnown =
      buildComputationAtKingdom("1", Computation.State.PENDING_REQUISITION_PARAMS)

    val systemApiRequisitions1 =
      REQUISITION_1.toSystemRequisition("2", Requisition.State.UNFULFILLED)
    val systemApiRequisitions2 =
      REQUISITION_2.toSystemRequisition("2", Requisition.State.UNFULFILLED)
    val confirmingUnknown =
      buildComputationAtKingdom(
        "2",
        Computation.State.PENDING_REQUISITION_PARAMS,
        systemApiRequisitions = listOf(systemApiRequisitions1, systemApiRequisitions2)
      )
    mockStreamActiveComputationsToReturn(confirmingKnown, confirmingUnknown)

    fakeComputationDatabase.addComputation(
      globalId = confirmingKnown.key.computationId,
      stage = LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage(),
      computationDetails = LLV2_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newInputBlobMetadata(0L, "input-blob"), newEmptyOutputBlobMetadata(1L))
    )

    aggregatorHerald.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(eq(setContinuationTokenRequest { this.token = "2" }))
    }
    assertThat(
        fakeComputationDatabase.mapValues { (_, fakeComputation) ->
          fakeComputation.computationStage
        }
      )
      .containsExactly(
        confirmingKnown.key.computationId.toLong(),
        LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage(),
        confirmingUnknown.key.computationId.toLong(),
        LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage()
      )

    assertThat(
        fakeComputationDatabase[confirmingUnknown.key.computationId.toLong()]?.requisitionsList
      )
      .containsExactly(
        REQUISITION_1.toRequisitionMetadata(Requisition.State.UNFULFILLED),
        REQUISITION_2.toRequisitionMetadata(Requisition.State.UNFULFILLED)
      )
    assertThat(
        fakeComputationDatabase[confirmingUnknown.key.computationId.toLong()]?.computationDetails
      )
      .isEqualTo(
        computationDetails {
          blobsStoragePrefix = "computation-blob-storage/2"
          kingdomComputation = kingdomComputationDetails {
            publicApiVersion = PUBLIC_API_VERSION
            measurementSpec = SERIALIZED_MEASUREMENT_SPEC
            measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.toDuchyEncryptionPublicKey()
            participantCount = 3
          }
          liquidLegionsV2 =
            LiquidLegionsSketchAggregationV2Kt.computationDetails {
              role = RoleInComputation.AGGREGATOR
              parameters =
                LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.parameters {
                  maximumFrequency = 10
                  sketchParameters = liquidLegionsSketchParameters {
                    decayRate = 12.0
                    size = 100_000L
                  }
                  noise = liquidLegionsV2NoiseConfig {
                    reachNoiseConfig = reachNoiseConfig {
                      blindHistogramNoise = duchyDifferentialPrivacyParams {
                        epsilon = 3.1
                        delta = 3.2
                      }
                      noiseForPublisherNoise = duchyDifferentialPrivacyParams {
                        epsilon = 4.1
                        delta = 4.2
                      }
                      globalReachDpNoise = duchyDifferentialPrivacyParams {
                        epsilon = 1.1
                        delta = 1.2
                      }
                    }
                    frequencyNoiseConfig = duchyDifferentialPrivacyParams {
                      epsilon = 2.1
                      delta = 2.2
                    }
                    noiseMechanism = LiquidLegionsV2NoiseConfig.NoiseMechanism.GEOMETRIC
                  }
                  ellipticCurveId = 415
                }
            }
        }
      )
  }

  @Test
  fun `syncStatuses creates new llv2 computations for reach-only`() = runTest {
    val confirmingKnown =
      buildComputationAtKingdom(
        "1",
        Computation.State.PENDING_REQUISITION_PARAMS,
        serializedMeasurementSpec = SERIALIZED_REACH_ONLY_MEASUREMENT_SPEC
      )

    val systemApiRequisitions1 =
      REACH_ONLY_REQUISITION_1.toSystemRequisition("2", Requisition.State.UNFULFILLED)
    val systemApiRequisitions2 =
      REACH_ONLY_REQUISITION_2.toSystemRequisition("2", Requisition.State.UNFULFILLED)
    val confirmingUnknown =
      buildComputationAtKingdom(
        "2",
        Computation.State.PENDING_REQUISITION_PARAMS,
        listOf(systemApiRequisitions1, systemApiRequisitions2),
        serializedMeasurementSpec = SERIALIZED_REACH_ONLY_MEASUREMENT_SPEC
      )
    mockStreamActiveComputationsToReturn(confirmingKnown, confirmingUnknown)

    fakeComputationDatabase.addComputation(
      globalId = confirmingKnown.key.computationId,
      stage = LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage(),
      computationDetails = LLV2_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newInputBlobMetadata(0L, "input-blob"), newEmptyOutputBlobMetadata(1L))
    )

    aggregatorHerald.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(eq(setContinuationTokenRequest { this.token = "2" }))
    }
    assertThat(
        fakeComputationDatabase.mapValues { (_, fakeComputation) ->
          fakeComputation.computationStage
        }
      )
      .containsExactly(
        confirmingKnown.key.computationId.toLong(),
        LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage(),
        confirmingUnknown.key.computationId.toLong(),
        LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage()
      )

    assertThat(
        fakeComputationDatabase[confirmingUnknown.key.computationId.toLong()]?.requisitionsList
      )
      .containsExactly(
        REACH_ONLY_REQUISITION_1.toRequisitionMetadata(Requisition.State.UNFULFILLED),
        REACH_ONLY_REQUISITION_2.toRequisitionMetadata(Requisition.State.UNFULFILLED)
      )
    assertThat(
        fakeComputationDatabase[confirmingUnknown.key.computationId.toLong()]?.computationDetails
      )
      .isEqualTo(
        computationDetails {
          blobsStoragePrefix = "computation-blob-storage/2"
          kingdomComputation = kingdomComputationDetails {
            publicApiVersion = PUBLIC_API_VERSION
            measurementSpec = SERIALIZED_REACH_ONLY_MEASUREMENT_SPEC
            measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.toDuchyEncryptionPublicKey()
            participantCount = 3
          }
          liquidLegionsV2 =
            LiquidLegionsSketchAggregationV2Kt.computationDetails {
              role = RoleInComputation.AGGREGATOR
              parameters =
                LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.parameters {
                  sketchParameters = liquidLegionsSketchParameters {
                    decayRate = 12.0
                    size = 100_000L
                  }
                  noise = liquidLegionsV2NoiseConfig {
                    noiseMechanism = LiquidLegionsV2NoiseConfig.NoiseMechanism.GEOMETRIC
                    reachNoiseConfig = reachNoiseConfig {
                      blindHistogramNoise = duchyDifferentialPrivacyParams {
                        epsilon = 3.1
                        delta = 3.2
                      }
                      noiseForPublisherNoise = duchyDifferentialPrivacyParams {
                        epsilon = 4.1
                        delta = 4.2
                      }
                      globalReachDpNoise = duchyDifferentialPrivacyParams {
                        epsilon = 1.1
                        delta = 1.2
                      }
                    }
                  }
                  ellipticCurveId = 415
                }
            }
        }
      )
  }

  @Test
  fun `syncStatuses creates new rollv2 computations for reach-only`() = runTest {
    val confirmingKnown =
      buildComputationAtKingdom(
        "1",
        Computation.State.PENDING_REQUISITION_PARAMS,
        serializedMeasurementSpec = SERIALIZED_REACH_ONLY_MEASUREMENT_SPEC,
        mpcProtocolConfig = RO_LLV2_MPC_PROTOCOL_CONFIG
      )

    val systemApiRequisitions1 =
      REACH_ONLY_REQUISITION_1.toSystemRequisition("2", Requisition.State.UNFULFILLED)
    val systemApiRequisitions2 =
      REACH_ONLY_REQUISITION_2.toSystemRequisition("2", Requisition.State.UNFULFILLED)
    val confirmingUnknown =
      buildComputationAtKingdom(
        "2",
        Computation.State.PENDING_REQUISITION_PARAMS,
        listOf(systemApiRequisitions1, systemApiRequisitions2),
        serializedMeasurementSpec = SERIALIZED_REACH_ONLY_MEASUREMENT_SPEC,
        mpcProtocolConfig = RO_LLV2_MPC_PROTOCOL_CONFIG
      )
    mockStreamActiveComputationsToReturn(confirmingKnown, confirmingUnknown)

    fakeComputationDatabase.addComputation(
      globalId = confirmingKnown.key.computationId,
      stage =
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage(),
      computationDetails = RO_LLV2_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newInputBlobMetadata(0L, "input-blob"), newEmptyOutputBlobMetadata(1L))
    )

    aggregatorHerald.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(eq(setContinuationTokenRequest { this.token = "2" }))
    }
    assertThat(
        fakeComputationDatabase.mapValues { (_, fakeComputation) ->
          fakeComputation.computationStage
        }
      )
      .containsExactly(
        confirmingKnown.key.computationId.toLong(),
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage(),
        confirmingUnknown.key.computationId.toLong(),
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage()
      )

    assertThat(
        fakeComputationDatabase[confirmingUnknown.key.computationId.toLong()]?.requisitionsList
      )
      .containsExactly(
        REACH_ONLY_REQUISITION_1.toRequisitionMetadata(Requisition.State.UNFULFILLED),
        REACH_ONLY_REQUISITION_2.toRequisitionMetadata(Requisition.State.UNFULFILLED)
      )
    assertThat(
        fakeComputationDatabase[confirmingUnknown.key.computationId.toLong()]?.computationDetails
      )
      .isEqualTo(
        computationDetails {
          blobsStoragePrefix = "computation-blob-storage/2"
          kingdomComputation = kingdomComputationDetails {
            publicApiVersion = PUBLIC_API_VERSION
            measurementSpec = SERIALIZED_REACH_ONLY_MEASUREMENT_SPEC
            measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.toDuchyEncryptionPublicKey()
            participantCount = 3
          }
          reachOnlyLiquidLegionsV2 =
            ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
              role = RoleInComputation.AGGREGATOR
              parameters =
                ReachOnlyLiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.parameters {
                  sketchParameters = liquidLegionsSketchParameters {
                    decayRate = 12.0
                    size = 100_000L
                  }
                  noise = liquidLegionsV2NoiseConfig {
                    noiseMechanism = LiquidLegionsV2NoiseConfig.NoiseMechanism.GEOMETRIC
                    reachNoiseConfig = reachNoiseConfig {
                      blindHistogramNoise = duchyDifferentialPrivacyParams {
                        epsilon = 3.1
                        delta = 3.2
                      }
                      noiseForPublisherNoise = duchyDifferentialPrivacyParams {
                        epsilon = 4.1
                        delta = 4.2
                      }
                      globalReachDpNoise = duchyDifferentialPrivacyParams {
                        epsilon = 1.1
                        delta = 1.2
                      }
                    }
                  }
                  ellipticCurveId = 415
                }
            }
        }
      )
  }

  @Test
  fun `syncStatuses confirms participants for llv2 computations`() = runTest {
    val globalId = "123456"
    val systemApiRequisitions1 =
      REQUISITION_1.toSystemRequisition(globalId, Requisition.State.FULFILLED, DUCHY_ONE)
    val systemApiRequisitions2 =
      REQUISITION_2.toSystemRequisition(globalId, Requisition.State.FULFILLED, DUCHY_TWO)
    val v2alphaApiElgamalPublicKey1 =
      ElGamalPublicKey.newBuilder()
        .apply {
          generator = ByteString.copyFromUtf8("generator_1")
          element = ByteString.copyFromUtf8("element_1")
        }
        .build()
    val v2alphaApiElgamalPublicKey2 =
      ElGamalPublicKey.newBuilder()
        .apply {
          generator = ByteString.copyFromUtf8("generator_2")
          element = ByteString.copyFromUtf8("element_2")
        }
        .build()
    val v2alphaApiElgamalPublicKey3 =
      ElGamalPublicKey.newBuilder()
        .apply {
          generator = ByteString.copyFromUtf8("generator_3")
          element = ByteString.copyFromUtf8("element_3")
        }
        .build()
    val systemComputationParticipant1 =
      SystemComputationParticipant.newBuilder()
        .apply {
          name = ComputationParticipantKey(globalId, DUCHY_ONE).toName()
          requisitionParamsBuilder.apply {
            duchyCertificate = "duchyCertificate_1"
            duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_1")
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = v2alphaApiElgamalPublicKey1.toByteString()
              elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_1")
            }
          }
        }
        .build()
    val systemComputationParticipant2 =
      SystemComputationParticipant.newBuilder()
        .apply {
          name = ComputationParticipantKey(globalId, DUCHY_TWO).toName()
          requisitionParamsBuilder.apply {
            duchyCertificate = "duchyCertificate_2"
            duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_2")
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = v2alphaApiElgamalPublicKey2.toByteString()
              elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_2")
            }
          }
        }
        .build()
    val systemComputationParticipant3 =
      SystemComputationParticipant.newBuilder()
        .apply {
          name = ComputationParticipantKey(globalId, DUCHY_THREE).toName()
          requisitionParamsBuilder.apply {
            duchyCertificate = "duchyCertificate_3"
            duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_3")
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = v2alphaApiElgamalPublicKey3.toByteString()
              elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_3")
            }
          }
        }
        .build()
    val waitingRequisitionsAndKeySet =
      buildComputationAtKingdom(
        globalId,
        Computation.State.PENDING_PARTICIPANT_CONFIRMATION,
        listOf(systemApiRequisitions1, systemApiRequisitions2),
        listOf(
          systemComputationParticipant1,
          systemComputationParticipant2,
          systemComputationParticipant3
        )
      )

    mockStreamActiveComputationsToReturn(waitingRequisitionsAndKeySet)

    fakeComputationDatabase.addComputation(
      globalId = globalId,
      stage =
        LiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage(),
      computationDetails = LLV2_NON_AGGREGATOR_COMPUTATION_DETAILS,
      requisitions =
        listOf(
          REQUISITION_1.toRequisitionMetadata(Requisition.State.UNFULFILLED),
          REQUISITION_2.toRequisitionMetadata(Requisition.State.UNFULFILLED)
        )
    )

    aggregatorHerald.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(
        eq(
          setContinuationTokenRequest {
            this.token = waitingRequisitionsAndKeySet.continuationToken()
          }
        )
      )
    }

    val duchyComputationToken = fakeComputationDatabase.readComputationToken(globalId)!!
    assertThat(duchyComputationToken.computationStage)
      .isEqualTo(LiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE.toProtocolStage())
    assertThat(duchyComputationToken.computationDetails.liquidLegionsV2.participantList)
      .isEqualTo(
        mutableListOf(
          LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant.newBuilder()
            .apply {
              duchyId = DUCHY_THREE
              publicKeyBuilder.apply {
                generator = ByteString.copyFromUtf8("generator_3")
                element = ByteString.copyFromUtf8("element_3")
              }
              elGamalPublicKey = v2alphaApiElgamalPublicKey3.toByteString()
              elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_3")
              duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_3")
            }
            .build(),
          LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant.newBuilder()
            .apply {
              duchyId = DUCHY_TWO
              publicKeyBuilder.apply {
                generator = ByteString.copyFromUtf8("generator_2")
                element = ByteString.copyFromUtf8("element_2")
              }
              elGamalPublicKey = v2alphaApiElgamalPublicKey2.toByteString()
              elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_2")
              duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_2")
            }
            .build(),
          LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant.newBuilder()
            .apply {
              duchyId = DUCHY_ONE
              publicKeyBuilder.apply {
                generator = ByteString.copyFromUtf8("generator_1")
                element = ByteString.copyFromUtf8("element_1")
              }
              elGamalPublicKey = v2alphaApiElgamalPublicKey1.toByteString()
              elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_1")
              duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_1")
            }
            .build()
        )
      )
    assertThat(duchyComputationToken.requisitionsList)
      .containsExactly(
        REQUISITION_1.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_ONE),
        REQUISITION_2.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_TWO)
      )
  }

  @Test
  fun `syncStatuses confirms participants for rollv2 computations`() = runTest {
    val globalId = "123456"
    val systemApiRequisitions1 =
      REQUISITION_1.toSystemRequisition(globalId, Requisition.State.FULFILLED, DUCHY_ONE)
    val systemApiRequisitions2 =
      REQUISITION_2.toSystemRequisition(globalId, Requisition.State.FULFILLED, DUCHY_TWO)
    val v2alphaApiElgamalPublicKey1 = elGamalPublicKey {
      generator = ByteString.copyFromUtf8("generator_1")
      element = ByteString.copyFromUtf8("element_1")
    }
    val v2alphaApiElgamalPublicKey2 = elGamalPublicKey {
      generator = ByteString.copyFromUtf8("generator_2")
      element = ByteString.copyFromUtf8("element_2")
    }
    val v2alphaApiElgamalPublicKey3 = elGamalPublicKey {
      generator = ByteString.copyFromUtf8("generator_3")
      element = ByteString.copyFromUtf8("element_3")
    }
    val systemComputationParticipant1 = systemComputationParticipant {
      name = ComputationParticipantKey(globalId, DUCHY_ONE).toName()
      requisitionParams = requisitionParams {
        duchyCertificate = "duchyCertificate_1"
        duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_1")
        reachOnlyLiquidLegionsV2 =
          RequisitionParamsKt.liquidLegionsV2 {
            elGamalPublicKey = v2alphaApiElgamalPublicKey1.toByteString()
            elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_1")
          }
      }
    }
    val systemComputationParticipant2 = systemComputationParticipant {
      name = ComputationParticipantKey(globalId, DUCHY_TWO).toName()
      requisitionParams = requisitionParams {
        duchyCertificate = "duchyCertificate_2"
        duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_2")
        reachOnlyLiquidLegionsV2 =
          RequisitionParamsKt.liquidLegionsV2 {
            elGamalPublicKey = v2alphaApiElgamalPublicKey2.toByteString()
            elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_2")
          }
      }
    }
    val systemComputationParticipant3 = systemComputationParticipant {
      name = ComputationParticipantKey(globalId, DUCHY_THREE).toName()
      requisitionParams = requisitionParams {
        duchyCertificate = "duchyCertificate_3"
        duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_3")
        reachOnlyLiquidLegionsV2 =
          RequisitionParamsKt.liquidLegionsV2 {
            elGamalPublicKey = v2alphaApiElgamalPublicKey3.toByteString()
            elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_3")
          }
      }
    }
    val waitingRequisitionsAndKeySet =
      buildComputationAtKingdom(
        globalId,
        Computation.State.PENDING_PARTICIPANT_CONFIRMATION,
        listOf(systemApiRequisitions1, systemApiRequisitions2),
        listOf(
          systemComputationParticipant1,
          systemComputationParticipant2,
          systemComputationParticipant3
        )
      )

    mockStreamActiveComputationsToReturn(waitingRequisitionsAndKeySet)

    fakeComputationDatabase.addComputation(
      globalId = globalId,
      stage =
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET
          .toProtocolStage(),
      computationDetails = RO_LLV2_NON_AGGREGATOR_COMPUTATION_DETAILS,
      requisitions =
        listOf(
          REQUISITION_1.toRequisitionMetadata(Requisition.State.UNFULFILLED),
          REQUISITION_2.toRequisitionMetadata(Requisition.State.UNFULFILLED)
        )
    )

    aggregatorHerald.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(
        eq(
          setContinuationTokenRequest {
            this.token = waitingRequisitionsAndKeySet.continuationToken()
          }
        )
      )
    }

    val duchyComputationToken = fakeComputationDatabase.readComputationToken(globalId)!!
    assertThat(duchyComputationToken.computationStage)
      .isEqualTo(
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE.toProtocolStage()
      )
    assertThat(duchyComputationToken.computationDetails.reachOnlyLiquidLegionsV2.participantList)
      .isEqualTo(
        listOf(
          ReachOnlyLiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.computationParticipant {
            duchyId = DUCHY_THREE
            publicKey = internalElgamalPublicKey {
              generator = ByteString.copyFromUtf8("generator_3")
              element = ByteString.copyFromUtf8("element_3")
            }
            elGamalPublicKey = v2alphaApiElgamalPublicKey3.toByteString()
            elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_3")
            duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_3")
          },
          ReachOnlyLiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.computationParticipant {
            duchyId = DUCHY_TWO
            publicKey = internalElgamalPublicKey {
              generator = ByteString.copyFromUtf8("generator_2")
              element = ByteString.copyFromUtf8("element_2")
            }
            elGamalPublicKey = v2alphaApiElgamalPublicKey2.toByteString()
            elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_2")
            duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_2")
          },
          ReachOnlyLiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.computationParticipant {
            duchyId = DUCHY_ONE
            publicKey = internalElgamalPublicKey {
              generator = ByteString.copyFromUtf8("generator_1")
              element = ByteString.copyFromUtf8("element_1")
            }
            elGamalPublicKey = v2alphaApiElgamalPublicKey1.toByteString()
            elGamalPublicKeySignature = ByteString.copyFromUtf8("elGamalPublicKeySignature_1")
            duchyCertificateDer = ByteString.copyFromUtf8("duchyCertificateDer_1")
          }
        )
      )
    assertThat(duchyComputationToken.requisitionsList)
      .containsExactly(
        REQUISITION_1.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_ONE),
        REQUISITION_2.toRequisitionMetadata(Requisition.State.FULFILLED, DUCHY_TWO)
      )
  }

  @Test
  fun `syncStatuses starts llv2 computations`() = runTest {
    val waitingToStart =
      buildComputationAtKingdom(COMPUTATION_GLOBAL_ID, Computation.State.PENDING_COMPUTATION)
    val addingNoise = buildComputationAtKingdom("231313", Computation.State.PENDING_COMPUTATION)
    mockStreamActiveComputationsToReturn(waitingToStart, addingNoise)

    fakeComputationDatabase.addComputation(
      globalId = waitingToStart.key.computationId,
      stage = LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START.toProtocolStage(),
      computationDetails = LLV2_NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newPassThroughBlobMetadata(0L, "local-copy-of-sketches"))
    )

    fakeComputationDatabase.addComputation(
      globalId = addingNoise.key.computationId,
      stage = LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE.toProtocolStage(),
      computationDetails = LLV2_AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(newInputBlobMetadata(0L, "inputs-to-add-noise"), newEmptyOutputBlobMetadata(1L))
    )

    aggregatorHerald.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(eq(setContinuationTokenRequest { this.token = "231313" }))
    }
    assertThat(
        fakeComputationDatabase.mapValues { (_, fakeComputation) ->
          fakeComputation.computationStage
        }
      )
      .containsExactly(
        waitingToStart.key.computationId.toLong(),
        LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE.toProtocolStage(),
        addingNoise.key.computationId.toLong(),
        LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE.toProtocolStage()
      )
  }

  @Test
  fun `syncStatuses starts rollv2 computations`() = runTest {
    val waitingToStart =
      buildComputationAtKingdom(COMPUTATION_GLOBAL_ID, Computation.State.PENDING_COMPUTATION)
    val addingNoise = buildComputationAtKingdom("231313", Computation.State.PENDING_COMPUTATION)
    mockStreamActiveComputationsToReturn(waitingToStart, addingNoise)

    fakeComputationDatabase.addComputation(
      globalId = waitingToStart.key.computationId,
      stage = ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START.toProtocolStage(),
      computationDetails = RO_LLV2_NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newPassThroughBlobMetadata(0L, "local-copy-of-sketches"))
    )

    fakeComputationDatabase.addComputation(
      globalId = addingNoise.key.computationId,
      stage = ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE.toProtocolStage(),
      computationDetails = RO_LLV2_AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(newInputBlobMetadata(0L, "inputs-to-add-noise"), newEmptyOutputBlobMetadata(1L))
    )

    aggregatorHerald.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(eq(setContinuationTokenRequest { this.token = "231313" }))
    }
    assertThat(
        fakeComputationDatabase.mapValues { (_, fakeComputation) ->
          fakeComputation.computationStage
        }
      )
      .containsExactly(
        waitingToStart.key.computationId.toLong(),
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE.toProtocolStage(),
        addingNoise.key.computationId.toLong(),
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE.toProtocolStage()
      )
  }

  @Test
  fun `syncStatuses starts computations with retries`() = runBlocking {
    val computation =
      buildComputationAtKingdom(COMPUTATION_GLOBAL_ID, Computation.State.PENDING_COMPUTATION)
    val streamActiveComputationsJob = Job()
    systemComputations.stub {
      onBlocking { streamActiveComputations(any()) }
        .thenReturn(
          flow {
            emit(
              streamActiveComputationsResponse {
                this.computation = computation
                this.continuationToken = computation.continuationToken()
              }
            )
            streamActiveComputationsJob.complete()
          }
        )
    }
    fakeComputationDatabase.addComputation(
      globalId = computation.key.computationId,
      stage = LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage(),
      computationDetails = LLV2_NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newInputBlobMetadata(0L, "local-copy-of-sketches"))
    )

    val syncResult = async { nonAggregatorHerald.syncStatuses() }

    // Verify that after first attempt, computation is still in INITIALIZATION_PHASE.
    streamActiveComputationsJob.join()

    assertThat(
        fakeComputationDatabase.mapValues { (_, fakeComputation) ->
          fakeComputation.computationStage
        }
      )
      .containsExactly(
        computation.key.computationId.toLong(),
        LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage()
      )

    // Update the state.
    fakeComputationDatabase.remove(computation.key.computationId.toLong())
    fakeComputationDatabase.addComputation(
      globalId = computation.key.computationId,
      stage = LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START.toProtocolStage(),
      computationDetails = LLV2_NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newPassThroughBlobMetadata(0L, "local-copy-of-sketches"))
    )
    // Verify that next attempt succeeds.
    syncResult.await()
    val finalComputation =
      assertNotNull(fakeComputationDatabase[computation.key.computationId.toLong()])
    assertThat(finalComputation.computationStage)
      .isEqualTo(LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE.toProtocolStage())
  }

  @Test
  fun `syncStatuses gives up on starting computations`() = runTest {
    val heraldWithOneRetry =
      Herald(
        NON_AGGREGATOR_HERALD_ID,
        NON_AGGREGATOR_DUCHY_ID,
        internalComputationsStub,
        systemComputationsStub,
        systemComputationParticipantsStub,
        ContinuationTokenManager(continuationTokensStub),
        NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG,
        Clock.systemUTC(),
        maxAttempts = 2
      )

    val computation =
      buildComputationAtKingdom(COMPUTATION_GLOBAL_ID, Computation.State.PENDING_COMPUTATION)
    mockStreamActiveComputationsToReturn(computation)

    fakeComputationDatabase.addComputation(
      globalId = computation.key.computationId,
      stage = LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE.toProtocolStage(),
      computationDetails = LLV2_NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newInputBlobMetadata(0L, "local-copy-of-sketches"))
    )

    heraldWithOneRetry.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(eq(setContinuationTokenRequest { this.token = COMPUTATION_GLOBAL_ID }))
    }
    verifyProtoArgument(
        systemComputationParticipants,
        SystemComputationParticipantsCoroutineImplBase::failComputationParticipant
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        failComputationParticipantRequest {
          name =
            ComputationParticipantKey(computation.key.computationId, NON_AGGREGATOR_DUCHY_ID)
              .toName()
          failure =
            ComputationParticipantKt.failure {
              participantChildReferenceId = NON_AGGREGATOR_HERALD_ID
            }
        }
      )
  }

  @Test
  fun `syncStatuses fails computation for non-transient error`() = runTest {
    // Build an invalid computation which causes non-transient error at Herald
    val invalidComputation =
      buildComputationAtKingdom(COMPUTATION_GLOBAL_ID, Computation.State.PENDING_REQUISITION_PARAMS)
        .copy { measurementSpec = "".toByteStringUtf8() }
    mockStreamActiveComputationsToReturn(invalidComputation)

    nonAggregatorHerald.syncStatuses()

    val failRequest =
      captureFirst<FailComputationParticipantRequest> {
        runBlocking { verify(systemComputationParticipants).failComputationParticipant(capture()) }
      }
    assertThat(failRequest.name)
      .isEqualTo(
        ComputationParticipantKey(invalidComputation.key.computationId, NON_AGGREGATOR_DUCHY_ID)
          .toName()
      )
    assertThat(failRequest.failure.errorMessage).contains("1 attempts")
  }

  @Test
  fun `syncStatuses fails computation for attempts-exhausted error`() = runTest {
    val herald =
      Herald(
        AGGREGATOR_HERALD_ID,
        AGGREGATOR_DUCHY_ID,
        mockBasedInternalComputationsStub,
        systemComputationsStub,
        systemComputationParticipantsStub,
        ContinuationTokenManager(continuationTokensStub),
        AGGREGATOR_PROTOCOLS_SETUP_CONFIG,
        Clock.systemUTC(),
        deletableComputationStates = setOf(Computation.State.SUCCEEDED, Computation.State.FAILED),
      )
    // Set up a new herald with mock services to raise certain exception
    internalComputationsMock.stub {
      onBlocking { createComputation(any()) }.thenThrow(Status.UNKNOWN.asRuntimeException())
      onBlocking { finishComputation(any()) }
        .thenReturn(FinishComputationResponse.getDefaultInstance())
      onBlocking { getComputationToken(any()) }.thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    val computation =
      buildComputationAtKingdom(COMPUTATION_GLOBAL_ID, Computation.State.PENDING_REQUISITION_PARAMS)
    mockStreamActiveComputationsToReturn(computation)

    herald.syncStatuses()

    val failRequest =
      captureFirst<FailComputationParticipantRequest> {
        runBlocking { verify(systemComputationParticipants).failComputationParticipant(capture()) }
      }
    assertThat(failRequest.name)
      .isEqualTo(
        ComputationParticipantKey(computation.key.computationId, AGGREGATOR_DUCHY_ID).toName()
      )
    assertThat(failRequest.failure.errorMessage).contains("3 attempts")
  }

  @Test
  fun `syncStatuses finishes multiple tasks under coordination of semaphore`() = runTest {
    val computations =
      (1..10).map {
        buildComputationAtKingdom(it.toString(), Computation.State.PENDING_REQUISITION_PARAMS)
      }
    mockStreamActiveComputationsToReturn(*computations.toTypedArray())

    aggregatorHerald.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(eq(setContinuationTokenRequest { this.token = "10" }))
    }
  }

  @Test
  fun `syncStatuses calls deleteComputation api for Computations in terminated states`() = runTest {
    val herald =
      Herald(
        AGGREGATOR_HERALD_ID,
        AGGREGATOR_DUCHY_ID,
        mockBasedInternalComputationsStub,
        systemComputationsStub,
        systemComputationParticipantsStub,
        ContinuationTokenManager(continuationTokensStub),
        AGGREGATOR_PROTOCOLS_SETUP_CONFIG,
        Clock.systemUTC(),
        deletableComputationStates = setOf(Computation.State.SUCCEEDED, Computation.State.FAILED),
      )

    internalComputationsMock.stub {
      onBlocking { getComputationToken(any()) }
        .thenAnswer { invocationOnMock ->
          val request = invocationOnMock.getArgument<GetComputationTokenRequest>(0)
          getComputationTokenResponse {
            token = computationToken {
              globalComputationId = request.globalComputationId
              localComputationId = request.globalComputationId.toLong()
              computationDetails = LLV2_AGGREGATOR_COMPUTATION_DETAILS
            }
          }
        }
      onBlocking { deleteComputation(any()) }.thenReturn(Empty.getDefaultInstance())
      onBlocking { finishComputation(any()) }
        .thenReturn(FinishComputationResponse.getDefaultInstance())
    }
    // The Herald deletes SUCCEEDED and FAILED Computations as configured.
    val computation1 = buildComputationAtKingdom("1", Computation.State.SUCCEEDED)
    val computation2 = buildComputationAtKingdom("2", Computation.State.FAILED)
    val computation3 = buildComputationAtKingdom("3", Computation.State.CANCELLED)
    mockStreamActiveComputationsToReturn(computation1, computation2, computation3)

    herald.syncStatuses()

    verifyBlocking(continuationTokensService, atLeastOnce()) {
      setContinuationToken(eq(setContinuationTokenRequest { this.token = "3" }))
    }

    // Verify that internalComputationService receives delete requests for SUCCEEDED and FAILED
    // Computations
    val internalComputationServiceCaptor: KArgumentCaptor<DeleteComputationRequest> =
      argumentCaptor()
    verifyBlocking(internalComputationsMock, times(2)) {
      deleteComputation(internalComputationServiceCaptor.capture())
    }
    val capturedInternalReportingSetRequests = internalComputationServiceCaptor.allValues
    assertThat(capturedInternalReportingSetRequests)
      .containsExactly(
        deleteComputationRequest { localComputationId = 1L },
        deleteComputationRequest { localComputationId = 2L },
      )
  }

  private fun mockStreamActiveComputationsToReturn(vararg computations: Computation) =
    systemComputations.stub {
      onBlocking { streamActiveComputations(any()) }
        .thenReturn(
          computations
            .toList()
            .map {
              StreamActiveComputationsResponse.newBuilder()
                .apply {
                  computation = it
                  continuationToken = it.continuationToken()
                }
                .build()
            }
            .asFlow()
        )
    }

  private fun Computation.continuationToken(): String = key.computationId

  companion object {
    private val ALL_COMPUTATION_PARTICIPANTS =
      listOf(
        computationParticipant {
          name = ComputationParticipantKey(COMPUTATION_GLOBAL_ID, DUCHY_ONE).toName()
        },
        computationParticipant {
          name = ComputationParticipantKey(COMPUTATION_GLOBAL_ID, DUCHY_TWO).toName()
        },
        computationParticipant {
          name = ComputationParticipantKey(COMPUTATION_GLOBAL_ID, DUCHY_THREE).toName()
        },
      )

    /**
     * Builds a kingdom system Api Computation using default values for fields not included in the
     * parameters.
     */
    private fun buildComputationAtKingdom(
      globalId: String,
      stateAtKingdom: Computation.State,
      systemApiRequisitions: List<Requisition> = listOf(),
      systemComputationParticipant: List<SystemComputationParticipant> =
        ALL_COMPUTATION_PARTICIPANTS,
      serializedMeasurementSpec: ByteString = SERIALIZED_MEASUREMENT_SPEC,
      mpcProtocolConfig: MpcProtocolConfig = LLV2_MPC_PROTOCOL_CONFIG
    ): Computation {
      return computation {
        name = ComputationKey(globalId).toName()
        publicApiVersion = PUBLIC_API_VERSION
        measurementSpec = serializedMeasurementSpec
        state = stateAtKingdom
        requisitions += systemApiRequisitions
        computationParticipants += systemComputationParticipant
        this.mpcProtocolConfig = mpcProtocolConfig
      }
    }
  }
}
