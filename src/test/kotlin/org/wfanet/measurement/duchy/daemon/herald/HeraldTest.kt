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
import com.google.protobuf.ByteString
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
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.daemon.testing.TestRequisition
import org.wfanet.measurement.duchy.daemon.utils.key
import org.wfanet.measurement.duchy.daemon.utils.toDuchyEncryptionPublicKey
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newPassThroughBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase as DuchyComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub as DuchyComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START
import org.wfanet.measurement.system.v1alpha.Computation
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
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as SystemComputationParticipantsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineImplBase as SystemComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.FailComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsResponse
import org.wfanet.measurement.system.v1alpha.computationParticipant
import org.wfanet.measurement.system.v1alpha.copy
import org.wfanet.measurement.system.v1alpha.differentialPrivacyParams
import org.wfanet.measurement.system.v1alpha.failComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.streamActiveComputationsResponse

private const val PUBLIC_API_VERSION = "v2alpha"
private const val EMPTY_TOKEN = ""
private const val DUCHY_ONE = "BOHEMIA"
private const val DUCHY_TWO = "SALZBURG"
private const val DUCHY_THREE = "AUSTRIA"

private val REQUISITION_1 = TestRequisition("1") { SERIALIZED_MEASUREMENT_SPEC }
private val REQUISITION_2 = TestRequisition("2") { SERIALIZED_MEASUREMENT_SPEC }

private val PUBLIC_API_ENCRYPTION_PUBLIC_KEY = encryptionPublicKey {
  format = EncryptionPublicKey.Format.TINK_KEYSET
  data = ByteString.copyFromUtf8("A nice encryption public key.")
}

private val PUBLIC_API_MEASUREMENT_SPEC =
  MeasurementSpec.newBuilder()
    .apply {
      measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.toByteString()
      reachAndFrequencyBuilder.apply {
        reachPrivacyParamsBuilder.apply {
          epsilon = 1.1
          delta = 1.2
        }
        frequencyPrivacyParamsBuilder.apply {
          epsilon = 2.1
          delta = 2.2
        }
      }
      addNonceHashes(REQUISITION_1.nonceHash)
      addNonceHashes(REQUISITION_2.nonceHash)
    }
    .build()
private val SERIALIZED_MEASUREMENT_SPEC: ByteString = PUBLIC_API_MEASUREMENT_SPEC.toByteString()

private val MPC_PROTOCOL_CONFIG = mpcProtocolConfig {
  liquidLegionsV2 = liquidLegionsV2 {
    sketchParams = liquidLegionsSketchParams {
      decayRate = 12.0
      maxSize = 100_000
    }
    mpcNoise = mpcNoise {
      blindedHistogramNoise = differentialPrivacyParams {
        epsilon = 3.1
        delta = 3.2
      }
      noiseForPublisherNoise = differentialPrivacyParams {
        epsilon = 4.1
        delta = 4.2
      }
    }
    ellipticCurveId = 415
    maximumFrequency = 10
  }
}

private const val AGGREGATOR_DUCHY_ID = "aggregator_duchy"
private const val AGGREGATOR_HERALD_ID = "aggregator_herald"
private const val NON_AGGREGATOR_DUCHY_ID = "worker_duchy"
private const val NON_AGGREGATOR_HERALD_ID = "worker_herald"

private val AGGREGATOR_PROTOCOLS_SETUP_CONFIG =
  ProtocolsSetupConfig.newBuilder()
    .apply {
      liquidLegionsV2Builder.apply {
        role = RoleInComputation.AGGREGATOR
        externalAggregatorDuchyId = DUCHY_ONE
      }
    }
    .build()
private val NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG =
  ProtocolsSetupConfig.newBuilder()
    .apply {
      liquidLegionsV2Builder.apply {
        role = RoleInComputation.NON_AGGREGATOR
        externalAggregatorDuchyId = DUCHY_ONE
      }
    }
    .build()

private val AGGREGATOR_COMPUTATION_DETAILS =
  ComputationDetails.newBuilder()
    .apply { liquidLegionsV2Builder.apply { role = RoleInComputation.AGGREGATOR } }
    .build()
private val NON_AGGREGATOR_COMPUTATION_DETAILS =
  ComputationDetails.newBuilder()
    .apply { liquidLegionsV2Builder.apply { role = RoleInComputation.NON_AGGREGATOR } }
    .build()

private const val COMPUTATION_GLOBAL_ID = "42314125676756"

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

  private val fakeComputationStorage = FakeComputationsDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(systemComputations)
    addService(
      ComputationsService(
        fakeComputationStorage,
        computationLogEntriesCoroutineStub,
        DUCHY_ONE,
        Clock.systemUTC()
      )
    )
    addService(systemComputationParticipants)
    addService(computationLogEntries)
  }

  private val internalComputationsStub: DuchyComputationsCoroutineStub by lazy {
    DuchyComputationsCoroutineStub(grpcTestServerRule.channel)
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

  private lateinit var aggregatorHerald: Herald
  private lateinit var nonAggregatorHerald: Herald

  @Before
  fun initHerald() {
    aggregatorHerald =
      Herald(
        AGGREGATOR_HERALD_ID,
        AGGREGATOR_DUCHY_ID,
        internalComputationsStub,
        systemComputationsStub,
        systemComputationParticipantsStub,
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
        NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG,
        Clock.systemUTC(),
      )
  }

  @Test
  fun `syncStatuses on empty stream retains same computation token`() = runTest {
    mockStreamActiveComputationsToReturn() // No items in stream.
    assertThat(nonAggregatorHerald.syncStatuses("TOKEN_OF_LAST_ITEM"))
      .isEqualTo("TOKEN_OF_LAST_ITEM")
    assertThat(fakeComputationStorage).isEmpty()
  }

  @Test
  fun `syncStatuses creates new computations`() = runTest {
    val confirmingKnown =
      buildComputationAtKingdom("454647484950", Computation.State.PENDING_REQUISITION_PARAMS)
    val systemApiRequisitions1 =
      REQUISITION_1.toSystemRequisition("321", Requisition.State.UNFULFILLED)
    val systemApiRequisitions2 =
      REQUISITION_2.toSystemRequisition("321", Requisition.State.UNFULFILLED)

    val confirmingUnknown =
      buildComputationAtKingdom(
        "321",
        Computation.State.PENDING_REQUISITION_PARAMS,
        listOf(systemApiRequisitions1, systemApiRequisitions2)
      )
    mockStreamActiveComputationsToReturn(confirmingKnown, confirmingUnknown)

    fakeComputationStorage.addComputation(
      globalId = confirmingKnown.key.computationId,
      stage = INITIALIZATION_PHASE.toProtocolStage(),
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newInputBlobMetadata(0L, "input-blob"), newEmptyOutputBlobMetadata(1L))
    )

    assertThat(aggregatorHerald.syncStatuses(EMPTY_TOKEN))
      .isEqualTo(confirmingUnknown.continuationToken())
    assertThat(
        fakeComputationStorage.mapValues { (_, fakeComputation) ->
          fakeComputation.computationStage
        }
      )
      .containsExactly(
        confirmingKnown.key.computationId.toLong(),
        INITIALIZATION_PHASE.toProtocolStage(),
        confirmingUnknown.key.computationId.toLong(),
        INITIALIZATION_PHASE.toProtocolStage()
      )

    assertThat(
        fakeComputationStorage[confirmingUnknown.key.computationId.toLong()]?.requisitionsList
      )
      .containsExactly(
        REQUISITION_1.toRequisitionMetadata(Requisition.State.UNFULFILLED),
        REQUISITION_2.toRequisitionMetadata(Requisition.State.UNFULFILLED)
      )
    assertThat(
        fakeComputationStorage[confirmingUnknown.key.computationId.toLong()]?.computationDetails
      )
      .isEqualTo(
        ComputationDetails.newBuilder()
          .apply {
            blobsStoragePrefix = "computation-blob-storage/321"
            kingdomComputationBuilder.apply {
              publicApiVersion = PUBLIC_API_VERSION
              measurementSpec = SERIALIZED_MEASUREMENT_SPEC
              measurementPublicKey = PUBLIC_API_ENCRYPTION_PUBLIC_KEY.toDuchyEncryptionPublicKey()
            }
            liquidLegionsV2Builder.apply {
              role = RoleInComputation.AGGREGATOR
              parametersBuilder.apply {
                maximumFrequency = 10
                liquidLegionsSketchBuilder.apply {
                  decayRate = 12.0
                  size = 100_000L
                }
                noiseBuilder.apply {
                  reachNoiseConfigBuilder.apply {
                    blindHistogramNoiseBuilder.apply {
                      epsilon = 3.1
                      delta = 3.2
                    }
                    noiseForPublisherNoiseBuilder.apply {
                      epsilon = 4.1
                      delta = 4.2
                    }
                    globalReachDpNoiseBuilder.apply {
                      epsilon = 1.1
                      delta = 1.2
                    }
                  }
                  frequencyNoiseConfigBuilder.apply {
                    epsilon = 2.1
                    delta = 2.2
                  }
                }
                ellipticCurveId = 415
              }
            }
          }
          .build()
      )
  }

  @Test
  fun `syncStatuses update llv2 computations in WAIT_REQUISITIONS_AND_KEY_SET`() = runTest {
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

    fakeComputationStorage.addComputation(
      globalId = globalId,
      stage = WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      requisitions =
        listOf(
          REQUISITION_1.toRequisitionMetadata(Requisition.State.UNFULFILLED),
          REQUISITION_2.toRequisitionMetadata(Requisition.State.UNFULFILLED)
        )
    )

    assertThat(aggregatorHerald.syncStatuses(EMPTY_TOKEN))
      .isEqualTo(waitingRequisitionsAndKeySet.continuationToken())

    val duchyComputationToken = fakeComputationStorage.readComputationToken(globalId)!!
    assertThat(duchyComputationToken.computationStage)
      .isEqualTo(CONFIRMATION_PHASE.toProtocolStage())
    assertThat(duchyComputationToken.computationDetails.liquidLegionsV2.participantList)
      .isEqualTo(
        mutableListOf(
          ComputationParticipant.newBuilder()
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
          ComputationParticipant.newBuilder()
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
          ComputationParticipant.newBuilder()
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
  fun `syncStatuses starts computations in wait_to_start`() = runTest {
    val waitingToStart =
      buildComputationAtKingdom(COMPUTATION_GLOBAL_ID, Computation.State.PENDING_COMPUTATION)
    val addingNoise = buildComputationAtKingdom("231313", Computation.State.PENDING_COMPUTATION)
    mockStreamActiveComputationsToReturn(waitingToStart, addingNoise)

    fakeComputationStorage.addComputation(
      globalId = waitingToStart.key.computationId,
      stage = WAIT_TO_START.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newPassThroughBlobMetadata(0L, "local-copy-of-sketches"))
    )

    fakeComputationStorage.addComputation(
      globalId = addingNoise.key.computationId,
      stage = SETUP_PHASE.toProtocolStage(),
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS,
      blobs =
        listOf(newInputBlobMetadata(0L, "inputs-to-add-noise"), newEmptyOutputBlobMetadata(1L))
    )

    assertThat(aggregatorHerald.syncStatuses(EMPTY_TOKEN))
      .isEqualTo(addingNoise.continuationToken())
    assertThat(
        fakeComputationStorage.mapValues { (_, fakeComputation) ->
          fakeComputation.computationStage
        }
      )
      .containsExactly(
        waitingToStart.key.computationId.toLong(),
        SETUP_PHASE.toProtocolStage(),
        addingNoise.key.computationId.toLong(),
        SETUP_PHASE.toProtocolStage()
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
    fakeComputationStorage.addComputation(
      globalId = computation.key.computationId,
      stage = INITIALIZATION_PHASE.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newInputBlobMetadata(0L, "local-copy-of-sketches"))
    )

    val syncResult = async { nonAggregatorHerald.syncStatuses(EMPTY_TOKEN) }

    // Verify that after first attempt, computation is still in INITIALIZATION_PHASE.
    streamActiveComputationsJob.join()

    assertThat(
        fakeComputationStorage.mapValues { (_, fakeComputation) ->
          fakeComputation.computationStage
        }
      )
      .containsExactly(
        computation.key.computationId.toLong(),
        INITIALIZATION_PHASE.toProtocolStage()
      )

    // Update the state.
    fakeComputationStorage.remove(computation.key.computationId.toLong())
    fakeComputationStorage.addComputation(
      globalId = computation.key.computationId,
      stage = WAIT_TO_START.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newPassThroughBlobMetadata(0L, "local-copy-of-sketches"))
    )
    // Verify that next attempt succeeds.
    syncResult.await()
    val finalComputation =
      assertNotNull(fakeComputationStorage[computation.key.computationId.toLong()])
    assertThat(finalComputation.computationStage).isEqualTo(SETUP_PHASE.toProtocolStage())
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
        NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG,
        Clock.systemUTC(),
        maxAttempts = 2
      )

    val computation =
      buildComputationAtKingdom(COMPUTATION_GLOBAL_ID, Computation.State.PENDING_COMPUTATION)
    mockStreamActiveComputationsToReturn(computation)

    fakeComputationStorage.addComputation(
      globalId = computation.key.computationId,
      stage = INITIALIZATION_PHASE.toProtocolStage(),
      computationDetails = NON_AGGREGATOR_COMPUTATION_DETAILS,
      blobs = listOf(newInputBlobMetadata(0L, "local-copy-of-sketches"))
    )

    assertThat(heraldWithOneRetry.syncStatuses("")).isEqualTo("token_for_$COMPUTATION_GLOBAL_ID")
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

    nonAggregatorHerald.syncStatuses(EMPTY_TOKEN)

    val failRequest =
      captureFirst<FailComputationParticipantRequest> {
        runBlocking { verify(systemComputationParticipants).failComputationParticipant(capture()) }
      }
    assertThat(failRequest.name)
      .isEqualTo(
        ComputationParticipantKey(invalidComputation.key.computationId, NON_AGGREGATOR_DUCHY_ID)
          .toName()
      )
    assertThat(failRequest.failure.errorMessage).contains("1 attempt(s)")
  }

  @Test
  fun `syncStatuses fails computation for attempts-exhausted error`() = runTest {
    // Set up a new herald with mock services to raise certain exception
    val internalComputationsService: DuchyComputationsCoroutineImplBase = mockService {
      onBlocking { createComputation(any()) }.thenThrow(Status.UNKNOWN.asRuntimeException())
      onBlocking { finishComputation(any()) }
        .thenReturn(FinishComputationResponse.getDefaultInstance())
    }
    val mockTestServerRule = GrpcTestServerRule {
      addService(systemComputations)
      addService(internalComputationsService)
      addService(systemComputationParticipants)
    }
    val mockInternalComputationsStub = DuchyComputationsCoroutineStub(mockTestServerRule.channel)
    val mockHerald =
      Herald(
        AGGREGATOR_HERALD_ID,
        AGGREGATOR_DUCHY_ID,
        mockInternalComputationsStub,
        systemComputationsStub,
        systemComputationParticipantsStub,
        AGGREGATOR_PROTOCOLS_SETUP_CONFIG,
        Clock.systemUTC(),
      )

    val computation =
      buildComputationAtKingdom(COMPUTATION_GLOBAL_ID, Computation.State.PENDING_REQUISITION_PARAMS)
    mockStreamActiveComputationsToReturn(computation)

    mockHerald.syncStatuses(EMPTY_TOKEN)

    val failRequest =
      captureFirst<FailComputationParticipantRequest> {
        runBlocking { verify(systemComputationParticipants).failComputationParticipant(capture()) }
      }
    assertThat(failRequest.name)
      .isEqualTo(
        ComputationParticipantKey(computation.key.computationId, AGGREGATOR_DUCHY_ID).toName()
      )
    assertThat(failRequest.failure.errorMessage).contains("3 attempt(s)")
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

  @Test
  fun `syncStatuses finishes multiple tasks under coordination of semaphore`() = runTest {
    val computations =
      (1..10).map {
        buildComputationAtKingdom(it.toString(), Computation.State.PENDING_REQUISITION_PARAMS)
      }
    mockStreamActiveComputationsToReturn(*computations.toTypedArray())

    assertThat(aggregatorHerald.syncStatuses(EMPTY_TOKEN)).isNotEmpty()
  }

  /**
   * Builds a kingdom system Api Computation using default values for fields not included in the
   * parameters.
   */
  private fun buildComputationAtKingdom(
    globalId: String,
    stateAtKingdom: Computation.State,
    systemApiRequisitions: List<Requisition> = listOf(),
    systemComputationParticipant: List<SystemComputationParticipant> = listOf()
  ): Computation {
    return Computation.newBuilder()
      .also {
        it.name = ComputationKey(globalId).toName()
        it.publicApiVersion = PUBLIC_API_VERSION
        it.measurementSpec = SERIALIZED_MEASUREMENT_SPEC
        it.state = stateAtKingdom
        it.addAllRequisitions(systemApiRequisitions)
        it.addAllComputationParticipants(systemComputationParticipant)
        it.mpcProtocolConfig = MPC_PROTOCOL_CONFIG
      }
      .build()
  }

  private fun Computation.continuationToken(): String = "token_for_${key.computationId}"
}
