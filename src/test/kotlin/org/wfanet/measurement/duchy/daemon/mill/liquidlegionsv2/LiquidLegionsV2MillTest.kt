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

package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verifyBlocking
import com.nhaarman.mockitokotlin2.verifyZeroInteractions
import com.nhaarman.mockitokotlin2.whenever
import io.grpc.Status
import java.time.Clock
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.size
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.mill.CryptoKeySet
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.LiquidLegionsV2Mill
import org.wfanet.measurement.duchy.daemon.mill.toElGamalKeyPair
import org.wfanet.measurement.duchy.daemon.mill.toElGamalPublicKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.service.internal.computation.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computation.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newPassThroughBlobMetadata
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.MetricValue
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineImplBase
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.internal.duchy.StreamMetricValueRequest
import org.wfanet.measurement.internal.duchy.StreamMetricValueResponse
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation
import org.wfanet.measurement.protocol.CompleteExecutionPhaseOneAtAggregatorRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseOneAtAggregatorResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseOneRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseOneResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseThreeAtAggregatorRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseThreeAtAggregatorResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseThreeRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseThreeResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseTwoAtAggregatorRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseTwoAtAggregatorResponse
import org.wfanet.measurement.protocol.CompleteExecutionPhaseTwoRequest
import org.wfanet.measurement.protocol.CompleteExecutionPhaseTwoResponse
import org.wfanet.measurement.protocol.CompleteSetupPhaseRequest
import org.wfanet.measurement.protocol.CompleteSetupPhaseResponse
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.COMPLETE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.CONFIRM_REQUISITIONS_PHASE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_TWO
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START
import org.wfanet.measurement.protocol.LiquidLegionsV2NoiseConfig
import org.wfanet.measurement.protocol.RequisitionKey
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.read
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.AdvanceComputationResponse
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.system.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputation
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate.ErrorDetails.ErrorType
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate.MpcAlgorithm
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2.Description.SETUP_PHASE_INPUT
import org.wfanet.measurement.system.v1alpha.MetricRequisitionKey

@RunWith(JUnit4::class)
class LiquidLegionsV2MillTest {
  private val mockLiquidLegionsComputationControl: ComputationControlCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockMetricValues: MetricValuesCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockGlobalComputations: GlobalComputationsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockComputationStats: ComputationStatsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockCryptoWorker: LiquidLegionsV2Encryption =
    mock(useConstructor = UseConstructor.parameterless())
  private val fakeComputationDb = FakeComputationsDatabase()

  private lateinit var computationDataClients: ComputationDataClients
  private lateinit var computationStore: ComputationStore

  private val tempDirectory = TemporaryFolder()

  private val blobCount = AtomicInteger()
  private val generatedBlobKeys = mutableListOf<String>()
  private fun ComputationToken.generateBlobKey(): String {
    return listOf(localComputationId, computationStage.name, blobCount.getAndIncrement())
      .joinToString("/")
      .also { generatedBlobKeys.add(it) }
  }

  private val aggregatorComputationDetails =
    ComputationDetails.newBuilder()
      .apply {
        liquidLegionsV2Builder.apply {
          role = RoleInComputation.AGGREGATOR
          totalRequisitionCount = PUBLISHER_COUNT
        }
      }
      .build()

  private val nonAggregatorComputationDetails =
    ComputationDetails.newBuilder()
      .apply {
        liquidLegionsV2Builder.apply {
          role = RoleInComputation.NON_AGGREGATOR
          totalRequisitionCount = PUBLISHER_COUNT
        }
      }
      .build()

  private val testNoiseConfig =
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

  private val grpcTestServerRule = GrpcTestServerRule {
    computationStore =
      ComputationStore.forTesting(FileSystemStorageClient(tempDirectory.root)) { generateBlobKey() }
    computationDataClients =
      ComputationDataClients.forTesting(
        ComputationsCoroutineStub(channel),
        computationStore,
        otherDuchyNames
      )
    addService(mockLiquidLegionsComputationControl)
    addService(mockMetricValues)
    addService(mockGlobalComputations)
    addService(mockComputationStats)
    addService(
      ComputationsService(
        fakeComputationDb,
        globalComputationStub,
        DUCHY_THREE_NAME,
        Clock.systemUTC()
      )
    )
  }

  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

  private val workerStub: ComputationControlCoroutineStub by lazy {
    ComputationControlCoroutineStub(grpcTestServerRule.channel)
  }

  private val globalComputationStub: GlobalComputationsCoroutineStub by lazy {
    GlobalComputationsCoroutineStub(grpcTestServerRule.channel)
  }

  private val computationStatsStub: ComputationStatsCoroutineStub by lazy {
    ComputationStatsCoroutineStub(grpcTestServerRule.channel)
  }

  private val metricValuesStub: MetricValuesCoroutineStub by lazy {
    MetricValuesCoroutineStub(grpcTestServerRule.channel)
  }

  private lateinit var computationControlRequests: List<AdvanceComputationRequest>

  // Just use the same workerStub for all other duchies, since it is not relevant to this test.a
  private val workerStubs = mapOf(DUCHY_TWO_NAME to workerStub, DUCHY_THREE_NAME to workerStub)

  private lateinit var aggregatorMill: LiquidLegionsV2Mill
  private lateinit var nonAggregatorMill: LiquidLegionsV2Mill

  private fun String.toMetricChunkResponse() = ByteString.copyFromUtf8(this).toMetricChunkResponse()

  private fun ByteString.toMetricChunkResponse(): StreamMetricValueResponse {
    return StreamMetricValueResponse.newBuilder().also { it.chunkBuilder.data = this }.build()
  }

  private fun String.toMetricValueResourceKey() =
    MetricValue.ResourceKey.newBuilder()
      .setCampaignResourceId("campaignId_$this")
      .setDataProviderResourceId("dataProvideId_$this")
      .setMetricRequisitionResourceId("requisitionId_$this")
      .build()

  private fun String.toRequisitionKey() =
    RequisitionKey.newBuilder()
      .setCampaignId("campaignId_$this")
      .setDataProviderId("dataProvideId_$this")
      .setMetricRequisitionId("requisitionId_$this")
      .build()

  private fun String.toMetricRequisitionKey() =
    MetricRequisitionKey.newBuilder()
      .setCampaignId("campaignId_$this")
      .setDataProviderId("dataProvideId_$this")
      .setMetricRequisitionId("requisitionId_$this")
      .build()

  private fun buildAdvanceComputationRequests(
    globalComputationId: String,
    description: LiquidLegionsV2.Description,
    vararg chunkContents: String
  ): List<AdvanceComputationRequest> {
    val header =
      AdvanceComputationRequest.newBuilder()
        .apply {
          headerBuilder.apply {
            keyBuilder.globalComputationId = globalComputationId
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
  fun setup() {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(60))
    aggregatorMill =
      LiquidLegionsV2Mill(
        millId = MILL_ID,
        duchyId = DUCHY_ONE_NAME,
        dataClients = computationDataClients,
        metricValuesClient = metricValuesStub,
        globalComputationsClient = globalComputationStub,
        computationStatsClient = computationStatsStub,
        workerStubs = workerStubs,
        cryptoKeySet = cryptoKeySet,
        cryptoWorker = mockCryptoWorker,
        throttler = throttler,
        requestChunkSizeBytes = 20,
        maxFrequency = MAX_FREQUENCY,
        noiseConfig = testNoiseConfig,
        aggregatorId = DUCHY_ONE_NAME
      )

    nonAggregatorMill =
      LiquidLegionsV2Mill(
        millId = MILL_ID,
        duchyId = DUCHY_ONE_NAME,
        dataClients = computationDataClients,
        metricValuesClient = metricValuesStub,
        globalComputationsClient = globalComputationStub,
        computationStatsClient = computationStatsStub,
        workerStubs = workerStubs,
        cryptoKeySet = cryptoKeySet,
        cryptoWorker = mockCryptoWorker,
        throttler = throttler,
        requestChunkSizeBytes = 20,
        maxFrequency = MAX_FREQUENCY,
        noiseConfig = testNoiseConfig,
        aggregatorId = DUCHY_THREE_NAME
      )

    whenever(runBlocking { mockLiquidLegionsComputationControl.advanceComputation(any()) })
      .thenAnswer {
        val request: Flow<AdvanceComputationRequest> = it.getArgument(0)
        computationControlRequests = runBlocking { request.toList() }
        AdvanceComputationResponse.getDefaultInstance()
      }
  }

  @Test
  fun `confirm requisition, no local requisitions required at aggregator`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = CONFIRM_REQUISITIONS_PHASE.toProtocolStage(),
      computationDetails = aggregatorComputationDetails,
      blobs = listOf(newEmptyOutputBlobMetadata(0L))
    )

    // Stage 1. Process the above computation
    aggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(WAIT_SETUP_PHASE_INPUTS.toProtocolStage())
          .addAllBlobs(
            listOf(
              newPassThroughBlobMetadata(0, blobKey),
              newEmptyOutputBlobMetadata(1),
              newEmptyOutputBlobMetadata(2)
            )
          )
          .setStageSpecificDetails(
            ComputationStageDetails.newBuilder().apply {
              liquidLegionsV2Builder.waitSetupPhaseInputsDetailsBuilder.apply {
                putExternalDuchyLocalBlobId("DUCHY_TWO", 1L)
                putExternalDuchyLocalBlobId("DUCHY_THREE", 2L)
              }
            }
          )
          .setComputationDetails(aggregatorComputationDetails)
          .setVersion(3) // CreateComputation + write blob + transitionStage
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString()).isEmpty()

    verifyZeroInteractions(mockMetricValues)
    verifyProtoArgument(
        mockGlobalComputations,
        GlobalComputationsCoroutineImplBase::confirmGlobalComputation
      )
      .isEqualTo(
        ConfirmGlobalComputationRequest.newBuilder()
          .setKey(GlobalComputation.Key.newBuilder().setGlobalComputationId(GLOBAL_ID))
          .build()
      )
  }

  @Test
  fun `confirm requisition, all local requisitions available non-aggregator`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = CONFIRM_REQUISITIONS_PHASE.toProtocolStage(),
      computationDetails = nonAggregatorComputationDetails,
      blobs = listOf(newEmptyOutputBlobMetadata(0L)),
      stageDetails =
        ComputationStageDetails.newBuilder()
          .apply {
            liquidLegionsV2Builder.toConfirmRequisitionsStageDetailsBuilder.apply {
              addKeys("1".toRequisitionKey())
              addKeys("2".toRequisitionKey())
            }
          }
          .build()
    )

    lateinit var metricValuesRequest1: StreamMetricValueRequest
    lateinit var metricValuesRequest2: StreamMetricValueRequest
    whenever(mockMetricValues.streamMetricValue(any()))
      .thenAnswer {
        metricValuesRequest1 = it.getArgument(0)
        flowOf(
          StreamMetricValueResponse.newBuilder()
            .setHeader(StreamMetricValueResponse.Header.getDefaultInstance())
            .build(),
          // Add a header to test filtering
          "A_chunk_1_".toMetricChunkResponse(),
          "A_chunk_2_".toMetricChunkResponse(),
          "A_chunk_3_".toMetricChunkResponse()
        )
      }
      .thenAnswer {
        metricValuesRequest2 = it.getArgument(0)
        flowOf("B_chunk_1_".toMetricChunkResponse(), "B_chunk_2_".toMetricChunkResponse())
      }

    // Stage 1. Process the above computation
    nonAggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(WAIT_TO_START.toProtocolStage())
          .addBlobs(newPassThroughBlobMetadata(0, blobKey))
          .setVersion(3) // CreateComputation + write blob + transitionStage
          .setComputationDetails(nonAggregatorComputationDetails)
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("A_chunk_1_A_chunk_2_A_chunk_3_B_chunk_1_B_chunk_2_")

    assertThat(metricValuesRequest1)
      .isEqualTo(
        StreamMetricValueRequest.newBuilder().setResourceKey("1".toMetricValueResourceKey()).build()
      )
    assertThat(metricValuesRequest2)
      .isEqualTo(
        StreamMetricValueRequest.newBuilder().setResourceKey("2".toMetricValueResourceKey()).build()
      )
    verifyProtoArgument(
        mockGlobalComputations,
        GlobalComputationsCoroutineImplBase::confirmGlobalComputation
      )
      .isEqualTo(
        ConfirmGlobalComputationRequest.newBuilder()
          .setKey(GlobalComputation.Key.newBuilder().setGlobalComputationId(GLOBAL_ID))
          .addReadyRequisitions("1".toMetricRequisitionKey())
          .addReadyRequisitions("2".toMetricRequisitionKey())
          .build()
      )
  }

  @Test
  fun `confirm requisition, missing requisition at aggregator`() =
    runBlocking<Unit> {
      // Stage 0. preparing the storage and set up mock
      val requisition1 = "1"
      val requisition2 = "2"
      fakeComputationDb.addComputation(
        globalId = GLOBAL_ID,
        stage = CONFIRM_REQUISITIONS_PHASE.toProtocolStage(),
        computationDetails = nonAggregatorComputationDetails,
        blobs = listOf(newEmptyOutputBlobMetadata(0L)),
        stageDetails =
          ComputationStageDetails.newBuilder()
            .apply {
              liquidLegionsV2Builder.toConfirmRequisitionsStageDetailsBuilder.apply {
                addKeys(requisition1.toRequisitionKey())
                addKeys(requisition2.toRequisitionKey())
              }
            }
            .build()
      )

      val metricValue =
        MetricValue.newBuilder().setResourceKey(requisition1.toMetricValueResourceKey()).build()
      val content = ByteString.copyFromUtf8("chunk")
      whenever(mockMetricValues.getMetricValue(any()))
        .thenReturn(metricValue)
        .thenThrow(Status.NOT_FOUND.asRuntimeException())
      whenever(mockMetricValues.streamMetricValue(any()))
        .thenReturn(
          flowOf(
            StreamMetricValueResponse.newBuilder()
              .apply {
                headerBuilder.metricValue = metricValue
                headerBuilder.dataSizeBytes = content.size.toLong()
              }
              .build(),
            content.toMetricChunkResponse()
          )
        )
        .thenThrow(Status.NOT_FOUND.asRuntimeException())
      whenever(mockGlobalComputations.createGlobalComputationStatusUpdate(any()))
        .thenReturn(GlobalComputationStatusUpdate.getDefaultInstance())
      whenever(mockGlobalComputations.confirmGlobalComputation(any()))
        .thenReturn(GlobalComputation.getDefaultInstance())

      // Stage 1. Process the above computation
      aggregatorMill.pollAndProcessNextComputation()

      // Stage 2. Check the status of the computation
      assertThat(fakeComputationDb[LOCAL_ID]!!)
        .isEqualTo(
          ComputationToken.newBuilder()
            .setGlobalComputationId(GLOBAL_ID)
            .setLocalComputationId(LOCAL_ID)
            .setAttempt(1)
            .setComputationStage(COMPLETE.toProtocolStage())
            .setVersion(2) // CreateComputation + transitionStage
            .setComputationDetails(nonAggregatorComputationDetails)
            .build()
        )

      // Only one requisition is confirmed
      verifyProtoArgument(
          mockGlobalComputations,
          GlobalComputationsCoroutineImplBase::confirmGlobalComputation
        )
        .isEqualTo(
          ConfirmGlobalComputationRequest.newBuilder()
            .setKey(GlobalComputation.Key.newBuilder().setGlobalComputationId(GLOBAL_ID))
            .addReadyRequisitions("1".toMetricRequisitionKey())
            .build()
        )

      argumentCaptor<CreateGlobalComputationStatusUpdateRequest> {
        verifyBlocking(mockGlobalComputations, times(3)) {
          createGlobalComputationStatusUpdate(capture())
        }
        assertThat(allValues[1])
          .comparingExpectedFieldsOnly()
          .isEqualTo(
            CreateGlobalComputationStatusUpdateRequest.newBuilder()
              .apply {
                parentBuilder.globalComputationId = GLOBAL_ID
                statusUpdateBuilder.apply {
                  selfReportedIdentifier = MILL_ID
                  stageDetailsBuilder.apply {
                    algorithm = MpcAlgorithm.LIQUID_LEGIONS_V2
                    stageNumber = CONFIRM_REQUISITIONS_PHASE.number.toLong()
                    stageName = CONFIRM_REQUISITIONS_PHASE.name
                    attemptNumber = 1
                  }
                  updateMessage =
                    "Computation $GLOBAL_ID at stage CONFIRM_REQUISITIONS_PHASE," +
                      " attempt 1 failed."
                  errorDetailsBuilder.apply { errorType = ErrorType.PERMANENT }
                }
              }
              .build()
          )
      }
    }

  @Test
  fun `setup phase at non-aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = SETUP_PHASE.toProtocolStage()
        )
        .build()
    computationStore.writeString(partialToken, "sketch")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = nonAggregatorComputationDetails,
      blobs =
        listOf(newInputBlobMetadata(0L, generatedBlobKeys.last()), newEmptyOutputBlobMetadata(1L))
    )

    var cryptoRequest = CompleteSetupPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeSetupPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeSetupPhase-done")
      CompleteSetupPhaseResponse.newBuilder()
        .setCombinedRegisterVector(cryptoRequest.combinedRegisterVector.concat(postFix))
        .build()
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage())
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.INPUT)
              .setBlobId(0)
              .setPath(blobKey)
          )
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.OUTPUT)
              .setBlobId(1)
          )
          .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
          .setComputationDetails(nonAggregatorComputationDetails)
          .build()
      )

    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("sketch-completeSetupPhase-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          SETUP_PHASE_INPUT,
          "sketch-completeSetup",
          "Phase-done"
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteSetupPhaseRequest.newBuilder()
          .apply {
            combinedRegisterVector = ByteString.copyFromUtf8("sketch")
            noiseParametersBuilder.apply {
              compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
              curveId = cryptoKeySet.curveId.toLong()
              contributorsCount = WORKER_COUNT
              totalSketchesCount = PUBLISHER_COUNT
              dpParamsBuilder.apply {
                blindHistogram = testNoiseConfig.reachNoiseConfig.blindHistogramNoise
                noiseForPublisherNoise = testNoiseConfig.reachNoiseConfig.noiseForPublisherNoise
                globalReachDpNoise = testNoiseConfig.reachNoiseConfig.globalReachDpNoise
              }
            }
          }
          .build()
      )
  }

  @Test
  fun `setup phase at aggregator using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = SETUP_PHASE.toProtocolStage()
        )
        .build()
    computationStore.writeString(partialToken, "sketch_1_")
    computationStore.writeString(partialToken, "sketch_2_")
    computationStore.writeString(partialToken, "sketch_3_")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = aggregatorComputationDetails,
      blobs =
        listOf(
          newInputBlobMetadata(0L, generatedBlobKeys[0]),
          newInputBlobMetadata(1L, generatedBlobKeys[1]),
          newInputBlobMetadata(2L, generatedBlobKeys[2]),
          newEmptyOutputBlobMetadata(3L)
        )
    )

    var cryptoRequest = CompleteSetupPhaseRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeSetupPhase(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeSetupPhase-done")
      CompleteSetupPhaseResponse.newBuilder()
        .setCombinedRegisterVector(cryptoRequest.combinedRegisterVector.concat(postFix))
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage())
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.INPUT)
              .setBlobId(0)
              .setPath(blobKey)
          )
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.OUTPUT)
              .setBlobId(1)
          )
          .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
          .setComputationDetails(aggregatorComputationDetails)
          .build()
      )

    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("sketch_1_sketch_2_sketch_3_-completeSetupPhase-done")

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(
          GLOBAL_ID,
          EXECUTION_PHASE_ONE_INPUT,
          "sketch_1_sketch_2_sk",
          "etch_3_-completeSetu",
          "pPhase-done"
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteSetupPhaseRequest.newBuilder()
          .apply {
            combinedRegisterVector = ByteString.copyFromUtf8("sketch_1_sketch_2_sketch_3_")
            noiseParametersBuilder.apply {
              compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
              curveId = cryptoKeySet.curveId.toLong()
              contributorsCount = WORKER_COUNT
              totalSketchesCount = PUBLISHER_COUNT
              dpParamsBuilder.apply {
                blindHistogram = testNoiseConfig.reachNoiseConfig.blindHistogramNoise
                noiseForPublisherNoise = testNoiseConfig.reachNoiseConfig.noiseForPublisherNoise
                globalReachDpNoise = testNoiseConfig.reachNoiseConfig.globalReachDpNoise
              }
            }
          }
          .build()
      )
  }

  @Test
  fun `execution phase one at non-aggregater using cached result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_ONE.toProtocolStage()
        )
        .build()
    computationStore.writeString(partialToken, "sketch")
    computationStore.writeString(partialToken, "cached result")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = nonAggregatorComputationDetails,
      blobs =
        listOf(
          newInputBlobMetadata(0L, generatedBlobKeys[0]),
          newOutputBlobMetadata(1L, generatedBlobKeys[1])
        )
    )

    // Stage 1. Process the above computation
    nonAggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage())
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.INPUT)
              .setBlobId(0)
              .setPath(generatedBlobKeys.last())
          )
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.OUTPUT)
              .setBlobId(1)
          )
          .setVersion(2) // CreateComputation + transitionStage
          .setComputationDetails(nonAggregatorComputationDetails)
          .build()
      )

    assertThat(computationControlRequests)
      .containsExactlyElementsIn(
        buildAdvanceComputationRequests(GLOBAL_ID, EXECUTION_PHASE_ONE_INPUT, "cached result")
      )
      .inOrder()
  }

  @Test
  fun `execution phase one at non-aggregater using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_ONE.toProtocolStage()
        )
        .build()
    computationStore.writeString(partialToken, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = nonAggregatorComputationDetails,
      blobs =
        listOf(newInputBlobMetadata(0L, generatedBlobKeys.last()), newEmptyOutputBlobMetadata(1L))
    )

    var cryptoRequest = CompleteExecutionPhaseOneRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseOne(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseOne-done")
      CompleteExecutionPhaseOneResponse.newBuilder()
        .setCombinedRegisterVector(cryptoRequest.combinedRegisterVector.concat(postFix))
        .build()
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage())
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.INPUT)
              .setBlobId(0)
              .setPath(blobKey)
          )
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.OUTPUT)
              .setBlobId(1)
          )
          .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
          .setComputationDetails(nonAggregatorComputationDetails)
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
          "onPhaseOne-done" // Chunk 2, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseOneRequest.newBuilder()
          .apply {
            combinedRegisterVector = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
            curveId = cryptoKeySet.curveId.toLong()
          }
          .build()
      )
  }

  @Test
  fun `execution phase one at aggregater using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_ONE.toProtocolStage()
        )
        .build()
    computationStore.writeString(partialToken, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = aggregatorComputationDetails,
      blobs =
        listOf(newInputBlobMetadata(0L, generatedBlobKeys.last()), newEmptyOutputBlobMetadata(1L))
    )

    var cryptoRequest = CompleteExecutionPhaseOneAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseOneAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseOneAtAggregator-done")
      CompleteExecutionPhaseOneAtAggregatorResponse.newBuilder()
        .setFlagCountTuples(cryptoRequest.combinedRegisterVector.concat(postFix))
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage())
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.INPUT)
              .setBlobId(0)
              .setPath(blobKey)
          )
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.OUTPUT)
              .setBlobId(1)
          )
          .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
          .setComputationDetails(aggregatorComputationDetails)
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
          "or-done" // Chunk 3, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseOneAtAggregatorRequest.newBuilder()
          .apply {
            combinedRegisterVector = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
            curveId = cryptoKeySet.curveId.toLong()
            noiseParametersBuilder.apply {
              maximumFrequency = MAX_FREQUENCY
              contributorsCount = WORKER_COUNT
              dpParams = testNoiseConfig.frequencyNoiseConfig
            }
            totalSketchesCount = PUBLISHER_COUNT
          }
          .build()
      )
  }

  @Test
  fun `execution phase two at non-aggregater using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_TWO.toProtocolStage()
        )
        .build()
    computationStore.writeString(partialToken, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = nonAggregatorComputationDetails,
      blobs =
        listOf(newInputBlobMetadata(0L, generatedBlobKeys.last()), newEmptyOutputBlobMetadata(1L))
    )

    var cryptoRequest = CompleteExecutionPhaseTwoRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseTwo(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseTwo-done")
      CompleteExecutionPhaseTwoResponse.newBuilder()
        .setFlagCountTuples(cryptoRequest.flagCountTuples.concat(postFix))
        .build()
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage())
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.INPUT)
              .setBlobId(0)
              .setPath(blobKey)
          )
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.OUTPUT)
              .setBlobId(1)
          )
          .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
          .setComputationDetails(nonAggregatorComputationDetails)
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
          "onPhaseTwo-done" // Chunk 2, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseTwoRequest.newBuilder()
          .apply {
            flagCountTuples = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
            partialCompositeElGamalPublicKey = DUCHY_THREE_PUBLIC_KEY.toElGamalPublicKey()
            curveId = cryptoKeySet.curveId.toLong()
            noiseParametersBuilder.apply {
              maximumFrequency = MAX_FREQUENCY
              contributorsCount = WORKER_COUNT
              dpParams = testNoiseConfig.frequencyNoiseConfig
            }
          }
          .build()
      )
  }

  @Test
  fun `execution phase two at aggregater using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_TWO.toProtocolStage()
        )
        .build()
    computationStore.writeString(partialToken, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = aggregatorComputationDetails,
      blobs =
        listOf(newInputBlobMetadata(0L, generatedBlobKeys.last()), newEmptyOutputBlobMetadata(1L))
    )

    val testReach = 123L
    var cryptoRequest = CompleteExecutionPhaseTwoAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseTwoAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseTwoAtAggregator-done")
      CompleteExecutionPhaseTwoAtAggregatorResponse.newBuilder()
        .setSameKeyAggregatorMatrix(cryptoRequest.flagCountTuples.concat(postFix))
        .setReach(testReach)
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage())
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.INPUT)
              .setBlobId(0)
              .setPath(blobKey)
          )
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.OUTPUT)
              .setBlobId(1)
          )
          .setVersion(
            4
          ) // CreateComputation + writeOutputBlob + ComputationDetails + transitionStage
          .setComputationDetails(
            aggregatorComputationDetails.toBuilder().apply {
              liquidLegionsV2Builder.reachEstimateBuilder.reach = testReach
            }
          )
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
          "or-done" // Chunk 3, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseTwoAtAggregatorRequest.newBuilder()
          .apply {
            flagCountTuples = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            compositeElGamalPublicKey = cryptoKeySet.clientPublicKey
            curveId = cryptoKeySet.curveId.toLong()
            maximumFrequency = MAX_FREQUENCY
            liquidLegionsParametersBuilder.apply {
              decayRate = 12.0
              size = 10_000_000L
            }
            reachDpNoiseBaselineBuilder.apply {
              contributorsCount = WORKER_COUNT
              globalReachDpNoise = testNoiseConfig.reachNoiseConfig.globalReachDpNoise
            }
            frequencyNoiseParametersBuilder.apply {
              contributorsCount = WORKER_COUNT
              maximumFrequency = MAX_FREQUENCY
              dpParams = testNoiseConfig.frequencyNoiseConfig
            }
          }
          .build()
      )
  }

  @Test
  fun `execution phase three at non-aggregater using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_THREE.toProtocolStage()
        )
        .build()
    computationStore.writeString(partialToken, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = nonAggregatorComputationDetails,
      blobs =
        listOf(newInputBlobMetadata(0L, generatedBlobKeys.last()), newEmptyOutputBlobMetadata(1L))
    )

    var cryptoRequest = CompleteExecutionPhaseThreeRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseThree(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      val postFix = ByteString.copyFromUtf8("-completeExecutionPhaseThree-done")
      CompleteExecutionPhaseThreeResponse.newBuilder()
        .setSameKeyAggregatorMatrix(cryptoRequest.sameKeyAggregatorMatrix.concat(postFix))
        .build()
    }

    // Stage 1. Process the above computation
    nonAggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(COMPLETE.toProtocolStage())
          .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
          .setComputationDetails(nonAggregatorComputationDetails)
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
          "onPhaseThree-done" // Chunk 2, the rest
        )
      )
      .inOrder()

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseThreeRequest.newBuilder()
          .apply {
            sameKeyAggregatorMatrix = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            curveId = cryptoKeySet.curveId.toLong()
          }
          .build()
      )
  }

  @Test
  fun `execution phase three at aggregater using calculated result`() = runBlocking {
    // Stage 0. preparing the storage and set up mock
    val partialToken =
      FakeComputationsDatabase.newPartialToken(
          localId = LOCAL_ID,
          stage = EXECUTION_PHASE_THREE.toProtocolStage()
        )
        .build()
    val computationDetailsWithReach =
      aggregatorComputationDetails
        .toBuilder()
        .apply { liquidLegionsV2Builder.apply { reachEstimateBuilder.reach = 123 } }
        .build()
    computationStore.writeString(partialToken, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = computationDetailsWithReach,
      blobs =
        listOf(newInputBlobMetadata(0L, generatedBlobKeys.last()), newEmptyOutputBlobMetadata(1L))
    )

    var cryptoRequest = CompleteExecutionPhaseThreeAtAggregatorRequest.getDefaultInstance()
    whenever(mockCryptoWorker.completeExecutionPhaseThreeAtAggregator(any())).thenAnswer {
      cryptoRequest = it.getArgument(0)
      CompleteExecutionPhaseThreeAtAggregatorResponse.newBuilder()
        .putAllFrequencyDistribution(mapOf(1L to 0.3, 2L to 0.7))
        .build()
    }

    // Stage 1. Process the above computation
    aggregatorMill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(COMPLETE.toProtocolStage())
          .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
          .setComputationDetails(computationDetailsWithReach)
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString()).isNotEmpty()

    verifyProtoArgument(
        mockGlobalComputations,
        GlobalComputationsCoroutineImplBase::finishGlobalComputation
      )
      .isEqualTo(
        FinishGlobalComputationRequest.newBuilder()
          .setKey(GlobalComputation.Key.newBuilder().setGlobalComputationId(GLOBAL_ID))
          .setResult(
            GlobalComputation.Result.newBuilder()
              .setReach(123L)
              .putFrequency(1, 0.3)
              .putFrequency(2, 0.7)
          )
          .build()
      )

    assertThat(cryptoRequest)
      .isEqualTo(
        CompleteExecutionPhaseThreeAtAggregatorRequest.newBuilder()
          .apply {
            sameKeyAggregatorMatrix = ByteString.copyFromUtf8("data")
            localElGamalKeyPair = cryptoKeySet.ownPublicAndPrivateKeys
            curveId = cryptoKeySet.curveId.toLong()
            maximumFrequency = MAX_FREQUENCY
            globalFrequencyDpNoisePerBucketBuilder.apply {
              contributorsCount = WORKER_COUNT
              dpParams = testNoiseConfig.frequencyNoiseConfig
            }
          }
          .build()
      )
  }

  @Test
  fun `getPartiallyCombinedPublicKey should be correct`() {
    // This id would result in a duchy ring of "DUCHY_TWO -> DUCHY_ONE -> DUCHY_THREE",
    // so the following duchy list is {DUCHY_THREE}.
    val token1 =
      ComputationToken.newBuilder()
        .apply {
          globalComputationId = "1234"
          computationDetailsBuilder.liquidLegionsV2Builder.role = RoleInComputation.NON_AGGREGATOR
        }
        .build()
    assertThat(nonAggregatorMill.getPartiallyCombinedPublicKey(token1))
      .isEqualTo(DUCHY_THREE_PUBLIC_KEY.toElGamalPublicKey())

    // This id would result in a duchy ring of "DUCHY_ONE -> DUCHY_TWO -> DUCHY_THREE",
    // so the following duchy list is {DUCHY_TWO, DUCHY_THREE}.
    val token2 =
      ComputationToken.newBuilder()
        .apply {
          globalComputationId = "5678"
          computationDetailsBuilder.liquidLegionsV2Builder.role = RoleInComputation.NON_AGGREGATOR
        }
        .build()
    assertThat(nonAggregatorMill.getPartiallyCombinedPublicKey(token2))
      .isEqualTo(DUCHY_TWO_THREE_COMBINED_PUBLIC_KEY.toElGamalPublicKey())
  }

  companion object {
    private const val PUBLISHER_COUNT = 10
    private const val WORKER_COUNT = 3
    private const val MILL_ID = "a nice mill"
    private const val DUCHY_ONE_NAME = "DUCHY_ONE"
    private const val DUCHY_TWO_NAME = "DUCHY_TWO"
    private const val DUCHY_THREE_NAME = "DUCHY_THREE"
    private const val MAX_FREQUENCY = 15

    private val otherDuchyNames = listOf(DUCHY_TWO_NAME, DUCHY_THREE_NAME)
    private const val LOCAL_ID = 1234L
    private const val GLOBAL_ID = LOCAL_ID.toString()

    // These keys are valid keys obtained from the crypto library tests, i.e.,
    // create a cipher using random keys and then get these keys.
    private const val DUCHY_ONE_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "02d1432ca007a6c6d739fce2d21feb56d9a2c35cf968265f9093c4b691e11386b3"
    private const val DUCHY_TWO_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "039ef370ff4d216225401781d88a03f5a670a5040e6333492cb4e0cd991abbd5a3"
    private const val DUCHY_THREE_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "02d0f25ab445fc9c29e7e2509adc93308430f432522ffa93c2ae737ceb480b66d7"
    private const val OWN_EL_GAMAL_KEY =
      DUCHY_ONE_PUBLIC_KEY + "057b22ef9c4e9626c22c13daed1363a1e6a5b309a930409f8d131f96ea2fa888"

    // combined from DUCHY_ONE_PUBLIC_KEY + DUCHY_TWO_PUBLIC_KEY + DUCHY_THREE_PUBLIC_KEY
    private const val CLIENT_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "02505d7b3ac4c3c387c74132ab677a3421e883b90d4c83dc766e400fe67acc1f04"

    // combine from DUCHY_ONE_PUBLIC_KEY + DUCHY_THREE_PUBLIC_KEY
    private const val DUCHY_TWO_THREE_COMBINED_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "031887eb8e4d4290fa97601c1ef6cda80ab3d2fe82da39ef8ed2e846cc7866a3b0"

    private const val CURVE_ID = 415 // NID_X9_62_prime256v1

    private val cryptoKeySet =
      CryptoKeySet(
        ownPublicAndPrivateKeys = OWN_EL_GAMAL_KEY.toElGamalKeyPair(),
        allDuchyPublicKeys =
          mapOf(
            DUCHY_ONE_NAME to DUCHY_ONE_PUBLIC_KEY.toElGamalPublicKey(),
            DUCHY_TWO_NAME to DUCHY_TWO_PUBLIC_KEY.toElGamalPublicKey(),
            DUCHY_THREE_NAME to DUCHY_THREE_PUBLIC_KEY.toElGamalPublicKey()
          ),
        clientPublicKey = CLIENT_PUBLIC_KEY.toElGamalPublicKey(),
        curveId = CURVE_ID
      )
  }
}

private suspend fun ComputationStore.Blob.readToString(): String = read().flatten().toStringUtf8()

private suspend fun ComputationStore.writeString(
  token: ComputationToken,
  content: String
): ComputationStore.Blob = write(token, ByteString.copyFromUtf8(content))
