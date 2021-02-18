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

package org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv1.crypto

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
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.size
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.mill.CryptoKeySet
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv1.LiquidLegionsV1Mill
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
import org.wfanet.measurement.protocol.AddNoiseToSketchRequest
import org.wfanet.measurement.protocol.AddNoiseToSketchResponse
import org.wfanet.measurement.protocol.BlindLastLayerIndexThenJoinRegistersRequest
import org.wfanet.measurement.protocol.BlindLastLayerIndexThenJoinRegistersResponse
import org.wfanet.measurement.protocol.BlindOneLayerRegisterIndexRequest
import org.wfanet.measurement.protocol.BlindOneLayerRegisterIndexResponse
import org.wfanet.measurement.protocol.DecryptLastLayerFlagAndCountResponse
import org.wfanet.measurement.protocol.DecryptLastLayerFlagAndCountResponse.FlagCount
import org.wfanet.measurement.protocol.DecryptOneLayerFlagAndCountRequest
import org.wfanet.measurement.protocol.DecryptOneLayerFlagAndCountResponse
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage as LiquidLegionsStage
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
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV1
import org.wfanet.measurement.system.v1alpha.MetricRequisitionKey

@RunWith(JUnit4::class)
class LiquidLegionsV1MillTest {
  private val mockComputationControl: ComputationControlCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockMetricValues: MetricValuesCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockGlobalComputations: GlobalComputationsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockComputationStats: ComputationStatsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockCryptoWorker: LiquidLegionsV1Encryption =
    mock(useConstructor = UseConstructor.parameterless())
  private val fakeComputationDb = FakeComputationsDatabase()

  private lateinit var computationDataClients:
    ComputationDataClients
  private lateinit var computationStore: ComputationStore

  private val tempDirectory = TemporaryFolder()

  private val blobCount = AtomicInteger()
  private val generatedBlobKeys = mutableListOf<String>()
  private fun ComputationToken.generateBlobKey(): String {
    return listOf(
      localComputationId,
      computationStage.name,
      blobCount.getAndIncrement()
    ).joinToString("/").also { generatedBlobKeys.add(it) }
  }

  private val duchyOrder = DuchyOrder(
    setOf(
      Duchy(DUCHY_NAME, 10L.toBigInteger()),
      Duchy(DUCHY_ONE_NAME, 200L.toBigInteger()),
      Duchy(DUCHY_TWO_NAME, 303L.toBigInteger())
    )
  )

  private val primaryComputationDetails = ComputationDetails.newBuilder().apply {
    liquidLegionsV1Builder.apply {
      role = LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation.PRIMARY
      primaryNodeId = DUCHY_NAME
      incomingNodeId = DUCHY_ONE_NAME
      outgoingNodeId = DUCHY_TWO_NAME
    }
  }.build()

  private val secondComputationDetails = ComputationDetails.newBuilder().apply {
    liquidLegionsV1Builder.apply {
      role = LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation.SECONDARY
      primaryNodeId = DUCHY_ONE_NAME
      incomingNodeId = DUCHY_NAME
      outgoingNodeId = DUCHY_TWO_NAME
    }
  }.build()

  private val grpcTestServerRule = GrpcTestServerRule {
    computationStore =
      ComputationStore.forTesting(FileSystemStorageClient(tempDirectory.root)) { generateBlobKey() }
    computationDataClients = ComputationDataClients.forTesting(
      ComputationsCoroutineStub(channel),
      computationStore,
      otherDuchyNames
    )
    addService(mockComputationControl)
    addService(mockMetricValues)
    addService(mockGlobalComputations)
    addService(mockComputationStats)
    addService(
      ComputationsService(
        fakeComputationDb,
        globalComputationStub,
        DUCHY_NAME,
        Clock.systemUTC()
      )
    )
  }

  @get:Rule
  val ruleChain = chainRulesSequentially(tempDirectory, grpcTestServerRule)

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

  lateinit var computationControlRequests: List<AdvanceComputationRequest>

  @Before
  fun mockComputationControlServiceToCaptureRequests() = runBlocking<Unit> {
    whenever(mockComputationControl.advanceComputation(any())).thenAnswer {
      val request: Flow<AdvanceComputationRequest> = it.getArgument(0)
      computationControlRequests = runBlocking { request.toList() }
      AdvanceComputationResponse.getDefaultInstance()
    }
  }

  // Just use the same workerStub for all other duchies, since it is not relevant to this test.
  private val workerStubs = mapOf(DUCHY_ONE_NAME to workerStub, DUCHY_TWO_NAME to workerStub)

  private lateinit var mill: LiquidLegionsV1Mill

  private fun String.toMetricChunkResponse() = ByteString.copyFromUtf8(this).toMetricChunkResponse()

  private fun ByteString.toMetricChunkResponse(): StreamMetricValueResponse {
    return StreamMetricValueResponse.newBuilder().also {
      it.chunkBuilder.data = this
    }.build()
  }

  private fun String.toMetricValueResourceKey() = MetricValue.ResourceKey.newBuilder()
    .setCampaignResourceId("campaignId_$this")
    .setDataProviderResourceId("dataProvideId_$this")
    .setMetricRequisitionResourceId("requisitionId_$this")
    .build()

  private fun String.toRequisitionKey() = RequisitionKey.newBuilder()
    .setCampaignId("campaignId_$this")
    .setDataProviderId("dataProvideId_$this")
    .setMetricRequisitionId("requisitionId_$this")
    .build()

  private fun String.toMetricRequisitionKey() = MetricRequisitionKey.newBuilder()
    .setCampaignId("campaignId_$this")
    .setDataProviderId("dataProvideId_$this")
    .setMetricRequisitionId("requisitionId_$this")
    .build()

  private fun newFlagCount(isNotDestroyed: Boolean, frequency: Int): FlagCount {
    return FlagCount.newBuilder().setIsNotDestroyed(isNotDestroyed).setFrequency(frequency).build()
  }

  @Before
  fun initMill() {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(60))
    mill =
      LiquidLegionsV1Mill(
        millId = MILL_ID,
        dataClients = computationDataClients,
        metricValuesClient = metricValuesStub,
        globalComputationsClient = globalComputationStub,
        computationStatsClient = computationStatsStub,
        workerStubs = workerStubs,
        cryptoKeySet = cryptoKeySet,
        cryptoWorker = mockCryptoWorker,
        throttler = throttler,
        requestChunkSizeBytes = 20
      )
  }

  @Test
  fun `to confirm requisition, no local requisitions required at primary`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = LiquidLegionsStage.TO_CONFIRM_REQUISITIONS.toProtocolStage(),
      computationDetails = primaryComputationDetails,
      blobs = listOf(newEmptyOutputBlobMetadata(0L))
    )

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(LiquidLegionsStage.WAIT_SKETCHES.toProtocolStage())
          .addAllBlobs(
            listOf(
              newPassThroughBlobMetadata(0, blobKey),
              newEmptyOutputBlobMetadata(1),
              newEmptyOutputBlobMetadata(2)
            )
          )
          .setStageSpecificDetails(
            ComputationStageDetails.newBuilder().apply {
              liquidLegionsV1Builder.waitSketchStageDetailsBuilder.apply {
                putExternalDuchyLocalBlobId("NEXT_WORKER", 1L)
                putExternalDuchyLocalBlobId("PRIMARY_WORKER", 2L)
              }
            }
          )
          .setComputationDetails(primaryComputationDetails)
          .setVersion(3) // CreateComputation + write blob + transitionStage
          .build()
      )
    assertThat(computationStore.get(blobKey)?.readToString()).isEmpty()

    verifyZeroInteractions(mockMetricValues)
    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::confirmGlobalComputation
    ).isEqualTo(
      ConfirmGlobalComputationRequest.newBuilder()
        .setKey(
          GlobalComputation.Key.newBuilder()
            .setGlobalComputationId(GLOBAL_ID)
        )
        .build()
    )
  }

  @Test
  fun `to confirm requisition, all local requisitions available non-primary`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = LiquidLegionsStage.TO_CONFIRM_REQUISITIONS.toProtocolStage(),
      computationDetails = secondComputationDetails,
      blobs = listOf(newEmptyOutputBlobMetadata(0L)),
      stageDetails = ComputationStageDetails.newBuilder().apply {
        liquidLegionsV1Builder.toConfirmRequisitionsStageDetailsBuilder.apply {
          addKeys("1".toRequisitionKey())
          addKeys("2".toRequisitionKey())
        }
      }.build()
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
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID]).isEqualTo(
      ComputationToken.newBuilder()
        .setGlobalComputationId(GLOBAL_ID)
        .setLocalComputationId(LOCAL_ID)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_TO_START.toProtocolStage())
        .addBlobs(newPassThroughBlobMetadata(0, blobKey))
        .setVersion(3) // CreateComputation + write blob + transitionStage
        .setComputationDetails(secondComputationDetails)
        .build()
    )
    assertThat(
      computationStore.get(blobKey)
        ?.readToString()
    ).isEqualTo("A_chunk_1_A_chunk_2_A_chunk_3_B_chunk_1_B_chunk_2_")

    assertThat(metricValuesRequest1).isEqualTo(
      StreamMetricValueRequest.newBuilder().setResourceKey("1".toMetricValueResourceKey()).build()
    )
    assertThat(metricValuesRequest2).isEqualTo(
      StreamMetricValueRequest.newBuilder().setResourceKey("2".toMetricValueResourceKey()).build()
    )
    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::confirmGlobalComputation
    )
      .isEqualTo(
        ConfirmGlobalComputationRequest.newBuilder()
          .setKey(
            GlobalComputation.Key.newBuilder()
              .setGlobalComputationId(GLOBAL_ID)
          )
          .addReadyRequisitions("1".toMetricRequisitionKey())
          .addReadyRequisitions("2".toMetricRequisitionKey())
          .build()
      )
  }

  @Test
  fun `to confirm requisition, missing requisition at primary`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val requisition1 = "1"
    val requisition2 = "2"
    fakeComputationDb.addComputation(
      globalId = GLOBAL_ID,
      stage = LiquidLegionsStage.TO_CONFIRM_REQUISITIONS.toProtocolStage(),
      computationDetails = secondComputationDetails,
      blobs = listOf(newEmptyOutputBlobMetadata(0L)),
      stageDetails = ComputationStageDetails.newBuilder().apply {
        liquidLegionsV1Builder.toConfirmRequisitionsStageDetailsBuilder.apply {
          addKeys(requisition1.toRequisitionKey())
          addKeys(requisition2.toRequisitionKey())
        }
      }
        .build()
    )

    val metricValue = MetricValue.newBuilder()
      .setResourceKey(requisition1.toMetricValueResourceKey())
      .build()
    val content = ByteString.copyFromUtf8("chunk")
    whenever(mockMetricValues.getMetricValue(any()))
      .thenReturn(metricValue)
      .thenThrow(Status.NOT_FOUND.asRuntimeException())
    whenever(mockMetricValues.streamMetricValue(any()))
      .thenReturn(
        flowOf(
          StreamMetricValueResponse.newBuilder().apply {
            headerBuilder.metricValue = metricValue
            headerBuilder.dataSizeBytes = content.size.toLong()
          }.build(),
          content.toMetricChunkResponse()
        )
      )
      .thenThrow(Status.NOT_FOUND.asRuntimeException())
    whenever(mockGlobalComputations.createGlobalComputationStatusUpdate(any())).thenReturn(
      GlobalComputationStatusUpdate.getDefaultInstance()
    )
    whenever(mockGlobalComputations.confirmGlobalComputation(any())).thenReturn(
      GlobalComputation.getDefaultInstance()
    )

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID]!!)
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(LiquidLegionsStage.COMPLETED.toProtocolStage())
          .setVersion(2) // CreateComputation + transitionStage
          .setComputationDetails(secondComputationDetails)
          .build()
      )

    // Only one requisition is confirmed
    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::confirmGlobalComputation
    )
      .isEqualTo(
        ConfirmGlobalComputationRequest.newBuilder()
          .setKey(
            GlobalComputation.Key.newBuilder()
              .setGlobalComputationId(GLOBAL_ID)
          )
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
          CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
            parentBuilder.globalComputationId = GLOBAL_ID
            statusUpdateBuilder.apply {
              selfReportedIdentifier = MILL_ID
              stageDetailsBuilder.apply {
                algorithm = MpcAlgorithm.LIQUID_LEGIONS_V1
                stageNumber = LiquidLegionsStage.TO_CONFIRM_REQUISITIONS.number.toLong()
                stageName = LiquidLegionsStage.TO_CONFIRM_REQUISITIONS.name
                attemptNumber = 1
              }
              updateMessage = "Computation $GLOBAL_ID at stage TO_CONFIRM_REQUISITIONS," +
                " attempt 1 failed."
              errorDetailsBuilder.apply {
                errorType = ErrorType.PERMANENT
              }
            }
          }
            .build()
        )
    }
  }

  @Test
  fun `to add noise using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val partialToken = FakeComputationsDatabase.newPartialToken(
      localId = LOCAL_ID,
      stage = LiquidLegionsStage.TO_ADD_NOISE.toProtocolStage()
    ).build()
    computationStore.writeString(partialToken, "sketch")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = secondComputationDetails,
      blobs = listOf(
        newInputBlobMetadata(0L, generatedBlobKeys.last()),
        newEmptyOutputBlobMetadata(1L)
      )
    )

    whenever(mockCryptoWorker.addNoiseToSketch(any()))
      .thenAnswer {
        val request: AddNoiseToSketchRequest = it.getArgument(0)
        val postFix = ByteString.copyFromUtf8("-AddedNoise")
        AddNoiseToSketchResponse.newBuilder().setSketch(request.sketch.concat(postFix)).build()
      }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID]).isEqualTo(
      ComputationToken.newBuilder()
        .setGlobalComputationId(GLOBAL_ID)
        .setLocalComputationId(LOCAL_ID)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_CONCATENATED.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath(blobKey)
        )
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.OUTPUT).setBlobId(1)
        )
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setComputationDetails(secondComputationDetails)
        .build()
    )

    assertThat(computationStore.get(blobKey)?.readToString()).isEqualTo("sketch-AddedNoise")

    assertThat(computationControlRequests).containsExactlyElementsIn(
      LiquidLegionsV1.Description.NOISED_SKETCH
        .toAdvanceComputationHeader()
        .withContent("sketch-AddedNoise")
    ).inOrder()
  }

  @Test
  fun `to append sketches and add noise using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val partialToken = FakeComputationsDatabase.newPartialToken(
      localId = LOCAL_ID,
      stage = LiquidLegionsStage.TO_APPEND_SKETCHES_AND_ADD_NOISE.toProtocolStage()
    ).build()
    computationStore.writeString(partialToken, "sketch_1_")
    computationStore.writeString(partialToken, "sketch_2_")
    computationStore.writeString(partialToken, "sketch_3_")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = primaryComputationDetails,
      blobs = listOf(
        newInputBlobMetadata(0L, generatedBlobKeys[0]),
        newInputBlobMetadata(1L, generatedBlobKeys[1]),
        newInputBlobMetadata(2L, generatedBlobKeys[2]),
        newEmptyOutputBlobMetadata(3L)
      )
    )

    whenever(mockCryptoWorker.addNoiseToSketch(any()))
      .thenAnswer {
        val request: AddNoiseToSketchRequest = it.getArgument(0)
        val postFix = ByteString.copyFromUtf8("-AddedNoise")
        AddNoiseToSketchResponse.newBuilder().setSketch(request.sketch.concat(postFix)).build()
      }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID]).isEqualTo(
      ComputationToken.newBuilder()
        .setGlobalComputationId(GLOBAL_ID)
        .setLocalComputationId(LOCAL_ID)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_CONCATENATED.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath(blobKey)
        )
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.OUTPUT).setBlobId(1)
        )
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setComputationDetails(primaryComputationDetails)
        .build()
    )

    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("sketch_1_sketch_2_sketch_3_-AddedNoise")

    assertThat(computationControlRequests).containsExactlyElementsIn(
      LiquidLegionsV1.Description.CONCATENATED_SKETCH
        .toAdvanceComputationHeader()
        .withContent("sketch_1_sketch_2_sk", "etch_3_-AddedNoise")
    ).inOrder()
  }

  @Test
  fun `to blind positions using cached result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val partialToken = FakeComputationsDatabase.newPartialToken(
      localId = LOCAL_ID,
      stage = LiquidLegionsStage.TO_BLIND_POSITIONS.toProtocolStage()
    ).build()
    computationStore.writeString(partialToken, "sketch")
    computationStore.writeString(partialToken, "cached result")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = secondComputationDetails,
      blobs = listOf(
        newInputBlobMetadata(0L, generatedBlobKeys[0]),
        newOutputBlobMetadata(1L, generatedBlobKeys[1])
      )
    )

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID]).isEqualTo(
      ComputationToken.newBuilder()
        .setGlobalComputationId(GLOBAL_ID)
        .setLocalComputationId(LOCAL_ID)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_FLAG_COUNTS.toProtocolStage())
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
        .setComputationDetails(secondComputationDetails)
        .build()
    )

    assertThat(computationControlRequests).containsExactlyElementsIn(
      LiquidLegionsV1.Description.CONCATENATED_SKETCH
        .toAdvanceComputationHeader()
        .withContent("cached result")
    ).inOrder()
  }

  @Test
  fun `to blind positions using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val partialToken = FakeComputationsDatabase.newPartialToken(
      localId = LOCAL_ID,
      stage = LiquidLegionsStage.TO_BLIND_POSITIONS.toProtocolStage()
    ).build()
    computationStore.writeString(partialToken, "sketch")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = secondComputationDetails,
      blobs = listOf(
        newInputBlobMetadata(0L, generatedBlobKeys.last()),
        newEmptyOutputBlobMetadata(1L)
      )
    )

    whenever(mockCryptoWorker.blindOneLayerRegisterIndex(any()))
      .thenAnswer {
        val request: BlindOneLayerRegisterIndexRequest = it.getArgument(0)
        val postFix = ByteString.copyFromUtf8("-BlindedOneLayerRegisterIndex")
        BlindOneLayerRegisterIndexResponse.newBuilder()
          .setSketch(request.sketch.concat(postFix))
          .build()
      }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID]).isEqualTo(
      ComputationToken.newBuilder()
        .setGlobalComputationId(GLOBAL_ID)
        .setLocalComputationId(LOCAL_ID)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_FLAG_COUNTS.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath(blobKey)
        )
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.OUTPUT).setBlobId(1)
        )
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setComputationDetails(secondComputationDetails)
        .build()
    )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("sketch-BlindedOneLayerRegisterIndex")

    assertThat(computationControlRequests).containsExactlyElementsIn(
      LiquidLegionsV1.Description.CONCATENATED_SKETCH
        .toAdvanceComputationHeader()
        .withContent(
          "sketch-BlindedOneLay", // Chunk 1, size 20
          "erRegisterIndex" // Chunk 2, the rest
        )
    ).inOrder()
  }

  @Test
  fun `to blind positions and merge register using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val partialToken = FakeComputationsDatabase.newPartialToken(
      localId = LOCAL_ID,
      stage = LiquidLegionsStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS.toProtocolStage()
    ).build()
    computationStore.writeString(partialToken, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = secondComputationDetails,
      blobs = listOf(
        newInputBlobMetadata(0L, generatedBlobKeys.last()),
        newEmptyOutputBlobMetadata(1L)
      )
    )

    whenever(mockCryptoWorker.blindLastLayerIndexThenJoinRegisters(any()))
      .thenAnswer {
        val request: BlindLastLayerIndexThenJoinRegistersRequest = it.getArgument(0)
        val postFix = ByteString.copyFromUtf8("-BlindedLastLayerIndexThenJoinRegisters")
        BlindLastLayerIndexThenJoinRegistersResponse.newBuilder()
          .setFlagCounts(request.sketch.concat(postFix))
          .build()
      }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID]).isEqualTo(
      ComputationToken.newBuilder()
        .setGlobalComputationId(GLOBAL_ID)
        .setLocalComputationId(LOCAL_ID)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_FLAG_COUNTS.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath(blobKey)
        )
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.OUTPUT).setBlobId(1)
        )
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setComputationDetails(secondComputationDetails)
        .build()
    )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("data-BlindedLastLayerIndexThenJoinRegisters")

    assertThat(computationControlRequests).containsExactlyElementsIn(
      LiquidLegionsV1.Description.ENCRYPTED_FLAGS_AND_COUNTS
        .toAdvanceComputationHeader()
        .withContent(
          "data-BlindedLastLaye", // Chunk 1, size 20
          "rIndexThenJoinRegist", // Chunk 2, size 20
          "ers" // Chunk 3, the rest
        )
    ).inOrder()
  }

  @Test
  fun `to decrypt FlagCounts using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val partialToken = FakeComputationsDatabase.newPartialToken(
      localId = LOCAL_ID,
      stage = LiquidLegionsStage.TO_DECRYPT_FLAG_COUNTS.toProtocolStage()
    ).build()
    computationStore.writeString(partialToken, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = secondComputationDetails,
      blobs = listOf(
        newInputBlobMetadata(0L, generatedBlobKeys.last()),
        newEmptyOutputBlobMetadata(1L)
      )
    )

    whenever(mockCryptoWorker.decryptOneLayerFlagAndCount(any()))
      .thenAnswer {
        val request: DecryptOneLayerFlagAndCountRequest = it.getArgument(0)
        val postFix = ByteString.copyFromUtf8("-DecryptedOneLayerFlagAndCount")
        DecryptOneLayerFlagAndCountResponse.newBuilder()
          .setFlagCounts(request.flagCounts.concat(postFix))
          .build()
      }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID]).isEqualTo(
      ComputationToken.newBuilder()
        .setGlobalComputationId(GLOBAL_ID)
        .setLocalComputationId(LOCAL_ID)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.COMPLETED.toProtocolStage())
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setComputationDetails(secondComputationDetails)
        .build()
    )
    assertThat(computationStore.get(blobKey)?.readToString())
      .isEqualTo("data-DecryptedOneLayerFlagAndCount")

    assertThat(computationControlRequests).containsExactlyElementsIn(
      LiquidLegionsV1.Description.ENCRYPTED_FLAGS_AND_COUNTS
        .toAdvanceComputationHeader()
        .withContent(
          "data-DecryptedOneLay", // Chunk 1, size 20
          "erFlagAndCount" // Chunk 2, the rest
        ).asIterable()
    ).inOrder()
  }

  @Test
  fun `to decrypt flag count and compute metric`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val partialToken = FakeComputationsDatabase.newPartialToken(
      localId = LOCAL_ID,
      stage = LiquidLegionsStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS.toProtocolStage()
    ).build()
    computationStore.writeString(partialToken, "data")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = primaryComputationDetails,
      blobs = listOf(
        newInputBlobMetadata(0L, generatedBlobKeys.last()),
        newEmptyOutputBlobMetadata(1L)
      )
    )

    whenever(mockCryptoWorker.decryptLastLayerFlagAndCount(any()))
      .thenReturn(
        DecryptLastLayerFlagAndCountResponse.newBuilder()
          .addFlagCounts(newFlagCount(isNotDestroyed = true, frequency = 1))
          .addFlagCounts(newFlagCount(isNotDestroyed = true, frequency = 2))
          .addFlagCounts(newFlagCount(isNotDestroyed = true, frequency = 2))
          .addFlagCounts(newFlagCount(isNotDestroyed = false, frequency = 2))
          .addFlagCounts(newFlagCount(isNotDestroyed = false, frequency = 2))
          .addFlagCounts(newFlagCount(isNotDestroyed = true, frequency = 3))
          .addFlagCounts(newFlagCount(isNotDestroyed = true, frequency = 3))
          .addFlagCounts(newFlagCount(isNotDestroyed = true, frequency = 3))
          .build()
      )

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val blobKey = generatedBlobKeys.last()
    assertThat(fakeComputationDb[LOCAL_ID]).isEqualTo(
      ComputationToken.newBuilder()
        .setGlobalComputationId(GLOBAL_ID)
        .setLocalComputationId(LOCAL_ID)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.COMPLETED.toProtocolStage())
        .setVersion(3) // CreateComputation + write blob + transitionStage
        .setComputationDetails(primaryComputationDetails)
        .build()
    )
    assertThat(computationStore.get(blobKey)?.readToString()).isNotEmpty()

    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::finishGlobalComputation
    )
      .isEqualTo(
        FinishGlobalComputationRequest.newBuilder()
          .setKey(
            GlobalComputation.Key.newBuilder()
              .setGlobalComputationId(GLOBAL_ID)
          )
          .setResult(
            GlobalComputation.Result.newBuilder()
              .setReach(9)
              .putFrequency(1, 1.0 / 6)
              .putFrequency(2, 2.0 / 6)
              .putFrequency(3, 3.0 / 6)
          )
          .build()
      )
  }

  @Test
  fun `permanent crypto worker failure, computation should FAIL`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val partialToken = FakeComputationsDatabase.newPartialToken(
      localId = LOCAL_ID,
      stage = LiquidLegionsStage.TO_BLIND_POSITIONS.toProtocolStage()
    ).build()
    computationStore.writeString(partialToken, "sketch")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = secondComputationDetails,
      blobs = listOf(
        newInputBlobMetadata(0L, generatedBlobKeys.last()),
        newEmptyOutputBlobMetadata(1L)
      )
    )

    whenever(mockCryptoWorker.blindOneLayerRegisterIndex(any()))
      .thenThrow(Status.INVALID_ARGUMENT.asRuntimeException())
    whenever(mockGlobalComputations.createGlobalComputationStatusUpdate(any())).thenReturn(
      GlobalComputationStatusUpdate.getDefaultInstance()
    )

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(LiquidLegionsStage.COMPLETED.toProtocolStage())
          .setVersion(2) // CreateComputation + transitionStage
          .setComputationDetails(secondComputationDetails)
          .build()
      )

    argumentCaptor<CreateGlobalComputationStatusUpdateRequest> {
      verifyBlocking(mockGlobalComputations, times(3)) {
        createGlobalComputationStatusUpdate(capture())
      }
      assertThat(allValues[1])
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
            parentBuilder.globalComputationId = GLOBAL_ID
            statusUpdateBuilder.apply {
              selfReportedIdentifier = MILL_ID
              stageDetailsBuilder.apply {
                algorithm = MpcAlgorithm.LIQUID_LEGIONS_V1
                stageNumber = LiquidLegionsStage.TO_BLIND_POSITIONS.number.toLong()
                stageName = LiquidLegionsStage.TO_BLIND_POSITIONS.name
                attemptNumber = 1
              }
              updateMessage = "Computation $GLOBAL_ID at stage TO_BLIND_POSITIONS," +
                " attempt 1 failed."
              errorDetailsBuilder.apply {
                errorType = ErrorType.PERMANENT
              }
            }
          }
            .build()
        )
    }
  }

  @Test
  fun `trancient grpc failure, result should be cached`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val partialToken = FakeComputationsDatabase.newPartialToken(
      localId = LOCAL_ID,
      stage = LiquidLegionsStage.TO_BLIND_POSITIONS.toProtocolStage()
    ).build()
    computationStore.writeString(partialToken, "sketch")
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      computationDetails = secondComputationDetails,
      blobs = listOf(
        newInputBlobMetadata(0L, generatedBlobKeys.last()),
        newEmptyOutputBlobMetadata(1L)
      )
    )

    whenever(mockComputationControl.advanceComputation(any()))
      .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException())
    whenever(mockCryptoWorker.blindOneLayerRegisterIndex(any()))
      .thenAnswer {
        val request: BlindOneLayerRegisterIndexRequest = it.getArgument(0)
        val postFix = ByteString.copyFromUtf8("-BlindedOneLayerRegisterIndex")
        BlindOneLayerRegisterIndexResponse.newBuilder()
          .setSketch(request.sketch.concat(postFix))
          .build()
      }

    whenever(mockGlobalComputations.createGlobalComputationStatusUpdate(any())).thenReturn(
      GlobalComputationStatusUpdate.getDefaultInstance()
    )

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val inputBlobKey = generatedBlobKeys[0]
    val outputBlobKey = generatedBlobKeys[1]
    assertThat(fakeComputationDb[LOCAL_ID])
      .isEqualTo(
        ComputationToken.newBuilder()
          .setGlobalComputationId(GLOBAL_ID)
          .setLocalComputationId(LOCAL_ID)
          .setAttempt(1)
          .setComputationStage(LiquidLegionsStage.TO_BLIND_POSITIONS.toProtocolStage())
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.INPUT)
              .setBlobId(0)
              .setPath(inputBlobKey)
          )
          .addBlobs(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.OUTPUT)
              .setBlobId(1)
              .setPath(outputBlobKey)
          )
          .setVersion(3) // CreateComputation + writeOutputBlob + enqueue
          .setComputationDetails(secondComputationDetails)
          .build()
      )
    assertThat(computationStore.get(outputBlobKey)?.readToString())
      .isEqualTo("sketch-BlindedOneLayerRegisterIndex")

    argumentCaptor<CreateGlobalComputationStatusUpdateRequest> {
      verifyBlocking(mockGlobalComputations, times(2)) {
        createGlobalComputationStatusUpdate(capture())
      }
      assertThat(allValues[1])
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
            parentBuilder.globalComputationId = GLOBAL_ID
            statusUpdateBuilder.apply {
              selfReportedIdentifier = MILL_ID
              stageDetailsBuilder.apply {
                algorithm = MpcAlgorithm.LIQUID_LEGIONS_V1
                stageNumber = LiquidLegionsStage.TO_BLIND_POSITIONS.number.toLong()
                stageName = LiquidLegionsStage.TO_BLIND_POSITIONS.name
                attemptNumber = 1
              }
              updateMessage = "Computation $GLOBAL_ID at stage TO_BLIND_POSITIONS," +
                " attempt 1 failed."
              errorDetailsBuilder.apply {
                errorType = ErrorType.TRANSIENT
              }
            }
          }
            .build()
        )
    }
  }

  private fun LiquidLegionsV1.Description.toAdvanceComputationHeader():
    AdvanceComputationRequest.Header =
      AdvanceComputationRequest.Header.newBuilder().apply {
        keyBuilder.globalComputationId = GLOBAL_ID
        liquidLegionsV1Builder.description = this@toAdvanceComputationHeader
      }.build()

  private fun AdvanceComputationRequest.Header.withContent(
    vararg bodyContent: String
  ): Iterable<AdvanceComputationRequest> {
    return (
      sequenceOf(AdvanceComputationRequest.newBuilder().setHeader(this@withContent).build()) +
        bodyContent.asSequence().map {
          AdvanceComputationRequest.newBuilder().apply {
            bodyChunkBuilder.apply {
              partialData = ByteString.copyFromUtf8(it)
            }
          }.build()
        }
      ).asIterable()
  }

  companion object {
    private const val MILL_ID = "a nice mill"
    private const val DUCHY_NAME = "THIS_WORKER"
    private const val DUCHY_ONE_NAME = "NEXT_WORKER"
    private const val DUCHY_TWO_NAME = "PRIMARY_WORKER"
    private val otherDuchyNames = listOf(DUCHY_ONE_NAME, DUCHY_TWO_NAME)
    private const val LOCAL_ID = 1111L
    private const val GLOBAL_ID = LOCAL_ID.toString()

    // These keys are valid keys obtained from the crypto library tests, i.e.,
    // create a cipher using random keys and then get these keys.
    private const val OWN_EL_GAMAL_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "02d1432ca007a6c6d739fce2d21feb56d9a2c35cf968265f9093c4b691e11386b3" +
        "057b22ef9c4e9626c22c13daed1363a1e6a5b309a930409f8d131f96ea2fa888"
    private const val DUCHY_ONE_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "039ef370ff4d216225401781d88a03f5a670a5040e6333492cb4e0cd991abbd5a3"
    private const val DUCHY_TWO_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "02d0f25ab445fc9c29e7e2509adc93308430f432522ffa93c2ae737ceb480b66d7"
    private const val CLIENT_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "02505d7b3ac4c3c387c74132ab677a3421e883b90d4c83dc766e400fe67acc1f04"

    private const val CURVE_ID = 415 // NID_X9_62_prime256v1

    private val cryptoKeySet =
      CryptoKeySet(
        ownPublicAndPrivateKeys = OWN_EL_GAMAL_KEY.toElGamalKeyPair(),
        otherDuchyPublicKeys = mapOf(
          DUCHY_ONE_NAME to DUCHY_ONE_PUBLIC_KEY.toElGamalPublicKey(),
          DUCHY_TWO_NAME to DUCHY_TWO_PUBLIC_KEY.toElGamalPublicKey()
        ),
        clientPublicKey = CLIENT_PUBLIC_KEY.toElGamalPublicKey(),
        curveId = CURVE_ID
      )
  }
}

private suspend fun ComputationStore.Blob.readToString(): String =
  read().flatten().toStringUtf8()

private suspend fun ComputationStore.writeString(
  token: ComputationToken,
  content: String
): ComputationStore.Blob = write(token, ByteString.copyFromUtf8(content))

private suspend fun ComputationStore.writeString(
  tokenBuilder: ComputationToken.Builder,
  content: String
) = writeString(tokenBuilder.build(), content)
