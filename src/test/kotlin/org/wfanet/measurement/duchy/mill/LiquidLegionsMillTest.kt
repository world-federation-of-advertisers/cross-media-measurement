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

package org.wfanet.measurement.duchy.mill

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.verifyZeroInteractions
import com.nhaarman.mockitokotlin2.whenever
import io.grpc.Status
import java.nio.charset.Charset
import java.time.Clock
import java.time.Duration
import kotlin.test.assertEquals
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v1alpha.ConfirmGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.FinishGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.GlobalComputation
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineImplBase
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.computation.testing.FakeComputationStorage
import org.wfanet.measurement.db.duchy.computation.testing.FakeComputationsBlobDb
import org.wfanet.measurement.duchy.mill.testing.FakeLiquidLegionsCryptoWorker
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage as LiquidLegionsStage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchResponse
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsRequest
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsResponse
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchResponse
import org.wfanet.measurement.internal.duchy.MetricValue
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineImplBase
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.internal.duchy.StreamMetricValueRequest
import org.wfanet.measurement.internal.duchy.StreamMetricValueResponse
import org.wfanet.measurement.internal.duchy.ToConfirmRequisitionsStageDetails
import org.wfanet.measurement.internal.duchy.ToConfirmRequisitionsStageDetails.RequisitionKey
import org.wfanet.measurement.internal.duchy.WaitSketchesStageDetails
import org.wfanet.measurement.service.internal.duchy.computation.storage.ComputationStorageServiceImpl
import org.wfanet.measurement.service.internal.duchy.computation.storage.newEmptyOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.newInputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.newOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage
import org.wfanet.measurement.service.testing.GrpcTestServerRule

class LiquidLegionsMillTest {

  private val mockLiquidLegionsComputationControl: ComputationControlServiceCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockMetricValues: MetricValuesCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val mockGlobalComputations: GlobalComputationsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val fakeBlobs = mutableMapOf<String, ByteArray>()
  private val fakeComputationStorage = FakeComputationStorage(otherDuchyNames)
  private val cryptoWorker = FakeLiquidLegionsCryptoWorker()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    computationStorageClients = LiquidLegionsSketchAggregationComputationStorageClients(
      ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub(channel),
      FakeComputationsBlobDb(fakeBlobs),
      otherDuchyNames
    )
    listOf(
      mockLiquidLegionsComputationControl,
      mockMetricValues,
      mockGlobalComputations,
      ComputationStorageServiceImpl(fakeComputationStorage)
    )
    addService(mockLiquidLegionsComputationControl)
    addService(mockMetricValues)
    addService(mockGlobalComputations)
    addService(ComputationStorageServiceImpl(fakeComputationStorage))
  }

  private lateinit var computationStorageClients:
    LiquidLegionsSketchAggregationComputationStorageClients

  private val workerStub: ComputationControlServiceCoroutineStub by lazy {
    ComputationControlServiceCoroutineStub(grpcTestServerRule.channel)
  }

  private val globalComputationStub: GlobalComputationsCoroutineStub by lazy {
    GlobalComputationsCoroutineStub(grpcTestServerRule.channel)
  }

  private val metricValuesStub: MetricValuesCoroutineStub by lazy {
    MetricValuesCoroutineStub(grpcTestServerRule.channel)
  }

  // Just use the same workerStub for all other duchies, since it is not relevant to this test.
  private val workerStubs = mapOf(DUCHY_ONE_NAME to workerStub, DUCHY_TWO_NAME to workerStub)

  private lateinit var mill: LiquidLegionsMill

  private fun String.toMetricChuckResponse() = StreamMetricValueResponse.newBuilder().setChunk(
    StreamMetricValueResponse.Chunk.newBuilder().setData(
      ByteString.copyFromUtf8(this)
    )
  ).build()

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

  private fun String.toMetricRequisitionKey() = MetricRequisition.Key.newBuilder()
    .setCampaignId("campaignId_$this")
    .setDataProviderId("dataProvideId_$this")
    .setMetricRequisitionId("requisitionId_$this")
    .build()

  @Before
  fun initMill() {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(60))
    mill =
      LiquidLegionsMill(
        millId = MILL_ID,
        storageClients = computationStorageClients,
        metricValuesClient = metricValuesStub,
        globalComputationsClient = globalComputationStub,
        workerStubs = workerStubs,
        cryptoKeySet = cryptoKeySet,
        cryptoWorker = cryptoWorker,
        throttler = throttler,
        chunkSize = 20
      )
  }

  @Test
  fun `to confirm requisition, no local requisitions required at primary`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_CONFIRM_REQUISITIONS.toProtocolStage(),
      role = RoleInComputation.PRIMARY,
      blobs = listOf(newEmptyOutputBlobMetadata(0L))
    )

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_SKETCHES.toProtocolStage())
        .addAllBlobs(
          listOf(
            ComputationStageBlobMetadata.newBuilder()
              .setDependencyType(ComputationBlobDependency.INPUT)
              .setBlobId(0)
              .setPath("1111/TO_CONFIRM_REQUISITIONS_1_output").build(),
            newEmptyOutputBlobMetadata(1),
            newEmptyOutputBlobMetadata(2)
          )
        )
        .setStageSpecificDetails(
          ComputationStageDetails.newBuilder()
            .setWaitSketchStageDetails(
              WaitSketchesStageDetails.newBuilder()
                .putExternalDuchyLocalBlobId("NEXT_WORKER", 1L)
                .putExternalDuchyLocalBlobId("PRIMARY_WORKER", 2L)
            )
        )
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(3) // CreateComputation + write blob + transitionStage
        .setRole(RoleInComputation.PRIMARY)
        .build()
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)
    assertThat(fakeBlobs["1111/TO_CONFIRM_REQUISITIONS_1_output"]).isEmpty()

    verifyZeroInteractions(mockMetricValues)
    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::confirmGlobalComputation
    )
      .isEqualTo(
        ConfirmGlobalComputationRequest.getDefaultInstance()
      )
  }

  @Test
  fun `to confirm requisition, all local requisitions available non-primary`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_CONFIRM_REQUISITIONS.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(newEmptyOutputBlobMetadata(0L)),
      stageDetails = ComputationStageDetails.newBuilder()
        .setToConfirmRequisitionsStageDetails(
          ToConfirmRequisitionsStageDetails.newBuilder()
            .addKeys("1".toRequisitionKey())
            .addKeys("2".toRequisitionKey())
        )
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
          "A_chunk_1_".toMetricChuckResponse(),
          "A_chunk_2_".toMetricChuckResponse(),
          "A_chunk_3_".toMetricChuckResponse()
        )
      }
      .thenAnswer {
        metricValuesRequest2 = it.getArgument(0)
        flowOf("B_chunk_1_".toMetricChuckResponse(), "B_chunk_2_".toMetricChuckResponse())
      }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_TO_START.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath("1111/TO_CONFIRM_REQUISITIONS_1_output")
        )
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(3) // CreateComputation + transitionStage
        .setRole(RoleInComputation.SECONDARY)
        .build()
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)
    assertEquals(
      "A_chunk_1_A_chunk_2_A_chunk_3_B_chunk_1_B_chunk_2_",
      fakeBlobs["1111/TO_CONFIRM_REQUISITIONS_1_output"]!!.toString(Charset.defaultCharset())
    )

    ProtoTruth.assertThat(metricValuesRequest1).isEqualTo(
      StreamMetricValueRequest.newBuilder().setResourceKey("1".toMetricValueResourceKey()).build()
    )
    ProtoTruth.assertThat(metricValuesRequest2).isEqualTo(
      StreamMetricValueRequest.newBuilder().setResourceKey("2".toMetricValueResourceKey()).build()
    )
    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::confirmGlobalComputation
    )
      .isEqualTo(
        ConfirmGlobalComputationRequest.newBuilder()
          .addReadyRequisitions("1".toMetricRequisitionKey())
          .addReadyRequisitions("2".toMetricRequisitionKey())
          .build()
      )
  }

  @Test
  fun `to confirm requisition, missing requisition at primary`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_CONFIRM_REQUISITIONS.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(newEmptyOutputBlobMetadata(0L)),
      stageDetails = ComputationStageDetails.newBuilder()
        .setToConfirmRequisitionsStageDetails(
          ToConfirmRequisitionsStageDetails.newBuilder()
            .addKeys("1".toRequisitionKey())
            .addKeys("2".toRequisitionKey())
        )
        .build()
    )

    whenever(mockMetricValues.streamMetricValue(any()))
      .thenReturn(flowOf("chunk".toMetricChuckResponse()))
      .thenThrow(
        Status.NOT_FOUND.asRuntimeException()
      )

    whenever(mockGlobalComputations.confirmGlobalComputation(any())).thenReturn(
      GlobalComputation.getDefaultInstance()
    )

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.COMPLETED.toProtocolStage())
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(2) // CreateComputation + transitionStage
        .setRole(RoleInComputation.SECONDARY)
        .build()
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)

    // Only one requisition is confirmed
    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::confirmGlobalComputation
    )
      .isEqualTo(
        ConfirmGlobalComputationRequest.newBuilder()
          .addReadyRequisitions("1".toMetricRequisitionKey())
          .build()
      )
  }

  @Test
  fun `to add noise using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val inputBlobPath = "to_add_noise/input"
    val computationId = 1234L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_ADD_NOISE.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(
        newInputBlobMetadata(0L, inputBlobPath),
        newEmptyOutputBlobMetadata(1L)
      )
    )
    fakeBlobs[inputBlobPath] = "sketch".toByteArray()

    lateinit var computationControlRequests: List<HandleNoisedSketchRequest>
    whenever(mockLiquidLegionsComputationControl.handleNoisedSketch(any())).thenAnswer {
      val request: Flow<HandleNoisedSketchRequest> = it.getArgument(0)
      computationControlRequests = runBlocking { request.toList() }
      HandleNoisedSketchResponse.getDefaultInstance()
    }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_CONCATENATED.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath("1234/TO_ADD_NOISE_1_output")
        )
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.OUTPUT).setBlobId(1)
        )
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setRole(RoleInComputation.SECONDARY)
        .build()
    assertThat(expectTokenAfterProcess).isEqualTo(fakeComputationStorage[computationId]!!)

    assertEquals(
      "sketch-AddedNoise",
      fakeBlobs["1234/TO_ADD_NOISE_1_output"]!!.toString(Charset.defaultCharset())
    )

    assertThat(computationControlRequests).containsExactly(
      HandleNoisedSketchRequest.newBuilder()
        .setPartialSketch(ByteString.copyFromUtf8("sketch-AddedNoise"))
        .setComputationId(computationId).build()
    )
  }

  @Test
  fun `to append sketches any add noise using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val inputBlobPath1 = "to_append_sketches_and_add_noise/input1"
    val inputBlobPath2 = "to_append_sketches_and_add_noise/input2"
    val inputBlobPath3 = "to_append_sketches_and_add_noise/input3"
    val computationId = 1234L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_APPEND_SKETCHES_AND_ADD_NOISE.toProtocolStage(),
      role = RoleInComputation.PRIMARY,
      blobs = listOf(
        newInputBlobMetadata(0L, inputBlobPath1),
        newInputBlobMetadata(1L, inputBlobPath2),
        newInputBlobMetadata(2L, inputBlobPath3),
        newEmptyOutputBlobMetadata(3L)
      )
    )
    fakeBlobs[inputBlobPath1] = "sketch_1_".toByteArray()
    fakeBlobs[inputBlobPath2] = "sketch_2_".toByteArray()
    fakeBlobs[inputBlobPath3] = "sketch_3_".toByteArray()

    lateinit var computationControlRequests: List<HandleConcatenatedSketchRequest>
    whenever(mockLiquidLegionsComputationControl.handleConcatenatedSketch(any())).thenAnswer {
      val request: Flow<HandleConcatenatedSketchRequest> = it.getArgument(0)
      computationControlRequests = runBlocking { request.toList() }
      HandleConcatenatedSketchResponse.getDefaultInstance()
    }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_CONCATENATED.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath("1234/TO_APPEND_SKETCHES_AND_ADD_NOISE_1_output")
        )
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.OUTPUT).setBlobId(1)
        )
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setRole(RoleInComputation.PRIMARY)
        .build()
    assertThat(expectTokenAfterProcess).isEqualTo(fakeComputationStorage[computationId]!!)

    assertEquals(
      "sketch_1_sketch_2_sketch_3_-AddedNoise",
      fakeBlobs["1234/TO_APPEND_SKETCHES_AND_ADD_NOISE_1_output"]!!
        .toString(Charset.defaultCharset())
    )

    assertThat(computationControlRequests).containsExactly(
      HandleConcatenatedSketchRequest.newBuilder()
        .setPartialSketch(ByteString.copyFromUtf8("sketch_1_sketch_2_sk"))
        .setComputationId(computationId).build(),
      HandleConcatenatedSketchRequest.newBuilder()
        .setPartialSketch(ByteString.copyFromUtf8("etch_3_-AddedNoise"))
        .setComputationId(computationId).build()
    )
  }

  @Test
  fun `to blind positions using cached result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val inputBlobPath = "to_blind_position/input"
    val outputBlobPath = "to_blind_position/output"
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_BLIND_POSITIONS.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(
        newInputBlobMetadata(0L, inputBlobPath),
        newOutputBlobMetadata(1L, outputBlobPath)
      )
    )
    fakeBlobs[inputBlobPath] = "sketch".toByteArray()
    fakeBlobs[outputBlobPath] = "cached result".toByteArray()

    lateinit var computationControlRequests: List<HandleConcatenatedSketchRequest>
    whenever(mockLiquidLegionsComputationControl.handleConcatenatedSketch(any())).thenAnswer {
      val request: Flow<HandleConcatenatedSketchRequest> = it.getArgument(0)
      computationControlRequests = runBlocking { request.toList() }
      HandleConcatenatedSketchResponse.getDefaultInstance()
    }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_FLAG_COUNTS.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath("to_blind_position/output")
        )
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.OUTPUT)
            .setBlobId(1)
        )
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(2) // CreateComputation + transitionStage
        .setRole(RoleInComputation.SECONDARY)
        .build()
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)

    assertThat(computationControlRequests).containsExactly(
      HandleConcatenatedSketchRequest.newBuilder()
        .setPartialSketch(ByteString.copyFromUtf8("cached result"))
        .setComputationId(computationId).build()
    )
  }

  @Test
  fun `to blind positions using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val inputBlobPath = "to_blind_position/input"
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_BLIND_POSITIONS.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(
        newInputBlobMetadata(0L, inputBlobPath),
        newEmptyOutputBlobMetadata(1L)
      )
    )
    fakeBlobs[inputBlobPath] = "sketch".toByteArray()

    lateinit var computationControlRequests: List<HandleConcatenatedSketchRequest>
    whenever(mockLiquidLegionsComputationControl.handleConcatenatedSketch(any())).thenAnswer {
      val request: Flow<HandleConcatenatedSketchRequest> = it.getArgument(0)
      computationControlRequests = runBlocking { request.toList() }
      HandleConcatenatedSketchResponse.getDefaultInstance()
    }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_FLAG_COUNTS.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath("1111/TO_BLIND_POSITIONS_1_output")
        )
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.OUTPUT).setBlobId(1)
        )
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setRole(RoleInComputation.SECONDARY)
        .build()
    val expectOutputBlob = "sketch-BlindedOneLayerRegisterIndex"
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)
    assertEquals(
      expectOutputBlob,
      fakeBlobs["1111/TO_BLIND_POSITIONS_1_output"]!!.toString(Charset.defaultCharset())
    )

    assertThat(computationControlRequests).containsExactly(
      HandleConcatenatedSketchRequest.newBuilder()
        .setPartialSketch(ByteString.copyFromUtf8("sketch-BlindedOneLay")) // Chunk 1, size 20
        .setComputationId(computationId).build(),
      HandleConcatenatedSketchRequest.newBuilder()
        .setPartialSketch(ByteString.copyFromUtf8("erRegisterIndex")) // Chunk 2, the rest
        .setComputationId(computationId).build()
    )
  }

  @Test
  fun `to blind positions and merge register using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val inputBlobPath = "TO_BLIND_POSITIONS_AND_JOIN_REGISTERS/input"
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(
        newInputBlobMetadata(0L, inputBlobPath),
        newEmptyOutputBlobMetadata(1L)
      )
    )
    fakeBlobs[inputBlobPath] = "data".toByteArray()

    lateinit var computationControlRequests: List<HandleEncryptedFlagsAndCountsRequest>
    whenever(mockLiquidLegionsComputationControl.handleEncryptedFlagsAndCounts(any())).thenAnswer {
      val request: Flow<HandleEncryptedFlagsAndCountsRequest> = it.getArgument(0)
      computationControlRequests = runBlocking { request.toList() }
      HandleEncryptedFlagsAndCountsResponse.getDefaultInstance()
    }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.WAIT_FLAG_COUNTS.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(0)
            .setPath("1111/TO_BLIND_POSITIONS_AND_JOIN_REGISTERS_1_output")
        )
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(ComputationBlobDependency.OUTPUT).setBlobId(1)
        )
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setRole(RoleInComputation.SECONDARY)
        .build()
    val expectOutputBlob = "data-BlindedLastLayerIndexThenJoinRegisters"
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)
    assertEquals(
      expectOutputBlob,
      fakeBlobs["1111/TO_BLIND_POSITIONS_AND_JOIN_REGISTERS_1_output"]!!
        .toString(Charset.defaultCharset())
    )

    assertThat(computationControlRequests).containsExactly(
      HandleEncryptedFlagsAndCountsRequest.newBuilder()
        .setPartialData(ByteString.copyFromUtf8("data-BlindedLastLaye")) // Chunk 1, size 20
        .setComputationId(computationId).build(),
      HandleEncryptedFlagsAndCountsRequest.newBuilder()
        .setPartialData(ByteString.copyFromUtf8("rIndexThenJoinRegist")) // Chunk 2, size 20
        .setComputationId(computationId).build(),
      HandleEncryptedFlagsAndCountsRequest.newBuilder()
        .setPartialData(ByteString.copyFromUtf8("ers")) // Chunk 3, the rest
        .setComputationId(computationId).build()
    )
  }

  @Test
  fun `to decrypt FlagCounts using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val inputBlobPath = "TO_DECRYPT_FLAG_COUNTS/input"
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_DECRYPT_FLAG_COUNTS.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(
        newInputBlobMetadata(0L, inputBlobPath),
        newEmptyOutputBlobMetadata(1L)
      )
    )
    fakeBlobs[inputBlobPath] = "data".toByteArray()

    lateinit var computationControlRequests: List<HandleEncryptedFlagsAndCountsRequest>
    whenever(mockLiquidLegionsComputationControl.handleEncryptedFlagsAndCounts(any())).thenAnswer {
      val request: Flow<HandleEncryptedFlagsAndCountsRequest> = it.getArgument(0)
      computationControlRequests = runBlocking { request.toList() }
      HandleEncryptedFlagsAndCountsResponse.getDefaultInstance()
    }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.COMPLETED.toProtocolStage())
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setRole(RoleInComputation.SECONDARY)
        .build()
    val expectOutputBlob = "data-DecryptedOneLayerFlagAndCount"
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)
    assertEquals(
      expectOutputBlob,
      fakeBlobs["1111/TO_DECRYPT_FLAG_COUNTS_1_output"]!!.toString(Charset.defaultCharset())
    )

    assertThat(computationControlRequests).containsExactly(
      HandleEncryptedFlagsAndCountsRequest.newBuilder()
        .setPartialData(ByteString.copyFromUtf8("data-DecryptedOneLay")) // Chunk 1, size 20
        .setComputationId(computationId).build(),
      HandleEncryptedFlagsAndCountsRequest.newBuilder()
        .setPartialData(ByteString.copyFromUtf8("erFlagAndCount")) // Chunk 2, the rest
        .setComputationId(computationId).build()
    )
  }

  @Test
  fun `to decrypt flag count and compute metric`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val inputBlobPath = "TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS/input"
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = LiquidLegionsStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS.toProtocolStage(),
      role = RoleInComputation.PRIMARY,
      blobs = listOf(
        newInputBlobMetadata(0L, inputBlobPath),
        newEmptyOutputBlobMetadata(1L)
      )
    )
    fakeBlobs[inputBlobPath] = "data".toByteArray()

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(LiquidLegionsStage.COMPLETED.toProtocolStage())
        .setNextDuchy("NEXT_WORKER")
        .setPrimaryDuchy("PRIMARY_WORKER")
        .setVersion(3) // CreateComputation + write blob + transitionStage
        .setRole(RoleInComputation.PRIMARY)
        .build()
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)
    assertThat(fakeBlobs["1111/TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS_1_output"]).isNotEmpty()

    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::finishGlobalComputation
    )
      .isEqualTo(
        FinishGlobalComputationRequest.newBuilder()
          .setKey(
            GlobalComputation.Key.newBuilder()
              .setGlobalComputationId(computationId.toString())
          )
          .setResult(
            GlobalComputation.Result.newBuilder()
              .setReach(9)
              .putFrequency(1, 1)
              .putFrequency(2, 2)
              .putFrequency(3, 3)
          )
          .build()
      )
  }

  companion object {
    private const val MILL_ID = "a nice mill"
    private const val DUCHY_ONE_NAME = "NEXT_WORKER"
    private const val DUCHY_TWO_NAME = "PRIMARY_WORKER"
    private val otherDuchyNames = listOf(DUCHY_ONE_NAME, DUCHY_TWO_NAME)

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
    private const val CURVE_ID = 415; // NID_X9_62_prime256v1

    private val cryptoKeySet = CryptoKeySet(
      ownPublicAndPrivateKeys = OWN_EL_GAMAL_KEY.toElGamalKeys(),
      otherDuchyPublicKeys = mapOf(
        DUCHY_ONE_NAME to DUCHY_ONE_PUBLIC_KEY.toElGamalPublicKeys(),
        DUCHY_TWO_NAME to DUCHY_TWO_PUBLIC_KEY.toElGamalPublicKeys()
      ),
      clientPublicKey = CLIENT_PUBLIC_KEY.toElGamalPublicKeys(),
      curveId = CURVE_ID
    )
  }
}
