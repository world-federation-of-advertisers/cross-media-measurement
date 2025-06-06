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

package org.wfanet.measurement.duchy.service.system.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.identity.testing.SenderContext
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.ETags
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest as AsyncAdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationResponse as AsyncAdvanceComputationResponse
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineImplBase
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.advanceComputationRequest as asyncAdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.computationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.getComputationTokenRequest
import org.wfanet.measurement.internal.duchy.getComputationTokenResponse
import org.wfanet.measurement.internal.duchy.getOutputBlobMetadataRequest
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle as HonestMajorityShareShuffleProtocol
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.testing.BlobSubject.Companion.assertThat
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.HonestMajorityShareShuffle
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2Stage
import org.wfanet.measurement.system.v1alpha.ReachOnlyLiquidLegionsV2
import org.wfanet.measurement.system.v1alpha.StageKey
import org.wfanet.measurement.system.v1alpha.computationStage
import org.wfanet.measurement.system.v1alpha.getComputationStageRequest
import org.wfanet.measurement.system.v1alpha.liquidLegionsV2Stage

private const val RUNNING_DUCHY_NAME = "Alsace"
private const val BAVARIA = "Bavaria"
private const val CARINTHIA = "Carinthia"
private val OTHER_DUCHY_NAMES = listOf(BAVARIA, CARINTHIA)
private const val BLOB_ID = 1234L
private val BLOB_CONTENT = "content".toByteStringUtf8()
private val SEED = "seed".toByteStringUtf8()

@RunWith(JUnit4::class)
class ComputationControlServiceTest {
  private val mockAsyncControlService: AsyncComputationControlCoroutineImplBase = mockService()
  private val mockComputationsService: ComputationsCoroutineImplBase = mockService()
  private val advanceAsyncComputationRequests = mutableListOf<AsyncAdvanceComputationRequest>()

  private fun stubAsyncService() {
    mockAsyncControlService.stub {
      onBlocking { advanceComputation(any()) }
        .thenAnswer {
          val req: AsyncAdvanceComputationRequest = it.getArgument(0)
          advanceAsyncComputationRequests.add(req)
          AsyncAdvanceComputationResponse.getDefaultInstance()
        }

      onBlocking { getOutputBlobMetadata(any()) }
        .thenReturn(
          computationStageBlobMetadata {
            dependencyType = ComputationBlobDependency.OUTPUT
            blobId = BLOB_ID
          }
        )
    }
  }

  private val tempDirectory = TemporaryFolder()
  private lateinit var computationStore: ComputationStore
  private lateinit var service: ComputationControlService
  private val duchyIdSetter = DuchyIdSetter(RUNNING_DUCHY_NAME, *OTHER_DUCHY_NAMES.toTypedArray())

  private lateinit var bavaria: DuchyIdentity
  private lateinit var carinthia: DuchyIdentity

  val grpcTestServerRule = GrpcTestServerRule {
    val storageClient = FileSystemStorageClient(tempDirectory.root)
    computationStore = ComputationStore(storageClient)
    addService(mockAsyncControlService)
    addService(mockComputationsService)
  }

  @get:Rule val ruleChain = chainRulesSequentially(tempDirectory, duchyIdSetter, grpcTestServerRule)

  private lateinit var senderContext: SenderContext<ComputationControlService>

  private suspend fun <R> withSender(
    sender: DuchyIdentity,
    rpcCall: suspend ComputationControlService.() -> R,
  ) = senderContext.withSender(sender, rpcCall)

  @Before
  fun initService() {
    stubAsyncService()
    bavaria = DuchyIdentity(BAVARIA)
    carinthia = DuchyIdentity(CARINTHIA)
    senderContext = SenderContext { duchyIdProvider ->
      service =
        ComputationControlService(
          RUNNING_DUCHY_NAME,
          ComputationsCoroutineStub(grpcTestServerRule.channel),
          AsyncComputationControlCoroutineStub(grpcTestServerRule.channel),
          computationStore,
          Dispatchers.Default,
          duchyIdentityProvider = duchyIdProvider,
        )
      service
    }
  }

  @Test
  fun `liquid legions v2 send setup inputs`() = runBlocking {
    val id = "311311"
    val blobKey = "$id/WAIT_SETUP_PHASE_INPUTS/$BLOB_ID"
    val carinthiaHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.SETUP_PHASE_INPUT, id)
    withSender(carinthia) { advanceComputation(carinthiaHeader.withContent(BLOB_CONTENT)) }

    verifyProtoArgument(
        mockAsyncControlService,
        AsyncComputationControlCoroutineImplBase::getOutputBlobMetadata,
      )
      .isEqualTo(
        getOutputBlobMetadataRequest {
          globalComputationId = id
          dataOrigin = CARINTHIA
        }
      )
    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder()
          .apply {
            globalComputationId = id
            computationStage =
              LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
            blobId = BLOB_ID
            blobPath = blobKey
          }
          .build()
      )
    val data = assertNotNull(computationStore.get(blobKey))
    assertThat(data).contentEqualTo(BLOB_CONTENT)
  }

  @Test
  fun `liquid legions v2 send reach phase inputs`() = runBlocking {
    val id = "444444"
    val blobKey = "$id/WAIT_EXECUTION_PHASE_ONE_INPUTS/$BLOB_ID"
    val carinthiaHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT, id)
    withSender(carinthia) { advanceComputation(carinthiaHeader.withContent(BLOB_CONTENT)) }

    verifyProtoArgument(
        mockAsyncControlService,
        AsyncComputationControlCoroutineImplBase::getOutputBlobMetadata,
      )
      .isEqualTo(
        getOutputBlobMetadataRequest {
          globalComputationId = id
          dataOrigin = CARINTHIA
        }
      )
    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder()
          .apply {
            globalComputationId = id
            computationStage =
              LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
                .toProtocolStage()
            blobId = BLOB_ID
            blobPath = blobKey
          }
          .build()
      )
    val data = assertNotNull(computationStore.get(blobKey))
    assertThat(data).contentEqualTo(BLOB_CONTENT)
  }

  @Test
  fun `liquid legions v2 send filtering phase inputs`() = runBlocking {
    val id = "55555"
    val blobKey = "$id/WAIT_EXECUTION_PHASE_TWO_INPUTS/$BLOB_ID"
    val bavariaHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT, id)
    withSender(bavaria) { advanceComputation(bavariaHeader.withContent(BLOB_CONTENT)) }

    verifyProtoArgument(
        mockAsyncControlService,
        AsyncComputationControlCoroutineImplBase::getOutputBlobMetadata,
      )
      .isEqualTo(
        getOutputBlobMetadataRequest {
          globalComputationId = id
          dataOrigin = BAVARIA
        }
      )
    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder()
          .apply {
            globalComputationId = id
            computationStage =
              LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
                .toProtocolStage()
            blobId = BLOB_ID
            blobPath = blobKey
          }
          .build()
      )

    val data = assertNotNull(computationStore.get(blobKey))
    assertThat(data).contentEqualTo(BLOB_CONTENT)
  }

  @Test
  fun `liquid legions v2 send frequency phase inputs`() = runBlocking {
    val id = "777777"
    val blobKey = "$id/WAIT_EXECUTION_PHASE_THREE_INPUTS/$BLOB_ID"
    val bavariaHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT, id)
    withSender(bavaria) { advanceComputation(bavariaHeader.withContent(BLOB_CONTENT)) }

    verifyProtoArgument(
        mockAsyncControlService,
        AsyncComputationControlCoroutineImplBase::getOutputBlobMetadata,
      )
      .isEqualTo(
        getOutputBlobMetadataRequest {
          globalComputationId = id
          dataOrigin = BAVARIA
        }
      )
    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder()
          .apply {
            globalComputationId = id
            computationStage =
              LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS
                .toProtocolStage()
            blobId = BLOB_ID
            blobPath = blobKey
          }
          .build()
      )
    val data = assertNotNull(computationStore.get(blobKey))
    assertThat(data).contentEqualTo(BLOB_CONTENT)
  }

  @Test
  fun `empty requests throw`() =
    runBlocking<Unit> {
      assertFailsWith<StatusRuntimeException> {
        withSender(bavaria) { advanceComputation(flowOf()) }
      }
      assertFailsWith<StatusRuntimeException> {
        withSender(bavaria) {
          advanceComputation(
            flowOf(
              AdvanceComputationRequest.newBuilder()
                .setHeader(
                  advanceComputationHeader(
                    LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT,
                    "1234",
                  )
                )
                .build()
            )
          )
        }
      }
    }

  @Test
  fun `malformed requests throw`() =
    runBlocking<Unit> {
      val goodHeader =
        advanceComputationHeader(LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT, "1234")
      assertFailsWith<StatusRuntimeException> {
        withSender(bavaria) { advanceComputation(flowOf()) }
      }
      assertFailsWith<StatusRuntimeException> {
        withSender(bavaria) {
          advanceComputation(
            flowOf(AdvanceComputationRequest.newBuilder().setHeader(goodHeader).build())
          )
        }
      }
      assertFailsWith<StatusRuntimeException> {
        withSender(bavaria) {
          advanceComputation(goodHeader.toBuilder().clearName().build().withContent(BLOB_CONTENT))
        }
      }
      assertFailsWith<StatusRuntimeException> {
        withSender(bavaria) {
          advanceComputation(
            goodHeader.toBuilder().clearProtocol().build().withContent(BLOB_CONTENT)
          )
        }
      }
    }

  @Test
  fun `reach only liquid legions v2 send setup inputs`() = runBlocking {
    val id = "311311"
    val blobKey = "$id/WAIT_SETUP_PHASE_INPUTS/$BLOB_ID"
    val carinthiaHeader =
      advanceComputationHeader(ReachOnlyLiquidLegionsV2.Description.SETUP_PHASE_INPUT, id)
    withSender(carinthia) { advanceComputation(carinthiaHeader.withContent(BLOB_CONTENT)) }

    verifyProtoArgument(
        mockAsyncControlService,
        AsyncComputationControlCoroutineImplBase::getOutputBlobMetadata,
      )
      .isEqualTo(
        getOutputBlobMetadataRequest {
          globalComputationId = id
          dataOrigin = CARINTHIA
        }
      )
    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        asyncAdvanceComputationRequest {
          globalComputationId = id
          computationStage =
            ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS
              .toProtocolStage()
          blobId = BLOB_ID
          blobPath = blobKey
        }
      )
    val data = assertNotNull(computationStore.get(blobKey))
    assertThat(data).contentEqualTo(BLOB_CONTENT)
  }

  @Test
  fun `reach only liquid legions v2 send execution phase inputs`() = runBlocking {
    val id = "444444"
    val blobKey = "$id/WAIT_EXECUTION_PHASE_INPUTS/$BLOB_ID"
    val carinthiaHeader =
      advanceComputationHeader(ReachOnlyLiquidLegionsV2.Description.EXECUTION_PHASE_INPUT, id)
    withSender(carinthia) { advanceComputation(carinthiaHeader.withContent(BLOB_CONTENT)) }

    verifyProtoArgument(
        mockAsyncControlService,
        AsyncComputationControlCoroutineImplBase::getOutputBlobMetadata,
      )
      .isEqualTo(
        getOutputBlobMetadataRequest {
          globalComputationId = id
          dataOrigin = CARINTHIA
        }
      )
    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        asyncAdvanceComputationRequest {
          globalComputationId = id
          computationStage =
            ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_INPUTS
              .toProtocolStage()
          blobId = BLOB_ID
          blobPath = blobKey
        }
      )
    val data = assertNotNull(computationStore.get(blobKey))
    assertThat(data).contentEqualTo(BLOB_CONTENT)
  }

  @Test
  fun `honest majority share shuffle sends input for WAIT_ON_SHUFFLE_INPUT_PHASE_ONE`() =
    runBlocking {
      val id = "444444"
      val blobKey = "$id/WAIT_ON_SHUFFLE_INPUT_PHASE_ONE/$BLOB_ID"
      val carinthiaHeader =
        advanceComputationHeader(HonestMajorityShareShuffle.Description.SHUFFLE_PHASE_INPUT_ONE, id)

      withSender(carinthia) { advanceComputation(carinthiaHeader.withContent(BLOB_CONTENT)) }

      verifyProtoArgument(
          mockAsyncControlService,
          AsyncComputationControlCoroutineImplBase::getOutputBlobMetadata,
        )
        .isEqualTo(
          getOutputBlobMetadataRequest {
            globalComputationId = id
            dataOrigin = CARINTHIA
          }
        )
      assertThat(advanceAsyncComputationRequests)
        .containsExactly(
          asyncAdvanceComputationRequest {
            globalComputationId = id
            computationStage =
              HonestMajorityShareShuffleProtocol.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE
                .toProtocolStage()
            blobId = BLOB_ID
            blobPath = blobKey
          }
        )
      val data = assertNotNull(computationStore.get(blobKey))
      assertThat(data).contentEqualTo(BLOB_CONTENT)
    }

  @Test
  fun `honest majority share shuffle sends aggregation phase input`() = runBlocking {
    val id = "444444"
    val blobKey = "$id/WAIT_ON_AGGREGATION_INPUT/$BLOB_ID"
    val carinthiaHeader =
      advanceComputationHeader(HonestMajorityShareShuffle.Description.AGGREGATION_PHASE_INPUT, id)

    withSender(carinthia) { advanceComputation(carinthiaHeader.withContent(BLOB_CONTENT)) }

    verifyProtoArgument(
        mockAsyncControlService,
        AsyncComputationControlCoroutineImplBase::getOutputBlobMetadata,
      )
      .isEqualTo(
        getOutputBlobMetadataRequest {
          globalComputationId = id
          dataOrigin = CARINTHIA
        }
      )
    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        asyncAdvanceComputationRequest {
          globalComputationId = id
          computationStage =
            HonestMajorityShareShuffleProtocol.Stage.WAIT_ON_AGGREGATION_INPUT.toProtocolStage()
          blobId = BLOB_ID
          blobPath = blobKey
        }
      )
    val data = assertNotNull(computationStore.get(blobKey))
    assertThat(data).contentEqualTo(BLOB_CONTENT)
  }

  @Test
  fun `getStage returns stage`() = runBlocking {
    val computationId = "123"
    val tokenVersion = 101L
    mockComputationsService.stub {
      onBlocking { getComputationToken(any()) }
        .thenAnswer {
          getComputationTokenResponse {
            token = computationToken {
              globalComputationId = computationId
              computationStage =
                LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
                  .toProtocolStage()
              version = tokenVersion
            }
          }
        }
    }

    val stage =
      withSender(carinthia) {
        getComputationStage(
          getComputationStageRequest { name = StageKey(computationId, RUNNING_DUCHY_NAME).toName() }
        )
      }

    verifyProtoArgument(mockComputationsService, ComputationsCoroutineImplBase::getComputationToken)
      .isEqualTo(getComputationTokenRequest { globalComputationId = computationId })
    assertThat(stage)
      .isEqualTo(
        computationStage {
          name = "computations/$computationId/participants/$RUNNING_DUCHY_NAME/stage"
          liquidLegionsV2Stage = liquidLegionsV2Stage {
            this.stage = LiquidLegionsV2Stage.Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS
          }
          this.etag = ETags.computeETag(tokenVersion)
        }
      )
  }

  @Test
  fun `getStage throw exception when duchy ids mismatch`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withSender(carinthia) {
          getComputationStage(
            getComputationStageRequest { name = StageKey("123", "wrong_duchy_id").toName() }
          )
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
  }
}

private fun AdvanceComputationRequest.Header.withContent(
  vararg bodyContent: ByteString
): Flow<AdvanceComputationRequest> {
  return bodyContent
    .asSequence()
    .map {
      AdvanceComputationRequest.newBuilder()
        .apply { bodyChunkBuilder.apply { partialData = it } }
        .build()
    }
    .asFlow()
    .onStart { emit(AdvanceComputationRequest.newBuilder().setHeader(this@withContent).build()) }
}
