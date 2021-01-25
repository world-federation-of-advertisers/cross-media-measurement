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
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.ExperimentalCoroutinesApi
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
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.identity.testing.SenderContext
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest as AsyncAdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationResponse as AsyncAdvanceComputationResponse
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineImplBase
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.AdvanceComputationRequest
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV1
import org.wfanet.measurement.system.v1alpha.LiquidLegionsV2

private const val RUNNING_DUCHY_NAME = "Alsace"
private const val BAVARIA = "Bavaria"
private const val CARINTHIA = "Carinthia"
private val OTHER_DUCHY_NAMES = listOf(BAVARIA, CARINTHIA)

@RunWith(JUnit4::class)
class ComputationControlServiceTest {
  private val mockAsyncControlService: AsyncComputationControlCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val advanceAsyncComputationRequests = mutableListOf<AsyncAdvanceComputationRequest>()
  fun mockAsyncService() = runBlocking<Unit> {
    whenever(mockAsyncControlService.advanceComputation(any())).thenAnswer {
      val req: AsyncAdvanceComputationRequest = it.getArgument(0)
      advanceAsyncComputationRequests.add(req)
      AsyncAdvanceComputationResponse.getDefaultInstance()
    }
  }

  private val tempDirectory = TemporaryFolder()
  private val duchyIdSetter = DuchyIdSetter(RUNNING_DUCHY_NAME, *OTHER_DUCHY_NAMES.toTypedArray())

  private lateinit var bavaria: DuchyIdentity
  private lateinit var carinthia: DuchyIdentity
  private lateinit var storageClient: StorageClient
  private suspend fun StorageClient.readBlobAsString(key: String): String {
    return getBlob(key)?.read(defaultBufferSizeBytes)?.flatten()?.toStringUtf8()!!
  }

  val grpcTestServerRule = GrpcTestServerRule {
    storageClient = FileSystemStorageClient(tempDirectory.root)
    addService(mockAsyncControlService)
  }

  @get:Rule
  val ruleChain = chainRulesSequentially(tempDirectory, duchyIdSetter, grpcTestServerRule)

  private lateinit var senderContext: SenderContext<ComputationControlService>
  private suspend fun <R> withSender(
    sender: DuchyIdentity,
    rpcCall: suspend ComputationControlService.() -> R
  ) = senderContext.withSender(sender, rpcCall)

  @Before
  fun initService() {
    mockAsyncService()
    bavaria = DuchyIdentity(BAVARIA)
    carinthia = DuchyIdentity(CARINTHIA)
    senderContext =
      SenderContext { duchyIdProvider ->
        ComputationControlService(
          AsyncComputationControlCoroutineStub(grpcTestServerRule.channel),
          storageClient = storageClient,
          duchyIdentityProvider = duchyIdProvider
        )
      }
  }

  @Test
  fun `liquid legions v1 send noised sketch`() = runBlocking<Unit> {
    val id = "21390"
    val bavariaHeader = advanceComputationHeader(LiquidLegionsV1.Description.NOISED_SKETCH, id)
    withSender(bavaria) {
      advanceComputation(bavariaHeader.withContent("blob-contents"))
    }
    val bavariaBlobKey = ComputationControlService.generateBlobKey(bavariaHeader, BAVARIA)

    val carinthiaHeader = advanceComputationHeader(LiquidLegionsV1.Description.NOISED_SKETCH, id)
    withSender(carinthia) {
      advanceComputation(carinthiaHeader.withContent("part1_", "part2_", "part3"))
    }
    val carinthiaBlobKey = ComputationControlService.generateBlobKey(bavariaHeader, CARINTHIA)

    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder().apply {
          globalComputationId = id
          computationStage = LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage()
          dataOrigin = BAVARIA
          blobPath = bavariaBlobKey
        }.build(),
        AsyncAdvanceComputationRequest.newBuilder().apply {
          globalComputationId = id
          computationStage = LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage()
          dataOrigin = CARINTHIA
          blobPath = carinthiaBlobKey
        }.build()
      )

    assertThat(storageClient.readBlobAsString(bavariaBlobKey)).isEqualTo("blob-contents")
    assertThat(storageClient.readBlobAsString(carinthiaBlobKey)).isEqualTo("part1_part2_part3")
  }

  @Test
  fun `liquid legions v1 send concatenated sketch`() = runBlocking<Unit> {
    val id = "345667"
    val bavariaHeader =
      advanceComputationHeader(LiquidLegionsV1.Description.CONCATENATED_SKETCH, id)
    withSender(bavaria) {
      advanceComputation(bavariaHeader.withContent("contents"))
    }
    val key = ComputationControlService.generateBlobKey(bavariaHeader, BAVARIA)

    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder().apply {
          globalComputationId = id
          computationStage =
            LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage()
          dataOrigin = BAVARIA
          blobPath = key
        }.build()
      )
    assertThat(storageClient.readBlobAsString(key)).isEqualTo("contents")
  }

  @Test
  fun `liquid legions v1 send flag counts`() = runBlocking<Unit> {
    val id = "45454545"
    val carinthiaHeader =
      advanceComputationHeader(LiquidLegionsV1.Description.ENCRYPTED_FLAGS_AND_COUNTS, id)
    withSender(carinthia) {
      advanceComputation(carinthiaHeader.withContent("contents"))
    }
    val key = ComputationControlService.generateBlobKey(carinthiaHeader, CARINTHIA)

    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder().apply {
          globalComputationId = id
          computationStage =
            LiquidLegionsSketchAggregationV1.Stage.WAIT_FLAG_COUNTS.toProtocolStage()
          dataOrigin = CARINTHIA
          blobPath = key
        }.build()
      )
    assertThat(storageClient.readBlobAsString(key)).isEqualTo("contents")
  }

  @Test
  fun `liquid legions v2 send setup inputs`() = runBlocking<Unit> {
    val id = "311311"
    val carinthiaHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.SETUP_PHASE_INPUT, id)
    withSender(carinthia) {
      advanceComputation(carinthiaHeader.withContent("contents"))
    }
    val key = ComputationControlService.generateBlobKey(carinthiaHeader, CARINTHIA)

    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder().apply {
          globalComputationId = id
          computationStage =
            LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          dataOrigin = CARINTHIA
          blobPath = key
        }.build()
      )
    assertThat(storageClient.readBlobAsString(key)).isEqualTo("contents")
  }

  @Test
  fun `liquid legions v2 send reach phase inputs`() = runBlocking<Unit> {
    val id = "444444"
    val carinthiaHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT, id)
    withSender(carinthia) {
      advanceComputation(carinthiaHeader.withContent("contents"))
    }
    val key = ComputationControlService.generateBlobKey(carinthiaHeader, CARINTHIA)

    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder().apply {
          globalComputationId = id
          computationStage =
            LiquidLegionsSketchAggregationV2.Stage
              .WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
          dataOrigin = CARINTHIA
          blobPath = key
        }.build()
      )
    assertThat(storageClient.readBlobAsString(key)).isEqualTo("contents")
  }

  @Test
  fun `liquid legions v2 resend reach phase inputs but already written`() = runBlocking<Unit> {
    val id = "444444"
    val carinthiaHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.EXECUTION_PHASE_ONE_INPUT, id)
    val key = ComputationControlService.generateBlobKey(carinthiaHeader, CARINTHIA)
    storageClient.createBlob(key, flowOf(ByteString.copyFromUtf8("already-written-contents")))
    withSender(carinthia) {
      advanceComputation(carinthiaHeader.withContent("contents"))
    }

    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder().apply {
          globalComputationId = id
          computationStage =
            LiquidLegionsSketchAggregationV2.Stage
              .WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
          dataOrigin = CARINTHIA
          blobPath = key
        }.build()
      )
    assertThat(storageClient.readBlobAsString(key)).isEqualTo("already-written-contents")
  }

  @Test
  fun `liquid legions v2 send filtering phase inputs`() = runBlocking<Unit> {
    val id = "55555"
    val bavariaHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT, id)
    withSender(bavaria) {
      advanceComputation(bavariaHeader.withContent("contents"))
    }
    val key = ComputationControlService.generateBlobKey(bavariaHeader, BAVARIA)

    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder().apply {
          globalComputationId = id
          computationStage =
            LiquidLegionsSketchAggregationV2.Stage
              .WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
          dataOrigin = BAVARIA
          blobPath = key
        }.build()
      )
    assertThat(storageClient.readBlobAsString(key)).isEqualTo("contents")
  }

  @Test
  fun `liquid legions v2 send frequency phase inputs`() = runBlocking<Unit> {
    val id = "777777"
    val bavariaHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.EXECUTION_PHASE_THREE_INPUT, id)
    withSender(bavaria) {
      advanceComputation(bavariaHeader.withContent("contents"))
    }
    val key = ComputationControlService.generateBlobKey(bavariaHeader, BAVARIA)

    assertThat(advanceAsyncComputationRequests)
      .containsExactly(
        AsyncAdvanceComputationRequest.newBuilder().apply {
          globalComputationId = id
          computationStage =
            LiquidLegionsSketchAggregationV2.Stage
              .WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
          dataOrigin = BAVARIA
          blobPath = key
        }.build()
      )
    assertThat(storageClient.readBlobAsString(key)).isEqualTo("contents")
  }

  @Test
  fun `empty requests throw`() = runBlocking<Unit> {
    assertFailsWith<StatusRuntimeException> {
      withSender(bavaria) { advanceComputation(flowOf()) }
    }
    assertFailsWith<StatusRuntimeException> {
      withSender(bavaria) {
        advanceComputation(
          flowOf(
            AdvanceComputationRequest.newBuilder().setHeader(
              advanceComputationHeader(
                LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT, "1234"
              )
            ).build()
          )
        )
      }
    }
  }

  @Test
  fun `malformed requests throw`() = runBlocking<Unit> {
    val goodHeader =
      advanceComputationHeader(LiquidLegionsV2.Description.EXECUTION_PHASE_TWO_INPUT, "1234")
    assertFailsWith<StatusRuntimeException> {
      withSender(bavaria) { advanceComputation(flowOf()) }
    }
    assertFailsWith<StatusRuntimeException> {
      withSender(bavaria) {
        advanceComputation(
          flowOf(
            AdvanceComputationRequest.newBuilder().setHeader(goodHeader).build()
          )
        )
      }
    }
    assertFailsWith<StatusRuntimeException> {
      withSender(bavaria) {
        advanceComputation(
          goodHeader.toBuilder().clearKey().build().withContent("blob-contents")
        )
      }
    }
    assertFailsWith<StatusRuntimeException> {
      withSender(bavaria) {
        advanceComputation(
          goodHeader.toBuilder().clearProtocol().build().withContent("blob-contents")
        )
      }
    }
  }
}

@OptIn(ExperimentalCoroutinesApi::class) // For `onStart`.
private fun AdvanceComputationRequest.Header.withContent(
  vararg bodyContent: String
): Flow<AdvanceComputationRequest> {
  return bodyContent.asSequence().map {
    AdvanceComputationRequest.newBuilder().apply {
      bodyChunkBuilder.apply {
        partialData = ByteString.copyFromUtf8(it)
      }
    }.build()
  }.asFlow()
    .onStart { emit(AdvanceComputationRequest.newBuilder().setHeader(this@withContent).build()) }
}
