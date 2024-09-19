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

package org.wfanet.measurement.duchy.service.internal.computationcontrol

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newPassThroughBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.toAdvanceComputationStageResponse
import org.wfanet.measurement.duchy.service.internal.computations.toGetComputationTokenResponse
import org.wfanet.measurement.duchy.service.internal.computations.toRecordOutputBlobPathResponse
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest.AfterTransition
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.advanceComputationRequest
import org.wfanet.measurement.internal.duchy.advanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.computationStageDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.getOutputBlobMetadataRequest
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage as HmssStage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage as Llv2Stage
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt
import org.wfanet.measurement.internal.duchy.recordOutputBlobPathRequest

private const val SETUP_BLOB_ID = 1L
private const val SETUP_BLOB_PATH = "setup_path"
private const val SHUFFLE_PHASE_INPUT_BLOB_ID = 1L
private const val SHUFFLE_PHASE_INPUT_BLOB_PATH = "shuffle_path"
private const val AGGREGATION_BLOB_ID_1 = 1L
private const val AGGREGATION_BLOB_ID_2 = 2L
private const val AGGREGATION_BLOB_PATH_1 = "aggregation_path_1"
private const val AGGREGATION_BLOB_PATH_2 = "aggregation_path_2"

@RunWith(JUnit4::class)
class AsyncComputationControlServiceTest {
  private val mockComputationsService: ComputationsCoroutineImplBase = mockService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(mockComputationsService) }

  private val service: AsyncComputationControlService by lazy {
    AsyncComputationControlService(
      ComputationsCoroutineStub(grpcTestServerRule.channel),
      maxAdvanceAttempts = Int.MAX_VALUE,
    )
  }

  private fun mockComputationsServiceCalls(
    tokenBeforeRecord: ComputationToken,
    tokenAfterRecord: ComputationToken,
  ) = runBlocking {
    whenever(mockComputationsService.getComputationToken(any())).thenAnswer {
      tokenBeforeRecord.toGetComputationTokenResponse()
    }

    val recordBlobRequests = mutableListOf<RecordOutputBlobPathRequest>()
    whenever(mockComputationsService.recordOutputBlobPath(any())).thenAnswer {
      val req: RecordOutputBlobPathRequest = it.getArgument(0)
      recordBlobRequests.add(req)
      tokenAfterRecord.toRecordOutputBlobPathResponse()
    }

    val advanceComputationRequests = mutableListOf<AdvanceComputationStageRequest>()
    whenever(mockComputationsService.advanceComputationStage(any())).thenAnswer {
      val req: AdvanceComputationStageRequest = it.getArgument(0)
      advanceComputationRequests.add(req)
      req.token.toAdvanceComputationStageResponse()
    }
    return@runBlocking recordBlobRequests to advanceComputationRequests
  }

  @Test
  fun `record only output and advance for llv2`() =
    runBlocking<Unit> {
      val tokenToWrite =
        ComputationToken.newBuilder()
          .apply {
            computationStage = Llv2Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
            addBlobs(newInputBlobMetadata(0L, "input-to-the-stage"))
            addBlobs(newEmptyOutputBlobMetadata(1L))
            computationDetails = detailsFor(RoleInComputation.AGGREGATOR)
          }
          .build()
      val tokenToAdvance =
        tokenToWrite
          .toBuilder()
          .apply {
            clearBlobs()
            addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
          }
          .build()

      val (recordBlobRequests, advanceComputationRequests) =
        mockComputationsServiceCalls(tokenToWrite, tokenToAdvance)

      service.advanceComputation(
        AdvanceComputationRequest.newBuilder()
          .apply {
            globalComputationId = COMPUTATION_ID
            computationStage = Llv2Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
            blobId = 1L
            blobPath = BLOB_KEY
          }
          .build()
      )
      assertThat(recordBlobRequests)
        .containsExactly(
          RecordOutputBlobPathRequest.newBuilder()
            .apply {
              token = tokenToWrite
              outputBlobId = 1
              blobPath = BLOB_KEY
            }
            .build()
        )
      assertThat(advanceComputationRequests)
        .containsExactly(
          AdvanceComputationStageRequest.newBuilder()
            .apply {
              token = tokenToAdvance
              nextComputationStage = Llv2Stage.EXECUTION_PHASE_ONE.toProtocolStage()
              addInputBlobs(BLOB_KEY)
              outputBlobs = 1
              stageDetails = ComputationStageDetails.getDefaultInstance()
              afterTransition =
                AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE
            }
            .build()
        )
    }

  @Test
  fun `record but more blobs to write so do not advance for llv2`() = runBlocking {
    val tokenBeforeRecord =
      ComputationToken.newBuilder()
        .apply {
          computationStage = Llv2Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          addBlobs(newPassThroughBlobMetadata(0L, "pass-through-blob"))
          addBlobs(newEmptyOutputBlobMetadata(1L))
          addBlobs(newEmptyOutputBlobMetadata(2L))
          computationDetails = detailsFor(RoleInComputation.AGGREGATOR)
          stageSpecificDetailsBuilder.apply {
            liquidLegionsV2Builder.apply {
              waitSetupPhaseInputsDetailsBuilder.putExternalDuchyLocalBlobId("alice", 2L)
              waitSetupPhaseInputsDetailsBuilder.putExternalDuchyLocalBlobId("bob", 1L)
            }
          }
        }
        .build()
    val tokenAfterRecord =
      tokenBeforeRecord
        .toBuilder()
        .apply {
          clearBlobs()
          addBlobs(newPassThroughBlobMetadata(0L, "pass-through-blob"))
          addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
          addBlobs(newEmptyOutputBlobMetadata(2L)) // There is still a blob without a key.
        }
        .build()

    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(tokenBeforeRecord, tokenAfterRecord)

    service.advanceComputation(
      AdvanceComputationRequest.newBuilder()
        .apply {
          globalComputationId = COMPUTATION_ID
          computationStage = Llv2Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          blobId = 1L
          blobPath = BLOB_KEY
        }
        .build()
    )
    assertThat(recordBlobRequests)
      .containsExactly(
        RecordOutputBlobPathRequest.newBuilder()
          .apply {
            token = tokenBeforeRecord
            outputBlobId = 1
            blobPath = BLOB_KEY
          }
          .build()
      )
    // Waiting on more outputs for the stage. Computation stage is not advanced.
    assertThat(advanceComputationRequests).isEmpty()
  }

  @Test
  fun `record last output blob for llv2`() =
    runBlocking<Unit> {
      val tokenBeforeRecord =
        ComputationToken.newBuilder()
          .apply {
            computationStage = Llv2Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
            addBlobs(newPassThroughBlobMetadata(0L, "pass-through-blob"))
            addBlobs(newEmptyOutputBlobMetadata(1L))
            addBlobs(newOutputBlobMetadata(2L, "written-output"))
            computationDetails = detailsFor(RoleInComputation.AGGREGATOR)
            stageSpecificDetailsBuilder.apply {
              liquidLegionsV2Builder.apply {
                waitSetupPhaseInputsDetailsBuilder.putExternalDuchyLocalBlobId("alice", 2L)
                waitSetupPhaseInputsDetailsBuilder.putExternalDuchyLocalBlobId("bob", 1L)
              }
            }
          }
          .build()
      val tokenAfterRecord =
        tokenBeforeRecord
          .toBuilder()
          .apply {
            clearBlobs()
            addBlobs(newPassThroughBlobMetadata(0L, "pass-through"))
            addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
            addBlobs(newOutputBlobMetadata(2L, "previously-written-output"))
          }
          .build()

      val (recordBlobRequests, advanceComputationRequests) =
        mockComputationsServiceCalls(tokenBeforeRecord, tokenAfterRecord)

      service.advanceComputation(
        AdvanceComputationRequest.newBuilder()
          .apply {
            globalComputationId = COMPUTATION_ID
            computationStage = Llv2Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
            blobId = 1L
            blobPath = BLOB_KEY
          }
          .build()
      )
      assertThat(recordBlobRequests)
        .containsExactly(
          RecordOutputBlobPathRequest.newBuilder()
            .apply {
              token = tokenBeforeRecord
              outputBlobId = 1
              blobPath = BLOB_KEY
            }
            .build()
        )
      assertThat(advanceComputationRequests)
        .containsExactly(
          AdvanceComputationStageRequest.newBuilder()
            .apply {
              token = tokenAfterRecord
              nextComputationStage = Llv2Stage.SETUP_PHASE.toProtocolStage()
              addInputBlobs("pass-through")
              addInputBlobs(BLOB_KEY)
              addInputBlobs("previously-written-output")
              outputBlobs = 1
              stageDetails = ComputationStageDetails.getDefaultInstance()
              afterTransition =
                AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE
            }
            .build()
        )
    }

  @Test
  fun `advanceComputation advances stage when blob is already recorded for llv2`() =
    runBlocking<Unit> {
      val tokenOfAlreadyRecordedOutput =
        ComputationToken.newBuilder()
          .apply {
            computationStage = Llv2Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
            addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
            computationDetails = detailsFor(RoleInComputation.NON_AGGREGATOR)
          }
          .build()

      val (recordBlobRequests, advanceComputationRequests) =
        mockComputationsServiceCalls(tokenOfAlreadyRecordedOutput, tokenOfAlreadyRecordedOutput)

      service.advanceComputation(
        AdvanceComputationRequest.newBuilder()
          .apply {
            globalComputationId = COMPUTATION_ID
            computationStage = Llv2Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
            blobId = 1L
            blobPath = BLOB_KEY
          }
          .build()
      )
      // The key of the output blob was already recorded.
      assertThat(recordBlobRequests).isEmpty()
      assertThat(advanceComputationRequests)
        .containsExactly(
          AdvanceComputationStageRequest.newBuilder()
            .apply {
              token = tokenOfAlreadyRecordedOutput
              nextComputationStage = Llv2Stage.EXECUTION_PHASE_ONE.toProtocolStage()
              addInputBlobs(BLOB_KEY)
              outputBlobs = 1
              stageDetails = ComputationStageDetails.getDefaultInstance()
              afterTransition =
                AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE
            }
            .build()
        )
    }

  @Test
  fun `advanceComputation is no-op when stage is already advanced for llv2`() = runBlocking {
    val tokenOfAlreadyRecordedOutput =
      ComputationToken.newBuilder()
        .apply {
          computationStage = Llv2Stage.EXECUTION_PHASE_ONE.toProtocolStage()
          addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
          computationDetails = detailsFor(RoleInComputation.NON_AGGREGATOR)
        }
        .build()

    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(tokenOfAlreadyRecordedOutput, tokenOfAlreadyRecordedOutput)

    service.advanceComputation(
      AdvanceComputationRequest.newBuilder()
        .apply {
          globalComputationId = COMPUTATION_ID
          computationStage = Llv2Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
          blobId = 1L
          blobPath = BLOB_KEY
        }
        .build()
    )
    assertThat(recordBlobRequests).isEmpty()
    assertThat(advanceComputationRequests).isEmpty()
  }

  @Test
  fun `advanceComputation throws ABORTED when stage does not match for llv2`() = runBlocking {
    val actualStage = Llv2Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
    val oldToken = computationToken {
      computationDetails = computationDetails {
        liquidLegionsV2 =
          LiquidLegionsSketchAggregationV2Kt.computationDetails {
            role = RoleInComputation.NON_AGGREGATOR
          }
      }
      computationStage = actualStage
      blobs += newEmptyOutputBlobMetadata(1L)
    }
    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(oldToken, oldToken)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.advanceComputation(
          AdvanceComputationRequest.newBuilder()
            .apply {
              globalComputationId = COMPUTATION_ID
              computationStage = Llv2Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
              blobId = 1L
              blobPath = BLOB_KEY
            }
            .build()
        )
      }

    assertThat(exception).hasMessageThat().ignoringCase().contains("stage")
    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(recordBlobRequests).isEmpty()
    assertThat(advanceComputationRequests).isEmpty()
  }

  @Test
  fun `advanceComputation records blob and advance for HMSS WAIT_ON_SHUFFLE_INPUT_PHASE_ONE`():
    Unit = runBlocking {
    val token = computationToken {
      computationStage = HmssStage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE.toProtocolStage()
      blobs += newEmptyOutputBlobMetadata(1L)
      computationDetails = computationDetails {
        honestMajorityShareShuffle =
          HonestMajorityShareShuffleKt.computationDetails {
            role = RoleInComputation.SECOND_NON_AGGREGATOR
          }
      }
    }
    val request = advanceComputationRequest {
      globalComputationId = COMPUTATION_ID
      computationStage = HmssStage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE.toProtocolStage()
      blobId = 1L
      blobPath = BLOB_KEY
    }
    val updatedToken =
      token.copy {
        blobs.clear()
        blobs += newOutputBlobMetadata(1L, BLOB_KEY)
      }
    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(token, updatedToken)

    service.advanceComputation(request)

    assertThat(recordBlobRequests)
      .containsExactly(
        recordOutputBlobPathRequest {
          this.token = token
          outputBlobId = 1
          blobPath = BLOB_KEY
        }
      )
    assertThat(advanceComputationRequests)
      .containsExactly(
        advanceComputationStageRequest {
          this.token = updatedToken
          nextComputationStage = HmssStage.SETUP_PHASE.toProtocolStage()
          inputBlobs += BLOB_KEY
          outputBlobs = 0
          stageDetails = ComputationStageDetails.getDefaultInstance()
          afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        }
      )
  }

  @Test
  fun `advanceComputation records blob and advance for HMSS WAIT_ON_SHUFFLE_INPUT_PHASE_TWO`():
    Unit = runBlocking {
    val token = computationToken {
      computationStage = HmssStage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO.toProtocolStage()
      blobs += newEmptyOutputBlobMetadata(1L)
      computationDetails = computationDetails {
        honestMajorityShareShuffle =
          HonestMajorityShareShuffleKt.computationDetails {
            role = RoleInComputation.FIRST_NON_AGGREGATOR
          }
      }
    }
    val request = advanceComputationRequest {
      globalComputationId = COMPUTATION_ID
      computationStage = HmssStage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO.toProtocolStage()
      blobId = 1L
      blobPath = BLOB_KEY
    }
    val updatedToken =
      token.copy {
        blobs.clear()
        blobs += newOutputBlobMetadata(1L, BLOB_KEY)
      }
    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(token, updatedToken)

    service.advanceComputation(request)

    assertThat(recordBlobRequests)
      .containsExactly(
        recordOutputBlobPathRequest {
          this.token = token
          outputBlobId = 1
          blobPath = BLOB_KEY
        }
      )
    assertThat(advanceComputationRequests)
      .containsExactly(
        advanceComputationStageRequest {
          this.token = updatedToken
          nextComputationStage = HmssStage.SHUFFLE_PHASE.toProtocolStage()
          inputBlobs += BLOB_KEY
          stageDetails = ComputationStageDetails.getDefaultInstance()
          afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        }
      )
  }

  @Test
  fun `advanceComputation records the last blob and advance for HMSS WAIT_ON_AGGREGATION`(): Unit =
    runBlocking {
      val token = computationToken {
        computationStage = HmssStage.WAIT_ON_AGGREGATION_INPUT.toProtocolStage()
        computationDetails = computationDetails {
          honestMajorityShareShuffle =
            HonestMajorityShareShuffleKt.computationDetails { role = RoleInComputation.AGGREGATOR }
        }
        blobs += newOutputBlobMetadata(AGGREGATION_BLOB_ID_1, AGGREGATION_BLOB_PATH_1)
        blobs += newEmptyOutputBlobMetadata(AGGREGATION_BLOB_ID_2)
      }
      val request = advanceComputationRequest {
        globalComputationId = COMPUTATION_ID
        computationStage = HmssStage.WAIT_ON_AGGREGATION_INPUT.toProtocolStage()
        blobId = AGGREGATION_BLOB_ID_2
        blobPath = AGGREGATION_BLOB_PATH_2
      }
      val updatedToken =
        token.copy {
          blobs.clear()
          blobs += newOutputBlobMetadata(AGGREGATION_BLOB_ID_1, AGGREGATION_BLOB_PATH_1)
          blobs += newOutputBlobMetadata(AGGREGATION_BLOB_ID_2, AGGREGATION_BLOB_PATH_2)
        }
      val (recordBlobRequests, advanceComputationRequests) =
        mockComputationsServiceCalls(token, updatedToken)

      service.advanceComputation(request)

      assertThat(recordBlobRequests)
        .containsExactly(
          recordOutputBlobPathRequest {
            this.token = token
            outputBlobId = AGGREGATION_BLOB_ID_2
            blobPath = AGGREGATION_BLOB_PATH_2
          }
        )
      assertThat(advanceComputationRequests)
        .containsExactly(
          advanceComputationStageRequest {
            this.token = updatedToken
            nextComputationStage = HmssStage.AGGREGATION_PHASE.toProtocolStage()
            inputBlobs += AGGREGATION_BLOB_PATH_1
            inputBlobs += AGGREGATION_BLOB_PATH_2
            stageDetails = ComputationStageDetails.getDefaultInstance()
            afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
          }
        )
    }

  @Test
  fun `advanceComputation records a blob that does not advance for HMSS WAIT_ON_AGGREGATION`() =
    runBlocking {
      val token = computationToken {
        computationStage = HmssStage.WAIT_ON_AGGREGATION_INPUT.toProtocolStage()
        computationDetails = computationDetails {
          honestMajorityShareShuffle =
            HonestMajorityShareShuffleKt.computationDetails { role = RoleInComputation.AGGREGATOR }
        }
        blobs += newEmptyOutputBlobMetadata(AGGREGATION_BLOB_ID_1)
        blobs += newEmptyOutputBlobMetadata(AGGREGATION_BLOB_ID_2)
      }
      val request = advanceComputationRequest {
        globalComputationId = COMPUTATION_ID
        computationStage = HmssStage.WAIT_ON_AGGREGATION_INPUT.toProtocolStage()
        blobId = AGGREGATION_BLOB_ID_2
        blobPath = AGGREGATION_BLOB_PATH_2
      }
      val updatedToken =
        token.copy {
          blobs.clear()
          blobs += newEmptyOutputBlobMetadata(AGGREGATION_BLOB_ID_1)
          blobs += newOutputBlobMetadata(AGGREGATION_BLOB_ID_2, AGGREGATION_BLOB_PATH_2)
        }
      val (recordBlobRequests, advanceComputationRequests) =
        mockComputationsServiceCalls(token, updatedToken)

      service.advanceComputation(request)

      assertThat(recordBlobRequests)
        .containsExactly(
          recordOutputBlobPathRequest {
            this.token = token
            outputBlobId = AGGREGATION_BLOB_ID_2
            blobPath = AGGREGATION_BLOB_PATH_2
          }
        )
      assertThat(advanceComputationRequests).isEmpty()
    }

  @Test
  fun `advanceComputation catch up stage when request stage is one step ahead`(): Unit =
    runBlocking {
      val token = computationToken {
        computationStage = HmssStage.SETUP_PHASE.toProtocolStage()
        computationDetails = computationDetails {
          honestMajorityShareShuffle =
            HonestMajorityShareShuffleKt.computationDetails {
              role = RoleInComputation.FIRST_NON_AGGREGATOR
            }
        }
        blobs += newOutputBlobMetadata(SETUP_BLOB_ID, SETUP_BLOB_PATH)
      }
      val request = advanceComputationRequest {
        globalComputationId = COMPUTATION_ID
        computationStage = HmssStage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO.toProtocolStage()
        blobId = 1L
        blobPath = SHUFFLE_PHASE_INPUT_BLOB_PATH
      }
      val caughtUpToken =
        token.copy {
          computationStage = HmssStage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO.toProtocolStage()
          blobs.clear()
          blobs += newInputBlobMetadata(SETUP_BLOB_ID, SETUP_BLOB_PATH)
          blobs += newEmptyOutputBlobMetadata(SHUFFLE_PHASE_INPUT_BLOB_ID)
        }
      val blobRecordedToken =
        caughtUpToken.copy {
          blobs.clear()
          blobs += newOutputBlobMetadata(SHUFFLE_PHASE_INPUT_BLOB_ID, SHUFFLE_PHASE_INPUT_BLOB_PATH)
        }
      val updatedToken =
        blobRecordedToken.copy {
          computationStage = HmssStage.SHUFFLE_PHASE.toProtocolStage()
          blobs.clear()
          blobs += newInputBlobMetadata(SHUFFLE_PHASE_INPUT_BLOB_ID, SHUFFLE_PHASE_INPUT_BLOB_PATH)
        }

      mockComputationsService.stub {
        onBlocking { getComputationToken(any()) }.thenReturn(token.toGetComputationTokenResponse())
        onBlocking { advanceComputationStage(any()) }
          .thenReturn(
            caughtUpToken.toAdvanceComputationStageResponse(),
            updatedToken.toAdvanceComputationStageResponse(),
          )
        onBlocking { recordOutputBlobPath(any()) }
          .thenReturn(blobRecordedToken.toRecordOutputBlobPathResponse())
      }

      service.advanceComputation(request)

      val computationsServiceCaptor: KArgumentCaptor<AdvanceComputationStageRequest> =
        argumentCaptor()
      verifyBlocking(mockComputationsService, times(2)) {
        advanceComputationStage(computationsServiceCaptor.capture())
      }
      assertThat(computationsServiceCaptor.allValues)
        .containsExactly(
          advanceComputationStageRequest {
            this.token = token
            nextComputationStage = HmssStage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO.toProtocolStage()
            inputBlobs += SETUP_BLOB_PATH
            outputBlobs = 1
            stageDetails = ComputationStageDetails.getDefaultInstance()
            afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
          },
          advanceComputationStageRequest {
            this.token = blobRecordedToken
            nextComputationStage = HmssStage.SHUFFLE_PHASE.toProtocolStage()
            inputBlobs += SHUFFLE_PHASE_INPUT_BLOB_PATH
            stageDetails = ComputationStageDetails.getDefaultInstance()
            afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
          },
        )
    }

  @Test
  fun `advanceComputation fail to catch up stage when computation output blob path is not set`() =
    runBlocking {
      val token = computationToken {
        computationStage = HmssStage.SETUP_PHASE.toProtocolStage()
        computationDetails = computationDetails {
          honestMajorityShareShuffle =
            HonestMajorityShareShuffleKt.computationDetails {
              role = RoleInComputation.FIRST_NON_AGGREGATOR
            }
        }
        // Setup has not been done.
        blobs += newEmptyOutputBlobMetadata(SETUP_BLOB_ID)
      }
      val request = advanceComputationRequest {
        globalComputationId = COMPUTATION_ID
        computationStage = HmssStage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO.toProtocolStage()
        blobId = 1L
        blobPath = SHUFFLE_PHASE_INPUT_BLOB_PATH
      }

      mockComputationsService.stub {
        onBlocking { getComputationToken(any()) }.thenReturn(token.toGetComputationTokenResponse())
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.advanceComputation(request) }
      assertThat(exception.status.code).isEqualTo(Status.FAILED_PRECONDITION.code)
    }

  @Test
  fun `getBlobOutputMetadata returns by origin Duchy in LLv2 WAIT_SETUP_PHASE_INPUTS`() {
    val token = computationToken {
      computationStage = Llv2Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
      blobs += newPassThroughBlobMetadata(0L, "pass-through-blob")
      blobs += newEmptyOutputBlobMetadata(1L)
      blobs += newEmptyOutputBlobMetadata(2L)
      stageSpecificDetails = computationStageDetails {
        liquidLegionsV2 =
          LiquidLegionsSketchAggregationV2Kt.stageDetails {
            waitSetupPhaseInputsDetails =
              LiquidLegionsSketchAggregationV2Kt.waitSetupPhaseInputsDetails {
                externalDuchyLocalBlobId["Buck"] = 1L
                externalDuchyLocalBlobId["Rippon"] = 2L
              }
          }
      }
    }
    mockComputationsService.stub {
      onBlocking { getComputationToken(any()) }.thenReturn(token.toGetComputationTokenResponse())
    }

    val blobMetadata = runBlocking {
      service.getOutputBlobMetadata(
        getOutputBlobMetadataRequest {
          globalComputationId = COMPUTATION_ID
          dataOrigin = "Buck"
        }
      )
    }

    assertThat(blobMetadata)
      .isEqualTo(
        computationStageBlobMetadata {
          dependencyType = ComputationBlobDependency.OUTPUT
          blobId = 1L
        }
      )
  }

  @Test
  fun `getBlobOutputMetadata returns by origin Duchy in HMSS WAIT_AGGREGATION_PHASE`() =
    runBlocking {
      val token = computationToken {
        computationStage = HmssStage.WAIT_ON_AGGREGATION_INPUT.toProtocolStage()
        blobs += newEmptyOutputBlobMetadata(1L)
        blobs += newEmptyOutputBlobMetadata(2L)
        stageSpecificDetails = computationStageDetails {
          honestMajorityShareShuffle =
            HonestMajorityShareShuffleKt.stageDetails {
              waitOnAggregationInputDetails =
                HonestMajorityShareShuffleKt.waitOnAggregationInputDetails {
                  externalDuchyLocalBlobId["Buck"] = 1L
                  externalDuchyLocalBlobId["Rippon"] = 2L
                }
            }
        }
      }
      mockComputationsService.stub {
        onBlocking { getComputationToken(any()) }.thenReturn(token.toGetComputationTokenResponse())
      }

      val blobMetadata =
        service.getOutputBlobMetadata(
          getOutputBlobMetadataRequest {
            globalComputationId = COMPUTATION_ID
            dataOrigin = "Buck"
          }
        )

      assertThat(blobMetadata)
        .isEqualTo(
          computationStageBlobMetadata {
            dependencyType = ComputationBlobDependency.OUTPUT
            blobId = 1L
          }
        )
    }

  @Test
  fun `getBlobOutputMetadata returns single output blob in LLv2 WAIT_EXECUTION_PHASE_INPUTS`() {
    val token = computationToken {
      computationStage = Llv2Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
      blobs += newInputBlobMetadata(0L, "input-blob")
      blobs += newEmptyOutputBlobMetadata(1L)
      stageSpecificDetails = computationStageDetails {
        liquidLegionsV2 = LiquidLegionsSketchAggregationV2Kt.stageDetails {}
      }
    }
    mockComputationsService.stub {
      onBlocking { getComputationToken(any()) }.thenReturn(token.toGetComputationTokenResponse())
    }

    val blobMetadata = runBlocking {
      service.getOutputBlobMetadata(
        getOutputBlobMetadataRequest {
          globalComputationId = COMPUTATION_ID
          dataOrigin = "Buck"
        }
      )
    }

    assertThat(blobMetadata)
      .isEqualTo(
        computationStageBlobMetadata {
          dependencyType = ComputationBlobDependency.OUTPUT
          blobId = 1L
        }
      )
  }

  @Test
  fun `getBlobOutputMetadata throws FAILED_PRECONDITION in unexpected LLv2 stage`() {
    val token = computationToken {
      computationStage = Llv2Stage.EXECUTION_PHASE_ONE.toProtocolStage()
    }
    mockComputationsService.stub {
      onBlocking { getComputationToken(any()) }.thenReturn(token.toGetComputationTokenResponse())
    }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> {
        service.getOutputBlobMetadata(
          getOutputBlobMetadataRequest {
            globalComputationId = COMPUTATION_ID
            dataOrigin = "Buck"
          }
        )
      }
    }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains(Llv2Stage.EXECUTION_PHASE_ONE.name)
  }

  @Test
  fun `getBlobOutputMetadata throws FAILED_PRECONDITION in unexpected HMSS stage`() {
    val token = computationToken { computationStage = HmssStage.SHUFFLE_PHASE.toProtocolStage() }
    mockComputationsService.stub {
      onBlocking { getComputationToken(any()) }.thenReturn(token.toGetComputationTokenResponse())
    }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> {
        service.getOutputBlobMetadata(
          getOutputBlobMetadataRequest {
            globalComputationId = COMPUTATION_ID
            dataOrigin = "Buck"
          }
        )
      }
    }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception).hasMessageThat().contains(HmssStage.SHUFFLE_PHASE.name)
  }

  companion object {
    private const val BLOB_KEY = "the-data-was-written-here"
    private const val COMPUTATION_ID = "1234"

    private fun detailsFor(r: RoleInComputation): ComputationDetails =
      ComputationDetails.newBuilder().apply { liquidLegionsV2Builder.apply { role = r } }.build()
  }
}
