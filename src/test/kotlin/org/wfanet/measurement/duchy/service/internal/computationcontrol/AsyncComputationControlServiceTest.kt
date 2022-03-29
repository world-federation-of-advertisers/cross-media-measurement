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
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
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
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.computationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.computationStageDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation
import org.wfanet.measurement.internal.duchy.getOutputBlobMetadataRequest
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt

@RunWith(JUnit4::class)
class AsyncComputationControlServiceTest {
  private val mockComputationsService: ComputationsCoroutineImplBase = mockService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(mockComputationsService) }

  private val service: AsyncComputationControlService by lazy {
    AsyncComputationControlService(ComputationsCoroutineStub(grpcTestServerRule.channel))
  }

  private fun mockComputationsServiceCalls(
    tokenBeforeRecord: ComputationToken,
    tokenAfterRecord: ComputationToken
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
  fun `record only output and advance`() =
    runBlocking<Unit> {
      val tokenToWrite =
        ComputationToken.newBuilder()
          .apply {
            computationStage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
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
            computationStage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
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
              nextComputationStage = Stage.EXECUTION_PHASE_ONE.toProtocolStage()
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
  fun `record but more blobs to write so do not advance`() = runBlocking {
    val tokenBeforeRecord =
      ComputationToken.newBuilder()
        .apply {
          computationStage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
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
          computationStage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
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
  fun `record last output blob`() =
    runBlocking<Unit> {
      val tokenBeforeRecord =
        ComputationToken.newBuilder()
          .apply {
            computationStage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
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
            computationStage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
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
              nextComputationStage = Stage.SETUP_PHASE.toProtocolStage()
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
  fun `advance when blob already written`() =
    runBlocking<Unit> {
      val tokenOfAlreadyRecordedOutput =
        ComputationToken.newBuilder()
          .apply {
            computationStage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
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
            computationStage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
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
              nextComputationStage = Stage.EXECUTION_PHASE_ONE.toProtocolStage()
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
  fun `advance when stage already advanced is a noop`() = runBlocking {
    val tokenOfAlreadyRecordedOutput =
      ComputationToken.newBuilder()
        .apply {
          computationStage = Stage.EXECUTION_PHASE_ONE.toProtocolStage()
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
          computationStage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
          blobPath = BLOB_KEY
        }
        .build()
    )
    assertThat(recordBlobRequests).isEmpty()
    assertThat(advanceComputationRequests).isEmpty()
  }

  @Test
  fun `advance stage doesn't match throws exception`() = runBlocking {
    val actualStage = Stage.EXECUTION_PHASE_ONE.toProtocolStage()
    val oldToken = ComputationToken.newBuilder().setComputationStage(actualStage).build()

    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(oldToken, oldToken)

    assertFailsWith<StatusRuntimeException>(
      message = "INVALID_ARGUMENT: Actual stage from computation ($actualStage) did not match"
    ) {
      service.advanceComputation(
        AdvanceComputationRequest.newBuilder()
          .apply {
            globalComputationId = COMPUTATION_ID
            computationStage = Stage.EXECUTION_PHASE_ONE.toProtocolStage()
          }
          .build()
      )
    }
    assertThat(recordBlobRequests).isEmpty()
    assertThat(advanceComputationRequests).isEmpty()
  }

  @Test
  fun `getBlobOutputMetadata returns by origin Duchy in LLv2 WAIT_SETUP_PHASE_INPUTS`() {
    val token = computationToken {
      computationStage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
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
  fun `getBlobOutputMetadata returns single output blob in LLv2 WAIT_EXECUTION_PHASE_INPUTS`() {
    val token = computationToken {
      computationStage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
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
    val token = computationToken { computationStage = Stage.EXECUTION_PHASE_ONE.toProtocolStage() }
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
    assertThat(exception).hasMessageThat().contains(Stage.EXECUTION_PHASE_ONE.name)
  }

  companion object {
    private const val BLOB_KEY = "the-data-was-written-here"
    private const val COMPUTATION_ID = "1234"

    private fun detailsFor(r: RoleInComputation): ComputationDetails =
      ComputationDetails.newBuilder().apply { liquidLegionsV2Builder.apply { role = r } }.build()
  }
}
