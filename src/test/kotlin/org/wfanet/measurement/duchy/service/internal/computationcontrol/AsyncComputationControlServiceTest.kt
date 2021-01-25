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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.service.internal.computation.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newPassThroughBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.toAdvanceComputationStageResponse
import org.wfanet.measurement.duchy.service.internal.computation.toGetComputationTokenResponse
import org.wfanet.measurement.duchy.service.internal.computation.toRecordOutputBlobPathResponse
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.Stage

@RunWith(JUnit4::class)
class AsyncComputationControlServiceTest {
  private val mockComputationsService: ComputationsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(mockComputationsService)
  }

  private val fakeService: AsyncComputationControlService by lazy {
    AsyncComputationControlService(
      ComputationsCoroutineStub(grpcTestServerRule.channel),
      ComputationProtocolStageDetails(listOf())
    )
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
  fun `record only output and advance`() = runBlocking<Unit> {
    val tokenToWrite = ComputationToken.newBuilder().apply {
      computationStage = Stage.WAIT_CONCATENATED.toProtocolStage()
      addBlobs(newInputBlobMetadata(0L, "input-to-the-stage"))
      addBlobs(newEmptyOutputBlobMetadata(1L))
      computationDetails = detailsFor(RoleInComputation.PRIMARY)
    }.build()
    val tokenToAdvance = tokenToWrite.toBuilder().apply {
      clearBlobs()
      addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
    }.build()

    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(tokenToWrite, tokenToAdvance)

    fakeService.advanceComputation(
      AdvanceComputationRequest.newBuilder().apply {
        globalComputationId = COMPUTATION_ID
        computationStage = Stage.WAIT_CONCATENATED.toProtocolStage()
        blobPath = BLOB_KEY
      }.build()
    )
    assertThat(recordBlobRequests)
      .containsExactly(
        RecordOutputBlobPathRequest.newBuilder().apply {
          token = tokenToWrite
          outputBlobId = 1
          blobPath = BLOB_KEY
        }.build()
      )
    assertThat(advanceComputationRequests)
      .containsExactly(
        AdvanceComputationStageRequest.newBuilder().apply {
          token = tokenToAdvance
          nextComputationStage = Stage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS.toProtocolStage()
          addInputBlobs(BLOB_KEY)
          outputBlobs = 1
          stageDetails = ComputationStageDetails.getDefaultInstance()
          afterTransition = AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        }.build()
      )
  }

  @Test
  fun `record but more blobs to write so do not advance`() = runBlocking<Unit> {
    val tokenBeforeRecord = ComputationToken.newBuilder().apply {
      computationStage = Stage.WAIT_SKETCHES.toProtocolStage()
      addBlobs(newPassThroughBlobMetadata(0L, "pass-through-blob"))
      addBlobs(newEmptyOutputBlobMetadata(1L))
      addBlobs(newEmptyOutputBlobMetadata(2L))
      computationDetails = detailsFor(RoleInComputation.PRIMARY)
      stageSpecificDetailsBuilder.apply {
        liquidLegionsV1Builder.apply {
          waitSketchStageDetailsBuilder.putExternalDuchyLocalBlobId("alice", 2L)
          waitSketchStageDetailsBuilder.putExternalDuchyLocalBlobId("bob", 1L)
        }
      }
    }.build()
    val tokenAfterRecord = tokenBeforeRecord.toBuilder().apply {
      clearBlobs()
      addBlobs(newPassThroughBlobMetadata(0L, "pass-through-blob"))
      addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
      addBlobs(newEmptyOutputBlobMetadata(2L)) // There is still a blob without a key.
    }.build()

    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(tokenBeforeRecord, tokenAfterRecord)

    fakeService.advanceComputation(
      AdvanceComputationRequest.newBuilder().apply {
        globalComputationId = COMPUTATION_ID
        computationStage = Stage.WAIT_SKETCHES.toProtocolStage()
        dataOrigin = "bob"
        blobPath = BLOB_KEY
      }.build()
    )
    assertThat(recordBlobRequests)
      .containsExactly(
        RecordOutputBlobPathRequest.newBuilder().apply {
          token = tokenBeforeRecord
          outputBlobId = 1
          blobPath = BLOB_KEY
        }.build()
      )
    // Waiting on more outputs for the stage. Computation stage is not advanced.
    assertThat(advanceComputationRequests).isEmpty()
  }

  @Test
  fun `record last output blob`() = runBlocking<Unit> {
    val tokenBeforeRecord = ComputationToken.newBuilder().apply {
      computationStage = Stage.WAIT_SKETCHES.toProtocolStage()
      addBlobs(newPassThroughBlobMetadata(0L, "pass-through-blob"))
      addBlobs(newEmptyOutputBlobMetadata(1L))
      addBlobs(newOutputBlobMetadata(2L, "written-output"))
      computationDetails = detailsFor(RoleInComputation.PRIMARY)
      stageSpecificDetailsBuilder.apply {
        liquidLegionsV1Builder.apply {
          waitSketchStageDetailsBuilder.putExternalDuchyLocalBlobId("alice", 2L)
          waitSketchStageDetailsBuilder.putExternalDuchyLocalBlobId("bob", 1L)
        }
      }
    }.build()
    val tokenAfterRecord = tokenBeforeRecord.toBuilder().apply {
      clearBlobs()
      addBlobs(newPassThroughBlobMetadata(0L, "pass-through"))
      addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
      addBlobs(newOutputBlobMetadata(2L, "previously-written-output"))
    }.build()

    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(tokenBeforeRecord, tokenAfterRecord)

    fakeService.advanceComputation(
      AdvanceComputationRequest.newBuilder().apply {
        globalComputationId = COMPUTATION_ID
        computationStage = Stage.WAIT_SKETCHES.toProtocolStage()
        dataOrigin = "bob"
        blobPath = BLOB_KEY
      }.build()
    )
    assertThat(recordBlobRequests)
      .containsExactly(
        RecordOutputBlobPathRequest.newBuilder().apply {
          token = tokenBeforeRecord
          outputBlobId = 1
          blobPath = BLOB_KEY
        }.build()
      )
    assertThat(advanceComputationRequests)
      .containsExactly(
        AdvanceComputationStageRequest.newBuilder().apply {
          token = tokenAfterRecord
          nextComputationStage = Stage.TO_APPEND_SKETCHES_AND_ADD_NOISE.toProtocolStage()
          addInputBlobs("pass-through")
          addInputBlobs(BLOB_KEY)
          addInputBlobs("previously-written-output")
          outputBlobs = 1
          stageDetails = ComputationStageDetails.getDefaultInstance()
          afterTransition = AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        }.build()
      )
  }

  @Test
  fun `advance when blob already written`() = runBlocking<Unit> {
    val tokenOfAlreadyRecordedOutput = ComputationToken.newBuilder().apply {
      computationStage = Stage.WAIT_CONCATENATED.toProtocolStage()
      addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
      computationDetails = detailsFor(RoleInComputation.SECONDARY)
    }.build()

    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(tokenOfAlreadyRecordedOutput, tokenOfAlreadyRecordedOutput)

    fakeService.advanceComputation(
      AdvanceComputationRequest.newBuilder().apply {
        globalComputationId = COMPUTATION_ID
        computationStage = Stage.WAIT_CONCATENATED.toProtocolStage()
        blobPath = BLOB_KEY
      }.build()
    )
    // The key of the output blob was already recorded.
    assertThat(recordBlobRequests).isEmpty()
    assertThat(advanceComputationRequests)
      .containsExactly(
        AdvanceComputationStageRequest.newBuilder().apply {
          token = tokenOfAlreadyRecordedOutput
          nextComputationStage = Stage.TO_BLIND_POSITIONS.toProtocolStage()
          addInputBlobs(BLOB_KEY)
          outputBlobs = 1
          stageDetails = ComputationStageDetails.getDefaultInstance()
          afterTransition = AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        }.build()
      )
  }

  @Test
  fun `advance when stage already advanced is a noop`() = runBlocking<Unit> {
    val tokenOfAlreadyRecordedOutput = ComputationToken.newBuilder().apply {
      computationStage = Stage.TO_BLIND_POSITIONS.toProtocolStage()
      addBlobs(newOutputBlobMetadata(1L, BLOB_KEY))
      computationDetails = detailsFor(RoleInComputation.SECONDARY)
    }.build()

    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(tokenOfAlreadyRecordedOutput, tokenOfAlreadyRecordedOutput)

    fakeService.advanceComputation(
      AdvanceComputationRequest.newBuilder().apply {
        globalComputationId = COMPUTATION_ID
        computationStage = Stage.WAIT_CONCATENATED.toProtocolStage()
        blobPath = BLOB_KEY
      }.build()
    )
    assertThat(recordBlobRequests).isEmpty()
    assertThat(advanceComputationRequests).isEmpty()
  }

  @Test
  fun `advance stage doesn't match throws exception`() = runBlocking<Unit> {
    val actualStage = Stage.TO_BLIND_POSITIONS.toProtocolStage()
    val oldToken = ComputationToken.newBuilder().setComputationStage(actualStage).build()

    val (recordBlobRequests, advanceComputationRequests) =
      mockComputationsServiceCalls(oldToken, oldToken)

    assertFailsWith<StatusRuntimeException>(
      message = "INVALID_ARGUMENT: Actual stage from computation ($actualStage) did not match"
    ) {
      fakeService.advanceComputation(
        AdvanceComputationRequest.newBuilder().apply {
          globalComputationId = COMPUTATION_ID
          computationStage = Stage.WAIT_CONCATENATED.toProtocolStage()
        }.build()
      )
    }
    assertThat(recordBlobRequests).isEmpty()
    assertThat(advanceComputationRequests).isEmpty()
  }

  companion object {
    private const val BLOB_KEY = "the-data-was-written-here"
    private const val COMPUTATION_ID = "1234"

    private fun detailsFor(r: RoleInComputation): ComputationDetails =
      ComputationDetails.newBuilder().apply { liquidLegionsV1Builder.apply { role = r } }.build()
  }
}
