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

package org.wfanet.measurement.duchy.service.internal.computation

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.mock
import java.time.Clock
import kotlin.test.assertFailsWith
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationDb
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.system.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub

@RunWith(JUnit4::class)
@ExperimentalCoroutinesApi
class ComputationsServiceTest {

  companion object {

    val primaryComputationDetails = ComputationDetails.newBuilder().apply {
      liquidLegionsV1Builder.apply {
        role = LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation.PRIMARY
        primaryNodeId = "BOHEMIA"
        incomingNodeId = "SALZBURG"
        outgoingNodeId = "AUSTRIA"
      }
    }.build()

    val secondComputationDetails = ComputationDetails.newBuilder().apply {
      liquidLegionsV1Builder.apply {
        role = LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation.SECONDARY
        primaryNodeId = "BOHEMIA"
        incomingNodeId = "SALZBURG"
        outgoingNodeId = "BOHEMIA"
      }
    }.build()

    val aggregatorComputationDetails = ComputationDetails.newBuilder().apply {
      liquidLegionsV2Builder.apply {
        role = LiquidLegionsSketchAggregationV2.ComputationDetails.RoleInComputation.AGGREGATOR
        aggregatorNodeId = "BOHEMIA"
        incomingNodeId = "SALZBURG"
        outgoingNodeId = "AUSTRIA"
      }
    }.build()
  }

  private val fakeDatabase = FakeComputationDb()
  private val mockGlobalComputations: GlobalComputationsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(mockGlobalComputations)
  }

  private val fakeService: ComputationsService by lazy {
    ComputationsService(
      fakeDatabase,
      GlobalComputationsCoroutineStub(grpcTestServerRule.channel),
      "BOHEMIA",
      Clock.systemUTC()
    )
  }

  @Test
  fun `update computationDetails successfully`() = runBlocking {
    val id = "1234"
    fakeDatabase.addComputation(
      id,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      aggregatorComputationDetails,
      listOf()
    )
    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token
    val newComputationDetails = aggregatorComputationDetails.toBuilder().apply {
      liquidLegionsV2Builder.reachEstimateBuilder.reach = 123
    }.build()
    val request = UpdateComputationDetailsRequest.newBuilder().apply {
      token = tokenAtStart
      details = newComputationDetails
    }.build()

    assertThat(fakeService.updateComputationDetails(request))
      .isEqualTo(
        tokenAtStart.toBuilder().apply {
          version = 1
          computationDetails = newComputationDetails
        }.build().toUpdateComputationDetailsResponse()
      )
  }

  @Test
  fun `update computationDetails, protocol doesn't match should fail`() = runBlocking<Unit> {
    val id = "1234"
    fakeDatabase.addComputation(
      id,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      aggregatorComputationDetails,
      listOf()
    )

    val request = UpdateComputationDetailsRequest.newBuilder().apply {
      token = fakeService.getComputationToken(id.toGetTokenRequest()).token
      details = primaryComputationDetails
    }.build()
    assertFailsWith<IllegalArgumentException> {
      fakeService.updateComputationDetails(request)
    }
  }

  @Test
  fun `end failed computation`() = runBlocking {
    val id = "1234"
    fakeDatabase.addComputation(
      id,
      LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage(),
      primaryComputationDetails,
      listOf()
    )
    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token
    val request = FinishComputationRequest.newBuilder().apply {
      token = tokenAtStart
      endingComputationStage = LiquidLegionsSketchAggregationV1.Stage.COMPLETED.toProtocolStage()
      reason = ComputationDetails.CompletedReason.FAILED
    }.build()

    assertThat(fakeService.finishComputation(request))
      .isEqualTo(
        tokenAtStart.toBuilder().clearStageSpecificDetails().apply {
          version = 1
          computationStage = LiquidLegionsSketchAggregationV1.Stage.COMPLETED.toProtocolStage()
        }.build().toFinishComputationResponse()
      )

    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::createGlobalComputationStatusUpdate
    ).comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
          parentBuilder.globalComputationId = id
          statusUpdateBuilder.apply {
            selfReportedIdentifier = "BOHEMIA"
            stageDetailsBuilder.apply {
              algorithm = GlobalComputationStatusUpdate.MpcAlgorithm.LIQUID_LEGIONS_V1
              stageNumber = LiquidLegionsSketchAggregationV1.Stage.COMPLETED.number.toLong()
              stageName = LiquidLegionsSketchAggregationV1.Stage.COMPLETED.name
              attemptNumber = 0
            }
            updateMessage = "Computation $id at stage COMPLETED, attempt 0"
          }
        }
          .build()
      )
  }

  @Test
  fun `write reference to output blob and advance stage`() = runBlocking {
    val id = "67890"
    fakeDatabase.addComputation(
      id,
      LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS.toProtocolStage(),
      secondComputationDetails,
      listOf(
        newInputBlobMetadata(id = 0L, key = "an_input_blob"),
        newEmptyOutputBlobMetadata(id = 1L)
      )
    )
    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token

    val tokenAfterRecordingBlob =
      fakeService.recordOutputBlobPath(
        RecordOutputBlobPathRequest.newBuilder().apply {
          token = tokenAtStart
          outputBlobId = 1L
          blobPath = "the_writen_output_blob"
        }.build()
      ).token

    val request = AdvanceComputationStageRequest.newBuilder().apply {
      token = tokenAfterRecordingBlob
      nextComputationStage =
        LiquidLegionsSketchAggregationV1.Stage.WAIT_FLAG_COUNTS.toProtocolStage()
      addAllInputBlobs(listOf("inputs_to_new_stage"))
      outputBlobs = 1
      afterTransition = AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE
    }.build()

    assertThat(fakeService.advanceComputationStage(request))
      .isEqualTo(
        tokenAtStart.toBuilder().clearBlobs().clearStageSpecificDetails().apply {
          version = 2
          attempt = 1
          computationStage =
            LiquidLegionsSketchAggregationV1.Stage.WAIT_FLAG_COUNTS.toProtocolStage()
          addBlobs(newInputBlobMetadata(id = 0L, key = "inputs_to_new_stage"))
          addBlobs(newEmptyOutputBlobMetadata(id = 1L))
        }.build().toAdvanceComputationStageResponse()
      )

    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::createGlobalComputationStatusUpdate
    ).comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
          parentBuilder.globalComputationId = id
          statusUpdateBuilder.apply {
            selfReportedIdentifier = "BOHEMIA"
            stageDetailsBuilder.apply {
              algorithm = GlobalComputationStatusUpdate.MpcAlgorithm.LIQUID_LEGIONS_V1
              stageNumber = LiquidLegionsSketchAggregationV1.Stage.WAIT_FLAG_COUNTS.number.toLong()
              stageName = LiquidLegionsSketchAggregationV1.Stage.WAIT_FLAG_COUNTS.name
              attemptNumber = 0
            }
            updateMessage = "Computation $id at stage WAIT_FLAG_COUNTS, attempt 0"
          }
        }
          .build()
      )
  }

  @Test
  fun `get computation ids`() = runBlocking {
    val blindId = "67890"
    val completedId = "12341"
    val decryptId = "4342242"
    fakeDatabase.addComputation(
      blindId,
      LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS.toProtocolStage(),
      secondComputationDetails,
      listOf()
    )
    fakeDatabase.addComputation(
      completedId,
      LiquidLegionsSketchAggregationV1.Stage.COMPLETED.toProtocolStage(),
      secondComputationDetails,
      listOf()
    )
    fakeDatabase.addComputation(
      decryptId,
      LiquidLegionsSketchAggregationV1.Stage.TO_DECRYPT_FLAG_COUNTS.toProtocolStage(),
      secondComputationDetails,
      listOf()
    )
    val getIdsInMillStagesRequest = GetComputationIdsRequest.newBuilder().apply {
      addAllStages(
        setOf(
          LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS.toProtocolStage(),
          LiquidLegionsSketchAggregationV1.Stage.TO_DECRYPT_FLAG_COUNTS.toProtocolStage()
        )
      )
    }.build()
    assertThat(fakeService.getComputationIds(getIdsInMillStagesRequest))
      .isEqualTo(
        GetComputationIdsResponse.newBuilder().apply {
          addAllGlobalIds(setOf(blindId, decryptId))
        }.build()
      )
  }

  @Test
  fun `claim task`() = runBlocking {
    val unclaimed = "12345678"
    val claimed = "23456789"
    fakeDatabase.addComputation(
      unclaimed,
      LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS.toProtocolStage(),
      secondComputationDetails,
      listOf()
    )
    val unclaimedAtStart =
      fakeService.getComputationToken(unclaimed.toGetTokenRequest()).token
    fakeDatabase.addComputation(
      claimed,
      LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS.toProtocolStage(),
      secondComputationDetails,
      listOf()
    )
    fakeDatabase.claimedComputationIds.add(claimed)
    val claimedAtStart =
      fakeService.getComputationToken(claimed.toGetTokenRequest()).token
    val owner = "TheOwner"
    val request = ClaimWorkRequest.newBuilder()
      .setComputationType(ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1)
      .setOwner(owner)
      .build()
    assertThat(fakeService.claimWork(request))
      .isEqualTo(
        unclaimedAtStart.toBuilder().setVersion(1).setAttempt(1).build()
          .toClaimWorkResponse()
      )
    assertThat(fakeService.claimWork(request)).isEqualToDefaultInstance()
    assertThat(fakeService.getComputationToken(claimed.toGetTokenRequest()))
      .isEqualTo(claimedAtStart.toGetComputationTokenResponse())

    verifyProtoArgument(
      mockGlobalComputations,
      GlobalComputationsCoroutineImplBase::createGlobalComputationStatusUpdate
    ).comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
          parentBuilder.globalComputationId = unclaimed
          statusUpdateBuilder.apply {
            selfReportedIdentifier = "BOHEMIA"
            stageDetailsBuilder.apply {
              algorithm = GlobalComputationStatusUpdate.MpcAlgorithm.LIQUID_LEGIONS_V1
              stageNumber =
                LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS.number.toLong()
              stageName = LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS.name
              attemptNumber = 1
            }
            updateMessage = "Computation $unclaimed at stage TO_BLIND_POSITIONS, attempt 1"
          }
        }
          .build()
      )
  }
}
