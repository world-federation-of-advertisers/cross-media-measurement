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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.duchy.db.computation.ExternalRequisitionKey
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.RecordRequisitionBlobPathRequest
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig.RoleInComputation
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.system.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputationStatusUpdate
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineImplBase
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub

@RunWith(JUnit4::class)
@ExperimentalCoroutinesApi
class ComputationsServiceTest {

  companion object {
    val aggregatorComputationDetails =
      ComputationDetails.newBuilder()
        .apply { liquidLegionsV2Builder.apply { role = RoleInComputation.AGGREGATOR } }
        .build()

    val nonAggregatorComputationDetails =
      ComputationDetails.newBuilder()
        .apply { liquidLegionsV2Builder.apply { role = RoleInComputation.NON_AGGREGATOR } }
        .build()
  }

  private val fakeDatabase = FakeComputationsDatabase()
  private val mockGlobalComputations: GlobalComputationsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(mockGlobalComputations) }

  private val fakeService: ComputationsService by lazy {
    ComputationsService(
      fakeDatabase,
      GlobalComputationsCoroutineStub(grpcTestServerRule.channel),
      "BOHEMIA",
      Clock.systemUTC()
    )
  }

  @Test
  fun `get computation token`() = runBlocking {
    val id = "1234"
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = aggregatorComputationDetails,
      requisitions = listOf(ExternalRequisitionKey("edp1", "1234"))
    )

    val expectedToken =
      ComputationToken.newBuilder()
        .apply {
          localComputationId = 1234
          globalComputationId = "1234"
          computationStageBuilder.liquidLegionsSketchAggregationV2 =
            LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE
          computationDetails = aggregatorComputationDetails
          addRequisitionsBuilder().apply {
            externalDataProviderId = "edp1"
            externalRequisitionId = "1234"
          }
        }
        .build()

    assertThat(fakeService.getComputationToken(id.toGetTokenRequest()))
      .isEqualTo(expectedToken.toGetComputationTokenResponse())
    assertThat(fakeService.getComputationToken(id.toGetTokenRequest()))
      .isEqualTo(expectedToken.toGetComputationTokenResponse())
  }

  @Test
  fun `update computationDetails successfully`() = runBlocking {
    val id = "1234"
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = aggregatorComputationDetails
    )
    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token
    val newComputationDetails =
      aggregatorComputationDetails
        .toBuilder()
        .apply { liquidLegionsV2Builder.reachEstimateBuilder.reach = 123 }
        .build()
    val request =
      UpdateComputationDetailsRequest.newBuilder()
        .apply {
          token = tokenAtStart
          details = newComputationDetails
        }
        .build()

    assertThat(fakeService.updateComputationDetails(request))
      .isEqualTo(
        tokenAtStart
          .toBuilder()
          .apply {
            version = 1
            computationDetails = newComputationDetails
          }
          .build()
          .toUpdateComputationDetailsResponse()
      )
  }

  @Test
  fun `update computations details and requisition details`() = runBlocking {
    val id = "1234"
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = aggregatorComputationDetails,
      requisitions =
        listOf(ExternalRequisitionKey("edp1", "1234"), ExternalRequisitionKey("edp2", "5678"))
    )
    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token
    val newComputationDetails =
      aggregatorComputationDetails
        .toBuilder()
        .apply { liquidLegionsV2Builder.reachEstimateBuilder.reach = 123 }
        .build()
    val requisitionDetails1 =
      RequisitionDetails.newBuilder().apply { externalFulfillingDuchyId = "duchy-1" }.build()
    val requisitionDetails2 =
      RequisitionDetails.newBuilder().apply { externalFulfillingDuchyId = "duchy-2" }.build()
    val request =
      UpdateComputationDetailsRequest.newBuilder()
        .apply {
          token = tokenAtStart
          details = newComputationDetails
          addRequisitionDetailUpdatesBuilder().apply {
            externalDataProviderId = "edp1"
            externalRequisitionId = "1234"
            details = requisitionDetails1
          }
          addRequisitionDetailUpdatesBuilder().apply {
            externalDataProviderId = "edp2"
            externalRequisitionId = "5678"
            details = requisitionDetails2
          }
        }
        .build()

    assertThat(fakeService.updateComputationDetails(request))
      .isEqualTo(
        tokenAtStart
          .toBuilder()
          .clearRequisitions()
          .apply {
            version = 1
            computationDetails = newComputationDetails
            addRequisitionsBuilder().apply {
              externalDataProviderId = "edp1"
              externalRequisitionId = "1234"
              details = requisitionDetails1
            }
            addRequisitionsBuilder().apply {
              externalDataProviderId = "edp2"
              externalRequisitionId = "5678"
              details = requisitionDetails2
            }
          }
          .build()
          .toUpdateComputationDetailsResponse()
      )
  }

  @Test
  fun `end failed computation`() = runBlocking {
    val id = "1234"
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
      computationDetails = aggregatorComputationDetails
    )
    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token
    val request =
      FinishComputationRequest.newBuilder()
        .apply {
          token = tokenAtStart
          endingComputationStage = LiquidLegionsSketchAggregationV2.Stage.COMPLETE.toProtocolStage()
          reason = ComputationDetails.CompletedReason.FAILED
        }
        .build()

    assertThat(fakeService.finishComputation(request))
      .isEqualTo(
        tokenAtStart
          .toBuilder()
          .clearStageSpecificDetails()
          .apply {
            version = 1
            computationStage = LiquidLegionsSketchAggregationV2.Stage.COMPLETE.toProtocolStage()
          }
          .build()
          .toFinishComputationResponse()
      )

    verifyProtoArgument(
        mockGlobalComputations,
        GlobalComputationsCoroutineImplBase::createGlobalComputationStatusUpdate
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateGlobalComputationStatusUpdateRequest.newBuilder()
          .apply {
            parentBuilder.globalComputationId = id
            statusUpdateBuilder.apply {
              selfReportedIdentifier = "BOHEMIA"
              stageDetailsBuilder.apply {
                algorithm = GlobalComputationStatusUpdate.MpcAlgorithm.LIQUID_LEGIONS_V2
                stageNumber = LiquidLegionsSketchAggregationV2.Stage.COMPLETE.number.toLong()
                stageName = LiquidLegionsSketchAggregationV2.Stage.COMPLETE.name
                attemptNumber = 0
              }
              updateMessage = "Computation $id at stage COMPLETE, attempt 0"
            }
          }
          .build()
      )
  }

  @Test
  fun `write reference to output blob and advance stage`() = runBlocking {
    val id = "67890"
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = nonAggregatorComputationDetails,
      blobs =
        listOf(
          newInputBlobMetadata(id = 0L, key = "an_input_blob"),
          newEmptyOutputBlobMetadata(id = 1L)
        )
    )
    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token

    val tokenAfterRecordingBlob =
      fakeService.recordOutputBlobPath(
          RecordOutputBlobPathRequest.newBuilder()
            .apply {
              token = tokenAtStart
              outputBlobId = 1L
              blobPath = "the_writen_output_blob"
            }
            .build()
        )
        .token

    val request =
      AdvanceComputationStageRequest.newBuilder()
        .apply {
          token = tokenAfterRecordingBlob
          nextComputationStage =
            LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
          addAllInputBlobs(listOf("inputs_to_new_stage"))
          outputBlobs = 1
          afterTransition = AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE
        }
        .build()

    assertThat(fakeService.advanceComputationStage(request))
      .isEqualTo(
        tokenAtStart
          .toBuilder()
          .clearBlobs()
          .clearStageSpecificDetails()
          .apply {
            version = 2
            attempt = 1
            computationStage =
              LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS
                .toProtocolStage()
            addBlobs(newInputBlobMetadata(id = 0L, key = "inputs_to_new_stage"))
            addBlobs(newEmptyOutputBlobMetadata(id = 1L))
          }
          .build()
          .toAdvanceComputationStageResponse()
      )

    verifyProtoArgument(
        mockGlobalComputations,
        GlobalComputationsCoroutineImplBase::createGlobalComputationStatusUpdate
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateGlobalComputationStatusUpdateRequest.newBuilder()
          .apply {
            parentBuilder.globalComputationId = id
            statusUpdateBuilder.apply {
              selfReportedIdentifier = "BOHEMIA"
              stageDetailsBuilder.apply {
                algorithm = GlobalComputationStatusUpdate.MpcAlgorithm.LIQUID_LEGIONS_V2
                stageNumber =
                  LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.number
                    .toLong()
                stageName =
                  LiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.name
                attemptNumber = 0
              }
              updateMessage = "Computation $id at stage WAIT_EXECUTION_PHASE_TWO_INPUTS, attempt 0"
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
      globalId = blindId,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = nonAggregatorComputationDetails
    )
    fakeDatabase.addComputation(
      completedId,
      LiquidLegionsSketchAggregationV2.Stage.COMPLETE.toProtocolStage(),
      nonAggregatorComputationDetails,
      listOf()
    )
    fakeDatabase.addComputation(
      decryptId,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE.toProtocolStage(),
      nonAggregatorComputationDetails,
      listOf()
    )
    val getIdsInMillStagesRequest =
      GetComputationIdsRequest.newBuilder()
        .apply {
          addAllStages(
            setOf(
              LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
              LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_THREE.toProtocolStage()
            )
          )
        }
        .build()
    assertThat(fakeService.getComputationIds(getIdsInMillStagesRequest))
      .isEqualTo(
        GetComputationIdsResponse.newBuilder()
          .apply { addAllGlobalIds(setOf(blindId, decryptId)) }
          .build()
      )
  }

  @Test
  fun `claim task`() = runBlocking {
    val unclaimed = "12345678"
    val claimed = "23456789"
    fakeDatabase.addComputation(
      globalId = unclaimed,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = nonAggregatorComputationDetails
    )
    val unclaimedAtStart = fakeService.getComputationToken(unclaimed.toGetTokenRequest()).token
    fakeDatabase.addComputation(
      claimed,
      LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      nonAggregatorComputationDetails,
      listOf()
    )
    fakeDatabase.claimedComputationIds.add(claimed)
    val claimedAtStart = fakeService.getComputationToken(claimed.toGetTokenRequest()).token
    val owner = "TheOwner"
    val request =
      ClaimWorkRequest.newBuilder()
        .setComputationType(ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2)
        .setOwner(owner)
        .build()
    assertThat(fakeService.claimWork(request))
      .isEqualTo(
        unclaimedAtStart.toBuilder().setVersion(1).setAttempt(1).build().toClaimWorkResponse()
      )
    assertThat(fakeService.claimWork(request)).isEqualToDefaultInstance()
    assertThat(fakeService.getComputationToken(claimed.toGetTokenRequest()))
      .isEqualTo(claimedAtStart.toGetComputationTokenResponse())

    verifyProtoArgument(
        mockGlobalComputations,
        GlobalComputationsCoroutineImplBase::createGlobalComputationStatusUpdate
      )
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        CreateGlobalComputationStatusUpdateRequest.newBuilder()
          .apply {
            parentBuilder.globalComputationId = unclaimed
            statusUpdateBuilder.apply {
              selfReportedIdentifier = "BOHEMIA"
              stageDetailsBuilder.apply {
                algorithm = GlobalComputationStatusUpdate.MpcAlgorithm.LIQUID_LEGIONS_V2
                stageNumber =
                  LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.number.toLong()
                stageName = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.name
                attemptNumber = 1
              }
              updateMessage = "Computation $unclaimed at stage EXECUTION_PHASE_ONE, attempt 1"
            }
          }
          .build()
      )
  }

  @Test
  fun `record requisition blob path`() = runBlocking {
    val id = "1234"
    fakeDatabase.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
      computationDetails = aggregatorComputationDetails,
      requisitions = listOf(ExternalRequisitionKey("edp1", "1234"))
    )

    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token

    val request =
      RecordRequisitionBlobPathRequest.newBuilder()
        .apply {
          token = tokenAtStart
          keyBuilder.apply {
            externalDataProviderId = "edp1"
            externalRequisitionId = "1234"
          }
          blobPath = "this is a new path"
        }
        .build()

    assertThat(fakeService.recordRequisitionBlobPath(request))
      .isEqualTo(
        tokenAtStart
          .toBuilder()
          .clearRequisitions()
          .apply {
            version = 1
            addRequisitionsBuilder().apply {
              externalDataProviderId = "edp1"
              externalRequisitionId = "1234"
              path = "this is a new path"
            }
          }
          .build()
          .toRecordRequisitionBlobPathResponse()
      )
  }
}
