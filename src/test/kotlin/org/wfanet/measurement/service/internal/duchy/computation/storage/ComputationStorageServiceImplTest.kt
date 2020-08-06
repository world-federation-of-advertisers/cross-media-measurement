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

package org.wfanet.measurement.service.internal.duchy.computation.storage

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.duchy.testing.FakeComputationStorage
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
@ExperimentalCoroutinesApi
class ComputationStorageServiceImplTest {
  private val fakeDatabase = FakeComputationStorage(listOf("A", "B", "C", "D"))

  private val fakeService = ComputationStorageServiceImpl(fakeDatabase)

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { listOf(fakeService) }

  lateinit var client: ComputationStorageServiceCoroutineStub

  @Before
  fun setup() {
    client = ComputationStorageServiceCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `end failed computation`() = runBlocking {
    val id = 1234L
    fakeDatabase.addComputation(
      id,
      SketchAggregationStage.WAIT_SKETCHES.toProtocolStage(),
      RoleInComputation.PRIMARY,
      listOf()
    )
    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token
    val request = FinishComputationRequest.newBuilder().apply {
      token = tokenAtStart
      endingComputationStage = SketchAggregationStage.COMPLETED.toProtocolStage()
      reason = ComputationDetails.CompletedReason.FAILED
    }.build()

    assertThat(fakeService.finishComputation(request))
      .isEqualTo(
        tokenAtStart.toBuilder().clearStageSpecificDetails().apply {
          version = 1
          computationStage = SketchAggregationStage.COMPLETED.toProtocolStage()
        }.build().toFinishComputationResponse()
      )
  }

  @Test
  fun `write reference to output blob and advance stage`() = runBlocking {
    val id = 67890L
    fakeDatabase.addComputation(
      id, SketchAggregationStage.TO_BLIND_POSITIONS.toProtocolStage(), RoleInComputation.SECONDARY,
      listOf(
        newInputBlobMetadata(id = 0L, key = "an_input_blob"),
        newEmptyOutputBlobMetadata(id = 1L)
      )
    )
    val tokenAtStart = fakeService.getComputationToken(id.toGetTokenRequest()).token

    val tokenAfterRecordingBlob =
      client.recordOutputBlobPath(
        RecordOutputBlobPathRequest.newBuilder().apply {
          token = tokenAtStart
          outputBlobId = 1L
          blobPath = "the_writen_output_blob"
        }.build()
      ).token

    val request = AdvanceComputationStageRequest.newBuilder().apply {
      token = tokenAfterRecordingBlob
      nextComputationStage = SketchAggregationStage.WAIT_FLAG_COUNTS.toProtocolStage()
      addAllInputBlobs(listOf("inputs_to_new_stage"))
      outputBlobs = 1
      afterTransition = AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE
    }.build()

    assertThat(fakeService.advanceComputationStage(request))
      .isEqualTo(
        tokenAtStart.toBuilder().clearBlobs().clearStageSpecificDetails().apply {
          version = 2
          attempt = 1
          computationStage = SketchAggregationStage.WAIT_FLAG_COUNTS.toProtocolStage()
          addBlobs(newInputBlobMetadata(id = 0L, key = "inputs_to_new_stage"))
          addBlobs(newEmptyOutputBlobMetadata(id = 1L))
        }.build().toAdvanceComputationStageResponse()
      )
  }

  @Test
  fun `get computation ids`() = runBlocking {
    val blindId = 67890L
    val completedId = 12341L
    val decryptId = 4342242L
    fakeDatabase.addComputation(
      blindId,
      SketchAggregationStage.TO_BLIND_POSITIONS.toProtocolStage(),
      RoleInComputation.SECONDARY,
      listOf()
    )
    fakeDatabase.addComputation(
      completedId,
      SketchAggregationStage.COMPLETED.toProtocolStage(),
      RoleInComputation.SECONDARY,
      listOf()
    )
    fakeDatabase.addComputation(
      decryptId,
      SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS.toProtocolStage(),
      RoleInComputation.SECONDARY,
      listOf()
    )
    val getIdsInMillStagesRequest = GetComputationIdsRequest.newBuilder().apply {
      addAllStages(
        setOf(
          SketchAggregationStage.TO_BLIND_POSITIONS.toProtocolStage(),
          SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS.toProtocolStage()
        )
      )
    }.build()
    assertThat(client.getComputationIds(getIdsInMillStagesRequest))
      .isEqualTo(
        GetComputationIdsResponse.newBuilder().apply {
          addAllGlobalIds(setOf(blindId, decryptId))
        }.build()
      )
  }

  @Test
  fun `claim task`() = runBlocking {
    val unclaimed = 12345678L
    val claimed = 23456789L
    fakeDatabase.addComputation(
      unclaimed,
      SketchAggregationStage.TO_BLIND_POSITIONS.toProtocolStage(),
      RoleInComputation.SECONDARY,
      listOf()
    )
    val unclaimedAtStart =
      fakeService.getComputationToken(unclaimed.toGetTokenRequest()).token
    fakeDatabase.addComputation(
      claimed,
      SketchAggregationStage.TO_BLIND_POSITIONS.toProtocolStage(),
      RoleInComputation.SECONDARY,
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
  }
}
