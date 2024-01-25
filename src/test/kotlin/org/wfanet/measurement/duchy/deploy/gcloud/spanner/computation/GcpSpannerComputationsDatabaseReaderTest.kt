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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newOutputBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage

@RunWith(JUnit4::class)
class GcpSpannerComputationsDatabaseReaderTest :
  UsingSpannerEmulator(Schemata.DUCHY_CHANGELOG_PATH) {

  companion object {
    val DETAILS_WHEN_NON_AGGREGATOR: ComputationDetails =
      ComputationDetails.newBuilder()
        .apply { liquidLegionsV2Builder.apply { role = RoleInComputation.NON_AGGREGATOR } }
        .build()
    val DETAILS_WHEN_AGGREGATOR: ComputationDetails =
      ComputationDetails.newBuilder()
        .apply {
          liquidLegionsV2Builder.apply {
            role = RoleInComputation.AGGREGATOR
            addParticipantBuilder().apply { duchyId = "non_aggregator_1" }
            addParticipantBuilder().apply { duchyId = "non_aggregator_2" }
            addParticipantBuilder().apply { duchyId = "aggregator" }
          }
        }
        .build()
  }

  private val computationMutations =
    ComputationMutations(
      ComputationTypes,
      ComputationProtocolStages,
      ComputationProtocolStageDetails,
    )

  private lateinit var liquidLegionsSketchAggregationSpannerReader:
    GcpSpannerComputationsDatabaseReader

  @Before
  fun initDatabase() {
    liquidLegionsSketchAggregationSpannerReader =
      GcpSpannerComputationsDatabaseReader(databaseClient, ComputationProtocolStages)
  }

  @Test
  fun `readComputationToken wait_sketches`() = runBlocking {
    val globalId = "777"
    val localId = 0xABCDEFL
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val computationRow =
      computationMutations.insertComputation(
        localId = localId,
        creationTime = lastUpdated.toGcloudTimestamp(),
        updateTime = lastUpdated.toGcloudTimestamp(),
        globalId = globalId,
        protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
        details = DETAILS_WHEN_AGGREGATOR,
      )
    val waitSetupPhaseInputComputationStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
        nextAttempt = 2,
        creationTime = lastUpdated.minusSeconds(2).toGcloudTimestamp(),
        endTime = lastUpdated.minusMillis(200).toGcloudTimestamp(),
        details =
          computationMutations.detailsFor(
            Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
            DETAILS_WHEN_AGGREGATOR,
          ),
      )
    val outputBlob1ForWaitSetupPhaseInputComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
        blobId = 0L,
        dependencyType = ComputationBlobDependency.OUTPUT,
      )
    val outputBlob2ForWaitSetupPhaseInputComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
        blobId = 1L,
        dependencyType = ComputationBlobDependency.OUTPUT,
      )

    databaseClient.write(
      listOf(
        computationRow,
        waitSetupPhaseInputComputationStageRow,
        outputBlob1ForWaitSetupPhaseInputComputationStageRow,
        outputBlob2ForWaitSetupPhaseInputComputationStageRow,
      )
    )

    val expectedToken =
      ComputationToken.newBuilder()
        .apply {
          globalComputationId = globalId
          localComputationId = localId
          computationStage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          computationDetails = DETAILS_WHEN_AGGREGATOR
          attempt = 1
          version = lastUpdated.toEpochMilli()
          stageSpecificDetails =
            computationMutations.detailsFor(
              Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
              DETAILS_WHEN_AGGREGATOR,
            )
          addBlobs(newEmptyOutputBlobMetadata(0L))
          addBlobs(newEmptyOutputBlobMetadata(1L))
        }
        .build()

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(expectedToken)
  }

  @Test
  fun readComputationToken() = runBlocking {
    val globalId = "998877665555"
    val localId = 100L
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val requisition1Key = externalRequisitionKey {
      externalRequisitionId = "111"
      requisitionFingerprint = "A".toByteStringUtf8()
    }
    val requisition2Key = externalRequisitionKey {
      externalRequisitionId = "222"
      requisitionFingerprint = "B".toByteStringUtf8()
    }
    val requisition3Key = externalRequisitionKey {
      externalRequisitionId = "333"
      requisitionFingerprint = "B".toByteStringUtf8()
    }
    val computationRow =
      computationMutations.insertComputation(
        localId = localId,
        creationTime = lastUpdated.toGcloudTimestamp(),
        updateTime = lastUpdated.toGcloudTimestamp(),
        globalId = globalId,
        protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        details = DETAILS_WHEN_NON_AGGREGATOR,
      )
    val setupPhaseComputationStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = Stage.SETUP_PHASE.toProtocolStage(),
        nextAttempt = 45,
        creationTime = lastUpdated.minusSeconds(2).toGcloudTimestamp(),
        endTime = lastUpdated.minusMillis(200).toGcloudTimestamp(),
        details =
          computationMutations.detailsFor(
            Stage.SETUP_PHASE.toProtocolStage(),
            DETAILS_WHEN_NON_AGGREGATOR,
          ),
      )
    val outputBlobForToSetupPhaseComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.SETUP_PHASE.toProtocolStage(),
        blobId = 0L,
        dependencyType = ComputationBlobDependency.OUTPUT,
        pathToBlob = "blob-key",
      )
    val waitExecutionPhaseOneInputStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        nextAttempt = 2,
        creationTime = lastUpdated.toGcloudTimestamp(),
        details =
          computationMutations.detailsFor(
            Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
            DETAILS_WHEN_NON_AGGREGATOR,
          ),
      )
    val inputBlobForWaitExecutionPhaseOneInputStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        blobId = 33L,
        dependencyType = ComputationBlobDependency.INPUT,
        pathToBlob = "blob-key",
      )
    val outputBlobForWaitExecutionPhaseOneInputStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        blobId = 44L,
        dependencyType = ComputationBlobDependency.OUTPUT,
      )
    val fulfilledRequisitionRowOne =
      computationMutations.insertRequisition(
        localComputationId = localId,
        requisitionId = 1L,
        externalRequisitionId = requisition1Key.externalRequisitionId,
        requisitionFingerprint = requisition1Key.requisitionFingerprint,
        pathToBlob = "foo/111",
      )
    val fulfilledRequisitionRowTwo =
      computationMutations.insertRequisition(
        localComputationId = localId,
        requisitionId = 2L,
        externalRequisitionId = requisition2Key.externalRequisitionId,
        requisitionFingerprint = requisition3Key.requisitionFingerprint,
        pathToBlob = "foo/222",
      )
    val unfulfilledRequisitionRowOne =
      computationMutations.insertRequisition(
        localComputationId = localId,
        requisitionId = 3L,
        externalRequisitionId = requisition3Key.externalRequisitionId,
        requisitionFingerprint = requisition3Key.requisitionFingerprint,
      )
    databaseClient.write(
      listOf(
        computationRow,
        setupPhaseComputationStageRow,
        outputBlobForToSetupPhaseComputationStageRow,
        waitExecutionPhaseOneInputStageRow,
        inputBlobForWaitExecutionPhaseOneInputStageRow,
        outputBlobForWaitExecutionPhaseOneInputStageRow,
        fulfilledRequisitionRowOne,
        fulfilledRequisitionRowTwo,
        unfulfilledRequisitionRowOne,
      )
    )
    val expectedTokenWhenOutputNotWritten =
      ComputationToken.newBuilder()
        .apply {
          globalComputationId = globalId
          localComputationId = localId
          computationStage =
            ComputationStage.newBuilder()
              .setLiquidLegionsSketchAggregationV2(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS)
              .build()
          computationDetails = DETAILS_WHEN_NON_AGGREGATOR
          attempt = 1
          version = lastUpdated.toEpochMilli()
          stageSpecificDetails =
            computationMutations.detailsFor(
              Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
              DETAILS_WHEN_NON_AGGREGATOR,
            )
          addBlobs(newInputBlobMetadata(33L, "blob-key"))
          addBlobs(newEmptyOutputBlobMetadata(44L))
          addRequisitionsBuilder().apply {
            externalKey = requisition1Key
            path = "foo/111"
            details = RequisitionDetails.getDefaultInstance()
          }
          addRequisitionsBuilder().apply {
            externalKey = requisition2Key
            path = "foo/222"
            details = RequisitionDetails.getDefaultInstance()
          }
          addRequisitionsBuilder().apply {
            externalKey = requisition3Key
            details = RequisitionDetails.getDefaultInstance()
          }
        }
        .build()

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(expectedTokenWhenOutputNotWritten)
    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(requisition1Key))
      .isEqualTo(expectedTokenWhenOutputNotWritten)

    val writenOutputBlobForWaitExecutionPhaseOneStageRow =
      computationMutations.updateComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        blobId = 44L,
        pathToBlob = "written-output-key",
      )
    databaseClient.write(listOf(writenOutputBlobForWaitExecutionPhaseOneStageRow))

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(
        expectedTokenWhenOutputNotWritten
          .toBuilder()
          .clearBlobs()
          .addBlobs(newInputBlobMetadata(33L, "blob-key"))
          .addBlobs(newOutputBlobMetadata(44L, "written-output-key"))
          .build()
      )
  }

  @Test
  fun `readComputationToken no references`() = runBlocking {
    val globalId = "998877665555"
    val localId = 100L
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val computationRow =
      computationMutations.insertComputation(
        localId = localId,
        creationTime = lastUpdated.toGcloudTimestamp(),
        updateTime = lastUpdated.toGcloudTimestamp(),
        globalId = globalId,
        protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        details = DETAILS_WHEN_NON_AGGREGATOR,
      )
    val waitExecutionPhaseOneInputRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        nextAttempt = 2,
        creationTime = lastUpdated.toGcloudTimestamp(),
        details =
          computationMutations.detailsFor(
            Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
            DETAILS_WHEN_NON_AGGREGATOR,
          ),
      )
    databaseClient.write(listOf(computationRow, waitExecutionPhaseOneInputRow))
    val expectedTokenWhenOutputNotWritten =
      ComputationToken.newBuilder()
        .apply {
          globalComputationId = globalId
          localComputationId = localId
          computationStage =
            ComputationStage.newBuilder()
              .setLiquidLegionsSketchAggregationV2(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS)
              .build()
          computationDetails = DETAILS_WHEN_NON_AGGREGATOR
          attempt = 1
          version = lastUpdated.toEpochMilli()
          stageSpecificDetails =
            computationMutations.detailsFor(
              Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
              DETAILS_WHEN_NON_AGGREGATOR,
            )
        }
        .build()

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(expectedTokenWhenOutputNotWritten)
  }

  @Test
  fun `readGlobalComputationIds by stage`() =
    runBlocking<Unit> {
      val lastUpdatedTimeStamp = Instant.ofEpochMilli(12345678910L).toGcloudTimestamp()
      val nonAggregatorSetupPhaseRow =
        computationMutations.insertComputation(
          localId = 123,
          creationTime = lastUpdatedTimeStamp,
          updateTime = lastUpdatedTimeStamp,
          protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          stage = Stage.SETUP_PHASE.toProtocolStage(),
          globalId = "A",
          details = DETAILS_WHEN_NON_AGGREGATOR,
        )
      val nonAggregatorExecutionPhaseOneRow =
        computationMutations.insertComputation(
          localId = 234,
          creationTime = lastUpdatedTimeStamp,
          updateTime = lastUpdatedTimeStamp,
          protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          stage = Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
          globalId = "B",
          details = DETAILS_WHEN_NON_AGGREGATOR,
        )
      val aggregatorSetupPhaseRow =
        computationMutations.insertComputation(
          localId = 345,
          creationTime = lastUpdatedTimeStamp,
          updateTime = lastUpdatedTimeStamp,
          protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          stage = Stage.SETUP_PHASE.toProtocolStage(),
          globalId = "C",
          details = DETAILS_WHEN_AGGREGATOR,
        )
      val aggregatorCompletedRow =
        computationMutations.insertComputation(
          localId = 456,
          creationTime = lastUpdatedTimeStamp,
          updateTime = lastUpdatedTimeStamp,
          protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          stage = Stage.COMPLETE.toProtocolStage(),
          globalId = "D",
          details = DETAILS_WHEN_AGGREGATOR,
        )
      databaseClient.write(
        listOf(
          nonAggregatorSetupPhaseRow,
          nonAggregatorExecutionPhaseOneRow,
          aggregatorSetupPhaseRow,
          aggregatorCompletedRow,
        )
      )

      assertThat(
          liquidLegionsSketchAggregationSpannerReader.readGlobalComputationIds(
            setOf(Stage.SETUP_PHASE.toProtocolStage())
          )
        )
        .containsExactly("A", "C")
    }
}
