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
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.COMPUTATIONS_SCHEMA
import org.wfanet.measurement.duchy.service.internal.computation.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newOutputBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage

@RunWith(JUnit4::class)
class GcpSpannerComputationsDatabaseReaderTest : UsingSpannerEmulator(COMPUTATIONS_SCHEMA) {

  companion object {
    private const val NEXT_DUCHY_IN_RING = "NEXT_DUCHY_IN_RING"
    private const val THIRD_DUCHY_IN_RING = "THIRD_DUCHY_IN_RING"
    val DETAILS_WHEN_NON_AGGREGATOR: ComputationDetails =
      ComputationDetails.newBuilder()
        .apply {
          liquidLegionsV2Builder.apply {
            role = LiquidLegionsV2SetupConfig.RoleInComputation.NON_AGGREGATOR
          }
        }
        .build()
    val DETAILS_WHEN_AGGREGATOR: ComputationDetails =
      ComputationDetails.newBuilder()
        .apply {
          liquidLegionsV2Builder.apply {
            role = LiquidLegionsV2SetupConfig.RoleInComputation.AGGREGATOR
          }
        }
        .build()
  }

  private val computationMutations =
    ComputationMutations(
      ComputationTypes,
      ComputationProtocolStages,
      ComputationProtocolStageDetails(listOf(NEXT_DUCHY_IN_RING, THIRD_DUCHY_IN_RING))
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
        updateTime = lastUpdated.toGcloudTimestamp(),
        globalId = globalId,
        protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
        details = DETAILS_WHEN_AGGREGATOR
      )
    val waitSetupPhaseInputComputationStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
        nextAttempt = 2,
        creationTime = lastUpdated.minusSeconds(2).toGcloudTimestamp(),
        endTime = lastUpdated.minusMillis(200).toGcloudTimestamp(),
        details = computationMutations.detailsFor(Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage())
      )
    val outputBlob1ForWaitSetupPhaseInputComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
        blobId = 0L,
        dependencyType = ComputationBlobDependency.OUTPUT
      )
    val outputBlob2ForWaitSetupPhaseInputComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage(),
        blobId = 1L,
        dependencyType = ComputationBlobDependency.OUTPUT
      )

    databaseClient.write(
      listOf(
        computationRow,
        waitSetupPhaseInputComputationStageRow,
        outputBlob1ForWaitSetupPhaseInputComputationStageRow,
        outputBlob2ForWaitSetupPhaseInputComputationStageRow
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
            computationMutations.detailsFor(Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage())
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
    val computationRow =
      computationMutations.insertComputation(
        localId = localId,
        updateTime = lastUpdated.toGcloudTimestamp(),
        globalId = globalId,
        protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        details = DETAILS_WHEN_NON_AGGREGATOR
      )
    val setupPhaseComputationStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = Stage.SETUP_PHASE.toProtocolStage(),
        nextAttempt = 45,
        creationTime = lastUpdated.minusSeconds(2).toGcloudTimestamp(),
        endTime = lastUpdated.minusMillis(200).toGcloudTimestamp(),
        details = computationMutations.detailsFor(Stage.SETUP_PHASE.toProtocolStage())
      )
    val outputBlobForToSetupPhaseComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.SETUP_PHASE.toProtocolStage(),
        blobId = 0L,
        dependencyType = ComputationBlobDependency.OUTPUT,
        pathToBlob = "blob-key"
      )
    val waitExecutionPhaseOneInputStageRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        nextAttempt = 2,
        creationTime = lastUpdated.toGcloudTimestamp(),
        details =
          computationMutations.detailsFor(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage())
      )
    val inputBlobForWaitExecutionPhaseOneInputStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        blobId = 33L,
        dependencyType = ComputationBlobDependency.INPUT,
        pathToBlob = "blob-key"
      )
    val outputBlobForWaitExecutionPhaseOneInputStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        blobId = 44L,
        dependencyType = ComputationBlobDependency.OUTPUT
      )
    databaseClient.write(
      listOf(
        computationRow,
        setupPhaseComputationStageRow,
        outputBlobForToSetupPhaseComputationStageRow,
        waitExecutionPhaseOneInputStageRow,
        inputBlobForWaitExecutionPhaseOneInputStageRow,
        outputBlobForWaitExecutionPhaseOneInputStageRow
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
            computationMutations.detailsFor(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage())
          addBlobs(newInputBlobMetadata(33L, "blob-key"))
          addBlobs(newEmptyOutputBlobMetadata(44L))
        }
        .build()

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(expectedTokenWhenOutputNotWritten)

    val writenOutputBlobForWaitExecutionPhaseOneStageRow =
      computationMutations.updateComputationBlobReference(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        blobId = 44L,
        pathToBlob = "written-output-key"
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
        updateTime = lastUpdated.toGcloudTimestamp(),
        globalId = globalId,
        protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        details = DETAILS_WHEN_NON_AGGREGATOR
      )
    val waitExecutionPhaseOneInputRow =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage(),
        nextAttempt = 2,
        creationTime = lastUpdated.toGcloudTimestamp(),
        details =
          computationMutations.detailsFor(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage())
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
            computationMutations.detailsFor(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage())
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
          updateTime = lastUpdatedTimeStamp,
          protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          stage = Stage.SETUP_PHASE.toProtocolStage(),
          globalId = "A",
          details = DETAILS_WHEN_NON_AGGREGATOR
        )
      val nonAggregatorExecutionPhaseOneRow =
        computationMutations.insertComputation(
          localId = 234,
          updateTime = lastUpdatedTimeStamp,
          protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          stage = Stage.EXECUTION_PHASE_ONE.toProtocolStage(),
          globalId = "B",
          details = DETAILS_WHEN_NON_AGGREGATOR
        )
      val aggregatorSetupPhaseRow =
        computationMutations.insertComputation(
          localId = 345,
          updateTime = lastUpdatedTimeStamp,
          protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          stage = Stage.SETUP_PHASE.toProtocolStage(),
          globalId = "C",
          details = DETAILS_WHEN_AGGREGATOR
        )
      val aggregatorCompletedRow =
        computationMutations.insertComputation(
          localId = 456,
          updateTime = lastUpdatedTimeStamp,
          protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
          stage = Stage.COMPLETE.toProtocolStage(),
          globalId = "D",
          details = DETAILS_WHEN_AGGREGATOR
        )
      databaseClient.write(
        listOf(
          nonAggregatorSetupPhaseRow,
          nonAggregatorExecutionPhaseOneRow,
          aggregatorSetupPhaseRow,
          aggregatorCompletedRow
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
