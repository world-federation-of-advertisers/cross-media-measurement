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
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1

@RunWith(JUnit4::class)
class GcpSpannerReadOnlyComputationsRelationalDbTest :
  UsingSpannerEmulator(COMPUTATIONS_SCHEMA) {

  companion object {
    private const val NEXT_DUCHY_IN_RING = "NEXT_DUCHY_IN_RING"
    private const val THIRD_DUCHY_IN_RING = "THIRD_DUCHY_IN_RING"
    val DETAILS_WHEN_SECONDARY: ComputationDetails = ComputationDetails.newBuilder().apply {
      liquidLegionsV1Builder.apply {
        outgoingNodeId = NEXT_DUCHY_IN_RING
        role = LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation.SECONDARY
      }
    }.build()
    val DETAILS_WHEN_PRIMARY: ComputationDetails = ComputationDetails.newBuilder().apply {
      liquidLegionsV1Builder.apply {
        outgoingNodeId = NEXT_DUCHY_IN_RING
        role = LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation.PRIMARY
      }
    }.build()
  }

  private val computationMutations =
    ComputationMutations(
      ComputationTypes,
      ComputationProtocolStages,
      ComputationProtocolStageDetails(
        listOf(NEXT_DUCHY_IN_RING, THIRD_DUCHY_IN_RING)
      )
    )

  private lateinit var liquidLegionsSketchAggregationSpannerReader:
    GcpSpannerReadOnlyComputationsRelationalDb

  @Before
  fun initDatabase() {
    liquidLegionsSketchAggregationSpannerReader =
      GcpSpannerReadOnlyComputationsRelationalDb(
        databaseClient,
        ComputationProtocolStages
      )
  }

  @Test
  fun `readComputationToken wait_sketches`() = runBlocking<Unit> {
    val globalId = "777"
    val localId = 0xABCDEFL
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val computationRow = computationMutations.insertComputation(
      localId = localId,
      updateTime = lastUpdated.toGcloudTimestamp(),
      globalId = globalId,
      protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1,
      stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage(),
      details = DETAILS_WHEN_PRIMARY
    )
    val waitSketchesComputationStageRow = computationMutations.insertComputationStage(
      localId = localId,
      stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage(),
      nextAttempt = 2,
      creationTime = lastUpdated.minusSeconds(2).toGcloudTimestamp(),
      endTime = lastUpdated.minusMillis(200).toGcloudTimestamp(),
      details = computationMutations.detailsFor(
        LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage()
      )
    )
    val outputBlob1ForWaitSketchesComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage(),
        blobId = 0L,
        dependencyType = ComputationBlobDependency.OUTPUT
      )
    val outputBlob2ForWaitSketchesComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage(),
        blobId = 1L,
        dependencyType = ComputationBlobDependency.OUTPUT
      )

    databaseClient.write(
      listOf(
        computationRow,
        waitSketchesComputationStageRow,
        outputBlob1ForWaitSketchesComputationStageRow,
        outputBlob2ForWaitSketchesComputationStageRow
      )
    )

    val expectedToken =
      ComputationToken.newBuilder().apply {
        globalComputationId = globalId
        localComputationId = localId
        computationStage = LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage()
        computationDetails = DETAILS_WHEN_PRIMARY
        attempt = 1
        version = lastUpdated.toEpochMilli()
        stageSpecificDetails =
          computationMutations.detailsFor(
            LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage()
          )
        addBlobs(newEmptyOutputBlobMetadata(0L))
        addBlobs(newEmptyOutputBlobMetadata(1L))
      }.build()

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(expectedToken)
  }

  @Test
  fun readComputationToken() = runBlocking<Unit> {
    val globalId = "998877665555"
    val localId = 100L
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val computationRow = computationMutations.insertComputation(
      localId = localId,
      updateTime = lastUpdated.toGcloudTimestamp(),
      globalId = globalId,
      protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1,
      stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage(),
      details = DETAILS_WHEN_SECONDARY
    )
    val toAddNoiseComputationStageRow = computationMutations.insertComputationStage(
      localId = localId,
      stage = LiquidLegionsSketchAggregationV1.Stage.TO_ADD_NOISE.toProtocolStage(),
      nextAttempt = 45,
      creationTime = lastUpdated.minusSeconds(2).toGcloudTimestamp(),
      endTime = lastUpdated.minusMillis(200).toGcloudTimestamp(),
      details = computationMutations.detailsFor(
        LiquidLegionsSketchAggregationV1.Stage.TO_ADD_NOISE.toProtocolStage()
      )
    )
    val outputBlobForToAddNoiseComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = LiquidLegionsSketchAggregationV1.Stage.TO_ADD_NOISE.toProtocolStage(),
        blobId = 0L,
        dependencyType = ComputationBlobDependency.OUTPUT,
        pathToBlob = "blob-key"
      )
    val waitConcatenatedComputationStageRow = computationMutations.insertComputationStage(
      localId = localId,
      stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage(),
      nextAttempt = 2,
      creationTime = lastUpdated.toGcloudTimestamp(),
      details =
        computationMutations.detailsFor(
          LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage()
        )
    )
    val inputBlobForWaitConcatenatedComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage(),
        blobId = 33L,
        dependencyType = ComputationBlobDependency.INPUT,
        pathToBlob = "blob-key"
      )
    val outputBlobForWaitConcatenatedComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage(),
        blobId = 44L,
        dependencyType = ComputationBlobDependency.OUTPUT
      )
    databaseClient.write(
      listOf(
        computationRow,
        toAddNoiseComputationStageRow,
        outputBlobForToAddNoiseComputationStageRow,
        waitConcatenatedComputationStageRow,
        inputBlobForWaitConcatenatedComputationStageRow,
        outputBlobForWaitConcatenatedComputationStageRow
      )
    )
    val expectedTokenWhenOutputNotWritten =
      ComputationToken.newBuilder().apply {
        globalComputationId = globalId
        localComputationId = localId
        computationStage = ComputationStage.newBuilder()
          .setLiquidLegionsSketchAggregationV1(
            LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED
          )
          .build()
        computationDetails = DETAILS_WHEN_SECONDARY
        attempt = 1
        version = lastUpdated.toEpochMilli()
        stageSpecificDetails =
          computationMutations.detailsFor(
            LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage()
          )
        addBlobs(newInputBlobMetadata(33L, "blob-key"))
        addBlobs(newEmptyOutputBlobMetadata(44L))
      }.build()

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(expectedTokenWhenOutputNotWritten)

    val writenOutputBlobForWaitConcatenatedComputationStageRow =
      computationMutations.updateComputationBlobReference(
        localId = localId,
        stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage(),
        blobId = 44L,
        pathToBlob = "written-output-key"
      )
    databaseClient.write(listOf(writenOutputBlobForWaitConcatenatedComputationStageRow))

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(
        expectedTokenWhenOutputNotWritten.toBuilder()
          .clearBlobs()
          .addBlobs(newInputBlobMetadata(33L, "blob-key"))
          .addBlobs(newOutputBlobMetadata(44L, "written-output-key"))
          .build()
      )
  }

  @Test
  fun `readComputationToken no references`() = runBlocking<Unit> {
    val globalId = "998877665555"
    val localId = 100L
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val computationRow = computationMutations.insertComputation(
      localId = localId,
      updateTime = lastUpdated.toGcloudTimestamp(),
      globalId = globalId,
      protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1,
      stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage(),
      details = DETAILS_WHEN_SECONDARY
    )
    val waitConcatenatedComputationStageRow = computationMutations.insertComputationStage(
      localId = localId,
      stage = LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage(),
      nextAttempt = 2,
      creationTime = lastUpdated.toGcloudTimestamp(),
      details =
        computationMutations.detailsFor(
          LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage()
        )
    )
    databaseClient.write(
      listOf(
        computationRow,
        waitConcatenatedComputationStageRow
      )
    )
    val expectedTokenWhenOutputNotWritten =
      ComputationToken.newBuilder().apply {
        globalComputationId = globalId
        localComputationId = localId
        computationStage = ComputationStage.newBuilder()
          .setLiquidLegionsSketchAggregationV1(
            LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED
          )
          .build()
        computationDetails = DETAILS_WHEN_SECONDARY
        attempt = 1
        version = lastUpdated.toEpochMilli()
        stageSpecificDetails =
          computationMutations.detailsFor(
            LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage()
          )
      }.build()

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(expectedTokenWhenOutputNotWritten)
  }

  @Test
  fun `readGlobalComputationIds by stage`() = runBlocking<Unit> {
    val lastUpdatedTimeStamp = Instant.ofEpochMilli(12345678910L).toGcloudTimestamp()
    val toAddNoiseRow = computationMutations.insertComputation(
      localId = 123,
      updateTime = lastUpdatedTimeStamp,
      protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1,
      stage = LiquidLegionsSketchAggregationV1.Stage.TO_ADD_NOISE.toProtocolStage(),
      globalId = "A",
      details = DETAILS_WHEN_SECONDARY
    )
    val toBlindPositionsRow = computationMutations.insertComputation(
      localId = 234,
      updateTime = lastUpdatedTimeStamp,
      protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1,
      stage = LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS.toProtocolStage(),
      globalId = "B",
      details = DETAILS_WHEN_SECONDARY
    )
    val toAppendAndAddNoiseRow = computationMutations.insertComputation(
      localId = 345,
      updateTime = lastUpdatedTimeStamp,
      protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1,
      stage =
        LiquidLegionsSketchAggregationV1.Stage.TO_APPEND_SKETCHES_AND_ADD_NOISE.toProtocolStage(),
      globalId = "C",
      details = DETAILS_WHEN_PRIMARY
    )
    val completedRow = computationMutations.insertComputation(
      localId = 456,
      updateTime = lastUpdatedTimeStamp,
      protocol = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1,
      stage = LiquidLegionsSketchAggregationV1.Stage.COMPLETED.toProtocolStage(),
      globalId = "D",
      details = DETAILS_WHEN_PRIMARY
    )
    databaseClient.write(
      listOf(
        toAddNoiseRow,
        toBlindPositionsRow,
        toAppendAndAddNoiseRow,
        completedRow
      )
    )

    assertThat(
      liquidLegionsSketchAggregationSpannerReader.readGlobalComputationIds(
        setOf(
          LiquidLegionsSketchAggregationV1.Stage.TO_ADD_NOISE.toProtocolStage(),
          LiquidLegionsSketchAggregationV1.Stage.TO_APPEND_SKETCHES_AND_ADD_NOISE.toProtocolStage()
        )
      )
    ).containsExactly("A", "C")
  }
}
