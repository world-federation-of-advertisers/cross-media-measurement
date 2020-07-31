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

package org.wfanet.measurement.db.duchy.gcp

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.duchy.SketchAggregationStageDetails
import org.wfanet.measurement.db.duchy.SketchAggregationStages
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.service.internal.duchy.computation.storage.newEmptyOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.newInputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.newOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage
import java.time.Instant

@RunWith(JUnit4::class)
class GcpSpannerReadOnlyComputationsRelationalDbTest :
  UsingSpannerEmulator("/src/main/db/gcp/computations.sdl") {

  companion object {
    private const val NEXT_DUCHY_IN_RING = "NEXT_DUCHY_IN_RING"
    private const val THIRD_DUCHY_IN_RING = "THIRD_DUCHY_IN_RING"
    val DETAILS_WHEN_SECONDARY: ComputationDetails = ComputationDetails.newBuilder().apply {
      outgoingNodeId = NEXT_DUCHY_IN_RING
      role = ComputationDetails.RoleInComputation.SECONDARY
    }.build()
    val DETAILS_WHEN_PRIMARY: ComputationDetails = ComputationDetails.newBuilder().apply {
      outgoingNodeId = NEXT_DUCHY_IN_RING
      role = ComputationDetails.RoleInComputation.PRIMARY
    }.build()
  }

  private val computationMutations =
    ComputationMutations(
      SketchAggregationStages,
      SketchAggregationStageDetails(listOf(NEXT_DUCHY_IN_RING, THIRD_DUCHY_IN_RING))
    )

  private fun parseLiquidLegionsStageFromLong(value: Long) =
    computationMutations.longToEnum(value).toProtocolStage()

  private fun longFromLiquidLegionsStage(stage: ComputationStage) =
    computationMutations.enumToLong(stage.liquidLegionsSketchAggregation)

  private lateinit var liquidLegionsSketchAggregationSpannerReader:
    GcpSpannerReadOnlyComputationsRelationalDb

  @Before
  fun initDatabase() {
    liquidLegionsSketchAggregationSpannerReader =
      GcpSpannerReadOnlyComputationsRelationalDb(
        databaseClient,
        ::parseLiquidLegionsStageFromLong,
        ::longFromLiquidLegionsStage
      )
  }

  @Test
  fun `readComputationToken wait_sketches`() = runBlocking<Unit> {
    val globalId = 0x777L
    val localId = 0xABCDEFL
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val computationRow = computationMutations.insertComputation(
      localId = localId,
      updateTime = lastUpdated.toGcpTimestamp(),
      globalId = globalId,
      stage = SketchAggregationStage.WAIT_SKETCHES,
      details = DETAILS_WHEN_PRIMARY
    )
    val waitSketchesComputationStageRow = computationMutations.insertComputationStage(
      localId = localId,
      stage = SketchAggregationStage.WAIT_SKETCHES,
      nextAttempt = 2,
      creationTime = lastUpdated.minusSeconds(2).toGcpTimestamp(),
      endTime = lastUpdated.minusMillis(200).toGcpTimestamp(),
      details = computationMutations.detailsFor(SketchAggregationStage.WAIT_SKETCHES)
    )
    val outputBlob1ForWaitSketchesComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = SketchAggregationStage.WAIT_SKETCHES,
        blobId = 0L,
        dependencyType = ComputationBlobDependency.OUTPUT
      )
    val outputBlob2ForWaitSketchesComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = SketchAggregationStage.WAIT_SKETCHES,
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
        computationStage = ComputationStage.newBuilder()
          .setLiquidLegionsSketchAggregation(SketchAggregationStage.WAIT_SKETCHES)
          .build()
        role = DETAILS_WHEN_PRIMARY.role
        nextDuchy = DETAILS_WHEN_PRIMARY.outgoingNodeId
        attempt = 1
        version = lastUpdated.toEpochMilli()
        stageSpecificDetails = computationMutations.detailsFor(SketchAggregationStage.WAIT_SKETCHES)
        addBlobs(newEmptyOutputBlobMetadata(0L))
        addBlobs(newEmptyOutputBlobMetadata(1L))
      }.build()

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(expectedToken)
  }

  @Test
  fun readComputationToken() = runBlocking<Unit> {
    val globalId = 998877665555
    val localId = 100L
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val computationRow = computationMutations.insertComputation(
      localId = localId,
      updateTime = lastUpdated.toGcpTimestamp(),
      globalId = globalId,
      stage = SketchAggregationStage.WAIT_CONCATENATED,
      details = DETAILS_WHEN_SECONDARY
    )
    val toAddNoiseComputationStageRow = computationMutations.insertComputationStage(
      localId = localId,
      stage = SketchAggregationStage.TO_ADD_NOISE,
      nextAttempt = 45,
      creationTime = lastUpdated.minusSeconds(2).toGcpTimestamp(),
      endTime = lastUpdated.minusMillis(200).toGcpTimestamp(),
      details = computationMutations.detailsFor(SketchAggregationStage.TO_ADD_NOISE)
    )
    val outputBlobForToAddNoiseComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = SketchAggregationStage.TO_ADD_NOISE,
        blobId = 0L,
        dependencyType = ComputationBlobDependency.OUTPUT,
        pathToBlob = "blob-key"
      )
    val waitConcatenatedComputationStageRow = computationMutations.insertComputationStage(
      localId = localId,
      stage = SketchAggregationStage.WAIT_CONCATENATED,
      nextAttempt = 2,
      creationTime = lastUpdated.toGcpTimestamp(),
      details = computationMutations.detailsFor(SketchAggregationStage.WAIT_CONCATENATED)
    )
    val inputBlobForWaitConcatenatedComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = SketchAggregationStage.WAIT_CONCATENATED,
        blobId = 33L,
        dependencyType = ComputationBlobDependency.INPUT,
        pathToBlob = "blob-key"
      )
    val outputBlobForWaitConcatenatedComputationStageRow =
      computationMutations.insertComputationBlobReference(
        localId = localId,
        stage = SketchAggregationStage.WAIT_CONCATENATED,
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
          .setLiquidLegionsSketchAggregation(SketchAggregationStage.WAIT_CONCATENATED)
          .build()
        role = DETAILS_WHEN_SECONDARY.role
        nextDuchy = DETAILS_WHEN_SECONDARY.outgoingNodeId
        attempt = 1
        version = lastUpdated.toEpochMilli()
        stageSpecificDetails =
          computationMutations.detailsFor(SketchAggregationStage.WAIT_CONCATENATED)
        addBlobs(newInputBlobMetadata(33L, "blob-key"))
        addBlobs(newEmptyOutputBlobMetadata(44L))
      }.build()

    assertThat(liquidLegionsSketchAggregationSpannerReader.readComputationToken(globalId))
      .isEqualTo(expectedTokenWhenOutputNotWritten)

    val writenOutputBlobForWaitConcatenatedComputationStageRow =
      computationMutations.updateComputationBlobReference(
        localId = localId,
        stage = SketchAggregationStage.WAIT_CONCATENATED,
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
  fun `readGlobalComputationIds by stage`() = runBlocking<Unit> {
    val lastUpdatedTimeStamp = Instant.ofEpochMilli(12345678910L).toGcpTimestamp()
    val toAddNoiseRow = computationMutations.insertComputation(
      localId = 123,
      updateTime = lastUpdatedTimeStamp,
      stage = SketchAggregationStage.TO_ADD_NOISE,
      globalId = 0xA,
      details = DETAILS_WHEN_SECONDARY
    )
    val toBlindPositionsRow = computationMutations.insertComputation(
      localId = 234,
      updateTime = lastUpdatedTimeStamp,
      stage = SketchAggregationStage.TO_BLIND_POSITIONS,
      globalId = 0xB,
      details = DETAILS_WHEN_SECONDARY
    )
    val toAppendAndAddNoiseRow = computationMutations.insertComputation(
      localId = 345,
      updateTime = lastUpdatedTimeStamp,
      stage = SketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE,
      globalId = 0xC,
      details = DETAILS_WHEN_PRIMARY
    )
    val completedRow = computationMutations.insertComputation(
      localId = 456,
      updateTime = lastUpdatedTimeStamp,
      stage = SketchAggregationStage.COMPLETED,
      globalId = 0xD,
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
          SketchAggregationStage.TO_ADD_NOISE.toProtocolStage(),
          SketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE.toProtocolStage()
        )
      )
    ).containsExactly(0xAL, 0xCL)
  }
}
