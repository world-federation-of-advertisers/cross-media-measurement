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

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.extensions.proto.ProtoTruth
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.db.duchy.LiquidLegionsSketchAggregationProtocol
import org.wfanet.measurement.db.duchy.SketchAggregationComputationManager
import org.wfanet.measurement.db.duchy.testing.FakeComputationStorage
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.SketchAggregationStage.COMPLETED
import org.wfanet.measurement.internal.SketchAggregationStage.CREATED
import org.wfanet.measurement.internal.SketchAggregationStage.TO_ADD_NOISE
import org.wfanet.measurement.internal.SketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_SKETCHES
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.ComputationStorageServiceImpl
import org.wfanet.measurement.service.internal.duchy.computation.storage.newEmptyOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.toBlobPath
import org.wfanet.measurement.service.internal.duchy.computation.storage.toGetTokenRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import java.math.BigInteger
import java.time.Instant
import kotlin.test.assertEquals

@RunWith(JUnit4::class)
class GcpComputationManagersTest : UsingSpannerEmulator("/src/main/db/gcp/computations.sdl") {

  companion object {
    const val ID_WHERE_ALSACE_IS_NOT_PRIMARY = 0xFEED5L
    const val ID_WHERE_ALSACE_IS_PRIMARY = 0x41324132444
    const val ALSACE = "Alsace"
    const val BAVARIA = "Bavaria"
    const val CARINTHIA = "Carinthia"
    private val duchies = listOf(ALSACE, BAVARIA, CARINTHIA)
    private val publicKeysMap =
      duchies.mapIndexed { idx, name -> name to BigInteger.valueOf(idx.toLong()) }
        .toMap()
  }

  private val fakeService =
    ComputationStorageServiceImpl(
      FakeComputationStorage(duchies.subList(1, 3))
    )

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { listOf(fakeService) }

  @Test
  fun runProtocolAtNonPrimaryWorker() = runBlocking<Unit> {
    val testClock = TestClockWithNamedInstants(Instant.ofEpochMilli(100L))
    val computation = SingleComputationManager(
      newCascadingLegionsSketchAggregationGcpComputationManager(
        ALSACE,
        duchyPublicKeys = publicKeysMap,
        googleCloudStorageOptions = LocalStorageHelper.getOptions(),
        storageBucket = "test-mill-bucket",
        computationStorageServiceChannel = grpcTestServerRule.channel
      ),
      ID_WHERE_ALSACE_IS_NOT_PRIMARY,
      testClock
    )
    val fakeRpcService = computation.FakeRpcService()
    computation.writeOutputs(CREATED)
    computation.gatherLocalSketches()
    computation.enqueue()

    computation.claimWorkFor("some-mill")
    computation.writeOutputs(TO_ADD_NOISE)
    computation.runWaitStage(WAIT_CONCATENATED)

    fakeRpcService.receiveConcatenatedSketchGrpc()

    computation.claimWorkFor("some-other-mill")
    computation.writeOutputs(TO_BLIND_POSITIONS)
    computation.runWaitStage(WAIT_FLAG_COUNTS)

    fakeRpcService.receiveFlagCountsGrpc()

    computation.claimWorkFor("yet-another-mill")
    computation.writeOutputs(TO_DECRYPT_FLAG_COUNTS)
    computation.end(reason = CompletedReason.SUCCEEDED)
  }

  @Test
  fun runProtocolAtPrimaryWorker() = runBlocking<Unit> {
    val testClock = TestClockWithNamedInstants(Instant.ofEpochMilli(100L))
    val computation = SingleComputationManager(
      newCascadingLegionsSketchAggregationGcpComputationManager(
        ALSACE,
        duchyPublicKeys = publicKeysMap,
        googleCloudStorageOptions = LocalStorageHelper.getOptions(),
        storageBucket = "test-mill-bucket",
        computationStorageServiceChannel = grpcTestServerRule.channel
      ),
      ID_WHERE_ALSACE_IS_PRIMARY,
      testClock
    )
    val fakeRpcService = computation.FakeRpcService()
    computation.writeOutputs(CREATED)
    computation.waitForSketches(
      LiquidLegionsSketchAggregationProtocol.EnumStages.Details(duchies.subList(1, 3)).detailsFor(
        WAIT_SKETCHES
      )
    )
    fakeRpcService.receiveSketch(BAVARIA)
    fakeRpcService.receiveSketch(CARINTHIA)

    computation.claimWorkFor("some-mill")
    computation.writeOutputs(TO_APPEND_SKETCHES_AND_ADD_NOISE)
    computation.runWaitStage(WAIT_CONCATENATED)

    fakeRpcService.receiveConcatenatedSketchGrpc()

    computation.claimWorkFor("some-other-mill")
    computation.writeOutputs(TO_BLIND_POSITIONS_AND_JOIN_REGISTERS)
    computation.runWaitStage(WAIT_FLAG_COUNTS)
    fakeRpcService.receiveFlagCountsGrpc()

    computation.claimWorkFor("yet-another-mill")
    computation.writeOutputs(TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS)
    computation.end(reason = CompletedReason.SUCCEEDED)
  }
}

/** Data about a single step of a computation. .*/
data class ComputationStep(
  val token: org.wfanet.measurement.internal.duchy.ComputationToken,
  val inputs: List<ComputationStageBlobMetadata>,
  val outputs: List<ComputationStageBlobMetadata>
)

/**
 * Encapsulates a computation and the operations to run on it for the MPC.
 *
 * The computation is inserted when the object is created.
 *
 * Because it is a view of a single computation, the computation token is saved after each
 * operation.
 */
class SingleComputationManager(
  private val manager: SketchAggregationComputationManager,
  globalId: Long,
  private val testClock: TestClockWithNamedInstants
) {

  private var token: org.wfanet.measurement.internal.duchy.ComputationToken = runBlocking {
    manager.computationStorageClient.createComputation(
      CreateComputationRequest.newBuilder().apply {
        globalComputationId = globalId
        computationType = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
      }.build()
    ).token
  }
  val localId by lazy { token.localComputationId }

  suspend fun writeOutputs(stage: SketchAggregationStage) {
    assertEquals(stage.toProtocolStage(), token.computationStage)
    testClock.tickSeconds(
      "${token.computationStage.liquidLegionsSketchAggregation}_$token.attempt_outputs"
    )
    token.blobsList.filter { it.dependencyType == ComputationBlobDependency.OUTPUT }
      .forEach {
        token =
          manager.computationStorageClient.recordOutputBlobPath(
            RecordOutputBlobPathRequest.newBuilder()
              .setToken(token)
              .setOutputBlobId(it.blobId)
              .setBlobPath(token.toBlobPath("output"))
              .build()
          ).token
      }
  }

  /** Runs an operation and checks the returned token from the operation matches the expected. */
  private fun assertTokenChangesTo(
    expected: org.wfanet.measurement.internal.duchy.ComputationToken,
    run: suspend (ComputationStep) -> org.wfanet.measurement.internal.duchy.ComputationToken
  ) = runBlocking {
    testClock.tickSeconds("${expected.computationStage}_$expected.attempt")
    // Some stages use the inputs to their predecessor as inputs it itself. If the inputs are needed
    // they will be fetched.
    val inputsToCurrentStage = token.blobsList.ofType(ComputationBlobDependency.INPUT)
    val outputsToCurrentStage = token.blobsList.ofType(ComputationBlobDependency.OUTPUT)
    val result = run(ComputationStep(token, inputsToCurrentStage, outputsToCurrentStage))
    ProtoTruth.assertThat(result)
      .isEqualTo(expected.toBuilder().setVersion(token.version + 1).build())
    token = result
  }

  /** Add computation to work queue and verify that it has no owner. */
  suspend fun enqueue() {
    assertTokenChangesTo(token.toBuilder().setAttempt(0).build()) {
      manager.computationStorageClient.enqueueComputation(
        EnqueueComputationRequest.newBuilder().setToken(token).build()
      )
      manager.computationStorageClient
        .getComputationToken(token.globalComputationId.toGetTokenRequest())
        .token
    }
  }

  /** Get computation from work queue and verify it is owned by the [workerId]. */
  suspend fun claimWorkFor(workerId: String) {
    assertTokenChangesTo(token.toBuilder().setAttempt(1).build()) {
      val claimed = manager.computationStorageClient.claimWork(
        ClaimWorkRequest.newBuilder()
          .setOwner(workerId)
          .setComputationType(ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1)
          .build()
      )
      ProtoTruth.assertThat(claimed).isNotEqualToDefaultInstance()
      claimed.tokenList.single()
    }
  }

  inner class FakeRpcService {
    /**
     * Fake receiving a sketch from a [sender], if all sketches have been received
     * set stage to [TO_BLIND_POSITIONS_AND_JOIN_REGISTERS].
     */
    suspend fun receiveSketch(sender: String) {
      val stageDetails = token.stageSpecificDetails.waitSketchStageDetails

      val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[sender]) - 1
      val path = token.toBlobPath("sketch_from_$sender")
      token = manager.computationStorageClient.recordOutputBlobPath(
        RecordOutputBlobPathRequest.newBuilder()
          .setToken(token)
          .setOutputBlobId(blobId)
          .setBlobPath(path)
          .build()
      ).token

      val notWritten =
        token.blobsList.count {
          it.dependencyType == ComputationBlobDependency.OUTPUT &&
            it.path.isEmpty()
        }
      if (notWritten == 0) {
        assertTokenChangesTo(
          token.outputBlobsToInputBlobs()
            .addEmptyOutputs(1)
            .clearStageSpecificDetails()
            .setComputationStage(
              TO_APPEND_SKETCHES_AND_ADD_NOISE.toProtocolStage()
            ).setAttempt(0).build()
        ) {
          manager.transitionComputationToStage(
            it.token,
            it.inputs.paths() + it.outputs.paths(),
            TO_APPEND_SKETCHES_AND_ADD_NOISE
          )
        }
      }
    }

    /** Fakes receiving the concatenated sketch from the incoming duchy. */
    suspend fun receiveConcatenatedSketchGrpc() {
      writeOutputs(WAIT_CONCATENATED)
      val stage =
        if (token.role == RoleInComputation.PRIMARY) TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
        else TO_BLIND_POSITIONS
      assertTokenChangesTo(
        token.outputBlobsToInputBlobs()
          .addBlobs(newEmptyOutputBlobMetadata(1))
          .setComputationStage(stage.toProtocolStage())
          .setAttempt(0)
          .build()
      ) {
        manager.transitionComputationToStage(
          it.token, it.outputs.paths(), stage
        )
      }
    }

    /** Fakes receiving the joined sketch from the incoming duchy. */
    suspend fun receiveFlagCountsGrpc() {
      writeOutputs(WAIT_FLAG_COUNTS)
      val stage =
        if (token.role == RoleInComputation.PRIMARY) TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
        else TO_DECRYPT_FLAG_COUNTS
      assertTokenChangesTo(
        token
          .outputBlobsToInputBlobs()
          .addEmptyOutputs(1)
          .setComputationStage(stage.toProtocolStage()).setAttempt(0).build()
      ) {
        manager.transitionComputationToStage(
          it.token, it.outputs.paths(), stage
        )
      }
    }
  }

  suspend fun gatherLocalSketches() {
    assertTokenChangesTo(
      token.toBuilder().setComputationStage(TO_ADD_NOISE.toProtocolStage()).setAttempt(0)
        .addEmptyOutputs(1)
        .build()
    ) {
      manager.transitionComputationToStage(
        it.token,
        it.outputs.paths(),
        TO_ADD_NOISE
      )
    }
  }

  /** Move to a waiting stage and make sure the computation is not in the work queue. */
  suspend fun waitForSketches(details: ComputationStageDetails) {
    assertTokenChangesTo(
      token
        .outputBlobsToInputBlobs()
        .addEmptyOutputs(2)
        .setComputationStage(WAIT_SKETCHES.toProtocolStage())
        .setAttempt(1)
        .setStageSpecificDetails(details)
        .build()
    ) {
      manager.transitionComputationToStage(it.token, it.outputs.paths(), WAIT_SKETCHES)
    }
  }

  /** Move to a waiting stage and make sure the computation is not in the work queue. */
  suspend fun runWaitStage(stage: SketchAggregationStage) {
    assertTokenChangesTo(
      token
        .outputBlobsToInputBlobs()
        .addEmptyOutputs(1)
        .setComputationStage(stage.toProtocolStage()).setAttempt(1)
        .build()
    ) {
      manager.transitionComputationToStage(it.token, it.outputs.paths(), stage)
    }
  }

  suspend fun end(reason: CompletedReason) {
    token = manager.computationStorageClient.finishComputation(
      FinishComputationRequest.newBuilder()
        .setToken(token)
        .setEndingComputationStage(COMPLETED.toProtocolStage())
        .setReason(reason)
        .build()
    ).token
  }
}

fun List<ComputationStageBlobMetadata>.paths() = map { it.path }
fun List<ComputationStageBlobMetadata>.ofType(dependencyType: ComputationBlobDependency) =
  filter { it.dependencyType == dependencyType }

fun org.wfanet.measurement.internal.duchy.ComputationToken.outputBlobsToInputBlobs():
  org.wfanet.measurement.internal.duchy.ComputationToken.Builder {
    return toBuilder().clearBlobs().addAllBlobs(
      blobsList.filter { it.dependencyType == ComputationBlobDependency.OUTPUT }
        .mapIndexed { index, blob ->
          blob.toBuilder()
            .setDependencyType(ComputationBlobDependency.INPUT)
            .setBlobId(index.toLong())
            .build()
        }
    )
  }

fun org.wfanet.measurement.internal.duchy.ComputationToken.Builder.addEmptyOutputs(
  n: Int
): org.wfanet.measurement.internal.duchy.ComputationToken.Builder {
  val currentMaxIndex = blobsCount.toLong()
  (0 until n).forEach {
    addBlobs(newEmptyOutputBlobMetadata(currentMaxIndex + it))
  }
  return this
}
