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

package org.wfanet.measurement.duchy.db.computation

import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Instant
import kotlin.test.assertEquals
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.common.ElGamalPublicKey
import org.wfanet.measurement.common.byteStringOf
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.common.withPadding
import org.wfanet.measurement.duchy.DuchyPublicKeyMap
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.service.internal.computation.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computation.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.toGetTokenRequest
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub

private val EL_GAMAL_GENERATOR =
  byteStringOf(
    0x03,
    0x6B,
    0x17,
    0xD1,
    0xF2,
    0xE1,
    0x2C,
    0x42,
    0x47,
    0xF8,
    0xBC,
    0xE6,
    0xE5,
    0x63,
    0xA4,
    0x40,
    0xF2,
    0x77,
    0x03,
    0x7D,
    0x81,
    0x2D,
    0xEB,
    0x33,
    0xA0,
    0xF4,
    0xA1,
    0x39,
    0x45,
    0xD8,
    0x98,
    0xC2,
    0x96
  )
private const val ID_WHERE_ALSACE_IS_NOT_PRIMARY = "456"
private const val ID_WHERE_ALSACE_IS_PRIMARY = "123"
private const val ALSACE = "Alsace"
private const val BAVARIA = "Bavaria"
private const val CARINTHIA = "Carinthia"
private val DUCHIES = listOf(ALSACE, BAVARIA, CARINTHIA)

@RunWith(JUnit4::class)
class ComputationDataClientsTest {
  private val fakeDatabase = FakeComputationsDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    globalComputationClient = GlobalComputationsCoroutineStub(channel)
    addService(
      ComputationsService(fakeDatabase, globalComputationClient, ALSACE, Clock.systemUTC())
    )
  }

  private lateinit var globalComputationClient: GlobalComputationsCoroutineStub

  private val dummyStorageClient =
    object : StorageClient {
      override val defaultBufferSizeBytes: Int
        get() {
          throw NotImplementedError("Unused by test")
        }

      override suspend fun createBlob(
        blobKey: String,
        content: Flow<ByteString>
      ): StorageClient.Blob {
        throw NotImplementedError("Unused by test")
      }

      override fun getBlob(blobKey: String): StorageClient.Blob? {
        throw NotImplementedError("Unused by test")
      }
    }

  @Test
  fun runProtocolAtNonPrimaryWorker() = runBlocking {
    val testClock = TestClockWithNamedInstants(Instant.ofEpochMilli(100L))
    val computation =
      SingleLiquidLegionsV2Computation(
        ComputationDataClients(
          ComputationsCoroutineStub(channel = grpcTestServerRule.channel),
          storageClient = dummyStorageClient,
          otherDuchies = publicKeysMap.keys.minus(ALSACE).toList()
        ),
        ID_WHERE_ALSACE_IS_NOT_PRIMARY,
        LiquidLegionsV2SetupConfig.RoleInComputation.NON_AGGREGATOR,
        testClock
      )
    val fakeRpcService = computation.FakeRpcService()
    computation.enqueue()
    computation.claimWorkFor("mill-1")
    computation.writeOutputs(Stage.CONFIRM_REQUISITIONS_PHASE)
    computation.runWaitStage(Stage.WAIT_TO_START, numOfOutput = 0)
    computation.start()

    computation.claimWorkFor("mill-2")
    computation.writeOutputs(Stage.SETUP_PHASE)
    computation.runWaitStage(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS)

    fakeRpcService.receiveIntermediateDataGrpc(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS)

    computation.claimWorkFor("mill-3")
    computation.writeOutputs(Stage.EXECUTION_PHASE_ONE)
    computation.runWaitStage(Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS)

    fakeRpcService.receiveIntermediateDataGrpc(Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS)

    computation.claimWorkFor("mill-4")
    computation.writeOutputs(Stage.EXECUTION_PHASE_TWO)
    computation.runWaitStage(Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS)

    fakeRpcService.receiveIntermediateDataGrpc(Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS)

    computation.claimWorkFor("mill-5")
    computation.writeOutputs(Stage.EXECUTION_PHASE_THREE)
    computation.end(reason = CompletedReason.SUCCEEDED)
  }

  @Test
  fun runProtocolAtPrimaryWorker() = runBlocking {
    val testClock = TestClockWithNamedInstants(Instant.ofEpochMilli(100L))
    val computation =
      SingleLiquidLegionsV2Computation(
        ComputationDataClients(
          ComputationsCoroutineStub(channel = grpcTestServerRule.channel),
          storageClient = dummyStorageClient,
          otherDuchies = publicKeysMap.keys.minus(ALSACE).toList()
        ),
        ID_WHERE_ALSACE_IS_PRIMARY,
        LiquidLegionsV2SetupConfig.RoleInComputation.AGGREGATOR,
        testClock
      )
    val fakeRpcService = computation.FakeRpcService()

    computation.enqueue()
    computation.claimWorkFor("mill-1")
    computation.writeOutputs(Stage.CONFIRM_REQUISITIONS_PHASE)
    computation.waitForSketches(
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.Details(DUCHIES.subList(1, 3))
        .detailsFor(Stage.WAIT_SETUP_PHASE_INPUTS)
    )
    fakeRpcService.receiveSketch(BAVARIA)
    fakeRpcService.receiveSketch(CARINTHIA)

    computation.claimWorkFor("mill-2")
    computation.writeOutputs(Stage.SETUP_PHASE)
    computation.runWaitStage(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS)

    fakeRpcService.receiveIntermediateDataGrpc(Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS)

    computation.claimWorkFor("mill-3")
    computation.writeOutputs(Stage.EXECUTION_PHASE_ONE)
    computation.runWaitStage(Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS)
    fakeRpcService.receiveIntermediateDataGrpc(Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS)

    computation.claimWorkFor("mill-4")
    computation.writeOutputs(Stage.EXECUTION_PHASE_TWO)
    computation.runWaitStage(Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS)
    fakeRpcService.receiveIntermediateDataGrpc(Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS)

    computation.claimWorkFor("mill-5")
    computation.writeOutputs(Stage.EXECUTION_PHASE_THREE)
    computation.end(reason = CompletedReason.SUCCEEDED)
  }

  companion object {
    private val publicKeysMap: DuchyPublicKeyMap =
      DUCHIES
        .mapIndexed { idx, name ->
          name to
            ElGamalPublicKey.newBuilder()
              .apply {
                generator = EL_GAMAL_GENERATOR
                element = byteStringOf(idx).withPadding(33)
              }
              .build()
        }
        .toMap()
  }
}

/** Data about a single step of a computation. . */
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
class SingleLiquidLegionsV2Computation(
  private val dataClients: ComputationDataClients,
  globalId: String,
  roleInComputation: LiquidLegionsV2SetupConfig.RoleInComputation,
  private val testClock: TestClockWithNamedInstants
) {

  private var token: org.wfanet.measurement.internal.duchy.ComputationToken = runBlocking {
    dataClients.computationsClient.createComputation(
        CreateComputationRequest.newBuilder()
          .apply {
            globalComputationId = globalId
            computationType = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
            // For the purpose of this test, only set role in the computationDetails.
            computationDetailsBuilder.liquidLegionsV2Builder.apply { role = roleInComputation }
          }
          .build()
      )
      .token
  }

  suspend fun writeOutputs(stage: Stage) {
    assertEquals(stage.toProtocolStage(), token.computationStage)
    testClock.tickSeconds(
      "${token.computationStage.liquidLegionsSketchAggregationV2}_$token.attempt_outputs"
    )
    token.blobsList.filter { it.dependencyType == ComputationBlobDependency.OUTPUT }.forEach {
      token =
        dataClients.computationsClient.recordOutputBlobPath(
            RecordOutputBlobPathRequest.newBuilder()
              .setToken(token)
              .setOutputBlobId(it.blobId)
              .setBlobPath("unused_output_${it.blobId}")
              .build()
          )
          .token
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
    val outputsToCurrentStage =
      (token.blobsList.ofType(ComputationBlobDependency.OUTPUT) +
        token.blobsList.ofType(ComputationBlobDependency.PASS_THROUGH))
    val result = run(ComputationStep(token, inputsToCurrentStage, outputsToCurrentStage))
    ProtoTruth.assertThat(result)
      .isEqualTo(expected.toBuilder().setVersion(token.version + 1).build())
    token = result
  }

  /** Add computation to work queue and verify that it has no owner. */
  suspend fun enqueue() {
    assertTokenChangesTo(token.toBuilder().setAttempt(0).build()) {
      dataClients.computationsClient.enqueueComputation(
        EnqueueComputationRequest.newBuilder().setToken(token).build()
      )
      dataClients.computationsClient.getComputationToken(
          token.globalComputationId.toGetTokenRequest()
        )
        .token
    }
  }

  /** Get computation from work queue and verify it is owned by the [workerId]. */
  suspend fun claimWorkFor(workerId: String) {
    assertTokenChangesTo(token.toBuilder().setAttempt(1).build()) {
      val claimed =
        dataClients.computationsClient.claimWork(
          ClaimWorkRequest.newBuilder()
            .setOwner(workerId)
            .setComputationType(ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2)
            .build()
        )
      ProtoTruth.assertThat(claimed).isNotEqualToDefaultInstance()
      claimed.token
    }
  }

  inner class FakeRpcService {
    /**
     * Fake receiving a sketch from a [sender], if all sketches have been received set stage to
     * [TO_BLIND_POSITIONS_AND_JOIN_REGISTERS].
     */
    suspend fun receiveSketch(sender: String) {
      val stageDetails = token.stageSpecificDetails.liquidLegionsV2.waitSetupPhaseInputsDetails

      val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[sender])
      val path = "unused_${sender}_$blobId"
      token =
        dataClients.computationsClient.recordOutputBlobPath(
            RecordOutputBlobPathRequest.newBuilder()
              .setToken(token)
              .setOutputBlobId(blobId)
              .setBlobPath(path)
              .build()
          )
          .token

      val notWritten =
        token.blobsList.count {
          it.dependencyType == ComputationBlobDependency.OUTPUT && it.path.isEmpty()
        }
      if (notWritten == 0) {
        assertTokenChangesTo(
          token
            .outputBlobsToInputBlobs(keepInputs = true)
            .addEmptyOutputs(1)
            .clearStageSpecificDetails()
            .setComputationStage(Stage.SETUP_PHASE.toProtocolStage())
            .setAttempt(0)
            .build()
        ) {
          dataClients.transitionComputationToStage(
            computationToken = it.token,
            inputsToNextStage = it.inputs.paths() + it.outputs.paths(),
            stage = Stage.SETUP_PHASE.toProtocolStage()
          )
        }
      }
    }

    /** Fakes receiving the intermediate data from the incoming duchy. */
    suspend fun receiveIntermediateDataGrpc(currentStage: Stage) {
      writeOutputs(currentStage)
      val nextStage =
        LiquidLegionsSketchAggregationV2Protocol.EnumStages.validSuccessors[currentStage]?.first()!!
      assertTokenChangesTo(
        token
          .outputBlobsToInputBlobs()
          .addBlobs(newEmptyOutputBlobMetadata(1))
          .setComputationStage(nextStage.toProtocolStage())
          .setAttempt(0)
          .build()
      ) {
        dataClients.transitionComputationToStage(
          computationToken = it.token,
          inputsToNextStage = it.outputs.paths(),
          stage = nextStage.toProtocolStage()
        )
      }
    }
  }

  /** Move to a waiting stage and make sure the computation is not in the work queue. */
  suspend fun waitForSketches(details: ComputationStageDetails) {
    assertTokenChangesTo(
      token
        .outputBlobsToInputBlobs()
        .addEmptyOutputs(2)
        .setComputationStage(Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage())
        .setAttempt(1)
        .setStageSpecificDetails(details)
        .build()
    ) {
      dataClients.transitionComputationToStage(
        computationToken = it.token,
        inputsToNextStage = it.outputs.paths(),
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
      )
    }
  }

  suspend fun start() {
    assertTokenChangesTo(
      token
        .outputBlobsToInputBlobs(keepInputs = true)
        .addEmptyOutputs(1)
        .setComputationStage(Stage.SETUP_PHASE.toProtocolStage())
        .setAttempt(0)
        .build()
    ) {
      dataClients.transitionComputationToStage(
        computationToken = it.token,
        inputsToNextStage = it.inputs.paths(),
        stage = Stage.SETUP_PHASE.toProtocolStage()
      )
    }
  }

  /** Move to a waiting stage and make sure the computation is not in the work queue. */
  suspend fun runWaitStage(stage: Stage, numOfOutput: Int = 1) {
    assertTokenChangesTo(
      token
        .outputBlobsToInputBlobs()
        .addEmptyOutputs(numOfOutput)
        .setComputationStage(stage.toProtocolStage())
        .setAttempt(1)
        .build()
    ) {
      dataClients.transitionComputationToStage(
        computationToken = it.token,
        inputsToNextStage = it.outputs.paths(),
        stage = stage.toProtocolStage()
      )
    }
  }

  suspend fun end(reason: CompletedReason) {
    token =
      dataClients.computationsClient.finishComputation(
          FinishComputationRequest.newBuilder()
            .setToken(token)
            .setEndingComputationStage(Stage.COMPLETE.toProtocolStage())
            .setReason(reason)
            .build()
        )
        .token
  }
}

fun List<ComputationStageBlobMetadata>.paths() = map { it.path }

fun List<ComputationStageBlobMetadata>.ofType(dependencyType: ComputationBlobDependency) = filter {
  it.dependencyType == dependencyType
}

fun org.wfanet.measurement.internal.duchy.ComputationToken.outputBlobsToInputBlobs(
  keepInputs: Boolean = false
): org.wfanet.measurement.internal.duchy.ComputationToken.Builder {
  return toBuilder()
    .clearBlobs()
    .addAllBlobs(
      blobsList
        .filter { keepInputs || it.dependencyType == ComputationBlobDependency.OUTPUT }
        .mapIndexed { index, blob ->
          blob
            .toBuilder()
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
  (0 until n).forEach { addBlobs(newEmptyOutputBlobMetadata(currentMaxIndex + it)) }
  return this
}
