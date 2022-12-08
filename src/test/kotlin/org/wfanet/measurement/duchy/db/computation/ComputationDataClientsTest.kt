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
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.toGetTokenRequest
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails.CompletedReason
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub as SystemComputationLogEntriesCoroutineStub

private const val ID_WHERE_ALSACE_IS_NOT_PRIMARY = "456"
private const val ID_WHERE_ALSACE_IS_PRIMARY = "123"
private const val ALSACE = "Alsace"
private const val BAVARIA = "Bavaria"
private const val CARINTHIA = "Carinthia"

@RunWith(JUnit4::class)
class ComputationDataClientsTest {
  private val fakeDatabase = FakeComputationsDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    val storageClient = InMemoryStorageClient()

    systemComputationLogEntriesClient = SystemComputationLogEntriesCoroutineStub(channel)
    addService(
      ComputationsService(
        fakeDatabase,
        systemComputationLogEntriesClient,
        ComputationStore(storageClient),
        RequisitionStore(storageClient),
        ALSACE,
        Clock.systemUTC()
      )
    )
  }

  private lateinit var systemComputationLogEntriesClient: SystemComputationLogEntriesCoroutineStub

  private val dummyStorageClient =
    object : StorageClient {
      override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
        throw NotImplementedError("Unused by test")
      }

      override suspend fun writeBlob(
        blobKey: String,
        content: Flow<ByteString>,
      ): StorageClient.Blob {
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
          storageClient = dummyStorageClient
        ),
        ID_WHERE_ALSACE_IS_NOT_PRIMARY,
        LiquidLegionsV2SetupConfig.RoleInComputation.NON_AGGREGATOR,
        testClock
      )
    val fakeRpcService = computation.FakeRpcService()
    computation.enqueue()

    computation.claimWorkFor("mill-0")
    computation.runWaitStage(Stage.WAIT_REQUISITIONS_AND_KEY_SET, numOfOutput = 0)
    computation.getRequisitionAndKeySet()

    computation.claimWorkFor("mill-1")
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
          storageClient = dummyStorageClient
        ),
        ID_WHERE_ALSACE_IS_PRIMARY,
        LiquidLegionsV2SetupConfig.RoleInComputation.AGGREGATOR,
        testClock
      )
    val fakeRpcService = computation.FakeRpcService()

    computation.enqueue()
    computation.claimWorkFor("mill-0")
    computation.runWaitStage(Stage.WAIT_REQUISITIONS_AND_KEY_SET, numOfOutput = 0)
    computation.getRequisitionAndKeySet()

    computation.claimWorkFor("mill-1")

    computation.waitForSketches(
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.Details.detailsFor(
        Stage.WAIT_SETUP_PHASE_INPUTS,
        LiquidLegionsSketchAggregationV2.ComputationDetails.newBuilder()
          .apply {
            addParticipantBuilder().apply { duchyId = CARINTHIA }
            addParticipantBuilder().apply { duchyId = BAVARIA }
            addParticipantBuilder().apply { duchyId = ALSACE }
          }
          .build()
      )
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
}

/** Data about a single step of a computation. . */
data class ComputationStep(
  val token: ComputationToken,
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

  private var token: ComputationToken = runBlocking {
    dataClients.computationsClient
      .createComputation(
        CreateComputationRequest.newBuilder()
          .apply {
            globalComputationId = globalId
            computationType = ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
            // For the purpose of this test, only set role and participants in the
            // computationDetails.
            computationDetailsBuilder.liquidLegionsV2Builder.apply {
              role = roleInComputation
              addParticipantBuilder().apply { duchyId = CARINTHIA }
              addParticipantBuilder().apply { duchyId = BAVARIA }
              addParticipantBuilder().apply { duchyId = ALSACE }
            }
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
    token.blobsList
      .filter { it.dependencyType == ComputationBlobDependency.OUTPUT }
      .forEach {
        token =
          dataClients.computationsClient
            .recordOutputBlobPath(
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
    expected: ComputationToken,
    run: suspend (ComputationStep) -> ComputationToken
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
      dataClients.computationsClient
        .getComputationToken(token.globalComputationId.toGetTokenRequest())
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
        dataClients.computationsClient
          .recordOutputBlobPath(
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
        .toBuilder()
        .clearBlobs()
        .addEmptyOutputs(2)
        .setComputationStage(Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage())
        .setAttempt(1)
        .setStageSpecificDetails(details)
        .build()
    ) {
      dataClients.transitionComputationToStage(
        computationToken = it.token,
        stage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
      )
    }
  }

  suspend fun getRequisitionAndKeySet() {
    assertTokenChangesTo(
      token
        .outputBlobsToInputBlobs(keepInputs = true)
        .setComputationStage(Stage.CONFIRMATION_PHASE.toProtocolStage())
        .setAttempt(0)
        .build()
    ) {
      dataClients.transitionComputationToStage(
        computationToken = it.token,
        inputsToNextStage = it.inputs.paths(),
        stage = Stage.CONFIRMATION_PHASE.toProtocolStage()
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
      dataClients.computationsClient
        .finishComputation(
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

fun ComputationToken.outputBlobsToInputBlobs(
  keepInputs: Boolean = false
): ComputationToken.Builder {
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

fun ComputationToken.Builder.addEmptyOutputs(n: Int): ComputationToken.Builder {
  val currentMaxIndex = blobsCount.toLong()
  (0 until n).forEach { addBlobs(newEmptyOutputBlobMetadata(currentMaxIndex + it)) }
  return this
}
