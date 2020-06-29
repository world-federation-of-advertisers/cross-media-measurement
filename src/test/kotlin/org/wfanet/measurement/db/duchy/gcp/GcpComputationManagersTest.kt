package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import java.math.BigInteger
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.SketchAggregationComputationManager
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.internal.SketchAggregationState
import org.wfanet.measurement.internal.SketchAggregationState.COMPLETED
import org.wfanet.measurement.internal.SketchAggregationState.CREATED
import org.wfanet.measurement.internal.SketchAggregationState.TO_ADD_NOISE
import org.wfanet.measurement.internal.SketchAggregationState.TO_APPEND_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationState.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationState.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationState.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationState.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_SKETCHES

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

  @Test
  fun runProtocolAtNonPrimaryWorker() {
    val testClock = TestClockWithNamedInstants(Instant.ofEpochMilli(100L))
    val computation = SingleComputationManager(
      newCascadingLegionsSketchAggregationGcpComputationManager(
        ALSACE,
        duchyPublicKeys = publicKeysMap,
        databaseClient = databaseClient,
        googleCloudStorageOptions = LocalStorageHelper.getOptions(),
        storageBucket = "test-peasant-bucket",
        clock = testClock
      ),
      ID_WHERE_ALSACE_IS_NOT_PRIMARY,
      testClock
    )
    val fakeRpcService = computation.FakeRpcService()
    computation.writeOutputs(CREATED)
    computation.gatherLocalSketches()
    computation.enqueue()

    computation.claimWorkFor("some-peasant")
    computation.writeOutputs(TO_ADD_NOISE)
    computation.runWaitStage(WAIT_CONCATENATED)

    fakeRpcService.receiveConcatenatedSketchGrpc()

    computation.claimWorkFor("some-other-peasant")
    computation.writeOutputs(TO_BLIND_POSITIONS)
    computation.runWaitStage(WAIT_FLAG_COUNTS)

    fakeRpcService.receiveFlagCountsGrpc()

    computation.claimWorkFor("yet-another-peasant")
    computation.writeOutputs(TO_DECRYPT_FLAG_COUNTS)
    computation.runWaitStage(COMPLETED)

    computation.assertTokenEquals(
      ComputationToken(
        // Cheat on the generated local id for the computation, it is tested elsewhere.
        localId = computation.localId,
        globalId = ID_WHERE_ALSACE_IS_NOT_PRIMARY,
        owner = null,
        attempt = 1,
        nextWorker = BAVARIA,
        lastUpdateTime = testClock.last().toEpochMilli(),
        role = DuchyRole.SECONDARY,
        state = COMPLETED
      )
    )
  }

  @Test
  fun runProtocolAtPrimaryWorker() {
    val testClock = TestClockWithNamedInstants(Instant.ofEpochMilli(100L))
    val computation = SingleComputationManager(
      newCascadingLegionsSketchAggregationGcpComputationManager(
        ALSACE,
        duchyPublicKeys = publicKeysMap,
        databaseClient = databaseClient,
        googleCloudStorageOptions = LocalStorageHelper.getOptions(),
        storageBucket = "test-peasant-bucket",
        clock = testClock
      ),
      ID_WHERE_ALSACE_IS_PRIMARY,
      testClock
    )
    val fakeRpcService = computation.FakeRpcService()
    computation.writeOutputs(CREATED)
    computation.gatherLocalSketches()
    computation.enqueue()

    computation.claimWorkFor("some-peasant")
    computation.writeOutputs(TO_ADD_NOISE)
    computation.runWaitStage(WAIT_SKETCHES)

    fakeRpcService.receiveSketch(BAVARIA)
    fakeRpcService.receiveSketch(CARINTHIA)

    computation.claimWorkFor("some-peasant")
    computation.writeOutputs(TO_APPEND_SKETCHES)
    computation.runWaitStage(WAIT_CONCATENATED)

    fakeRpcService.receiveConcatenatedSketchGrpc()

    computation.claimWorkFor("some-other-peasant")
    computation.writeOutputs(TO_BLIND_POSITIONS_AND_JOIN_REGISTERS)
    computation.runWaitStage(WAIT_FLAG_COUNTS)
    fakeRpcService.receiveFlagCountsGrpc()

    computation.claimWorkFor("yet-another-peasant")
    computation.writeOutputs(TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS)
    computation.runWaitStage(COMPLETED)

    computation.assertTokenEquals(
      ComputationToken(
        // Cheat on the generated local id for the computation, it is tested elsewhere.
        localId = computation.localId,
        globalId = ID_WHERE_ALSACE_IS_PRIMARY,
        owner = null,
        attempt = 1,
        nextWorker = BAVARIA,
        lastUpdateTime = testClock.last().toEpochMilli(),
        role = DuchyRole.PRIMARY,
        state = COMPLETED
      )
    )
  }
}

/** Data about a single step of a computation. .*/
data class ComputationStep(
  val token: ComputationToken<SketchAggregationState>,
  val inputs: List<String>,
  val outputs: List<String?>
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

  private var token = manager.createComputation(globalId, CREATED)
  val localId by lazy { token.localId }

  fun writeOutputs(stage: SketchAggregationState) {
    assertEquals(stage, token.state)
    testClock.tickSeconds("${token.state.name}_$token.attempt_outputs")
    manager.readBlobReferences(token, BlobDependencyType.OUTPUT)
      .map { BlobRef(it.key, manager.newBlobPath(token, "outputs")) }
      .forEach { manager.writeAndRecordOutputBlob(token, it, stage.name.toByteArray()) }
  }

  /** Runs an operation and checks the returned token from the operation matches the expected. */
  private fun assertTokenChangesTo(
    expected: ComputationToken<SketchAggregationState>,
    run: (ComputationStep) -> ComputationToken<SketchAggregationState>
  ) {
    testClock.tickSeconds("${expected.state.name}_$expected.attempt")
    // Some stages use the inputs to their predecessor as inputs it itself. If the inputs are needed
    // they will be fetched.
    val inputsToCurrentStage by lazy {
      manager.readBlobReferences(token, BlobDependencyType.INPUT).map {
        checkNotNull(it.value) {
          "Unwritten input $it. All blobs must be written before transitioning computation stages."
        }
      }
    }
    // Some stages use the outputs to their predecessor as inputs it itself. If the outputs are
    // needed they will be fetched.
    val outputsToCurrentStage by lazy {
      manager.readBlobReferences(token, BlobDependencyType.OUTPUT).map { it.value }
    }
    val result = run(ComputationStep(token, inputsToCurrentStage, outputsToCurrentStage))
    assertEquals(expected.copy(lastUpdateTime = testClock.last().toEpochMilli()), result)
    token = result
  }

  fun assertTokenEquals(expected: ComputationToken<SketchAggregationState>) =
    assertEquals(token, expected)

  /** Add computation to work queue and verify that it has no owner. */
  fun enqueue() {
    assertTokenChangesTo(token.copy(owner = null, attempt = 0)) {
      manager.enqueue(token)
      assertNotNull(manager.getToken(token.globalId))
    }
  }

  /** Get computation from work queue and verify it is owned by the [workerId]. */
  fun claimWorkFor(workerId: String) {
    assertTokenChangesTo(token.copy(owner = workerId, attempt = 1)) {
      assertNotNull(manager.claimWork(workerId))
    }
  }

  inner class FakeRpcService {
    /**
     * Fake receiving a sketch from a [sender], if all sketches have been received
     * set state to [TO_BLIND_POSITIONS_AND_JOIN_REGISTERS].
     */
    fun receiveSketch(sender: String) {
      val stageDetails = manager.readStageSpecificDetails(token).waitSketchStageDetails

      val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[sender])
      val contents = "message from $sender"
      val path = manager.newBlobPath(token, sender)
      manager.writeAndRecordOutputBlob(token, BlobRef(blobId, path), contents.toByteArray())

      assertThat(manager.readBlobReferences(token, BlobDependencyType.OUTPUT)[blobId]).isEqualTo(
        path
      )

      val notWritten =
        manager.readBlobReferences(token, BlobDependencyType.OUTPUT).values.count { it == null }
      println("$notWritten")
      if (notWritten == 0) {
        println("Moving to append sketches state")
        assertTokenChangesTo(token.copy(state = TO_APPEND_SKETCHES, attempt = 0)) {
          manager.transitionComputationToStage(
            it.token,
            it.inputs + it.outputs.requireNoNulls(),
            TO_APPEND_SKETCHES
          )
        }
      }
    }

    /** Fakes receiving the concatenated sketch from the incoming duchy. */
    fun receiveConcatenatedSketchGrpc() {
      writeOutputs(WAIT_CONCATENATED)
      val state =
        if (token.role == DuchyRole.PRIMARY) TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
        else TO_BLIND_POSITIONS
      assertTokenChangesTo(token.copy(state = state, attempt = 0)) {
        manager.transitionComputationToStage(
          it.token, it.outputs.requireNoNulls(), state
        )
      }
    }

    /** Fakes receiving the joined sketch from the incoming duchy. */
    fun receiveFlagCountsGrpc() {
      writeOutputs(WAIT_FLAG_COUNTS)
      val state =
        if (token.role == DuchyRole.PRIMARY) TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
        else TO_DECRYPT_FLAG_COUNTS
      assertTokenChangesTo(token.copy(state = state, attempt = 0)) {
        manager.transitionComputationToStage(
          it.token, it.outputs.requireNoNulls(), state
        )
      }
    }
  }

  fun gatherLocalSketches() {
    assertTokenChangesTo(token.copy(state = TO_ADD_NOISE, attempt = 0, owner = null)) {
      manager.transitionComputationToStage(
        it.token,
        it.outputs.requireNoNulls(),
        TO_ADD_NOISE
      )
    }
  }

  /** Move to a waiting stage and make sure the computation is not in the work queue. */
  fun runWaitStage(stage: SketchAggregationState) {
    assertTokenChangesTo(token.copy(state = stage, attempt = 1, owner = null)) {
      manager.transitionComputationToStage(it.token, it.outputs.requireNoNulls(), stage)
    }
  }
}
