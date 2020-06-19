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
import org.wfanet.measurement.internal.SketchAggregationState.ADDING_NOISE
import org.wfanet.measurement.internal.SketchAggregationState.BLINDING_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationState.COMBINING_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationState.COMPUTING_METRICS
import org.wfanet.measurement.internal.SketchAggregationState.DECRYPTING_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationState.FLAG_COUNTS_DECRYPTED
import org.wfanet.measurement.internal.SketchAggregationState.METRICS_COMPUTED
import org.wfanet.measurement.internal.SketchAggregationState.NOISE_ADDED
import org.wfanet.measurement.internal.SketchAggregationState.POSITIONS_BLINDED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_JOINED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationState.REGISTERS_COMBINED
import org.wfanet.measurement.internal.SketchAggregationState.STARTING
import org.wfanet.measurement.internal.SketchAggregationState.TRANSMITTED_SKETCH
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_FINISHED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_JOINED
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
        spannerOptions = spanner.spanner.options,
        spannerDatabaseId = spanner.databaseId,
        googleCloudStorageOptions = LocalStorageHelper.getOptions(),
        storageBucket = "test-peasant-bucket",
        clock = testClock
      ),
      ID_WHERE_ALSACE_IS_NOT_PRIMARY,
      testClock)
    val fakeRpcService = computation.FakeRpcService()

    // Each of these operations makes assertions about the state of the computation via changes
    // to the computation token.
    computation.enqueue()

    computation.claimWorkFor("some-peasant")
    computation.runTransientStage(ADDING_NOISE to NOISE_ADDED)
    fakeRpcService.sendNoisedSketchesToPrimary()

    computation.runWaitStage(WAIT_CONCATENATED)

    fakeRpcService.receiveConcatenatedSketchGrpc()

    computation.claimWorkFor("some-other-peasant")
    computation.runTransientStage(BLINDING_POSITIONS to POSITIONS_BLINDED)
    computation.runWaitStage(WAIT_JOINED)

    fakeRpcService.receiveJoinedSketchGrpc()

    computation.claimWorkFor("yet-another-peasant")
    computation.runTransientStage(DECRYPTING_FLAG_COUNTS to FLAG_COUNTS_DECRYPTED)
    computation.runWaitStage(WAIT_FINISHED)

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
        state = WAIT_FINISHED
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
        spannerOptions = spanner.spanner.options,
        spannerDatabaseId = spanner.databaseId,
        googleCloudStorageOptions = LocalStorageHelper.getOptions(),
        storageBucket = "test-peasant-bucket",
        clock = testClock
      ),
      ID_WHERE_ALSACE_IS_PRIMARY,
      testClock)
    val fakeRpcService = computation.FakeRpcService()

    computation.enqueue()
    computation.claimWorkFor("some-peasant")
    computation.runTransientStage(ADDING_NOISE to NOISE_ADDED)
    computation.runWaitStage(WAIT_SKETCHES)

    fakeRpcService.receiveSketch(BAVARIA)
    fakeRpcService.receiveSketch(CARINTHIA)

    computation.claimWorkFor("some-peasant")
    computation.runWaitStage(WAIT_CONCATENATED)

    fakeRpcService.receiveConcatenatedSketchGrpc()

    computation.claimWorkFor("some-other-peasant")
    computation.runTransientStage(BLINDING_POSITIONS to POSITIONS_BLINDED)
    computation.runTransientStage(COMBINING_REGISTERS to REGISTERS_COMBINED)
    computation.runWaitStage(WAIT_JOINED)

    fakeRpcService.receiveJoinedSketchGrpc()

    computation.claimWorkFor("yet-another-peasant")
    computation.runTransientStage(DECRYPTING_FLAG_COUNTS to FLAG_COUNTS_DECRYPTED)
    computation.runTransientStage(COMPUTING_METRICS to METRICS_COMPUTED)
    computation.runWaitStage(WAIT_FINISHED)

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
        state = WAIT_FINISHED
      )
    )
  }
}

/** Data about a single step of a computation. .*/
data class ComputationStep(
  val token: ComputationToken<SketchAggregationState>,
  val inputs: List<String>,
  val outputs: List<String>
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

  private var token = manager.createComputation(globalId, STARTING)
  val localId by lazy { token.localId }

  private fun writeAllOutputsForCurrentState(content: String) {
    testClock.tickSeconds("${token.state.name}_$token.attempt_outputs")
    manager.readBlobReferences(token, BlobDependencyType.OUTPUT)
      .map { BlobRef(it.key, manager.newBlobPath(token, "outputs")) }
      .forEach { manager.writeAndRecordOutputBlob(token, it, content.toByteArray()) }
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
      manager.readBlobReferences(token, BlobDependencyType.OUTPUT).map {
        checkNotNull(it.value) {
          "Unwritten output $it. All blobs must be written before transitioning computation stages."
        }
      }
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
    /** Set state to [TRANSMITTED_SKETCH]. */
    fun sendNoisedSketchesToPrimary() {
      assertTokenChangesTo(token.copy(state = TRANSMITTED_SKETCH, attempt = 1)) {
        manager.transitionComputationToStage(it.token, it.inputs, it.outputs, TRANSMITTED_SKETCH)
      }
    }

    /**
     * Fake receiving a sketch from a [sender], if all sketches have been received
     * set state to [RECEIVED_SKETCHES].
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
      if (notWritten == 0) {
        assertTokenChangesTo(token.copy(state = RECEIVED_SKETCHES, attempt = 0)) {
          manager.transitionComputationToStage(it.token, it.inputs, it.outputs, RECEIVED_SKETCHES)
        }
      }
    }

    /** Fakes receiving the concatenated sketch from the incoming duchy. */
    fun receiveConcatenatedSketchGrpc() {
      writeAllOutputsForCurrentState("ConcatenatedSketch")
      assertTokenChangesTo(token.copy(state = RECEIVED_CONCATENATED, attempt = 0)) {
        manager.transitionComputationToStage(it.token, it.inputs, it.outputs, RECEIVED_CONCATENATED)
      }
    }

    /** Fakes receiving the joined sketch from the incoming duchy. */
    fun receiveJoinedSketchGrpc() {
      writeAllOutputsForCurrentState("JoinedSketch")
      assertTokenChangesTo(token.copy(state = RECEIVED_JOINED, attempt = 0)) {
        manager.transitionComputationToStage(it.token, it.inputs, it.outputs, RECEIVED_JOINED)
      }
    }
  }

  /**
   *  Runs a two stage process mimicking a long running operation for a Peasant.
   *  This moves the computations to the first stage in the pair, then writes a fake output blob,
   *  and finally moves the computation to the second stage.
   */
  fun runTransientStage(pair: Pair<SketchAggregationState, SketchAggregationState>) {
    assertTokenChangesTo(token.copy(state = pair.first, attempt = 1)) {
      manager.transitionComputationToStage(it.token, it.inputs, it.outputs, pair.first)
    }

    writeAllOutputsForCurrentState("Data_${pair.first}->${pair.second}")
    assertTokenChangesTo(token.copy(state = pair.second, attempt = 1)) {
      manager.transitionComputationToStage(it.token, it.inputs, it.outputs, pair.second)
    }
  }

  /** Move to a waiting stage and make sure the computation is not in the work queue. */
  fun runWaitStage(stage: SketchAggregationState) {
    assertTokenChangesTo(token.copy(state = stage, attempt = 1, owner = null)) {
      manager.transitionComputationToStage(it.token, it.inputs, it.outputs, stage)
    }
  }
}
