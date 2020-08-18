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

package org.wfanet.measurement.duchy.mill

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import java.nio.charset.Charset
import java.time.Clock
import java.time.Duration
import kotlin.test.assertEquals
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.computation.testing.FakeComputationStorage
import org.wfanet.measurement.db.duchy.computation.testing.FakeComputationsBlobDb
import org.wfanet.measurement.duchy.mill.testing.FakeLiquidLegionsCryptoWorker
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency.INPUT
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency.OUTPUT
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchResponse
import org.wfanet.measurement.service.internal.duchy.computation.storage.ComputationStorageServiceImpl
import org.wfanet.measurement.service.internal.duchy.computation.storage.newEmptyOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.newInputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.newOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage
import org.wfanet.measurement.service.testing.GrpcTestServerRule

class LiquidLegionsMillTest {

  private val mockLiquidLegionsComputationControl: ComputationControlServiceCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())
  private val fakeBlobs = mutableMapOf<String, ByteArray>()
  private val fakeComputationStorage = FakeComputationStorage(otherDuchyNames)
  private val cryptoWorker = FakeLiquidLegionsCryptoWorker()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { channel ->
    computationStorageClients = LiquidLegionsSketchAggregationComputationStorageClients(
      ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub(channel),
      FakeComputationsBlobDb(fakeBlobs),
      otherDuchyNames
    )
    listOf(
      mockLiquidLegionsComputationControl,
      ComputationStorageServiceImpl(fakeComputationStorage)
    )
  }

  private lateinit var computationStorageClients:
    LiquidLegionsSketchAggregationComputationStorageClients

  private val workerStub: ComputationControlServiceCoroutineStub by lazy {
    ComputationControlServiceCoroutineStub(grpcTestServerRule.channel)
  }

  // Just use the same workerStub for all other duchies, since it is not relevant to this test.
  private val workerStubs = mapOf(DUCHY_ONE_NAME to workerStub, DUCHY_TWO_NAME to workerStub)

  private lateinit var mill: LiquidLegionsMill

  @Before
  fun initMill() {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(60))
    mill =
      LiquidLegionsMill(
        MILL_ID,
        computationStorageClients,
        workerStubs,
        cryptoKeySet,
        cryptoWorker,
        throttler,
        chunkSize = 20
      )
  }

  @Test
  fun `to blind positions using cached result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val inputBlobPath = "to_blind_position/input"
    val outputBlobPath = "to_blind_position/output"
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = TO_BLIND_POSITIONS.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(
        newInputBlobMetadata(0L, inputBlobPath),
        newOutputBlobMetadata(1L, outputBlobPath)
      )
    )
    fakeBlobs[inputBlobPath] = "sketch".toByteArray()
    fakeBlobs[outputBlobPath] = "cached result".toByteArray()

    lateinit var computationControlRequests: List<HandleConcatenatedSketchRequest>
    whenever(mockLiquidLegionsComputationControl.handleConcatenatedSketch(any())).thenAnswer {
      val request: Flow<HandleConcatenatedSketchRequest> = it.getArgument(0)
      computationControlRequests = runBlocking { request.toList() }
      HandleConcatenatedSketchResponse.getDefaultInstance()
    }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(WAIT_FLAG_COUNTS.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(INPUT)
            .setBlobId(0)
            .setPath("to_blind_position/output")
        )
        .addBlobs(ComputationStageBlobMetadata.newBuilder().setDependencyType(OUTPUT).setBlobId(1))
        .setNextDuchy("NEXT_WORKER")
        .setVersion(2) // CreateComputation + transitionStage
        .setRole(RoleInComputation.SECONDARY)
        .build()
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)

    assertThat(computationControlRequests).containsExactly(
      HandleConcatenatedSketchRequest.newBuilder()
        .setPartialSketch(ByteString.copyFromUtf8("cached result"))
        .setComputationId(computationId).build()
    )
  }

  @Test
  fun `to blind positions using calculated result`() = runBlocking<Unit> {
    // Stage 0. preparing the storage and set up mock
    val inputBlobPath = "to_blind_position/input"
    val computationId = 1111L
    fakeComputationStorage.addComputation(
      id = computationId,
      stage = TO_BLIND_POSITIONS.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(
        newInputBlobMetadata(0L, inputBlobPath),
        newEmptyOutputBlobMetadata(1L)
      )
    )
    fakeBlobs[inputBlobPath] = "sketch".toByteArray()

    lateinit var computationControlRequests: List<HandleConcatenatedSketchRequest>
    whenever(mockLiquidLegionsComputationControl.handleConcatenatedSketch(any())).thenAnswer {
      val request: Flow<HandleConcatenatedSketchRequest> = it.getArgument(0)
      computationControlRequests = runBlocking { request.toList() }
      HandleConcatenatedSketchResponse.getDefaultInstance()
    }

    // Stage 1. Process the above computation
    mill.pollAndProcessNextComputation()

    // Stage 2. Check the status of the computation
    val expectTokenAfterProcess =
      ComputationToken.newBuilder()
        .setGlobalComputationId(computationId)
        .setLocalComputationId(computationId)
        .setAttempt(1)
        .setComputationStage(WAIT_FLAG_COUNTS.toProtocolStage())
        .addBlobs(
          ComputationStageBlobMetadata.newBuilder()
            .setDependencyType(INPUT)
            .setBlobId(0)
            .setPath("1111/TO_BLIND_POSITIONS_1_output")
        )
        .addBlobs(ComputationStageBlobMetadata.newBuilder().setDependencyType(OUTPUT).setBlobId(1))
        .setNextDuchy("NEXT_WORKER")
        .setVersion(3) // CreateComputation + writeOutputBlob + transitionStage
        .setRole(RoleInComputation.SECONDARY)
        .build()
    val expectOutputBlob = "sketch-BlindedOneLayerRegisterIndex"
    assertEquals(expectTokenAfterProcess, fakeComputationStorage[computationId]!!)
    assertEquals(
      expectOutputBlob,
      fakeBlobs["1111/TO_BLIND_POSITIONS_1_output"]!!.toString(Charset.defaultCharset())
    )

    assertThat(computationControlRequests).containsExactly(
      HandleConcatenatedSketchRequest.newBuilder()
        .setPartialSketch(ByteString.copyFromUtf8("sketch-BlindedOneLay")) // Chunk 1, size 20
        .setComputationId(computationId).build(),
      HandleConcatenatedSketchRequest.newBuilder()
        .setPartialSketch(ByteString.copyFromUtf8("erRegisterIndex")) // Chunk 2, the rest
        .setComputationId(computationId).build()
    )
  }

  companion object {
    private const val MILL_ID = "a nice mill"
    private const val DUCHY_ONE_NAME = "NEXT_WORKER"
    private const val DUCHY_TWO_NAME = "NEXT NEXT_WORKER"
    private val otherDuchyNames = listOf(DUCHY_ONE_NAME, DUCHY_TWO_NAME)

    // These keys are valid keys obtained from the crypto library tests, i.e.,
    // create a cipher using random keys and then get these keys.
    private const val OWN_EL_GAMAL_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "02d1432ca007a6c6d739fce2d21feb56d9a2c35cf968265f9093c4b691e11386b3" +
        "057b22ef9c4e9626c22c13daed1363a1e6a5b309a930409f8d131f96ea2fa888"
    private const val DUCHY_ONE_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "039ef370ff4d216225401781d88a03f5a670a5040e6333492cb4e0cd991abbd5a3"
    private const val DUCHY_TWO_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "02d0f25ab445fc9c29e7e2509adc93308430f432522ffa93c2ae737ceb480b66d7"
    private const val CLIENT_PUBLIC_KEY =
      "036b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296" +
        "02505d7b3ac4c3c387c74132ab677a3421e883b90d4c83dc766e400fe67acc1f04"
    private const val CURVE_ID = 415L; // NID_X9_62_prime256v1

    private val cryptoKeySet = CryptoKeySet(
      ownPublicAndPrivateKeys = OWN_EL_GAMAL_KEY.toElGamalKeys(),
      otherDuchyPublicKeys = mapOf(
        DUCHY_ONE_NAME to DUCHY_ONE_PUBLIC_KEY.toElGamalPublicKeys(),
        DUCHY_TWO_NAME to DUCHY_TWO_PUBLIC_KEY.toElGamalPublicKeys()
      ),
      clientPublicKey = CLIENT_PUBLIC_KEY.toElGamalPublicKeys(),
      curveId = CURVE_ID
    )
  }
}
