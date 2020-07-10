package org.wfanet.measurement.service.internal.duchy.worker

import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.ByteString
import io.grpc.StatusException
import java.nio.charset.Charset
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertFailsWith
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.SketchAggregationComputationManager
import org.wfanet.measurement.db.duchy.SketchAggregationStageDetails
import org.wfanet.measurement.db.duchy.SketchAggregationStages
import org.wfanet.measurement.db.duchy.testing.FakeBlobMetadata
import org.wfanet.measurement.db.duchy.testing.FakeComputationStorage
import org.wfanet.measurement.db.duchy.testing.FakeComputationsBlobDb
import org.wfanet.measurement.db.duchy.testing.FakeComputationsBlobDb.Companion.blobPath
import org.wfanet.measurement.db.duchy.testing.FakeComputationsRelationalDatabase
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.BlindPositionsRequest
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.DecryptFlagAndCountRequest
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@ExperimentalCoroutinesApi
@RunWith(JUnit4::class)
class WorkerServiceImplTest {
  private val fakeComputationStorage =
    FakeComputationStorage<SketchAggregationStage, ComputationStageDetails>()
  val fakeBlobs = mutableMapOf<String, ByteArray>()

  private val duchyNames = listOf("Alsace", "Bavaria", "Carinthia")
  val fakeDuchyComputationManger = SketchAggregationComputationManager(
    FakeComputationsRelationalDatabase(
      fakeComputationStorage,
      SketchAggregationStages,
      SketchAggregationStageDetails(duchyNames.subList(1, duchyNames.size))
    ),
    FakeComputationsBlobDb(fakeBlobs),
    duchyNames.size
  )

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(WorkerServiceImpl(fakeDuchyComputationManger))
  }

  lateinit var client: WorkerServiceGrpcKt.WorkerServiceCoroutineStub

  @Before
  fun setup() {
    client = WorkerServiceGrpcKt.WorkerServiceCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `receive blind positions with partial sketches at primary`() = runBlocking<Unit> {
    val id = 123L
    fakeComputationStorage
      .addComputation(
        id = id,
        stage = SketchAggregationStage.WAIT_CONCATENATED,
        role = DuchyRole.PRIMARY,
        blobs = mutableMapOf(FakeBlobMetadata(0L, BlobDependencyType.OUTPUT) to null)
      )
    val token = assertNotNull(fakeDuchyComputationManger.getToken(id))
    val part1 = BlindPositionsRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part1_".toByteString())
      .build()
    val part2 = BlindPositionsRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part2_".toByteString())
      .build()
    val part3 = BlindPositionsRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part3".toByteString())
      .build()
    ProtoTruth.assertThat(client.blindPositions(flowOf(part1, part2, part3)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(fakeDuchyComputationManger.getToken(id))
    assertEquals(
      token.copy(
        stage = SketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS, lastUpdateTime = 2
      ),
      tokenAfter
    )
    assertEquals(
      mapOf(
        BlobRef(
          0L,
          blobPath(id, SketchAggregationStage.WAIT_CONCATENATED, "concatenated_sketch")
        ) to "part1_part2_part3"
      ),
      fakeDuchyComputationManger.readInputBlobs(tokenAfter).mapValues {
        it.value.toString(Charset.defaultCharset())
      }
    )
  }

  @Test
  fun `receive blind positions at secondary`() = runBlocking<Unit> {
    val id = 4567L
    fakeComputationStorage
      .addComputation(
        id = id,
        stage = SketchAggregationStage.WAIT_CONCATENATED,
        role = DuchyRole.SECONDARY,
        blobs = mutableMapOf(FakeBlobMetadata(0L, BlobDependencyType.OUTPUT) to null)
      )
    val token = assertNotNull(fakeDuchyComputationManger.getToken(id))
    val sketch = BlindPositionsRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("full_sketch".toByteString())
      .build()
    ProtoTruth.assertThat(client.blindPositions(flowOf(sketch)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(fakeDuchyComputationManger.getToken(id))
    assertEquals(
      token.copy(stage = SketchAggregationStage.TO_BLIND_POSITIONS, lastUpdateTime = 2),
      tokenAfter
    )
    assertEquals(
      mapOf(
        BlobRef(
          0L,
          blobPath(id, SketchAggregationStage.WAIT_CONCATENATED, "concatenated_sketch")
        ) to "full_sketch"
      ),
      fakeDuchyComputationManger.readInputBlobs(tokenAfter).mapValues {
        it.value.toString(Charset.defaultCharset())
      }
    )
  }

  @Test
  fun `receive blind positions when not expected`() = runBlocking<Unit> {
    fakeComputationStorage.addComputation(
      id = 55,
      stage = SketchAggregationStage.WAIT_FLAG_COUNTS,
      role = DuchyRole.SECONDARY,
      blobs = mutableMapOf()
    )
    val sketch = BlindPositionsRequest.newBuilder()
      .setComputationId(55)
      .setPartialSketch("full_sketch".toByteString())
      .build()
    assertFailsWith<StatusException> { client.blindPositions(flowOf(sketch)) }
  }

  @Test
  fun `receive decrypt flags at primary`() = runBlocking<Unit> {
    val id = 111213L
    fakeComputationStorage
      .addComputation(
        id = id,
        stage = SketchAggregationStage.WAIT_FLAG_COUNTS,
        role = DuchyRole.PRIMARY,
        blobs = mutableMapOf(FakeBlobMetadata(0L, BlobDependencyType.OUTPUT) to null)
      )
    val token = assertNotNull(fakeDuchyComputationManger.getToken(id))
    val sketch = DecryptFlagAndCountRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("full_sketch".toByteString())
      .build()
    ProtoTruth.assertThat(client.decryptFlagAndCount(flowOf(sketch)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(fakeDuchyComputationManger.getToken(id))
    assertEquals(
      token.copy(
        stage = SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS,
        lastUpdateTime = 2
      ),
      tokenAfter
    )
    assertEquals(
      mapOf(
        BlobRef(
          0L,
          blobPath(id, SketchAggregationStage.WAIT_FLAG_COUNTS, "encrypted_flag_counts")
        ) to "full_sketch"
      ),
      fakeDuchyComputationManger.readInputBlobs(tokenAfter).mapValues {
        it.value.toString(Charset.defaultCharset())
      }
    )
  }

  @Test
  fun `receive partial encrypted flags at secondary`() = runBlocking<Unit> {
    val id = 123L
    fakeComputationStorage
      .addComputation(
        id = id,
        stage = SketchAggregationStage.WAIT_FLAG_COUNTS,
        role = DuchyRole.SECONDARY,
        blobs = mutableMapOf(FakeBlobMetadata(0L, BlobDependencyType.OUTPUT) to null)
      )
    val token = assertNotNull(fakeDuchyComputationManger.getToken(id))
    val part1 = DecryptFlagAndCountRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part1_".toByteString())
      .build()
    val part2 = DecryptFlagAndCountRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part2_".toByteString())
      .build()
    val part3 = DecryptFlagAndCountRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part3".toByteString())
      .build()
    ProtoTruth.assertThat(client.decryptFlagAndCount(flowOf(part1, part2, part3)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(fakeDuchyComputationManger.getToken(id))
    assertEquals(
      token.copy(stage = SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS, lastUpdateTime = 2),
      tokenAfter
    )
    assertEquals(
      mapOf(
        BlobRef(
          0L,
          blobPath(id, SketchAggregationStage.WAIT_FLAG_COUNTS, "encrypted_flag_counts")
        ) to "part1_part2_part3"
      ),
      fakeDuchyComputationManger.readInputBlobs(tokenAfter).mapValues {
        it.value.toString(Charset.defaultCharset())
      }
    )
  }

  @Test
  fun `receive decrypt flags when not expected`() = runBlocking<Unit> {
    fakeComputationStorage.addComputation(
      id = 55,
      stage = SketchAggregationStage.WAIT_SKETCHES,
      role = DuchyRole.SECONDARY,
      blobs = mutableMapOf()
    )
    val sketch = DecryptFlagAndCountRequest.newBuilder()
      .setComputationId(55)
      .setPartialSketch("data".toByteString())
      .build()
    assertFailsWith<StatusException> { client.decryptFlagAndCount(flowOf(sketch)) }
  }
}

private fun String.toByteString(): ByteString = ByteString.copyFrom(this.toByteArray())
