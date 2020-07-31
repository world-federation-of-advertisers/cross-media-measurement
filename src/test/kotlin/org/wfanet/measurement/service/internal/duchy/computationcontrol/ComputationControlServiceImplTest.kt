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

package org.wfanet.measurement.service.internal.duchy.computationcontrol

import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.ByteString
import io.grpc.StatusException
import java.nio.charset.Charset
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
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
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.SketchAggregationComputationManager
import org.wfanet.measurement.db.duchy.SketchAggregationStageDetails
import org.wfanet.measurement.db.duchy.SketchAggregationStages
import org.wfanet.measurement.db.duchy.testing.FakeBlobMetadata
import org.wfanet.measurement.db.duchy.testing.FakeComputationStorage
import org.wfanet.measurement.db.duchy.testing.FakeComputationsBlobDb
import org.wfanet.measurement.db.duchy.testing.FakeComputationsRelationalDatabase
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsRequest
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class ComputationControlServiceImplTest {
  private val fakeComputationStorage =
    FakeComputationStorage<SketchAggregationStage, ComputationStageDetails>()
  private val fakeBlobs = mutableMapOf<String, ByteArray>()

  /** Always return the same numbers. */
  class FakeRandom : Random() {
    override fun nextBits(bitCount: Int): Int = 42
  }

  private val duchyNames = listOf("Alsace", "Bavaria", "Carinthia")
  private val fakeDuchyComputationManager = SketchAggregationComputationManager(
    FakeComputationsRelationalDatabase(
      fakeComputationStorage,
      SketchAggregationStages,
      SketchAggregationStageDetails(duchyNames.subList(1, duchyNames.size))
    ),
    FakeComputationsBlobDb(fakeBlobs),
    duchyNames.size,
    FakeRandom()
  )

  /** Blob name made deterministic with fake RNG. */
  // fun <StageT> blobPath(id: Long, stage: StageT, name: String): String = "$id-$stage-$name"
  private fun blobPath(id: Long, stage: SketchAggregationStage, name: String): String =
    fakeDuchyComputationManager.newBlobPath(
      ComputationToken<SketchAggregationStage>(
        localId = id,
        globalId = 0L,
        stage = stage,
        owner = "",
        nextWorker = "",
        role = DuchyRole.PRIMARY,
        attempt = 0L,
        lastUpdateTime = 0L
      ),
      name,
      FakeRandom()
    )

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(ComputationControlServiceImpl(fakeDuchyComputationManager))
  }

  lateinit var client: ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub

  @Before
  fun setup() {
    client =
      ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `receive sketches`() = runBlocking<Unit> {
    val id = 252525L
    val sender = duchyNames[1]
    val localSketches =
      blobPath(id, SketchAggregationStage.WAIT_SKETCHES, "noised_sketch_${duchyNames[0]}")
    fakeBlobs[localSketches] = "local_sketches".toByteArray()
    fakeComputationStorage
      .addComputation(
        id = id,
        stage = SketchAggregationStage.WAIT_SKETCHES,
        role = DuchyRole.PRIMARY,
        blobs = mutableMapOf(
          FakeBlobMetadata(0L, BlobDependencyType.INPUT) to localSketches,
          FakeBlobMetadata(1L, BlobDependencyType.OUTPUT) to null,
          FakeBlobMetadata(2L, BlobDependencyType.OUTPUT) to null
        )
      )
    val token = assertNotNull(fakeDuchyComputationManager.getToken(id))
    val part1 = HandleNoisedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part1_".toByteString())
      .setSender(sender)
      .build()
    val part2 = HandleNoisedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part2_".toByteString())
      .setSender(sender)
      .build()
    val part3 = HandleNoisedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part3".toByteString())
      .setSender(sender)
      .build()
    ProtoTruth.assertThat(client.handleNoisedSketch(flowOf(part1, part2, part3)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(fakeDuchyComputationManager.getToken(id))
    assertEquals(
      // the stage is the same because we are waiting for more sketches
      token.copy(lastUpdateTime = 1),
      tokenAfter
    )

    val secondSender = duchyNames[2]
    val fullSketch = HandleNoisedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("full_sketch_fit_in_message".toByteString())
      .setSender(secondSender)
      .build()
    ProtoTruth.assertThat(client.handleNoisedSketch(flowOf(fullSketch)))
      .isEqualToDefaultInstance()
    val tokenAfterSecondSketch = assertNotNull(fakeDuchyComputationManager.getToken(id))
    assertEquals(
      token.copy(
        stage = SketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE,
        lastUpdateTime = 3
      ),
      tokenAfterSecondSketch
    )
    assertEquals(
      mapOf(
        BlobRef(0L, localSketches) to "local_sketches",
        BlobRef(
          1L,
          blobPath(id, SketchAggregationStage.WAIT_SKETCHES, "noised_sketch_$sender")
        ) to "part1_part2_part3",
        BlobRef(
          2L,
          blobPath(id, SketchAggregationStage.WAIT_SKETCHES, "noised_sketch_$secondSender")
        ) to "full_sketch_fit_in_message"
      ),
      fakeDuchyComputationManager.readInputBlobs(tokenAfter).mapValues {
        it.value.toString(Charset.defaultCharset())
      }
    )
  }

  @Test
  fun `receive noised sketch when not expected`() = runBlocking<Unit> {
    fakeComputationStorage.addComputation(
      id = 55,
      stage = SketchAggregationStage.CREATED,
      role = DuchyRole.PRIMARY,
      blobs = mutableMapOf(FakeBlobMetadata(0L, BlobDependencyType.OUTPUT) to null)
    )
    val sketch = HandleNoisedSketchRequest.newBuilder()
      .setComputationId(55)
      .setPartialSketch("data".toByteString())
      .setSender(duchyNames.last())
      .build()
    assertFailsWith<StatusException> { client.handleNoisedSketch(flowOf(sketch)) }
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
    val token = assertNotNull(fakeDuchyComputationManager.getToken(id))
    val part1 = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part1_".toByteString())
      .build()
    val part2 = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part2_".toByteString())
      .build()
    val part3 = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part3".toByteString())
      .build()
    ProtoTruth.assertThat(client.handleConcatenatedSketch(flowOf(part1, part2, part3)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(fakeDuchyComputationManager.getToken(id))
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
      fakeDuchyComputationManager.readInputBlobs(tokenAfter).mapValues {
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
    val token = assertNotNull(fakeDuchyComputationManager.getToken(id))
    val sketch = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("full_sketch".toByteString())
      .build()
    ProtoTruth.assertThat(client.handleConcatenatedSketch(flowOf(sketch)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(fakeDuchyComputationManager.getToken(id))
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
      fakeDuchyComputationManager.readInputBlobs(tokenAfter).mapValues {
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
      blobs = mutableMapOf(FakeBlobMetadata(0L, BlobDependencyType.OUTPUT) to null)
    )
    val sketch = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId(55)
      .setPartialSketch("full_sketch".toByteString())
      .build()
    assertFailsWith<StatusException> { client.handleConcatenatedSketch(flowOf(sketch)) }
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
    val token = assertNotNull(fakeDuchyComputationManager.getToken(id))
    val sketch = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId(id)
      .setPartialData("full_sketch".toByteString())
      .build()
    ProtoTruth.assertThat(client.handleEncryptedFlagsAndCounts(flowOf(sketch)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(fakeDuchyComputationManager.getToken(id))
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
      fakeDuchyComputationManager.readInputBlobs(tokenAfter).mapValues {
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
    val token = assertNotNull(fakeDuchyComputationManager.getToken(id))
    val part1 = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId(id)
      .setPartialData("part1_".toByteString())
      .build()
    val part2 = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId(id)
      .setPartialData("part2_".toByteString())
      .build()
    val part3 = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId(id)
      .setPartialData("part3".toByteString())
      .build()
    ProtoTruth.assertThat(client.handleEncryptedFlagsAndCounts(flowOf(part1, part2, part3)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(fakeDuchyComputationManager.getToken(id))
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
      fakeDuchyComputationManager.readInputBlobs(tokenAfter).mapValues {
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
      blobs = mutableMapOf(FakeBlobMetadata(0L, BlobDependencyType.OUTPUT) to null)
    )
    val sketch = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId(55)
      .setPartialData("data".toByteString())
      .build()
    assertFailsWith<StatusException> { client.handleEncryptedFlagsAndCounts(flowOf(sketch)) }
  }
}

private fun String.toByteString(): ByteString = ByteString.copyFrom(this.toByteArray())
