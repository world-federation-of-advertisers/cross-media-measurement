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
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.duchy.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.testing.FakeComputationStorage
import org.wfanet.measurement.db.duchy.testing.FakeComputationsBlobDb
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.SketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_SKETCHES
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.HandleConcatenatedSketchRequest
import org.wfanet.measurement.internal.duchy.HandleEncryptedFlagsAndCountsRequest
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.ComputationStorageServiceImpl
import org.wfanet.measurement.service.internal.duchy.computation.storage.newEmptyOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.newInputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.toBlobPath
import org.wfanet.measurement.service.internal.duchy.computation.storage.toGetTokenRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import java.nio.charset.Charset
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

@RunWith(JUnit4::class)
class ComputationControlServiceImplTest {
  val fakeBlobs = mutableMapOf<String, ByteArray>()

  companion object {
    private const val RUNNING_DUCHY_NAME = "Alsace"
    private val otherDuchyNames = listOf("Bavaria", "Carinthia")
  }

  private val fakeComputationStorage = FakeComputationStorage(otherDuchyNames)

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { channel ->
    computationStorageClients = LiquidLegionsSketchAggregationComputationStorageClients(
      ComputationStorageServiceCoroutineStub(channel),
      FakeComputationsBlobDb(fakeBlobs),
      otherDuchyNames
    )
    listOf(
      ComputationStorageServiceImpl(
        fakeComputationStorage
      ),
      ComputationControlServiceImpl(computationStorageClients)
    )
  }

  private lateinit var computationStorageClients:
    LiquidLegionsSketchAggregationComputationStorageClients
  private lateinit var storageClient: ComputationStorageServiceCoroutineStub
  lateinit var client: ComputationControlServiceCoroutineStub

  @Before
  fun setup() {
    storageClient = ComputationStorageServiceCoroutineStub(grpcTestServerRule.channel)
    client = ComputationControlServiceCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `receive sketches`() = runBlocking<Unit> {
    val id = 252525L
    val sender = otherDuchyNames[0]
    val localSketches = "$id/$WAIT_SKETCHES/noised_sketch_$RUNNING_DUCHY_NAME"
    fakeBlobs[localSketches] = "local_sketches".toByteArray()
    fakeComputationStorage
      .addComputation(
        id = id,
        stage = WAIT_SKETCHES.toProtocolStage(),
        role = RoleInComputation.PRIMARY,
        blobs = listOf(
          newInputBlobMetadata(id = 0, key = localSketches),
          newEmptyOutputBlobMetadata(id = 1),
          newEmptyOutputBlobMetadata(id = 2)
        ),
        stageDetails = computationStorageClients
          .liquidLegionsStageDetails.detailsFor(WAIT_SKETCHES)
      )
    val token = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
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
    val tokenAfter = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token

    ProtoTruth.assertThat(tokenAfter.computationStage).isEqualTo(WAIT_SKETCHES.toProtocolStage())

    val secondSender = otherDuchyNames[1]
    val fullSketch = HandleNoisedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("full_sketch_fit_in_message".toByteString())
      .setSender(secondSender)
      .build()
    ProtoTruth.assertThat(client.handleNoisedSketch(flowOf(fullSketch)))
      .isEqualToDefaultInstance()
    val tokenAfterSecondSketch =
      assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token

    val localInputToAddNoise = newInputBlobMetadata(id = 0L, key = localSketches)
    val sender1InputToAddNoise =
      newInputBlobMetadata(id = 1L, key = token.toBlobPath("noised_sketch_$sender"))
    val sender2InputToAddNoise =
      newInputBlobMetadata(id = 2L, key = token.toBlobPath("noised_sketch_$secondSender"))
    val outputOfToAddNoise = newEmptyOutputBlobMetadata(id = 3L)

    ProtoTruth.assertThat(tokenAfterSecondSketch).isEqualTo(
      copyOf(
        tokenAfter,
        withBlobs = listOf(
          localInputToAddNoise,
          sender1InputToAddNoise,
          sender2InputToAddNoise,
          outputOfToAddNoise
        )
      )
        .setComputationStage(TO_APPEND_SKETCHES_AND_ADD_NOISE.toProtocolStage())
        .setVersion(3)
        .clearStageSpecificDetails()
        .build()
    )
    assertEquals(
      mapOf(
        localInputToAddNoise to "local_sketches",
        sender1InputToAddNoise to "part1_part2_part3",
        sender2InputToAddNoise to "full_sketch_fit_in_message"
      ),
      computationStorageClients.readInputBlobs(tokenAfterSecondSketch).mapValues {
        it.value.toString(Charset.defaultCharset())
      }
    )
  }

  @Test
  fun `receive noised sketch when not expected`() = runBlocking<Unit> {
    fakeComputationStorage.addComputation(
      id = 55,
      stage = SketchAggregationStage.CREATED.toProtocolStage(),
      role = RoleInComputation.PRIMARY,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
    )
    val sketch = HandleNoisedSketchRequest.newBuilder()
      .setComputationId(55)
      .setPartialSketch("data".toByteString())
      .setSender(otherDuchyNames.last())
      .build()
    assertFailsWith<StatusException> { client.handleNoisedSketch(flowOf(sketch)) }
  }

  @Test
  fun `receive blind positions with partial sketches at primary`() = runBlocking<Unit> {
    val id = 123L
    fakeComputationStorage
      .addComputation(
        id = id,
        stage = SketchAggregationStage.WAIT_CONCATENATED.toProtocolStage(),
        role = RoleInComputation.PRIMARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
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
    val tokenAfter = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val input = newInputBlobMetadata(id = 0, key = token.toBlobPath("concatenated_sketch"))
    val output = newEmptyOutputBlobMetadata(id = 1)

    ProtoTruth.assertThat(tokenAfter).isEqualTo(
      copyOf(token, withBlobs = listOf(input, output))
        .setComputationStage(TO_BLIND_POSITIONS_AND_JOIN_REGISTERS.toProtocolStage())
        .setVersion(2)
        .build()
    )
    assertEquals(
      mapOf(input to "part1_part2_part3"),
      computationStorageClients.readInputBlobs(tokenAfter).mapValues {
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
        stage = SketchAggregationStage.WAIT_CONCATENATED.toProtocolStage(),
        role = RoleInComputation.SECONDARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val sketch = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("full_sketch".toByteString())
      .build()
    ProtoTruth.assertThat(client.handleConcatenatedSketch(flowOf(sketch)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val input = newInputBlobMetadata(id = 0, key = token.toBlobPath("concatenated_sketch"))
    val output = newEmptyOutputBlobMetadata(id = 1)

    ProtoTruth.assertThat(tokenAfter).isEqualTo(
      copyOf(token, withBlobs = listOf(input, output))
        .setComputationStage(TO_BLIND_POSITIONS.toProtocolStage())
        .setVersion(2)
        .build()
    )
    assertEquals(
      mapOf(input to "full_sketch"),
      computationStorageClients.readInputBlobs(tokenAfter).mapValues {
        it.value.toString(Charset.defaultCharset())
      }
    )
  }

  @Test
  fun `receive concatenated sketch when not expected`() = runBlocking<Unit> {
    fakeComputationStorage.addComputation(
      id = 55,
      stage = SketchAggregationStage.WAIT_FLAG_COUNTS.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
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
        stage = SketchAggregationStage.WAIT_FLAG_COUNTS.toProtocolStage(),
        role = RoleInComputation.PRIMARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val sketch = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId(id)
      .setPartialData("full_sketch".toByteString())
      .build()
    ProtoTruth.assertThat(client.handleEncryptedFlagsAndCounts(flowOf(sketch)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val input = newInputBlobMetadata(id = 0, key = token.toBlobPath("encrypted_flag_counts"))
    val output = newEmptyOutputBlobMetadata(id = 1)

    ProtoTruth.assertThat(tokenAfter).isEqualTo(
      copyOf(token, withBlobs = listOf(input, output))
        .setComputationStage(TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS.toProtocolStage())
        .setVersion(2)
        .build()
    )
    assertEquals(
      mapOf(input to "full_sketch"),
      computationStorageClients.readInputBlobs(tokenAfter).mapValues {
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
        stage = SketchAggregationStage.WAIT_FLAG_COUNTS.toProtocolStage(),
        role = RoleInComputation.SECONDARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
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
    val tokenAfter = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val input = newInputBlobMetadata(id = 0, key = token.toBlobPath("encrypted_flag_counts"))
    val output = newEmptyOutputBlobMetadata(id = 1)

    ProtoTruth.assertThat(tokenAfter).isEqualTo(
      copyOf(token, withBlobs = listOf(input, output))
        .setComputationStage(TO_DECRYPT_FLAG_COUNTS.toProtocolStage())
        .setVersion(2)
        .build()
    )
    assertEquals(
      mapOf(input to "part1_part2_part3"),
      computationStorageClients.readInputBlobs(tokenAfter).mapValues {
        it.value.toString(Charset.defaultCharset())
      }
    )
  }

  @Test
  fun `receive decrypt flags when not expected`() = runBlocking<Unit> {
    fakeComputationStorage.addComputation(
      id = 55,
      stage = WAIT_SKETCHES.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
    )
    val sketch = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId(55)
      .setPartialData("data".toByteString())
      .build()
    assertFailsWith<StatusException> { client.handleEncryptedFlagsAndCounts(flowOf(sketch)) }
  }
}

private fun String.toByteString(): ByteString = ByteString.copyFrom(this.toByteArray())

private fun copyOf(
  token: ComputationToken,
  withBlobs: Iterable<ComputationStageBlobMetadata> = listOf()
): ComputationToken.Builder = token.toBuilder().clearBlobs().addAllBlobs(withBlobs)
