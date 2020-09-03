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

package org.wfanet.measurement.service.internal.duchy.computation.control

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import java.nio.charset.Charset
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
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.identity.withDuchyIdentities
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.computation.testing.FakeComputationStorage
import org.wfanet.measurement.db.duchy.computation.testing.FakeComputationsBlobDb
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_SKETCHES
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

@RunWith(JUnit4::class)
class LiquidLegionsComputationControlServiceImplTest {

  @get:Rule
  val duchyIdSetter = DuchyIdSetter(RUNNING_DUCHY_NAME, *otherDuchyNames.toTypedArray())
  private val fakeBlobs = mutableMapOf<String, ByteArray>()

  companion object {
    private const val RUNNING_DUCHY_NAME = "Alsace"
    private val otherDuchyNames = listOf("Bavaria", "Carinthia")
  }

  private val fakeComputationStorage = FakeComputationStorage(otherDuchyNames)

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    computationStorageClients = LiquidLegionsSketchAggregationComputationStorageClients(
      ComputationStorageServiceCoroutineStub(channel),
      FakeComputationsBlobDb(fakeBlobs),
      otherDuchyNames
    )

    addService(
      LiquidLegionsComputationControlServiceImpl(computationStorageClients)
        .withDuchyIdentities()
    )

    addService(ComputationStorageServiceImpl(fakeComputationStorage))
  }

  private lateinit var computationStorageClients:
    LiquidLegionsSketchAggregationComputationStorageClients
  private lateinit var storageClient: ComputationStorageServiceCoroutineStub
  lateinit var bavariaClient: ComputationControlServiceCoroutineStub
  lateinit var carinthiaClient: ComputationControlServiceCoroutineStub

  @Before
  fun setup() {
    storageClient =
      ComputationStorageServiceCoroutineStub(grpcTestServerRule.channel)
        .withDuchyId(RUNNING_DUCHY_NAME)

    bavariaClient =
      ComputationControlServiceCoroutineStub(grpcTestServerRule.channel)
        .withDuchyId(otherDuchyNames[0])

    carinthiaClient =
      ComputationControlServiceCoroutineStub(grpcTestServerRule.channel)
        .withDuchyId(otherDuchyNames[1])
  }

  @Test
  fun `receive sketches`() = runBlocking<Unit> {
    val id = "252525"
    val localSketches = "$id/$WAIT_SKETCHES/noised_sketch_$RUNNING_DUCHY_NAME"
    fakeBlobs[localSketches] = "local_sketches".toByteArray()
    fakeComputationStorage
      .addComputation(
        globalId = id,
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
      .build()
    val part2 = HandleNoisedSketchRequest.newBuilder()
      .setPartialSketch("part2_".toByteString())
      .build()
    val part3 = HandleNoisedSketchRequest.newBuilder()
      .setPartialSketch("part3".toByteString())
      .build()
    ProtoTruth.assertThat(bavariaClient.handleNoisedSketch(flowOf(part1, part2, part3)))
      .isEqualToDefaultInstance()
    val tokenAfter = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token

    ProtoTruth.assertThat(tokenAfter.computationStage).isEqualTo(WAIT_SKETCHES.toProtocolStage())

    val fullSketch = HandleNoisedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("full_sketch_fit_in_message".toByteString())
      .build()
    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      ProtoTruth.assertThat(carinthiaClient.handleNoisedSketch(flowOf(fullSketch)))
        .isEqualToDefaultInstance()
      val tokenAfterSecondSketch =
        assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token

      val localInputToAddNoise = newInputBlobMetadata(id = 0L, key = localSketches)
      val sender1InputToAddNoise =
        newInputBlobMetadata(id = 1L, key = token.toBlobPath("noised_sketch_Bavaria"))
      val sender2InputToAddNoise =
        newInputBlobMetadata(id = 2L, key = token.toBlobPath("noised_sketch_Carinthia"))
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
  }

  @Test
  fun `receive noised sketch when not expected`() = runBlocking<Unit> {
    val sketch = HandleNoisedSketchRequest.newBuilder()
      .setComputationId("55")
      .setPartialSketch("data".toByteString())
      .build()
    val notFound =
      assertFailsWith<StatusException> { carinthiaClient.handleNoisedSketch(flowOf(sketch)) }
    assertThat(notFound.status.code).isEqualTo(Status.Code.NOT_FOUND)
    fakeComputationStorage.addComputation(
      globalId = "55",
      stage = LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS.toProtocolStage(),
      role = RoleInComputation.PRIMARY,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
    )
    val exception =
      assertFailsWith<StatusException> { carinthiaClient.handleNoisedSketch(flowOf(sketch)) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `bad request stream`() = runBlocking<Unit> {
    val exception =
      assertFailsWith<StatusException> { carinthiaClient.handleNoisedSketch(flowOf()) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Empty request stream")
  }

  @Test
  fun `receive blind positions with partial sketches at primary`() = runBlocking<Unit> {
    val id = "123"
    fakeComputationStorage
      .addComputation(
        globalId = id,
        stage = LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED.toProtocolStage(),
        role = RoleInComputation.PRIMARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val part1 = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("part1_".toByteString())
      .build()
    val part2 = HandleConcatenatedSketchRequest.newBuilder()
      .setPartialSketch("part2_".toByteString())
      .build()
    val part3 = HandleConcatenatedSketchRequest.newBuilder()
      .setPartialSketch("part3".toByteString())
      .build()
    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      ProtoTruth.assertThat(carinthiaClient.handleConcatenatedSketch(flowOf(part1, part2, part3)))
        .isEqualToDefaultInstance()
      val tokenAfter =
        assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
      val input = newInputBlobMetadata(id = 0, key = token.toBlobPath("output"))
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
  }

  @Test
  fun `receive blind positions at secondary`() = runBlocking<Unit> {
    val id = "4567"
    fakeComputationStorage
      .addComputation(
        globalId = id,
        stage = LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED.toProtocolStage(),
        role = RoleInComputation.SECONDARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val sketch = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId(id)
      .setPartialSketch("full_sketch".toByteString())
      .build()
    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      ProtoTruth.assertThat(carinthiaClient.handleConcatenatedSketch(flowOf(sketch)))
        .isEqualToDefaultInstance()
      val tokenAfter =
        assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
      val input = newInputBlobMetadata(id = 0, key = token.toBlobPath("output"))
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
  }

  @Test
  fun `receive concatenated sketch when not expected`() = runBlocking<Unit> {
    val sketch = HandleConcatenatedSketchRequest.newBuilder()
      .setComputationId("55")
      .setPartialSketch("full_sketch".toByteString())
      .build()
    val notFound =
      assertFailsWith<StatusException> { carinthiaClient.handleConcatenatedSketch(flowOf(sketch)) }
    assertThat(notFound.status.code).isEqualTo(Status.Code.NOT_FOUND)
    fakeComputationStorage.addComputation(
      globalId = "55",
      stage = LiquidLegionsSketchAggregationStage.TO_ADD_NOISE.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
    )
    val exception =
      assertFailsWith<StatusException> { carinthiaClient.handleConcatenatedSketch(flowOf(sketch)) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `receive decrypt flags at primary`() = runBlocking<Unit> {
    val id = "111213"
    fakeComputationStorage
      .addComputation(
        globalId = id,
        stage = LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS.toProtocolStage(),
        role = RoleInComputation.PRIMARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val sketch = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId(id)
      .setPartialData("full_sketch".toByteString())
      .build()

    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      ProtoTruth.assertThat(carinthiaClient.handleEncryptedFlagsAndCounts(flowOf(sketch)))
        .isEqualToDefaultInstance()
      val tokenAfter =
        assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
      val input = newInputBlobMetadata(id = 0, key = token.toBlobPath("output"))
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
  }

  @Test
  fun `receive partial encrypted flags at secondary`() = runBlocking<Unit> {
    val id = "123"
    fakeComputationStorage
      .addComputation(
        globalId = id,
        stage = LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS.toProtocolStage(),
        role = RoleInComputation.SECONDARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token = assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
    val part1 = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId(id)
      .setPartialData("part1_".toByteString())
      .build()
    val part2 = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setPartialData("part2_".toByteString())
      .build()
    val part3 = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setPartialData("part3".toByteString())
      .build()
    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      ProtoTruth.assertThat(
        carinthiaClient.handleEncryptedFlagsAndCounts(flowOf(part1, part2, part3))
      ).isEqualToDefaultInstance()
      val tokenAfter =
        assertNotNull(storageClient.getComputationToken(id.toGetTokenRequest())).token
      val input = newInputBlobMetadata(id = 0, key = token.toBlobPath("output"))
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
  }

  @Test
  fun `receive decrypt flags when not expected`() = runBlocking<Unit> {
    val sketch = HandleEncryptedFlagsAndCountsRequest.newBuilder()
      .setComputationId("55")
      .setPartialData("data".toByteString())
      .build()
    val notFound = assertFailsWith<StatusException> {
      carinthiaClient.handleEncryptedFlagsAndCounts(flowOf(sketch))
    }
    assertThat(notFound.status.code).isEqualTo(Status.Code.NOT_FOUND)
    fakeComputationStorage.addComputation(
      globalId = "55",
      stage = WAIT_SKETCHES.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
    )
    val exception = assertFailsWith<StatusException> {
      carinthiaClient.handleEncryptedFlagsAndCounts(flowOf(sketch))
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}

private fun String.toByteString(): ByteString = ByteString.copyFrom(this.toByteArray())

private fun copyOf(
  token: ComputationToken,
  withBlobs: Iterable<ComputationStageBlobMetadata> = listOf()
): ComputationToken.Builder = token.toBuilder().clearBlobs().addAllBlobs(withBlobs)
