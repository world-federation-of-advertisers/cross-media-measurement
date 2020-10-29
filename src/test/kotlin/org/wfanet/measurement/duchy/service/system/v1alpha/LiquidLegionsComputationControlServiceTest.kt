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

package org.wfanet.measurement.duchy.service.system.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.identity.testing.SenderContext
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.duchy.db.computation.LiquidLegionsSketchAggregationComputationStorageClients as LiquidLegionsClients
import org.wfanet.measurement.duchy.db.computation.testing.FakeLiquidLegionsComputationDb
import org.wfanet.measurement.duchy.db.computation.toBlobRef
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.service.internal.computation.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computation.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.toGetTokenRequest
import org.wfanet.measurement.duchy.service.system.v1alpha.testing.buildConcatenatedSketchRequests
import org.wfanet.measurement.duchy.service.system.v1alpha.testing.buildEncryptedFlagsAndCountsRequests
import org.wfanet.measurement.duchy.service.system.v1alpha.testing.buildNoisedSketchRequests
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_SKETCHES
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub

private const val RUNNING_DUCHY_NAME = "Alsace"
private const val BAVARIA = "Bavaria"
private const val CARINTHIA = "Carinthia"
private val OTHER_DUCHY_NAMES = listOf(BAVARIA, CARINTHIA)

@RunWith(JUnit4::class)
class LiquidLegionsComputationControlServiceTest {
  private val fakeComputationDb = FakeLiquidLegionsComputationDb()
  private val duchyIdSetter = DuchyIdSetter(RUNNING_DUCHY_NAME, *OTHER_DUCHY_NAMES.toTypedArray())
  private val tempDirectory = TemporaryFolder()
  private val grpcTestServer = GrpcTestServerRule {
    addService(
      ComputationsService(
        fakeComputationDb,
        globalComputationsClient,
        RUNNING_DUCHY_NAME
      )
    )

    computationStore =
      ComputationStore.forTesting(FileSystemStorageClient(tempDirectory.root)) { generateBlobKey() }
    computationStorageClients = LiquidLegionsClients.forTesting(
      ComputationsCoroutineStub(channel),
      computationStore,
      OTHER_DUCHY_NAMES
    )
  }

  @get:Rule
  val ruleChain = chainRulesSequentially(tempDirectory, duchyIdSetter, grpcTestServer)

  private val globalComputationsClient: GlobalComputationsCoroutineStub =
    GlobalComputationsCoroutineStub(grpcTestServer.channel)
  private val computationStorageClient =
    ComputationsCoroutineStub(grpcTestServer.channel)
      .withDuchyId(RUNNING_DUCHY_NAME)
  private lateinit var computationStorageClients: LiquidLegionsClients
  private lateinit var computationStore: ComputationStore

  private lateinit var bavaria: DuchyIdentity
  private lateinit var carinthia: DuchyIdentity

  private val blobCount = AtomicInteger()
  private val generatedBlobKeys = mutableListOf<String>()
  private fun ComputationToken.generateBlobKey(): String {
    return listOf(
      localComputationId,
      computationStage.name,
      blobCount.getAndIncrement()
    ).joinToString("/").also { generatedBlobKeys.add(it) }
  }

  private lateinit var senderContext: SenderContext<LiquidLegionsComputationControlService>
  private suspend fun <R> withSender(
    sender: DuchyIdentity,
    rpcCall: suspend LiquidLegionsComputationControlService.() -> R
  ) = senderContext.withSender(sender, rpcCall)

  @Before
  fun initService() {
    bavaria = DuchyIdentity(BAVARIA)
    carinthia = DuchyIdentity(CARINTHIA)
    senderContext =
      SenderContext { duchyIdProvider ->
        LiquidLegionsComputationControlService(
          computationStorageClients,
          duchyIdProvider
        )
      }
  }

  @Test
  fun `receive sketches`() = runBlocking<Unit> {
    val partialToken = FakeLiquidLegionsComputationDb.newPartialToken(
      localId = 252525,
      stage = WAIT_SKETCHES.toProtocolStage()
    ).build()
    val id = partialToken.globalComputationId
    computationStore.write(partialToken, ByteString.copyFromUtf8("local_sketches"))
    fakeComputationDb.addComputation(
      partialToken.localComputationId,
      partialToken.computationStage,
      role = RoleInComputation.PRIMARY,
      blobs = listOf(
        newInputBlobMetadata(id = 0, key = generatedBlobKeys.last()),
        newEmptyOutputBlobMetadata(id = 1),
        newEmptyOutputBlobMetadata(id = 2)
      ),
      stageDetails = computationStorageClients.liquidLegionsStageDetails.detailsFor(WAIT_SKETCHES)
    )

    assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest()))
    val requests = buildNoisedSketchRequests(id, "part1_", "part2_", "part3")
    assertThat(withSender(bavaria) { processNoisedSketch(requests.asFlow()) })
      .isEqualToDefaultInstance()
    val tokenAfter =
      assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token

    assertThat(tokenAfter.computationStage).isEqualTo(WAIT_SKETCHES.toProtocolStage())

    val fullSketchRequests = buildNoisedSketchRequests(id, "full_sketch_fit_in_message")
    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      assertThat(withSender(carinthia) { processNoisedSketch(fullSketchRequests.asFlow()) })
        .isEqualToDefaultInstance()
      val tokenAfterSecondSketch =
        assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token

      val localInputToAddNoise = newInputBlobMetadata(id = 0L, key = generatedBlobKeys[0])
      val sender1InputToAddNoise = newInputBlobMetadata(id = 1L, key = generatedBlobKeys[1])
      val sender2InputToAddNoise = newInputBlobMetadata(id = 2L, key = generatedBlobKeys[2])
      val outputOfToAddNoise = newEmptyOutputBlobMetadata(id = 3L)

      assertThat(tokenAfterSecondSketch).isEqualTo(
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
      assertThat(computationStorageClients.readInputBlobs(tokenAfterSecondSketch))
        .containsExactlyEntriesIn(
          mapOf(
            localInputToAddNoise to "local_sketches",
            sender1InputToAddNoise to "part1_part2_part3",
            sender2InputToAddNoise to "full_sketch_fit_in_message"
          ).mapKeys { it.key.toBlobRef() }.mapValues { ByteString.copyFromUtf8(it.value) }
        )
    }
  }

  @Test
  fun `receive noised sketch when not expected`() = runBlocking<Unit> {
    val id = "55"
    val requests = buildNoisedSketchRequests(id, "data")
    val notFound = assertFailsWith<StatusRuntimeException> {
      withSender(carinthia) { processNoisedSketch(requests.asFlow()) }
    }
    assertThat(notFound.status.code).isEqualTo(Status.Code.NOT_FOUND)
    fakeComputationDb.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS.toProtocolStage(),
      role = RoleInComputation.PRIMARY,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
    )
    val exception = assertFailsWith<StatusRuntimeException> {
      withSender(carinthia) { processNoisedSketch(requests.asFlow()) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `bad request stream`() = runBlocking<Unit> {
    val exception = assertFailsWith<StatusRuntimeException> {
      withSender(carinthia) { processNoisedSketch(flowOf()) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Empty request stream")
  }

  @Test
  fun `receive blind positions with partial sketches at primary`() = runBlocking<Unit> {
    val id = "123"
    fakeComputationDb
      .addComputation(
        globalId = id,
        stage = LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED.toProtocolStage(),
        role = RoleInComputation.PRIMARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token =
      assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token
    val requests = buildConcatenatedSketchRequests(id, "part1_", "part2_", "part3")
    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      assertThat(withSender(carinthia) { processConcatenatedSketch(requests.asFlow()) })
        .isEqualToDefaultInstance()
      val tokenAfter =
        assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token
      val input = newInputBlobMetadata(id = 0, key = generatedBlobKeys[0])
      val output = newEmptyOutputBlobMetadata(id = 1)

      assertThat(tokenAfter).isEqualTo(
        copyOf(token, withBlobs = listOf(input, output))
          .setComputationStage(TO_BLIND_POSITIONS_AND_JOIN_REGISTERS.toProtocolStage())
          .setVersion(2)
          .build()
      )
      assertThat(computationStorageClients.readInputBlobs(tokenAfter)).containsExactly(
        input.toBlobRef(),
        ByteString.copyFromUtf8("part1_part2_part3")
      )
    }
  }

  @Test
  fun `receive blind positions at secondary`() = runBlocking<Unit> {
    val id = "4567"
    fakeComputationDb
      .addComputation(
        globalId = id,
        stage = LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED.toProtocolStage(),
        role = RoleInComputation.SECONDARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token =
      assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token
    val requests = buildConcatenatedSketchRequests(id, "full_sketch")
    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      assertThat(withSender(carinthia) { processConcatenatedSketch(requests.asFlow()) })
        .isEqualToDefaultInstance()
      val tokenAfter =
        assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token
      val input = newInputBlobMetadata(id = 0, key = generatedBlobKeys[0])
      val output = newEmptyOutputBlobMetadata(id = 1)

      assertThat(tokenAfter).isEqualTo(
        copyOf(token, withBlobs = listOf(input, output))
          .setComputationStage(TO_BLIND_POSITIONS.toProtocolStage())
          .setVersion(2)
          .build()
      )
      assertThat(computationStorageClients.readInputBlobs(tokenAfter)).containsExactly(
        input.toBlobRef(),
        ByteString.copyFromUtf8("full_sketch")
      )
    }
  }

  @Test
  fun `receive concatenated sketch when not expected`() = runBlocking<Unit> {
    val id = "55"
    val requests = buildConcatenatedSketchRequests(id, "full_sketch")
    val notFound = assertFailsWith<StatusRuntimeException> {
      withSender(carinthia) { processConcatenatedSketch(requests.asFlow()) }
    }
    assertThat(notFound.status.code).isEqualTo(Status.Code.NOT_FOUND)
    fakeComputationDb.addComputation(
      globalId = id,
      stage = LiquidLegionsSketchAggregationStage.TO_ADD_NOISE.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
    )
    val exception = assertFailsWith<StatusRuntimeException> {
      withSender(carinthia) { processConcatenatedSketch(requests.asFlow()) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `receive decrypt flags at primary`() = runBlocking<Unit> {
    val id = "111213"
    fakeComputationDb
      .addComputation(
        globalId = id,
        stage = LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS.toProtocolStage(),
        role = RoleInComputation.PRIMARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token =
      assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token
    val requests = buildEncryptedFlagsAndCountsRequests(id, "full_sketch")

    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      assertThat(withSender(carinthia) { processEncryptedFlagsAndCounts(requests.asFlow()) })
        .isEqualToDefaultInstance()
      val tokenAfter =
        assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token
      val input = newInputBlobMetadata(id = 0, key = generatedBlobKeys[0])
      val output = newEmptyOutputBlobMetadata(id = 1)

      assertThat(tokenAfter).isEqualTo(
        copyOf(token, withBlobs = listOf(input, output))
          .setComputationStage(TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS.toProtocolStage())
          .setVersion(2)
          .build()
      )
      assertThat(computationStorageClients.readInputBlobs(tokenAfter)).containsExactly(
        input.toBlobRef(),
        ByteString.copyFromUtf8("full_sketch")
      )
    }
  }

  @Test
  fun `receive partial encrypted flags at secondary`() = runBlocking<Unit> {
    val id = "123"
    fakeComputationDb
      .addComputation(
        globalId = id,
        stage = LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS.toProtocolStage(),
        role = RoleInComputation.SECONDARY,
        blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
      )
    val token =
      assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token
    val requests = buildEncryptedFlagsAndCountsRequests(id, "part1_", "part2_", "part3")
    // Test the idempotency of receiving the same request by sending and checking results multiple
    // times. The token should be the same each time because only the first request makes changes
    // to the database.
    repeat(times = 2) {
      assertThat(
        withSender(carinthia) { processEncryptedFlagsAndCounts(requests.asFlow()) }
      ).isEqualToDefaultInstance()
      val tokenAfter =
        assertNotNull(computationStorageClient.getComputationToken(id.toGetTokenRequest())).token
      val input = newInputBlobMetadata(id = 0, key = generatedBlobKeys[0])
      val output = newEmptyOutputBlobMetadata(id = 1)

      assertThat(tokenAfter).isEqualTo(
        copyOf(token, withBlobs = listOf(input, output))
          .setComputationStage(TO_DECRYPT_FLAG_COUNTS.toProtocolStage())
          .setVersion(2)
          .build()
      )
      assertThat(computationStorageClients.readInputBlobs(tokenAfter)).containsExactly(
        input.toBlobRef(),
        ByteString.copyFromUtf8("part1_part2_part3")
      )
    }
  }

  @Test
  fun `receive decrypt flags when not expected`() = runBlocking<Unit> {
    val id = "55"
    val requests = buildEncryptedFlagsAndCountsRequests(id, "data")
    val notFound = assertFailsWith<StatusRuntimeException> {
      withSender(carinthia) { processEncryptedFlagsAndCounts(requests.asFlow()) }
    }
    assertThat(notFound.status.code).isEqualTo(Status.Code.NOT_FOUND)
    fakeComputationDb.addComputation(
      globalId = id,
      stage = WAIT_SKETCHES.toProtocolStage(),
      role = RoleInComputation.SECONDARY,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0))
    )
    val exception = assertFailsWith<StatusRuntimeException> {
      withSender(carinthia) { processEncryptedFlagsAndCounts(requests.asFlow()) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}

private fun copyOf(
  token: ComputationToken,
  withBlobs: Iterable<ComputationStageBlobMetadata> = listOf()
): ComputationToken.Builder = token.toBuilder().clearBlobs().addAllBlobs(withBlobs)
