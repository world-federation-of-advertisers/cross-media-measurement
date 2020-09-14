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

package org.wfanet.measurement.loadtest

import com.google.common.io.Resources
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.TextFormat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.mock
import kotlin.math.max
import kotlin.math.min
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.anysketch.SketchProtos
import org.wfanet.measurement.api.v1alpha.GlobalComputation
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineImplBase as PublisherDataCoroutineService
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.crypto.ElGamalPublicKey
import org.wfanet.measurement.duchy.testing.TestKeys
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.storage.read

private const val RUN_ID = "TEST"
private const val OUTPUT_DIR = "Correctness"
private const val COMBINED_PUBLIC_KEY_ID = "1"

@RunWith(JUnit4::class)
class CorrectnessImplTest {

  private lateinit var sketchStorageClient: FileSystemStorageClient
  private lateinit var encryptedSketchStorageClient: FileSystemStorageClient
  private lateinit var reportStorageClient: FileSystemStorageClient
  private val publisherDataServiceMock: PublisherDataCoroutineService =
    mock(useConstructor = UseConstructor.parameterless())

  @Rule
  @JvmField
  val tempDirectory = TemporaryFolder()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(publisherDataServiceMock)
  }

  private val publisherDataStub: PublisherDataCoroutineStub by lazy {
    PublisherDataCoroutineStub(grpcTestServerRule.channel)
  }

  @Before
  fun init() {
    sketchStorageClient = FileSystemStorageClient(tempDirectory.root)
    encryptedSketchStorageClient = FileSystemStorageClient(tempDirectory.root)
    reportStorageClient = FileSystemStorageClient(tempDirectory.root)
  }

  @Test
  fun `generate reach succeeds`() {
    val campaignCount = 5
    val generatedSetSize = 10
    val universeSize = 100_000L
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = campaignCount,
      generatedSetSize = generatedSetSize,
      universeSize = universeSize
    )
    val reaches = correctness.generateReach()
    var actualCount = 0
    reaches.forEach {
      actualCount++
      assertThat(it.max()).isLessThan(universeSize)
      assertThat(it.min()).isGreaterThan(-1)
      assertThat(it.size).isEqualTo(generatedSetSize)
    }
    assertThat(actualCount).isEqualTo(campaignCount)
  }

  @Test
  fun `generate single sketch succeeds`() {
    val generatedSetSize = 5
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = 1,
      generatedSetSize = generatedSetSize,
      universeSize = 10_000_000_000L
    )
    val reach = setOf(100L, 30L, 500L, 13L, 813L)
    val actualSketch = correctness.generateSketch(reach).toSketchProto(sketchConfig)
    var minIndex: Long = Long.MAX_VALUE
    var maxIndex: Long = Long.MIN_VALUE
    var sumIndex: Long = 0
    actualSketch.registersList.forEach {
      minIndex = min(minIndex, it.index)
      maxIndex = max(maxIndex, it.index)
      sumIndex += it.index
    }
    val avgIndex = sumIndex / actualSketch.registersCount
    assertThat(maxIndex).isLessThan(333_000)
    assertThat(minIndex).isGreaterThan(-1)
    assertThat(minIndex).isLessThan(avgIndex)
    assertThat(maxIndex).isGreaterThan(avgIndex)
    assertThat(actualSketch.registersCount).isEqualTo(generatedSetSize)
  }

  @Test
  fun `generate multiple sketches succeeds`() {
    val generatedSetSize = 5
    val campaignCount = 2
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = campaignCount,
      generatedSetSize = generatedSetSize,
      universeSize = 10_000_000_000L
    )
    val reaches = sequenceOf(
      setOf(100L, 30L, 500L, 13L, 813L),
      setOf(2L, 7169L, 9999L, 130L, 28193L)
    )
    reaches.forEach {
      val actualSketch = correctness.generateSketch(it).toSketchProto(sketchConfig)
      assertThat(actualSketch.registersCount).isEqualTo(generatedSetSize)
    }
  }

  @Test
  fun `generate sketch with collided registers succeeds`() {
    val generatedSetSize = 100_000 // Setting something high so we expect collisions.
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = 1,
      generatedSetSize = generatedSetSize,
      universeSize = 10_000_000_000L
    )
    val reach = correctness.generateReach().first()
    val actualSketch = correctness.generateSketch(reach).toSketchProto(sketchConfig)
    assertThat(actualSketch.registersCount).isLessThan(generatedSetSize)
  }

  @Test
  fun `store single sketch match succeeds`() = runBlocking {
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = 1,
      generatedSetSize = 1,
      universeSize = 1
    )
    val expectedSketch = Sketch.newBuilder()
      .setConfig(sketchConfig)
      .addRegisters(Sketch.Register.newBuilder().setIndex(0).addValues(12678).addValues(1))
      .build()
    val blobKey = correctness.storeSketch(SketchProtos.toAnySketch(sketchConfig, expectedSketch))
    val actualSketch =
      Sketch.parseFrom(sketchStorageClient.getBlob(blobKey)!!.read().flatten())
    assertThat(actualSketch).isEqualTo(expectedSketch)
  }

  @Test
  fun `encrypt sketch succeeds`() {
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = 1,
      generatedSetSize = 1,
      universeSize = 1
    )
    val sketch = Sketch.newBuilder()
      .setConfig(sketchConfig)
      .addRegisters(Sketch.Register.newBuilder().setIndex(0).addValues(12678).addValues(1))
      .build()
    val actualEncryptedSketch = correctness.encryptSketch(sketch)
    assertThat(actualEncryptedSketch.isEmpty).isFalse()
  }

  @Test
  fun `store encrypted sketch succeeds`() = runBlocking {
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = 1,
      generatedSetSize = 1,
      universeSize = 1
    )
    val exptectedEncryptedSketch = "sketch123"
    val blobKey =
      correctness.storeEncryptedSketch(ByteString.copyFromUtf8(exptectedEncryptedSketch))
    val actualEncryptedSketch =
      encryptedSketchStorageClient.getBlob(blobKey)?.readToString()
    assertThat(actualEncryptedSketch).isEqualTo(exptectedEncryptedSketch)
  }

  @Test
  fun `estimate cardinality succeeds`() {
    val generatedSetSize = 100_000 // Setting something high so we expect collisions.
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = 1,
      generatedSetSize = generatedSetSize,
      universeSize = 10_000_000_000L
    )
    val anySketch1 = SketchProtos.toAnySketch(
      sketchConfig,
      Sketch.newBuilder()
        .setConfig(sketchConfig)
        .addRegisters(Sketch.Register.newBuilder().setIndex(0).addValues(12678).addValues(1))
        .build()
    )
    val anySketch2 = SketchProtos.toAnySketch(
      sketchConfig,
      Sketch.newBuilder()
        .setConfig(sketchConfig)
        .addRegisters(Sketch.Register.newBuilder().setIndex(1).addValues(12678).addValues(1))
        .build()
    )
    val anySketch3 = SketchProtos.toAnySketch(
      sketchConfig,
      Sketch.newBuilder()
        .setConfig(sketchConfig)
        .addRegisters(Sketch.Register.newBuilder().setIndex(1).addValues(12678).addValues(1))
        .build()
    )
    val anySketch4 = SketchProtos.toAnySketch(
      sketchConfig,
      Sketch.newBuilder()
        .setConfig(sketchConfig)
        .addRegisters(Sketch.Register.newBuilder().setIndex(3).addValues(12678).addValues(1))
        .addRegisters(Sketch.Register.newBuilder().setIndex(0).addValues(12678).addValues(1))
        .build()
    )
    val actualCardinality =
      correctness.estimateCardinality(listOf(anySketch1, anySketch2, anySketch3, anySketch4))
    assertThat(actualCardinality).isEqualTo(4)
  }

  @Test
  fun `estimate frequency succeeds`() {
    val generatedSetSize = 100_000 // Setting something high so we expect collisions.
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = 1,
      generatedSetSize = generatedSetSize,
      universeSize = 10_000_000_000L
    )
    val anySketch1 = SketchProtos.toAnySketch(
      sketchConfig,
      Sketch.newBuilder()
        .setConfig(sketchConfig)
        .addRegisters(Sketch.Register.newBuilder().setIndex(0).addValues(12678).addValues(1))
        .build()
    )
    val anySketch2 = SketchProtos.toAnySketch(
      sketchConfig,
      Sketch.newBuilder()
        .setConfig(sketchConfig)
        .addRegisters(Sketch.Register.newBuilder().setIndex(1).addValues(12678).addValues(1))
        .build()
    )
    val anySketch3 = SketchProtos.toAnySketch(
      sketchConfig,
      Sketch.newBuilder()
        .setConfig(sketchConfig)
        .addRegisters(Sketch.Register.newBuilder().setIndex(1).addValues(12678).addValues(1))
        .build()
    )
    val anySketch4 = SketchProtos.toAnySketch(
      sketchConfig,
      Sketch.newBuilder()
        .setConfig(sketchConfig)
        .addRegisters(Sketch.Register.newBuilder().setIndex(3).addValues(12678).addValues(1))
        .addRegisters(Sketch.Register.newBuilder().setIndex(0).addValues(12678).addValues(1))
        .build()
    )
    val actualFrequency =
      correctness.estimateFrequency(listOf(anySketch1, anySketch2, anySketch3, anySketch4))
    assertThat(actualFrequency).isEqualTo(mapOf((2L to 2L), (1L to 1L)))
  }

  @Test
  fun `store estimation results succeeds`() = runBlocking {
    val correctness = makeCorrectness(
      dataProviderCount = 1,
      campaignCount = 1,
      generatedSetSize = 1,
      universeSize = 1
    )
    val reach = 34512L
    val frequency = mapOf((1L to 4L), (2L to 3L))
    val blobKey = correctness.storeEstimationResults(reach, frequency)
    val expectedComputation = GlobalComputation.newBuilder().apply {
      keyBuilder.globalComputationId = "1"
      state = GlobalComputation.State.SUCCEEDED
      resultBuilder.apply {
        setReach(reach)
        putAllFrequency(frequency)
      }
    }.build()
    val actualComputation =
      GlobalComputation.parseFrom(reportStorageClient.getBlob(blobKey)!!.read().flatten())
    assertThat(actualComputation).isEqualTo(expectedComputation)
  }

  private fun makeCorrectness(
    dataProviderCount: Int,
    campaignCount: Int,
    generatedSetSize: Int,
    universeSize: Long
  ) = CorrectnessImpl(
    dataProviderCount,
    campaignCount,
    generatedSetSize,
    universeSize,
    RUN_ID,
    OUTPUT_DIR,
    sketchConfig,
    encryptionKey,
    sketchStorageClient,
    encryptedSketchStorageClient,
    reportStorageClient,
    COMBINED_PUBLIC_KEY_ID,
    publisherDataStub
  )

  companion object {
    private val encryptionKey = ElGamalPublicKey(
      TestKeys.CURVE_ID,
      TestKeys.COMBINED_EL_GAMAL_PUBLIC_KEY.elGamalG,
      TestKeys.COMBINED_EL_GAMAL_PUBLIC_KEY.elGamalY
    )
    private val sketchConfig = readSketchConfigTextproto()

    private fun readSketchConfigTextproto(): SketchConfig {
      val textproto: String =
        Resources.toString(
          CorrectnessImpl::class.java.getResource("config/liquid_legions_sketch_config.textproto"),
          Charsets.UTF_8
        )
      return TextFormat.parse(textproto, SketchConfig::class.java)
    }
  }
}

private suspend fun StorageClient.Blob.readToString(): String {
  return read().flatten().toStringUtf8()
}
