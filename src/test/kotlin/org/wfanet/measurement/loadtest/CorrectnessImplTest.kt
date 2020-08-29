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
import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.ByteString
import com.google.protobuf.TextFormat
import kotlin.math.max
import kotlin.math.min
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.anysketch.SketchProtos
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.FileSystemStorageClient

private const val RUN_ID = "TEST"
private const val OUTPUT_DIR = "Correctness"

@RunWith(JUnit4::class)
class CorrectnessImplTest {

  @Rule
  @JvmField
  val tempDirectory = TemporaryFolder()

  private lateinit var fileSystemStorageClient: FileSystemStorageClient

  @Before
  fun initStorageClient() {
    fileSystemStorageClient = FileSystemStorageClient(tempDirectory.root)
  }

  @Test
  fun `generate reach succeeds`() {
    val campaignCount = 5
    val generatedSetSize = 10
    val universeSize = 100_000L
    val correctness = makeCorrectness(
      campaignCount,
      generatedSetSize,
      universeSize
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
      1,
      generatedSetSize,
      1_0000_000_000L
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
      campaignCount,
      generatedSetSize,
      10_000_000_000L
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
      1,
      generatedSetSize,
      10_000_000_000L
    )
    val reach = correctness.generateReach().first()
    val actualSketch = correctness.generateSketch(reach).toSketchProto(sketchConfig)
    assertThat(actualSketch.registersCount).isLessThan(generatedSetSize)
  }

  @Test
  fun `store single sketch match succeeds`() = runBlocking {
    val correctness = makeCorrectness(
      1,
      1,
      1
    )
    val expectedSketch = Sketch.newBuilder()
      .setConfig(sketchConfig)
      .addRegisters(Sketch.Register.newBuilder().setIndex(0).addValues(12678).addValues(1))
      .build()
    val blobKey = correctness.storeSketch(SketchProtos.toAnySketch(sketchConfig, expectedSketch))
    val actualSketch =
      Sketch.parseFrom(
        fileSystemStorageClient.getBlob(blobKey.withBlobKeyPrefix("sketches"))!!
          .readAll()
      )
    ProtoTruth.assertThat(actualSketch).isEqualTo(expectedSketch)
  }

  @Test
  fun `store sketch wrong folder name fails`() {
    val correctness = makeCorrectness(
      1,
      100,
      10_000_000_000L
    )
    val expectedSketch = Sketch.newBuilder()
      .setConfig(sketchConfig)
      .addRegisters(Sketch.Register.newBuilder().setIndex(0).addValues(12678).addValues(1))
      .build()
    runBlocking {
      val blobKey = correctness.storeSketch(SketchProtos.toAnySketch(sketchConfig, expectedSketch))
      assertFailsWith(KotlinNullPointerException::class, "Folder name is incorrect.") {
        fileSystemStorageClient.getBlob(blobKey.withBlobKeyPrefix("encrypted_sketches"))!!
          .readAll()
      }
    }
  }

  private fun makeCorrectness(
    campaignCount: Int,
    generatedSetSize: Int,
    universeSize: Long
  ) = CorrectnessImpl(
    campaignCount,
    generatedSetSize,
    universeSize,
    RUN_ID,
    OUTPUT_DIR,
    sketchConfig,
    fileSystemStorageClient
  )

  private fun String.withBlobKeyPrefix(folder: String): String {
    return "/$OUTPUT_DIR/$RUN_ID/$folder/$this"
  }

  companion object {
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

private suspend fun StorageClient.Blob.readAll(): ByteString {
  return read(CorrectnessImpl.STORAGE_BUFFER_SIZE_BYTES).fold(ByteString.EMPTY) {
    result, value ->
    result.concat(value)
  }
}
