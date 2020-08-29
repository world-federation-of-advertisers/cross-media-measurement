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

import com.google.protobuf.ByteString
import java.util.UUID
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.SketchProtos
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.storage.StorageClient

class CorrectnessImpl(
  override val campaignCount: Int,
  override val generatedSetSize: Int = 1000,
  override val universeSize: Long = 10_000_000_000L,
  override val runId: String,
  override val outputDir: String,
  override val sketchConfig: SketchConfig,
  override val storageClient: StorageClient
) : Correctness {

  override fun generateReach(): Sequence<Set<Long>> {
    return generateIndependentSets(universeSize, generatedSetSize).take(campaignCount)
  }

  override fun generateSketch(reach: Set<Long>): AnySketch {
    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchConfig)
    for (value: Long in reach) {
      anySketch.insert(value, mapOf("frequency" to 1L))
    }
    return anySketch
  }

  override fun estimateCardinality(anySketches: List<AnySketch>): Long {
    TODO("Not yet implemented")
  }

  override fun encryptSketch(sketch: Sketch): ByteString {
    TODO("Not yet implemented")
  }

  override suspend fun storeSketch(anySketch: AnySketch): String {
    val sketch: Sketch = anySketch.toSketchProto(sketchConfig)
    val blobKey = generateBlobKey()
    storageClient.createBlob(
      blobKey.withBlobKeyPrefix("sketches"),
      sketch.toByteArray().asBufferedFlow(STORAGE_BUFFER_SIZE_BYTES)
    )
    return blobKey
  }

  override suspend fun storeEncryptedSketch(encryptedSketch: ByteString): String {
    TODO("Not yet implemented")
  }

  override suspend fun storeEstimationResult(result: Long): String {
    TODO("Not yet implemented")
  }

  override suspend fun sendToServer(encryptedSketch: ByteString) {
    TODO("Not yet implemented")
  }

  private fun generateBlobKey(): String {
    return UUID.randomUUID().toString()
  }

  private fun String.withBlobKeyPrefix(folder: String): String {
    return "/$outputDir/$runId/$folder/$this"
  }

  companion object {
    const val STORAGE_BUFFER_SIZE_BYTES = 1024 * 4 // 4 KiB
  }
}
