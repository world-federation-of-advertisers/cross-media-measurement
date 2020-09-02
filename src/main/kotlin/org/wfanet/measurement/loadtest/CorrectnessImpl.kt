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
import java.nio.file.Paths
import java.util.UUID
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.SketchProtos
import org.wfanet.anysketch.crypto.ElGamalPublicKeys
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.estimation.Estimators
import org.wfanet.estimation.ValueHistogram
import org.wfanet.measurement.api.v1alpha.GlobalComputation
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.client.v1alpha.publisherdata.org.wfanet.measurement.client.v1alpha.publisherdata.PublisherDataClient
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.crypto.ElGamalPublicKey
import org.wfanet.measurement.storage.StorageClient

class CorrectnessImpl(
  override val campaignCount: Int,
  override val generatedSetSize: Int = 1000,
  override val universeSize: Long = 10_000_000_000L,
  override val runId: String,
  override val outputDir: String,
  override val sketchConfig: SketchConfig,
  override val encryptionPublicKey: ElGamalPublicKey,
  override val storageClient: StorageClient,
  override val publisherDataClient: PublisherDataClient
) : Correctness {

  private val MAX_COUNTER_VALUE = 10
  private val DECAY_RATE = 23.0
  private val INDEX_SIZE = 330_000L

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
    val anySketch = anySketches.union()
    val activeRegisterCount = anySketch.toList().size.toLong()
    return Estimators.EstimateCardinalityLiquidLegions(
      DECAY_RATE,
      INDEX_SIZE,
      activeRegisterCount
    )
  }

  override fun estimateFrequency(anySketches: List<AnySketch>): Map<Long, Long> {
    val anySketch = anySketches.union()
    val valueIndex = anySketch.getValueIndex("SamplingIndicator").asInt
    return ValueHistogram.calculateHistogram(
      anySketch,
      "Frequency",
      { it.getValues().get(valueIndex) != -1L }
    )
  }

  override fun encryptSketch(sketch: Sketch): ByteString {
    val request: EncryptSketchRequest = EncryptSketchRequest.newBuilder()
      .setSketch(sketch)
      .setCurveId(encryptionPublicKey.ellipticCurveId.toLong())
      .setMaximumValue(MAX_COUNTER_VALUE)
      .setElGamalKeys(
        ElGamalPublicKeys.newBuilder()
          .setElGamalG(encryptionPublicKey.generator)
          .setElGamalY(encryptionPublicKey.element)
          .build()
      )
      .build()
    val response = EncryptSketchResponse.parseFrom(
      SketchEncrypterAdapter.EncryptSketch(request.toByteArray())
    )
    return response.encryptedSketch
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
    val blobKey = generateBlobKey()
    storageClient.createBlob(
      blobKey.withBlobKeyPrefix("encrypted_sketches"),
      encryptedSketch.toByteArray().asBufferedFlow(STORAGE_BUFFER_SIZE_BYTES)
    )
    return blobKey
  }

  override suspend fun storeEstimationResults(
    reach: Long,
    frequency: Map<Long, Long>,
    globalComputationId: String
  ): String {
    val computation = GlobalComputation.newBuilder().apply {
      keyBuilder.globalComputationId = globalComputationId
      state = GlobalComputation.State.SUCCEEDED
      resultBuilder.apply {
        setReach(reach)
        putAllFrequency(frequency)
      }
    }.build()
    val blobKey = generateBlobKey()
    storageClient.createBlob(
      blobKey.withBlobKeyPrefix("reports"),
      computation.toByteArray().asBufferedFlow(STORAGE_BUFFER_SIZE_BYTES)
    )
    return blobKey
  }

  override suspend fun sendToServer(
    dataProviderId: String,
    campaignId: String,
    metricRequisitionId: String,
    combinedPublicKeyId: String,
    encryptedSketch: ByteString
  ) {
    publisherDataClient.uploadMetricValue(
      dataProviderId,
      campaignId,
      metricRequisitionId,
      combinedPublicKeyId,
      encryptedSketch.toByteArray()
    )
  }

  private fun List<AnySketch>.union(): AnySketch {
    require(!this.isNullOrEmpty()) {
      "At least one AnySketch expected."
    }
    val anySketch = this.first()
    if (this.size > 1) {
      anySketch.mergeAll(this.subList(1, this.size))
    }
    return anySketch
  }

  private fun generateBlobKey(): String {
    return UUID.randomUUID().toString()
  }

  private fun String.withBlobKeyPrefix(folder: String): String {
    return "/$outputDir/$runId/$folder/$this"
  }

  companion object {
    const val STORAGE_BUFFER_SIZE_BYTES = 1024 * 4 // 4 KiB

    init {
      loadLibrary(
        name = "estimators",
        directoryPath = Paths.get("any_sketch/src/main/java/org/wfanet/estimation")
      )
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath = Paths.get("any_sketch/src/main/java/org/wfanet/anysketch/crypto")
      )
    }
  }
}
