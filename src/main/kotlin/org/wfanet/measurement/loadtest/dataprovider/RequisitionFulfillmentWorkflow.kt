// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.SketchProtos
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.ElGamalPublicKey as AnySketchElGamalPublicKey
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.storage.StorageClient

fun AnySketchElGamalPublicKey.toV2ElGamalPublicKey(): ElGamalPublicKey {
  return ElGamalPublicKey.newBuilder()
    .also {
      it.generator = generator
      it.element = element
    }
    .build()
}

fun Requisition.DuchyEntry.getElGamalKey(): AnySketchElGamalPublicKey {
  val key = ElGamalPublicKey.parseFrom(this.value.liquidLegionsV2.elGamalPublicKey.data)
  return AnySketchElGamalPublicKey.newBuilder()
    .also {
      it.generator = key.generator
      it.element = key.element
    }
    .build()
}

fun Requisition.getCombinedPublicKey(): ElGamalPublicKey {

  // todo: this needs to verify the duchy keys before using them

  val curveId = 415L // todo: fetch this from the ProtoConfig svc using `req.protocolConfig` ?

  val listOfKeys = this.duchiesList.map { it.getElGamalKey() }

  return CombineElGamalPublicKeysResponse.parseFrom(
      SketchEncrypterAdapter.CombineElGamalPublicKeys(
        CombineElGamalPublicKeysRequest.newBuilder()
          .also {
            it.curveId = curveId
            it.addAllElGamalKeys(listOfKeys)
          }
          .build()
          .toByteArray()
      )
    )
    .elGamalKeys
    .toV2ElGamalPublicKey()
}

class RequisitionFulfillmentWorkflow(
  private val unfulfilledRequisitionProvider: UnfulfilledRequisitionProvider,
  private val sketchGenerator: EncryptedSketchGenerator,
  private val requisitionFulfiller: RequisitionFulfiller,
  private val storageClient: StorageClient,
) {

  private fun Sketch.Builder.addRegister(index: Long, key: Long, count: Long) {
    addRegistersBuilder().also {
      it.index = index
      it.addValues(key)
      it.addValues(count)
    }
  }

  private fun decodeMeasurementSpec(requisition: Requisition): MeasurementSpec {
    val serializedMeasurementSpec = requisition.measurementSpec
    return MeasurementSpec.parseFrom(serializedMeasurementSpec.data)
  }

  private fun decodeRequisitionSpec(requisition: Requisition): RequisitionSpec {
    val signedData = SignedData.parseFrom(requisition.encryptedRequisitionSpec)
    return RequisitionSpec.parseFrom(signedData.data)
  }

  fun generateSketch(): Sketch {

    // todo: where do I get this from?
    val sketchCfg = SketchConfig.getDefaultInstance()

    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchCfg)

    // todo: make random
    anySketch.insert(123, mapOf("frequency" to 1L))
    anySketch.insert(122, mapOf("frequency" to 1L))
    anySketch.insert(332, mapOf("frequency" to 1L))
    anySketch.insert(111, mapOf("frequency" to 1L))

    return SketchProtos.fromAnySketch(anySketch, sketchCfg)
  }

  private fun encryptSketch(sketch: Sketch, combinedPublicKey: ElGamalPublicKey): Flow<ByteString> {
    val request: EncryptSketchRequest =
      EncryptSketchRequest.newBuilder()
        .apply {
          this.sketch = sketch
          maximumValue = 5
          curveId = combinedPublicKey.ellipticCurveId.toLong()
          elGamalKeysBuilder.generator = combinedPublicKey.generator
          elGamalKeysBuilder.element = combinedPublicKey.element
          destroyedRegisterStrategy =
            EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY // for LL_V2 protocol
        }
        .build()
    val response =
      EncryptSketchResponse.parseFrom(SketchEncrypterAdapter.EncryptSketch(request.toByteArray()))
    return response.encryptedSketch.asBufferedFlow(1024)
  }

  suspend fun execute() {
    val requisition: Requisition = unfulfilledRequisitionProvider.get() ?: return

    val measurementSpec = decodeMeasurementSpec(requisition)
    val requisitionSpec = decodeRequisitionSpec(requisition)
    val combinedPublicKey = requisition.getCombinedPublicKey()

    val sketch = generateSketch()

    val blobKey = "sketch/for-req-${requisition.name}"
    storageClient.createBlob(blobKey, sketch.toByteString().asBufferedFlow(1024))

    val sketchChunks: Flow<ByteString> = encryptSketch(sketch, combinedPublicKey)

    requisitionFulfiller.fulfillRequisition(requisition.name, sketchChunks)
  }
}
