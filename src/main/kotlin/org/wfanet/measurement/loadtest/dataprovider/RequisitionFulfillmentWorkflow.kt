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
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.SketchConfigKt.indexSpec
import org.wfanet.anysketch.SketchConfigKt.valueSpec
import org.wfanet.anysketch.SketchProtos
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.ElGamalPublicKey as AnySketchElGamalPublicKey
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.anysketch.distribution
import org.wfanet.anysketch.exponentialDistribution
import org.wfanet.anysketch.oracleDistribution
import org.wfanet.anysketch.sketchConfig
import org.wfanet.anysketch.uniformDistribution
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.HybridCipherSuite
import org.wfanet.measurement.api.v2alpha.LiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import org.wfanet.measurement.consent.crypto.hybridencryption.HybridCryptor
import org.wfanet.measurement.consent.crypto.hybridencryption.testing.ReversingHybridCryptor
import org.wfanet.measurement.consent.crypto.keystore.KeyStore
import org.wfanet.measurement.loadtest.storage.SketchStore

val EDP_PRIVATE_KEY_HANDLE_KEY = "edp-private-consent-signaling-key"

/** [RequisitionFulfillmentWorkflow] polls for unfulfilled requisitions and fulfills them */
class RequisitionFulfillmentWorkflow(
  private val externalDataProviderId: String,
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val requisitionFulfillmentStub: RequisitionFulfillmentCoroutineStub,
  private val sketchStore: SketchStore,
  private val keyStore: KeyStore,
  private val mcResourceName: String,
  private val measurementConsumersClient: MeasurementConsumersCoroutineStub,
  private val certificateServiceStub: CertificatesGrpcKt.CertificatesCoroutineStub
) {

  fun generateSketch(sketchConfig: SketchConfig): Sketch {

    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchConfig)

    // todo(@ohardt): make random
    anySketch.insert(123, mapOf("frequency" to 1L))
    anySketch.insert(122, mapOf("frequency" to 1L))
    anySketch.insert(332, mapOf("frequency" to 1L))
    anySketch.insert(111, mapOf("frequency" to 1L))

    return SketchProtos.fromAnySketch(anySketch, sketchConfig)
  }

  private fun encryptSketch(sketch: Sketch, combinedPublicKey: ElGamalPublicKey): Flow<ByteString> {
    val request: EncryptSketchRequest =
      EncryptSketchRequest.newBuilder()
        .apply {
          this.sketch = sketch

          // todo(@ohardt): read from protocolConfig when the proto is fixed
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

  private suspend fun fulfillRequisition(name: String, data: Flow<ByteString>) {
    requisitionFulfillmentStub.fulfillRequisition(
      flow {
        emit(makeFulfillRequisitionHeader(name))
        emitAll(data.map { makeFulfillRequisitionBody(it) })
      }
    )
  }

  private fun makeFulfillRequisitionHeader(name: String): FulfillRequisitionRequest {
    return FulfillRequisitionRequest.newBuilder().apply { headerBuilder.name = name }.build()
  }

  private fun makeFulfillRequisitionBody(bytes: ByteString): FulfillRequisitionRequest {
    return FulfillRequisitionRequest.newBuilder().apply { bodyChunkBuilder.data = bytes }.build()
  }

  private suspend fun getRequisition(): Requisition? {
    val req =
      ListRequisitionsRequest.newBuilder()
        .apply {
          parent = externalDataProviderId
          filterBuilder.addStates(Requisition.State.UNFULFILLED)
        }
        .build()

    val response = requisitionsStub.listRequisitions(req)

    return response.requisitionsList.firstOrNull()
  }

  /** Always returns [ReversingHybridCryptor] regardless of input [HybridCipherSuite]. */
  fun fakeGetHybridCryptorForCipherSuite(cipherSuite: HybridCipherSuite): HybridCryptor {
    return ReversingHybridCryptor()
  }

  /** execute runs the individual steps of the workflow */
  suspend fun execute() {
    val requisition: Requisition = getRequisition() ?: return

    val hybridCipherSuite = HybridCipherSuite.getDefaultInstance()

    val privateKeyHandle = keyStore.getPrivateKeyHandle(EDP_PRIVATE_KEY_HANDLE_KEY)
    checkNotNull(privateKeyHandle)

    val mSpec = MeasurementSpec.parseFrom(requisition.measurementSpec.data)

    val decryptedSignedDataRequisitionSpec =
      decryptRequisitionSpec(
        requisition.encryptedRequisitionSpec,
        privateKeyHandle,
        hybridCipherSuite,
        ::fakeGetHybridCryptorForCipherSuite
      )

    val decryptedRequisitionSpec =
      RequisitionSpec.parseFrom(decryptedSignedDataRequisitionSpec.data)

    val mcCert =
      certificateServiceStub.getCertificate(
        getCertificateRequest { name = decryptedRequisitionSpec.measurementPublicKey.toString() }
      )

    if (!verifyMeasurementSpec(
        measurementSpecSignature = requisition.measurementSpec.signature,
        measurementSpec = mSpec,
        measurementConsumerCertificate = readCertificate(mcCert.x509Der),
      )
    ) {
      logger.info("invalid measurementSpec ")
      return
    }

    if (!verifyRequisitionSpec(
        requisitionSpecSignature = decryptedSignedDataRequisitionSpec.signature,
        requisitionSpec = decryptedRequisitionSpec,
        measurementConsumerCertificate = readCertificate(mcCert.x509Der),
        measurementSpec = mSpec,
      )
    ) {
      logger.info("invalid requisitionSpec ")
      return
    }

    val combinedPublicKey =
      requisition.getCombinedPublicKey(requisition.protocolConfig.liquidLegionsV2.ellipticCurveId)

    val sketchConfig = requisition.protocolConfig.liquidLegionsV2.sketchParams.toSketchConfig()

    val sketch = generateSketch(sketchConfig)

    val blobKey = "sketch/for-req-${requisition.name}"
    sketchStore.write(blobKey, sketch.toByteString().asBufferedFlow(1024))

    val sketchChunks: Flow<ByteString> = encryptSketch(sketch, combinedPublicKey)

    fulfillRequisition(requisition.name, sketchChunks)
  }
  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private fun AnySketchElGamalPublicKey.toV2ElGamalPublicKey(): ElGamalPublicKey {
  return elGamalPublicKey {
    generator = generator
    element = element
  }
}

private fun Requisition.DuchyEntry.getElGamalKey(): AnySketchElGamalPublicKey {
  val key = ElGamalPublicKey.parseFrom(this.value.liquidLegionsV2.elGamalPublicKey.data)
  return AnySketchElGamalPublicKey.newBuilder()
    .also {
      it.generator = key.generator
      it.element = key.element
    }
    .build()
}

private fun Requisition.getCombinedPublicKey(curveId: Int): ElGamalPublicKey {

  // todo(@ohardt): this needs to verify the duchy keys before using them

  // val curveId = 415L // todo: fetch this from the ProtoConfig svc using `req.protocolConfig` ?

  val listOfKeys = this.duchiesList.map { it.getElGamalKey() }

  val request =
    CombineElGamalPublicKeysRequest.newBuilder()
      .also {
        it.curveId = curveId.toLong()
        it.addAllElGamalKeys(listOfKeys)
      }
      .build()

  val response =
    CombineElGamalPublicKeysResponse.parseFrom(
      SketchEncrypterAdapter.CombineElGamalPublicKeys(request.toByteArray())
    )

  return response.elGamalKeys.toV2ElGamalPublicKey()
}

private fun LiquidLegionsSketchParams.toSketchConfig(): SketchConfig {
  return sketchConfig {
    indexes +=
      indexSpec {
        name = "Index"
        distribution =
          distribution {
            exponential =
              exponentialDistribution {
                rate = decayRate
                numValues = maxSize
              }
          }
      }
    values +=
      valueSpec {
        name = "SamplingIndicator"
        aggregator = SketchConfig.ValueSpec.Aggregator.UNIQUE
        distribution =
          distribution { uniform = uniformDistribution { numValues = samplingIndicatorSize } }
      }

    values +=
      valueSpec {
        name = "Frequency"
        aggregator = SketchConfig.ValueSpec.Aggregator.SUM
        distribution = distribution { oracle = oracleDistribution { key = "frequency" } }
      }
  }
}
