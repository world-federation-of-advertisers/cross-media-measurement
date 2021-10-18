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
import java.nio.file.Paths
import java.util.logging.Logger
import kotlin.random.Random
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
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.LiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpecAndGenerateRequisitionFingerprint
import org.wfanet.measurement.consent.client.dataprovider.signRequisitionFingerprint
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import org.wfanet.measurement.consent.crypto.hybridencryption.testing.ReversingHybridCryptor
import org.wfanet.measurement.consent.crypto.keystore.KeyStore
import org.wfanet.measurement.loadtest.storage.SketchStore

data class EdpData(
  /** The EDP's public API resource name. */
  val name: String,
  /** The ID of the EDP's encryption private key in keyStore. */
  val encryptionPrivateKeyId: String,
  /** The ID of the EDP's consent signaling private key in keyStore. */
  val consentSignalingPrivateKeyId: String,
  /** The EDP's consent signaling certificate in DER format. */
  val consentSignalCertificateDer: ByteString,
)

/** A simulator handling EDP businesses. */
class EdpSimulator(
  private val edpData: EdpData,
  private val measurementConsumerName: String,
  private val certificatesStub: CertificatesCoroutineStub,
  private val eventGroupsStub: EventGroupsCoroutineStub,
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val requisitionFulfillmentStub: RequisitionFulfillmentCoroutineStub,
  private val sketchStore: SketchStore,
  private val keyStore: KeyStore,
  private val sketchGenerationParams: SketchGenerationParams,
  private val throttler: MinimumIntervalThrottler
) {

  /** A sequence of operations done in the simulator. */
  suspend fun process() {
    createEventGroup()
    throttler.loopOnReady {
      logAndSuppressExceptionSuspend { executeRequisitionFulfillingWorkflow() }
    }
  }

  /** Creates an eventGroup for the MC. */
  private suspend fun createEventGroup() {
    val eventGroup =
      eventGroupsStub.createEventGroup(
        createEventGroupRequest {
          parent = edpData.name
          eventGroup =
            eventGroup {
              measurementConsumer = measurementConsumerName
              eventGroupReferenceId = "001"
            }
        }
      )
    logger.info("Successfully created eventGroup ${eventGroup.name}...")
  }

  /** Executes the requisition fulfillment workflow. */
  private suspend fun executeRequisitionFulfillingWorkflow() {
    logger.info("Executing requisitionFulfillingWorkflow...")
    val requisition = getRequisition()
    if (requisition == null) {
      logger.info("No unfulfilled requisition. Polling again later...")
      return
    }
    logger.info("Processing requisition ${requisition.name}...")

    if (requisition.protocolConfig.protocolCase != ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2) {
      logger.info(
        "Skipping requisition ${requisition.name}, only LIQUID_LEGIONS_V2 is supported..."
      )
      return
    }

    val measurementConsumerCertificate =
      certificatesStub.getCertificate(
        getCertificateRequest { name = requisition.measurementConsumerCertificate }
      )

    val measurementSpec = MeasurementSpec.parseFrom(requisition.measurementSpec.data)
    if (!verifyMeasurementSpec(
        measurementSpecSignature = requisition.measurementSpec.signature,
        measurementSpec = measurementSpec,
        measurementConsumerCertificate = readCertificate(measurementConsumerCertificate.x509Der),
      )
    ) {
      logger.info("RequisitionFulfillmentWorkflow failed due to: invalid measurementSpec.")
      return
    }

    val requisitionSpecAndFingerprint =
      decryptRequisitionSpecAndGenerateRequisitionFingerprint(
        requisition,
        checkNotNull(keyStore.getPrivateKeyHandle(ENCRYPTION_PRIVATE_KEY_HANDLE_KEY)),
        ::ReversingHybridCryptor
      )

    val signedRequisitionSpec = requisitionSpecAndFingerprint.signedRequisitionSpec
    if (!verifyRequisitionSpec(
        requisitionSpecSignature = signedRequisitionSpec.signature,
        requisitionSpec = RequisitionSpec.parseFrom(signedRequisitionSpec.data),
        measurementConsumerCertificate = readCertificate(measurementConsumerCertificate.x509Der),
        measurementSpec = measurementSpec,
      )
    ) {
      logger.info("RequisitionFulfillmentWorkflow failed due to: invalid requisitionSpec.")
      return
    }

    val participationSignature =
      signRequisitionFingerprint(
        requisitionSpecAndFingerprint.requisitionFingerprint,
        checkNotNull(keyStore.getPrivateKeyHandle(edpData.consentSignalingPrivateKeyId)),
        readCertificate(edpData.consentSignalCertificateDer)
      )
    val combinedPublicKey =
      requisition.getCombinedPublicKey(requisition.protocolConfig.liquidLegionsV2.ellipticCurveId)
    val sketchConfig = requisition.protocolConfig.liquidLegionsV2.sketchParams.toSketchConfig()
    val sketch = generateSketch(sketchConfig, sketchGenerationParams)

    sketchStore.write(requisition.name, sketch.toByteString())
    val sketchChunks: Flow<ByteString> =
      encryptSketch(sketch, combinedPublicKey, requisition.protocolConfig.liquidLegionsV2)
    fulfillRequisition(requisition.name, participationSignature.signature, sketchChunks)
  }

  private fun generateSketch(
    sketchConfig: SketchConfig,
    sketchGenerationParams: SketchGenerationParams
  ): Sketch {
    logger.info("Generating Sketch...")
    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchConfig)

    for (i in 1..sketchGenerationParams.reach) {
      anySketch.insert(
        Random.nextInt(1, sketchGenerationParams.universeSize + 1).toLong(),
        mapOf("frequency" to 1L)
      )
    }
    return SketchProtos.fromAnySketch(anySketch, sketchConfig)
  }

  private fun encryptSketch(
    sketch: Sketch,
    combinedPublicKey: AnySketchElGamalPublicKey,
    protocolConfig: ProtocolConfig.LiquidLegionsV2
  ): Flow<ByteString> {
    logger.info("Encrypting Sketch...")
    val request =
      EncryptSketchRequest.newBuilder()
        .apply {
          this.sketch = sketch
          elGamalKeys = combinedPublicKey
          curveId = protocolConfig.ellipticCurveId.toLong()
          maximumValue = protocolConfig.maximumFrequency
          destroyedRegisterStrategy =
            EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY // for LL_V2 protocol
          // TODO(wangyaopw): add publisher noise
        }
        .build()
    val response =
      EncryptSketchResponse.parseFrom(SketchEncrypterAdapter.EncryptSketch(request.toByteArray()))

    return response.encryptedSketch.asBufferedFlow(1024)
  }

  private suspend fun fulfillRequisition(
    requisitionName: String,
    participationSignature: ByteString,
    data: Flow<ByteString>
  ) {
    logger.info("Fulfilling requisition $requisitionName...")
    requisitionFulfillmentStub.fulfillRequisition(
      flow {
        emit(makeFulfillRequisitionHeader(requisitionName, participationSignature))
        emitAll(data.map { makeFulfillRequisitionBody(it) })
      }
    )
  }

  private fun makeFulfillRequisitionHeader(
    requisitionName: String,
    participationSignature: ByteString
  ): FulfillRequisitionRequest {
    return FulfillRequisitionRequest.newBuilder()
      .apply {
        headerBuilder.apply {
          name = requisitionName
          dataProviderParticipationSignature = participationSignature
        }
      }
      .build()
  }

  private fun makeFulfillRequisitionBody(bytes: ByteString): FulfillRequisitionRequest {
    return FulfillRequisitionRequest.newBuilder().apply { bodyChunkBuilder.data = bytes }.build()
  }

  private fun Requisition.getCombinedPublicKey(curveId: Int): AnySketchElGamalPublicKey {
    logger.info("Getting combined public key...")
    val listOfKeys = this.duchiesList.map { it.getElGamalKey() }
    val request =
      CombineElGamalPublicKeysRequest.newBuilder()
        .also {
          it.curveId = curveId.toLong()
          it.addAllElGamalKeys(listOfKeys)
        }
        .build()

    return CombineElGamalPublicKeysResponse.parseFrom(
        SketchEncrypterAdapter.CombineElGamalPublicKeys(request.toByteArray())
      )
      .elGamalKeys
  }

  private suspend fun getRequisition(): Requisition? {
    val request =
      ListRequisitionsRequest.newBuilder()
        .apply {
          parent = edpData.name
          filterBuilder.addStates(Requisition.State.UNFULFILLED)
        }
        .build()

    return requisitionsStub.listRequisitions(request).requisitionsList.firstOrNull()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    init {
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath =
          Paths.get(
            "any_sketch_java",
            "src",
            "main",
            "java",
            "org",
            "wfanet",
            "anysketch",
            "crypto"
          )
      )
    }
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
