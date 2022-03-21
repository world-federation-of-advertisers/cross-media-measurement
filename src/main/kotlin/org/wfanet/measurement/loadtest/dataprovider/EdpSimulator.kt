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

import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import java.nio.file.Paths
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
import org.wfanet.estimation.VidSampler
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.LiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import org.wfanet.measurement.loadtest.storage.SketchStore

data class EdpData(
  /** The EDP's public API resource name. */
  val name: String,
  /** The EDP's display name. */
  val displayName: String,
  /** The EDP's consent signaling encryption key. */
  val encryptionKey: PrivateKeyHandle,
  /** The EDP's consent signaling signing key. */
  val signingKey: SigningKeyHandle
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
  private val eventQuery: EventQuery,
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
          eventGroup = eventGroup {
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
    val measurementConsumerCertificateX509 = readCertificate(measurementConsumerCertificate.x509Der)
    if (!verifyMeasurementSpec(
        measurementSpecSignature = requisition.measurementSpec.signature,
        measurementSpec = measurementSpec,
        measurementConsumerCertificate = measurementConsumerCertificateX509,
      )
    ) {
      logger.info("RequisitionFulfillmentWorkflow failed due to: invalid measurementSpec.")
      return
    }

    val requisitionFingerprint = computeRequisitionFingerprint(requisition)
    val signedRequisitionSpec: SignedData =
      decryptRequisitionSpec(requisition.encryptedRequisitionSpec, edpData.encryptionKey)
    val requisitionSpec = RequisitionSpec.parseFrom(signedRequisitionSpec.data)
    if (!verifyRequisitionSpec(
        requisitionSpecSignature = signedRequisitionSpec.signature,
        requisitionSpec = requisitionSpec,
        measurementConsumerCertificate = measurementConsumerCertificateX509,
        measurementSpec = measurementSpec,
      )
    ) {
      logger.info("RequisitionFulfillmentWorkflow failed due to: invalid requisitionSpec.")
      return
    }

    val combinedPublicKey =
      requisition.getCombinedPublicKey(requisition.protocolConfig.liquidLegionsV2.ellipticCurveId)
    val sketchConfig = requisition.protocolConfig.liquidLegionsV2.sketchParams.toSketchConfig()

    val vidSamplingIntervalStart = measurementSpec.reachAndFrequency.vidSamplingInterval.start
    val vidSamplingIntervalWidth = measurementSpec.reachAndFrequency.vidSamplingInterval.width
    val sketch = generateSketch(sketchConfig, vidSamplingIntervalStart, vidSamplingIntervalWidth)

    sketchStore.write(requisition.name, sketch.toByteString())
    val sketchChunks: Flow<ByteString> =
      encryptSketch(sketch, combinedPublicKey, requisition.protocolConfig.liquidLegionsV2)
    fulfillRequisition(
      requisition.name,
      requisitionFingerprint,
      requisitionSpec.nonce,
      sketchChunks
    )
  }

  private fun generateSketch(
    sketchConfig: SketchConfig,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float
  ): Sketch {
    logger.info("Generating Sketch...")
    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchConfig)

    // TODO(@wangyaopw): get the queryParameter from EventFilters when EventFilters is implemented.
    val queryParameter =
      QueryParameter(
        edpDisplayName = edpData.displayName,
        beginDate = "2021-03-01",
        endDate = "2021-03-28",
        sex = Sex.FEMALE,
        ageGroup = null,
        socialGrade = SocialGrade.ABC1,
        complete = Complete.COMPLETE
      )
    val vidSampler = VidSampler(Hashing.farmHashFingerprint64())
    eventQuery.getUserVirtualIds(queryParameter).forEach {
      if (vidSampler.vidIsInSamplingBucket(it, vidSamplingIntervalStart, vidSamplingIntervalWidth)
      ) {
        anySketch.insert(it, mapOf("frequency" to 1L))
      }
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
    requisitionFingerprint: ByteString,
    nonce: Long,
    data: Flow<ByteString>
  ) {
    logger.info("Fulfilling requisition $requisitionName...")
    requisitionFulfillmentStub.fulfillRequisition(
      flow {
        emit(
          fulfillRequisitionRequest {
            header = header {
              name = requisitionName
              this.requisitionFingerprint = requisitionFingerprint
              this.nonce = nonce
            }
          }
        )
        emitAll(data.map { fulfillRequisitionRequest { bodyChunk = bodyChunk { this.data = it } } })
      }
    )
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
    indexes += indexSpec {
      name = "Index"
      distribution = distribution {
        exponential = exponentialDistribution {
          rate = decayRate
          numValues = maxSize
        }
      }
    }
    values += valueSpec {
      name = "SamplingIndicator"
      aggregator = SketchConfig.ValueSpec.Aggregator.UNIQUE
      distribution = distribution {
        uniform = uniformDistribution { numValues = samplingIndicatorSize }
      }
    }

    values += valueSpec {
      name = "Frequency"
      aggregator = SketchConfig.ValueSpec.Aggregator.SUM
      distribution = distribution { oracle = oracleDistribution { key = "frequency" } }
    }
  }
}
