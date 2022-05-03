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
import com.google.protobuf.Timestamp
import com.google.protobuf.duration
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
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
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.LiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.watchDuration
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.consent.client.common.signMessage
import org.wfanet.measurement.consent.client.common.toPublicKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.loadtest.config.EventFilters.VID_SAMPLER_HASH_FUNCTION
import org.wfanet.measurement.loadtest.storage.SketchStore

private const val EVENT_TEMPLATE_CLASS_NAME =
  "wfanet.measurement.api.v2alpha.event_templates.testing"

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
  private val throttler: MinimumIntervalThrottler,
  private val eventTemplateNames: List<String>,
  private val privacyBudgetManager: PrivacyBudgetManager
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
            eventTemplates += eventTemplateNames.map { eventTemplate { type = it } }
          }
        }
      )
    logger.info("Successfully created eventGroup ${eventGroup.name}...")
  }

  /** Executes the requisition fulfillment workflow. */
  private suspend fun executeRequisitionFulfillingWorkflow() {
    logger.info("Executing requisitionFulfillingWorkflow...")
    val requisitions = getRequisitions()
    if (requisitions.isEmpty()) {
      logger.info("No unfulfilled requisition. Polling again later...")
      return
    }

    for (requisition in requisitions) {
      logger.info("Processing requisition ${requisition.name}...")

      val measurementConsumerCertificate =
        certificatesStub.getCertificate(
          getCertificateRequest { name = requisition.measurementConsumerCertificate }
        )

      val measurementSpec = MeasurementSpec.parseFrom(requisition.measurementSpec.data)
      val measurementConsumerCertificateX509 =
        readCertificate(measurementConsumerCertificate.x509Der)
      if (!verifyMeasurementSpec(
          measurementSpecSignature = requisition.measurementSpec.signature,
          measurementSpec = measurementSpec,
          measurementConsumerCertificate = measurementConsumerCertificateX509,
        )
      ) {
        logger.info("RequisitionFulfillmentWorkflow failed due to: invalid measurementSpec.")
        refuseRequisition(
          requisition.name,
          Requisition.Refusal.Justification.SPECIFICATION_INVALID,
          "Invalid measurementSpec"
        )
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
        refuseRequisition(
          requisition.name,
          Requisition.Refusal.Justification.SPECIFICATION_INVALID,
          "Invalid requisitionSpec"
        )
      }

      if (requisition.protocolConfig.protocolCase != ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2
      ) {
        when (measurementSpec.measurementTypeCase) {
          MeasurementSpec.MeasurementTypeCase.IMPRESSION ->
            fulfillImpressionMeasurement(requisition, requisitionSpec, measurementSpec)
          MeasurementSpec.MeasurementTypeCase.DURATION ->
            fulfillDurationMeasurement(requisition, requisitionSpec, measurementSpec)
          else ->
            logger.info("Skipping requisition ${requisition.name}, unsupported measurement type")
        }
      } else {
        fulfillRequisitionForReachAndFrequencyMeasurement(
          requisition,
          measurementSpec,
          requisitionFingerprint,
          requisitionSpec
        )
      }
    }
  }

  private suspend fun refuseRequisition(
    requisitionName: String,
    justification: Requisition.Refusal.Justification,
    message: String
  ): Requisition {
    return requisitionsStub.refuseRequisition(
      refuseRequisitionRequest {
        name = requisitionName
        refusal = refusal {
          this.justification = justification
          this.message = message
        }
      }
    )
  }

  private fun populateAnySketch(
    eventFilter: EventFilter,
    vidSampler: VidSampler,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float,
    anySketch: AnySketch
  ) {
    eventQuery
      .getUserVirtualIds(eventFilter)
      .filter {
        vidSampler.vidIsInSamplingBucket(it, vidSamplingIntervalStart, vidSamplingIntervalWidth)
      }
      .forEach { anySketch.insert(it, mapOf("frequency" to 1L)) }
  }

  suspend fun chargePrivacyBudget(
    requisitionName: String,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec
  ) =
    try {
      privacyBudgetManager.chargePrivacyBudget(
        measurementConsumerName,
        requisitionSpec,
        measurementSpec
      )
    } catch (e: PrivacyBudgetManagerException) {
      logger.log(
        Level.WARNING,
        "RequisitionFulfillmentWorkflow failed due to: Not Enough Privacy Budget",
        e
      )
      refuseRequisition(
        requisitionName,
        Requisition.Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET,
        "Privacy Budget Exceeded."
      )
    }

  suspend fun generateSketch(
    requisitionName: String,
    sketchConfig: SketchConfig,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec
  ): Sketch {
    chargePrivacyBudget(requisitionName, measurementSpec, requisitionSpec)
    val vidSamplingIntervalStart = measurementSpec.reachAndFrequency.vidSamplingInterval.start
    val vidSamplingIntervalWidth = measurementSpec.reachAndFrequency.vidSamplingInterval.width

    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchConfig)
    logger.info("Generating Sketch...")

    // TODO(@uakyol): Populate sketch based on all event groups not just the first one.
    populateAnySketch(
      requisitionSpec.eventGroupsList[0].value.filter,
      VidSampler(VID_SAMPLER_HASH_FUNCTION),
      vidSamplingIntervalStart,
      vidSamplingIntervalWidth,
      anySketch
    )

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

  private suspend fun fulfillRequisitionForReachAndFrequencyMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionFingerprint: ByteString,
    requisitionSpec: RequisitionSpec
  ) {
    val combinedPublicKey =
      requisition.getCombinedPublicKey(requisition.protocolConfig.liquidLegionsV2.ellipticCurveId)
    val sketchConfig = requisition.protocolConfig.liquidLegionsV2.sketchParams.toSketchConfig()

    val sketch =
      try {
        generateSketch(requisition.name, sketchConfig, measurementSpec, requisitionSpec)
      } catch (e: EventFilterValidationException) {
        logger.log(
          Level.WARNING,
          "RequisitionFulfillmentWorkflow failed due to: invalid EventFilter",
          e
        )
        return
      }

    sketchStore.write(requisition, sketch.toByteString())
    val sketchChunks: Flow<ByteString> =
      encryptSketch(sketch, combinedPublicKey, requisition.protocolConfig.liquidLegionsV2)
    fulfillRequisition(
      requisition.name,
      requisitionFingerprint,
      requisitionSpec.nonce,
      sketchChunks
    )
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

  private suspend fun getRequisitions(): List<Requisition> {
    val request = listRequisitionsRequest {
      parent = edpData.name
      filter = filter {
        states += Requisition.State.UNFULFILLED
        measurementStates += Measurement.State.AWAITING_REQUISITION_FULFILLMENT
      }
    }

    return requisitionsStub.listRequisitions(request).requisitionsList
  }

  private suspend fun fulfillImpressionMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec
  ) {
    val requisitionData =
      MeasurementKt.result {
        impression = impression {
          // Use externalDataProviderId since it's a known value the FrontendSimulator can verify.
          // TODO: Calculate impression from data.
          value = apiIdToExternalId(DataProviderKey.fromName(edpData.name)!!.dataProviderId)
        }
      }

    fulfillDirectMeasurement(requisition, requisitionSpec, measurementSpec, requisitionData)
  }

  private suspend fun fulfillDurationMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec
  ) {
    val requisitionData =
      MeasurementKt.result {
        watchDuration = watchDuration {
          value = duration {
            // Use externalDataProviderId since it's a known value the FrontendSimulator can verify.
            seconds = apiIdToExternalId(DataProviderKey.fromName(edpData.name)!!.dataProviderId)
          }
        }
      }

    fulfillDirectMeasurement(requisition, requisitionSpec, measurementSpec, requisitionData)
  }

  private suspend fun fulfillDirectMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    requisitionData: Measurement.Result
  ) {
    val measurementEncryptionPublicKey =
      EncryptionPublicKey.parseFrom(measurementSpec.measurementPublicKey)

    val signedData = signMessage(requisitionData, edpData.signingKey)

    val encryptedData =
      measurementEncryptionPublicKey.toPublicKeyHandle().hybridEncrypt(signedData.toByteString())

    requisitionsStub.fulfillDirectRequisition(
      fulfillDirectRequisitionRequest {
        name = requisition.name
        this.encryptedData = encryptedData
        nonce = requisitionSpec.nonce
      }
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    val celProtoTypeRegistry: ProtoTypeRegistry =
      ProtoTypeRegistry.newRegistry(
        TestVideoTemplate.getDefaultInstance(),
      )

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

private fun Timestamp.toLocalDate(timeZone: String): LocalDate =
  Instant.ofEpochSecond(this.getSeconds(), this.getNanos().toLong())
    .atZone(ZoneId.of(timeZone))
    .toLocalDate()
