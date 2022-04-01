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
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
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
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventTemplate
import org.wfanet.measurement.api.v2alpha.EventTemplateTypeRegistry
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.LiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
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
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidator
import org.wfanet.measurement.loadtest.storage.SketchStore

private const val EVENT_TEMPLATE_PACKAGE_NAME =
  "org.wfanet.measurement.api.v2alpha.event_templates.testing"

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
  private val eventTemplateNames: List<String> = emptyList()
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
        logger.info(
          "Skipping requisition ${requisition.name}, only LIQUID_LEGIONS_V2 is supported..."
        )
        // TODO(@tristanvuong): fulfill direct measurements
        continue
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

  private fun generateSketch(
    sketchConfig: SketchConfig,
    eventFilter: EventFilter,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float
  ): Sketch {
    logger.info("Generating Sketch...")
    if (eventFilter.expression.isNotBlank()) {
      validateEventFilter(eventFilter)
    }

    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchConfig)

    // TODO(@uakyol): change EventQuery getUserVirtualIds to accept EventFilter rather than
    // QueryParameter.
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

  private suspend fun fulfillRequisitionForReachAndFrequencyMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionFingerprint: ByteString,
    requisitionSpec: RequisitionSpec
  ) {
    val combinedPublicKey =
      requisition.getCombinedPublicKey(requisition.protocolConfig.liquidLegionsV2.ellipticCurveId)
    val sketchConfig = requisition.protocolConfig.liquidLegionsV2.sketchParams.toSketchConfig()

    val vidSamplingIntervalStart = measurementSpec.reachAndFrequency.vidSamplingInterval.start
    val vidSamplingIntervalWidth = measurementSpec.reachAndFrequency.vidSamplingInterval.width

    val sketch =
      try {
        generateSketch(
          sketchConfig,
          requisitionSpec.eventGroupsList[0].value.filter,
          vidSamplingIntervalStart,
          vidSamplingIntervalWidth
        )
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

  private fun validateEventFilter(eventFilter: EventFilter) {
    val decls =
      eventTemplateNames.map {
        Decls.newVar(
          EventTemplate(templateProtoTypeRegistry.getDescriptorForType(it)!!).name,
          Decls.newObjectType(it),
        )
      }

    val env =
      Env.newEnv(
        EnvOption.customTypeAdapter(celProtoTypeRegistry),
        EnvOption.customTypeProvider(celProtoTypeRegistry),
        EnvOption.declarations(decls),
      )

    EventFilterValidator.validate(eventFilter.expression, env)
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val templateProtoTypeRegistry: EventTemplateTypeRegistry =
      EventTemplateTypeRegistry.createRegistryForPackagePrefix(EVENT_TEMPLATE_PACKAGE_NAME)
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
