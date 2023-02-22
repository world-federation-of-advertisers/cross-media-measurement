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

import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.duration
import io.grpc.StatusException
import java.nio.file.Paths
import java.security.GeneralSecurityException
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.apache.commons.math3.distribution.LaplaceDistribution
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.SketchConfigKt.indexSpec
import org.wfanet.anysketch.SketchConfigKt.valueSpec
import org.wfanet.anysketch.SketchProtos
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysResponse
import org.wfanet.anysketch.crypto.ElGamalPublicKey as AnySketchElGamalPublicKey
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.anysketch.crypto.combineElGamalPublicKeysRequest
import org.wfanet.anysketch.crypto.elGamalPublicKey as anySketchElGamalPublicKey
import org.wfanet.anysketch.distribution
import org.wfanet.anysketch.exponentialDistribution
import org.wfanet.anysketch.oracleDistribution
import org.wfanet.anysketch.sketchConfig
import org.wfanet.anysketch.uniformDistribution
import org.wfanet.estimation.VidSampler
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.api.v2alpha.EventGroupKt.metadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.LiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.watchDuration
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.DuchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.createEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt as TestMetadataMessages
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.consent.client.common.NonceMismatchException
import org.wfanet.measurement.consent.client.common.PublicKeyMismatchException
import org.wfanet.measurement.consent.client.common.toPublicKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.computeRequisitionFingerprint
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.consent.client.dataprovider.verifyElGamalPublicKey
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.consent.client.measurementconsumer.verifyEncryptionPublicKey
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper
import org.wfanet.measurement.loadtest.config.EventFilters.VID_SAMPLER_HASH_FUNCTION
import org.wfanet.measurement.loadtest.config.TestIdentifiers
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
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
  private val certificatesStub: CertificatesCoroutineStub,
  private val eventGroupsStub: EventGroupsCoroutineStub,
  private val eventGroupMetadataDescriptorsStub: EventGroupMetadataDescriptorsCoroutineStub,
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val requisitionFulfillmentStub: RequisitionFulfillmentCoroutineStub,
  private val sketchStore: SketchStore,
  private val eventQuery: EventQuery,
  private val throttler: Throttler,
  private val eventTemplateNames: List<String>,
  private val privacyBudgetManager: PrivacyBudgetManager,
  private val trustedCertificates: Map<ByteString, X509Certificate>
) {

  /** A sequence of operations done in the simulator. */
  suspend fun run() {
    throttler.loopOnReady { executeRequisitionFulfillingWorkflow() }
  }

  /** Creates an eventGroup for the MC. */
  suspend fun createEventGroup(): EventGroup {
    val measurementConsumer: MeasurementConsumer =
      try {
        measurementConsumersStub.getMeasurementConsumer(
          getMeasurementConsumerRequest { name = measurementConsumerName }
        )
      } catch (e: StatusException) {
        throw Exception("Error getting MeasurementConsumer $measurementConsumerName", e)
      }

    verifyEncryptionPublicKey(
      measurementConsumer.publicKey,
      getCertificate(measurementConsumer.certificate)
    )

    val descriptorResource: EventGroupMetadataDescriptor =
      try {
        val metadataDescriptor: Descriptors.Descriptor = TestMetadataMessage.getDescriptor()
        eventGroupMetadataDescriptorsStub.createEventGroupMetadataDescriptor(
          createEventGroupMetadataDescriptorRequest {
            parent = edpData.name
            eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
              descriptorSet = ProtoReflection.buildFileDescriptorSet(metadataDescriptor)
            }
            requestId = "type.googleapis.com/${metadataDescriptor.fullName}"
          }
        )
      } catch (e: StatusException) {
        throw Exception("Error creating EventGroupMetadataDescriptor", e)
      }

    val request = createEventGroupRequest {
      parent = edpData.name
      eventGroup = eventGroup {
        this.measurementConsumer = measurementConsumerName
        eventGroupReferenceId =
          "${TestIdentifiers.EVENT_GROUP_REFERENCE_ID_PREFIX}-${edpData.displayName}"
        eventTemplates += eventTemplateNames.map { eventTemplate { type = it } }
        measurementConsumerCertificate = measurementConsumer.certificate
        measurementConsumerPublicKey = measurementConsumer.publicKey
        encryptedMetadata =
          encryptMetadata(
            metadata {
              this.eventGroupMetadataDescriptor = descriptorResource.name
              this.metadata =
                Any.pack(
                  testMetadataMessage { name = TestMetadataMessages.name { value = "John Doe" } }
                )
            },
            EncryptionPublicKey.parseFrom(measurementConsumer.publicKey.data)
          )
      }
    }
    val eventGroup =
      try {
        eventGroupsStub.createEventGroup(request)
      } catch (e: StatusException) {
        throw Exception("Error creating event group", e)
      }
    logger.info("Successfully created eventGroup ${eventGroup.name}...")
    return eventGroup
  }

  private data class Specifications(
    val measurementSpec: MeasurementSpec,
    val requisitionSpec: RequisitionSpec
  )

  private class VerificationException(message: String? = null, cause: Throwable? = null) :
    GeneralSecurityException(message, cause)

  private fun verifySpecifications(
    requisition: Requisition,
    measurementConsumerCertificate: Certificate
  ): Specifications {
    val x509Certificate = readCertificate(measurementConsumerCertificate.x509Der)
    // Look up the trusted issuer certificate for this MC certificate. Note that this doesn't
    // confirm that this is the trusted issuer for the right MC. In a production environment,
    // consider having a mapping of MC to root/CA cert.
    val trustedIssuer =
      trustedCertificates[checkNotNull(x509Certificate.authorityKeyIdentifier)]
        ?: throw VerificationException(
          "Issuer of ${measurementConsumerCertificate.name} is not trusted"
        )

    try {
      verifyMeasurementSpec(requisition.measurementSpec, x509Certificate, trustedIssuer)
    } catch (e: CertPathValidatorException) {
      throw VerificationException(
        "Certificate path for ${measurementConsumerCertificate.name} is invalid",
        e
      )
    } catch (e: SignatureException) {
      throw VerificationException("MeasurementSpec signature is invalid", e)
    }

    val measurementSpec = MeasurementSpec.parseFrom(requisition.measurementSpec.data)

    val signedRequisitionSpec: SignedData =
      try {
        decryptRequisitionSpec(requisition.encryptedRequisitionSpec, edpData.encryptionKey)
      } catch (e: GeneralSecurityException) {
        throw VerificationException("RequisitionSpec decryption failed", e)
      }
    val requisitionSpec = RequisitionSpec.parseFrom(signedRequisitionSpec.data)
    try {
      verifyRequisitionSpec(
        signedRequisitionSpec,
        requisitionSpec,
        measurementSpec,
        x509Certificate,
        trustedIssuer
      )
    } catch (e: CertPathValidatorException) {
      throw VerificationException(
        "Certificate path for ${measurementConsumerCertificate.name} is invalid",
        e
      )
    } catch (e: SignatureException) {
      throw VerificationException("RequisitionSpec signature is invalid", e)
    } catch (e: NonceMismatchException) {
      throw VerificationException(e.message, e)
    } catch (e: PublicKeyMismatchException) {
      throw VerificationException(e.message, e)
    }

    // TODO(@uakyol): Validate that collection interval is not outside of privacy landscape.

    return Specifications(measurementSpec, requisitionSpec)
  }

  private fun verifyDuchyEntry(
    duchyEntry: DuchyEntry,
    duchyCertificate: Certificate,
    protocol: ProtocolConfig.Protocol.ProtocolCase
  ) {
    require(protocol == ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2) {
      "Unsupported protocol $protocol"
    }

    val duchyX509Certificate: X509Certificate = readCertificate(duchyCertificate.x509Der)
    // Look up the trusted issuer certificate for this Duchy certificate. Note that this doesn't
    // confirm that this is the trusted issuer for the right Duchy. In a production environment,
    // consider having a mapping of Duchy to issuer certificate.
    val trustedIssuer =
      trustedCertificates[checkNotNull(duchyX509Certificate.authorityKeyIdentifier)]
        ?: throw VerificationException("Issuer of ${duchyCertificate.name} is not trusted")

    try {
      verifyElGamalPublicKey(
        duchyEntry.value.liquidLegionsV2.elGamalPublicKey,
        duchyX509Certificate,
        trustedIssuer
      )
    } catch (e: CertPathValidatorException) {
      throw VerificationException("Certificate path for ${duchyCertificate.name} is invalid", e)
    } catch (e: SignatureException) {
      throw VerificationException(
        "ElGamal public key signature is invalid for Duchy ${duchyEntry.key}",
        e
      )
    }
  }

  private fun verifyEncryptionPublicKey(
    signedEncryptionPublicKey: SignedData,
    measurementConsumerCertificate: Certificate
  ) {
    val x509Certificate = readCertificate(measurementConsumerCertificate.x509Der)
    // Look up the trusted issuer certificate for this MC certificate. Note that this doesn't
    // confirm that this is the trusted issuer for the right MC. In a production environment,
    // consider having a mapping of MC to root/CA cert.
    val trustedIssuer =
      trustedCertificates[checkNotNull(x509Certificate.authorityKeyIdentifier)]
        ?: throw VerificationException(
          "Issuer of ${measurementConsumerCertificate.name} is not trusted"
        )
    // TODO(world-federation-of-advertisers/consent-signaling-client#41): Use method from
    // DataProviders client instead of MeasurementConsumers client.
    try {
      verifyEncryptionPublicKey(signedEncryptionPublicKey, x509Certificate, trustedIssuer)
    } catch (e: CertPathValidatorException) {
      throw VerificationException(
        "Certificate path for ${measurementConsumerCertificate.name} is invalid",
        e
      )
    } catch (e: SignatureException) {
      throw VerificationException("EncryptionPublicKey signature is invalid", e)
    }
  }

  private suspend fun getCertificate(resourceName: String): Certificate {
    return try {
      certificatesStub.getCertificate(getCertificateRequest { name = resourceName })
    } catch (e: StatusException) {
      throw Exception("Error fetching certificate $resourceName", e)
    }
  }

  /** Executes the requisition fulfillment workflow. */
  suspend fun executeRequisitionFulfillingWorkflow() {
    logger.info("Executing requisitionFulfillingWorkflow...")
    val requisitions = getRequisitions()
    if (requisitions.isEmpty()) {
      logger.fine("No unfulfilled requisition. Polling again later...")
      return
    }

    for (requisition in requisitions) {
      logger.info("Processing requisition ${requisition.name}...")
      val measurementConsumerCertificate: Certificate =
        getCertificate(requisition.measurementConsumerCertificate)

      val (measurementSpec, requisitionSpec) =
        try {
          verifySpecifications(requisition, measurementConsumerCertificate)
        } catch (e: VerificationException) {
          logger.log(Level.WARNING, e) {
            "Consent signaling verification failed for ${requisition.name}"
          }
          refuseRequisition(
            requisition.name,
            Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
            e.message.orEmpty()
          )
          continue
        }

      val requisitionFingerprint = computeRequisitionFingerprint(requisition)

      val protocols: List<ProtocolConfig.Protocol> = requisition.protocolConfig.protocolsList
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
      when (measurementSpec.measurementTypeCase) {
        MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
          if (protocols.any { it.hasDirect() }) {
            fulfillDirectReachAndFrequencyMeasurement(requisition, requisitionSpec, measurementSpec)
            continue
          }
          if (protocols.any { it.hasLiquidLegionsV2() }) {
            try {
              for (duchyEntry in requisition.duchiesList) {
                val duchyCertificate: Certificate =
                  getCertificate(duchyEntry.value.duchyCertificate)
                verifyDuchyEntry(
                  duchyEntry,
                  duchyCertificate,
                  ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2
                )
              }
            } catch (e: VerificationException) {
              logger.log(Level.WARNING, e) {
                "Consent signaling verification failed for ${requisition.name}"
              }
              refuseRequisition(
                requisition.name,
                Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
                e.message.orEmpty()
              )
              continue
            }

            fulfillRequisitionForReachAndFrequencyMeasurement(
              requisition,
              measurementSpec,
              requisitionFingerprint,
              requisitionSpec
            )
            continue
          }
        }
        MeasurementSpec.MeasurementTypeCase.IMPRESSION -> {
          if (protocols.any { it.hasDirect() }) {
            fulfillImpressionMeasurement(requisition, requisitionSpec, measurementSpec)
            continue
          }
        }
        MeasurementSpec.MeasurementTypeCase.DURATION -> {
          if (protocols.any { it.hasDirect() }) {
            fulfillDurationMeasurement(requisition, requisitionSpec, measurementSpec)
            continue
          }
        }
        MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
          error("Measurement type not set for ${requisition.name}")
      }
      logger.warning {
        "Skipping ${requisition.name}: No supported protocol for measurement type ${measurementSpec.measurementTypeCase}"
      }
    }
  }

  private suspend fun refuseRequisition(
    requisitionName: String,
    justification: Requisition.Refusal.Justification,
    message: String
  ): Requisition {
    try {
      return requisitionsStub.refuseRequisition(
        refuseRequisitionRequest {
          name = requisitionName
          refusal = refusal {
            this.justification = justification
            this.message = message
          }
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error refusing requisition $requisitionName", e)
    }
  }

  private fun populateAnySketch(
    eventSpec: RequisitionSpec.EventGroupEntry.Value,
    vidSampler: VidSampler,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float,
    anySketch: AnySketch
  ) {
    eventQuery
      .getUserVirtualIds(eventSpec.collectionInterval, eventSpec.filter)
      .filter {
        vidSampler.vidIsInSamplingBucket(it, vidSamplingIntervalStart, vidSamplingIntervalWidth)
      }
      .forEach { anySketch.insert(it, mapOf("frequency" to 1L)) }
  }

  private suspend fun chargePrivacyBudget(
    requisitionName: String,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec
  ) =
    try {
      privacyBudgetManager.chargePrivacyBudget(
        PrivacyQueryMapper.getPrivacyQuery(
          Reference(measurementConsumerName, requisitionName, false),
          requisitionSpec,
          measurementSpec
        )
      )
    } catch (e: PrivacyBudgetManagerException) {
      when (e.errorType) {
        PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED -> {
          refuseRequisition(
            requisitionName,
            Requisition.Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET,
            "Privacy budget exceeded"
          )
        }
        PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER -> {
          refuseRequisition(
            requisitionName,
            Requisition.Refusal.Justification.SPECIFICATION_INVALID,
            "Invalid event filter"
          )
        }
        PrivacyBudgetManagerExceptionType.DATABASE_UPDATE_ERROR,
        PrivacyBudgetManagerExceptionType.UPDATE_AFTER_COMMIT,
        PrivacyBudgetManagerExceptionType.NESTED_TRANSACTION,
        PrivacyBudgetManagerExceptionType.BACKING_STORE_CLOSED -> {
          throw Exception("Unexpected PBM error", e)
        }
      }
      logger.log(Level.WARNING, "RequisitionFulfillmentWorkflow failed due to ${e.errorType}", e)
    }

  private suspend fun generateSketch(
    requisitionName: String,
    sketchConfig: SketchConfig,
    measurementSpec: MeasurementSpec,
    requisitionSpec: RequisitionSpec
  ): Sketch {
    chargePrivacyBudget(requisitionName, measurementSpec, requisitionSpec)
    val vidSamplingIntervalStart = measurementSpec.vidSamplingInterval.start
    val vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width

    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchConfig)
    logger.info("Generating Sketch...")

    requisitionSpec.eventGroupsList.forEach {
      populateAnySketch(
        it.value,
        VidSampler(VID_SAMPLER_HASH_FUNCTION),
        vidSamplingIntervalStart,
        vidSamplingIntervalWidth,
        anySketch
      )
    }
    return SketchProtos.fromAnySketch(anySketch, sketchConfig)
  }

  private fun encryptSketch(
    sketch: Sketch,
    combinedPublicKey: AnySketchElGamalPublicKey,
    protocolConfig: ProtocolConfig.LiquidLegionsV2
  ): ByteString {
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

    return response.encryptedSketch
  }

  /**
   * Calculate reach and frequency for measurement with multiple EDPs by creating encrypted sketch
   * and send to Duchy to perform MPC and fulfillRequisition
   */
  private suspend fun fulfillRequisitionForReachAndFrequencyMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionFingerprint: ByteString,
    requisitionSpec: RequisitionSpec
  ) {
    val llv2Protocol: ProtocolConfig.Protocol =
      requireNotNull(
        requisition.protocolConfig.protocolsList.find { protocol -> protocol.hasLiquidLegionsV2() }
      ) {
        "Protocol with LiquidLegionsV2 is missing"
      }
    val liquidLegionsV2: ProtocolConfig.LiquidLegionsV2 = llv2Protocol.liquidLegionsV2
    val combinedPublicKey = requisition.getCombinedPublicKey(liquidLegionsV2.ellipticCurveId)
    val sketchConfig = liquidLegionsV2.sketchParams.toSketchConfig()

    val sketch =
      try {
        generateSketch(requisition.name, sketchConfig, measurementSpec, requisitionSpec)
      } catch (e: EventFilterValidationException) {
        refuseRequisition(
          requisition.name,
          Requisition.Refusal.Justification.SPECIFICATION_INVALID,
          "Invalid event filter (${e.code}): ${e.code.description}"
        )
        logger.log(
          Level.WARNING,
          "RequisitionFulfillmentWorkflow failed due to invalid event filter",
          e
        )
        return
      }

    logger.info("Writing sketch to storage")
    sketchStore.write(requisition, sketch.toByteString())

    val encryptedSketch = encryptSketch(sketch, combinedPublicKey, liquidLegionsV2)
    fulfillRequisition(
      requisition.name,
      requisitionFingerprint,
      requisitionSpec.nonce,
      encryptedSketch
    )
  }

  private suspend fun fulfillRequisition(
    requisitionName: String,
    requisitionFingerprint: ByteString,
    nonce: Long,
    data: ByteString,
  ) {
    logger.info("Fulfilling requisition $requisitionName...")
    val requests: Flow<FulfillRequisitionRequest> = flow {
      emit(
        fulfillRequisitionRequest {
          header = header {
            name = requisitionName
            this.requisitionFingerprint = requisitionFingerprint
            this.nonce = nonce
          }
        }
      )
      emitAll(
        data.asBufferedFlow(RPC_CHUNK_SIZE_BYTES).map {
          fulfillRequisitionRequest { bodyChunk = bodyChunk { this.data = it } }
        }
      )
    }
    try {
      requisitionFulfillmentStub.fulfillRequisition(requests)
    } catch (e: StatusException) {
      throw Exception("Error fulfilling requisition $requisitionName", e)
    }
  }

  private fun Requisition.getCombinedPublicKey(curveId: Int): AnySketchElGamalPublicKey {
    logger.info("Getting combined public key...")
    val elGamalPublicKeys: List<AnySketchElGamalPublicKey> =
      this.duchiesList.map {
        ElGamalPublicKey.parseFrom(it.value.liquidLegionsV2.elGamalPublicKey.data)
          .toAnySketchElGamalPublicKey()
      }

    val request = combineElGamalPublicKeysRequest {
      this.curveId = curveId.toLong()
      this.elGamalKeys += elGamalPublicKeys
    }

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

    try {
      return requisitionsStub.listRequisitions(request).requisitionsList
    } catch (e: StatusException) {
      throw Exception("Error listing requisitions", e)
    }
  }

  /**
   * Calculate direct reach and frequency for measurement with single EDP by summing up VIDs
   * directly and fulfillDirectMeasurement
   */
  private suspend fun fulfillDirectReachAndFrequencyMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec
  ) {
    logger.info("Calculating direct reach and frequency...")
    val vidSampler = VidSampler(VID_SAMPLER_HASH_FUNCTION)
    val vidSamplingInterval = measurementSpec.vidSamplingInterval
    val vidSamplingIntervalStart = vidSamplingInterval.start
    val vidSamplingIntervalWidth = vidSamplingInterval.width

    require(vidSamplingIntervalWidth > 0 && vidSamplingIntervalWidth <= 1.0) {
      "Invalid vidSamplingIntervalWidth $vidSamplingIntervalWidth"
    }
    require(
      vidSamplingIntervalStart < 1 &&
        vidSamplingIntervalStart >= 0 &&
        vidSamplingIntervalWidth > 0 &&
        vidSamplingIntervalStart + vidSamplingIntervalWidth <= 1
    ) {
      "Invalid vidSamplingInterval: $vidSamplingInterval"
    }

    val vidList: List<Long> =
      requisitionSpec.eventGroupsList
        .distinctBy { eventGroup -> eventGroup.key }
        .flatMap { eventGroup ->
          eventQuery.getUserVirtualIds(eventGroup.value.collectionInterval, eventGroup.value.filter)
        }
        .filter { vid ->
          vidSampler.vidIsInSamplingBucket(vid, vidSamplingIntervalStart, vidSamplingIntervalWidth)
        }
    val (sampledReachValue, frequencyMap) = calculateDirectReachAndFrequency(vidList)

    logger.info("Adding publisher noise to direct reach and frequency...")
    val (sampledNoisedReachValue, noisedFrequencyMap) =
      addPublisherNoise(sampledReachValue, frequencyMap, measurementSpec.reachAndFrequency)

    // Differentially private reach value is calculated by reach_dp = (reach + noise) /
    // sampling_rate.
    val scaledNoisedReachValue = (sampledNoisedReachValue / vidSamplingIntervalWidth).toLong()
    val requisitionData =
      MeasurementKt.result {
        reach = reach { value = scaledNoisedReachValue }
        frequency = frequency { relativeFrequencyDistribution.putAll(noisedFrequencyMap) }
      }

    fulfillDirectMeasurement(requisition, requisitionSpec, measurementSpec, requisitionData)
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

    // TODO(world-federation-of-advertisers/consent-signaling-client#41): Use method from
    // DataProviders client instead of Duchies client.
    val signedData = signResult(requisitionData, edpData.signingKey)

    val encryptedData =
      measurementEncryptionPublicKey.toPublicKeyHandle().hybridEncrypt(signedData.toByteString())

    try {
      requisitionsStub.fulfillDirectRequisition(
        fulfillDirectRequisitionRequest {
          name = requisition.name
          this.encryptedData = encryptedData
          nonce = requisitionSpec.nonce
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error fulfilling direct requisition ${requisition.name}", e)
    }
  }

  companion object {
    private const val RPC_CHUNK_SIZE_BYTES = 32 * 1024 // 32 KiB

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

    /**
     * Calculate direct reach and frequency from VIDs in
     * fulfillDirectReachAndFrequencyMeasurement().
     *
     * @param vidList List of VIDs.
     * @return Pair of reach value and frequency map.
     */
    private fun calculateDirectReachAndFrequency(
      vidList: List<Long>,
    ): Pair<Long, Map<Long, Double>> {
      // Example: vidList: [1L, 1L, 1L, 2L, 2L, 3L, 4L, 5L]
      // 5 unique people(1, 2, 3, 4, 5) being reached
      // reach = 5
      // 1 reach -> 0.6(3/5)(VID 3L, 4L, 5L)
      // 2 reach -> 0.2(1/5)(VID 2L)
      // 3 reach -> 0.2(1/5)(VID 1L)
      // frequencyMap = {1L: 0.6, 2L to 0.2, 3L: 0.2}
      val reachValue = vidList.toSet().size.toLong()
      val frequencyMap = mutableMapOf<Long, Double>().withDefault { 0.0 }

      vidList
        .groupingBy { it }
        .eachCount()
        .forEach { (_, frequency) ->
          frequencyMap[frequency.toLong()] = frequencyMap.getValue(frequency.toLong()) + 1.0
        }

      frequencyMap.forEach { (frequency, _) ->
        frequencyMap[frequency] = frequencyMap.getValue(frequency) / reachValue.toDouble()
      }

      return Pair(reachValue, frequencyMap)
    }

    //
    /**
     * Add Laplace publisher noise to calculated direct reach and frequency. TODO(@iverson52000):
     * Create a noiser class for this function when we need to add Gaussian noise.
     *
     * @param reachValue Direct reach value.
     * @param frequencyMap Direct frequency.
     * @param reachAndFrequency ReachAndFrequency from MeasurementSpec.
     * @return Pair of noised reach value and frequency map.
     */
    private fun addPublisherNoise(
      reachValue: Long,
      frequencyMap: Map<Long, Double>,
      reachAndFrequency: MeasurementSpec.ReachAndFrequency
    ): Pair<Long, Map<Long, Double>> {
      val laplaceForReach =
        LaplaceDistribution(0.0, 1 / reachAndFrequency.reachPrivacyParams.epsilon)
      // Reseed random number generator so the results can be verified in tests.
      // TODO(@iverson52000): make randomSeed an input once create noiser class.
      laplaceForReach.reseedRandomGenerator(1)

      val noisedReachValue = (reachValue + laplaceForReach.sample().toInt())

      val laplaceForFrequency =
        LaplaceDistribution(0.0, 1 / reachAndFrequency.frequencyPrivacyParams.epsilon)
      laplaceForFrequency.reseedRandomGenerator(1)

      val noisedFrequencyMap = mutableMapOf<Long, Double>()
      frequencyMap.forEach { (frequency, percentage) ->
        noisedFrequencyMap[frequency] =
          (percentage * reachValue.toDouble() + laplaceForFrequency.sample()) /
            reachValue.toDouble()
      }

      return Pair(noisedReachValue, noisedFrequencyMap)
    }
  }
}

private fun ElGamalPublicKey.toAnySketchElGamalPublicKey(): AnySketchElGamalPublicKey {
  val source = this
  return anySketchElGamalPublicKey {
    generator = source.generator
    element = source.element
  }
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
