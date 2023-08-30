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
import com.google.protobuf.Message
import com.google.protobuf.duration
import io.grpc.Status
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.log2
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.apache.commons.math3.distribution.ConstantRealDistribution
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.crypto.ElGamalPublicKey as AnySketchElGamalPublicKey
import org.wfanet.anysketch.crypto.elGamalPublicKey as anySketchElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupKt.eventTemplate
import org.wfanet.measurement.api.v2alpha.EventGroupKt.metadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.bodyChunk
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt.header
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.watchDuration
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.DuchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.identity.apiIdToExternalId
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
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.CompositionMechanism
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper.getDirectAcdpQuery
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper.getDpQuery
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper.getLiquidLegionsV2AcdpQuery
import org.wfanet.measurement.loadtest.config.TestIdentifiers.CONSENT_SIGNAL_INVALID_EVENT_GROUP_ID
import org.wfanet.measurement.loadtest.config.TestIdentifiers.DECLINED_EVENT_GROUP_ID
import org.wfanet.measurement.loadtest.config.TestIdentifiers.INSUFFICIENT_PRIVACY_BUDGET_EVENT_GROUP_ID
import org.wfanet.measurement.loadtest.config.TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
import org.wfanet.measurement.loadtest.config.TestIdentifiers.SPEC_INVALID_EVENT_GROUP_ID
import org.wfanet.measurement.loadtest.config.TestIdentifiers.UNFULFILLABLE_EVENT_GROUP_ID
import org.wfanet.measurement.loadtest.config.VidSampling

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
  private val eventQuery: EventQuery<Message>,
  private val throttler: Throttler,
  private val privacyBudgetManager: PrivacyBudgetManager,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  private val directNoiseMechanism: DirectNoiseMechanism,
  private val sketchEncrypter: SketchEncrypter = SketchEncrypter.Default,
  private val random: Random = Random,
  private val compositionMechanism: CompositionMechanism,
) {
  /** A sequence of operations done in the simulator. */
  suspend fun run() {
    throttler.loopOnReady { executeRequisitionFulfillingWorkflow() }
  }

  /**
   * Ensures that an appropriate [EventGroup] with an appropriate [EventGroupMetadataDescriptor]
   * exists for the [MeasurementConsumer].
   *
   * TODO(@SanjayVas): Create multiple EventGroups with different synthetic data specs.
   */
  suspend fun ensureEventGroup(eventGroupMetadata: Message): EventGroup {
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
      ensureMetadataDescriptor(eventGroupMetadata.descriptorForType)

    val eventGroupReferenceId = "$SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX-${edpData.displayName}"
    return ensureEventGroup(
      measurementConsumer,
      eventGroupReferenceId,
      eventGroupMetadata,
      descriptorResource
    )
  }

  /**
   * Ensures that an [EventGroup] exists for [measurementConsumer] with the specified
   * [eventGroupReferenceId] and [descriptorResource].
   */
  private suspend fun ensureEventGroup(
    measurementConsumer: MeasurementConsumer,
    eventGroupReferenceId: String,
    eventGroupMetadata: Message,
    descriptorResource: EventGroupMetadataDescriptor,
  ): EventGroup {
    val existingEventGroup: EventGroup? = getEventGroupByReferenceId(eventGroupReferenceId)
    val encryptedMetadata: ByteString =
      encryptMetadata(
        metadata {
          eventGroupMetadataDescriptor = descriptorResource.name
          metadata = Any.pack(eventGroupMetadata)
        },
        EncryptionPublicKey.parseFrom(measurementConsumer.publicKey.data)
      )

    if (existingEventGroup == null) {
      val request = createEventGroupRequest {
        parent = edpData.name
        eventGroup = eventGroup {
          this.measurementConsumer = measurementConsumerName
          this.eventGroupReferenceId = eventGroupReferenceId
          eventTemplates += EVENT_TEMPLATES
          measurementConsumerCertificate = measurementConsumer.certificate
          measurementConsumerPublicKey = measurementConsumer.publicKey
          this.encryptedMetadata = encryptedMetadata
        }
      }

      return try {
        eventGroupsStub.createEventGroup(request).also {
          logger.info { "Successfully created ${it.name}..." }
        }
      } catch (e: StatusException) {
        throw Exception("Error creating event group", e)
      }
    }

    val request = updateEventGroupRequest {
      eventGroup =
        existingEventGroup.copy {
          eventTemplates.clear()
          eventTemplates += EVENT_TEMPLATES
          measurementConsumerCertificate = measurementConsumer.certificate
          measurementConsumerPublicKey = measurementConsumer.publicKey
          this.encryptedMetadata = encryptedMetadata
        }
    }
    return try {
      eventGroupsStub.updateEventGroup(request).also {
        logger.info { "Successfully updated ${it.name}..." }
      }
    } catch (e: StatusException) {
      throw Exception("Error updating event group", e)
    }
  }

  /**
   * Returns the first [EventGroup] for this `DataProvider` and [MeasurementConsumer] with
   * [eventGroupReferenceId], or `null` if not found.
   */
  private suspend fun getEventGroupByReferenceId(eventGroupReferenceId: String): EventGroup? {
    val response =
      try {
        eventGroupsStub.listEventGroups(
          listEventGroupsRequest {
            parent = edpData.name
            filter =
              ListEventGroupsRequestKt.filter { measurementConsumers += measurementConsumerName }
            pageSize = Int.MAX_VALUE
          }
        )
      } catch (e: StatusException) {
        throw Exception("Error listing EventGroups", e)
      }

    // TODO(@SanjayVas): Support filtering by reference ID so we don't need to handle multiple pages
    // of EventGroups.
    check(response.nextPageToken.isEmpty()) {
      "Too many EventGroups for ${edpData.name} and $measurementConsumerName"
    }
    return response.eventGroupsList.find { it.eventGroupReferenceId == eventGroupReferenceId }
  }

  private suspend fun ensureMetadataDescriptor(
    metadataDescriptor: Descriptors.Descriptor
  ): EventGroupMetadataDescriptor {
    val descriptorSet = ProtoReflection.buildFileDescriptorSet(metadataDescriptor)
    val descriptorResource =
      try {
        eventGroupMetadataDescriptorsStub.createEventGroupMetadataDescriptor(
          createEventGroupMetadataDescriptorRequest {
            parent = edpData.name
            eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
              this.descriptorSet = ProtoReflection.buildFileDescriptorSet(metadataDescriptor)
            }
            requestId = "type.googleapis.com/${metadataDescriptor.fullName}"
          }
        )
      } catch (e: StatusException) {
        throw Exception("Error creating EventGroupMetadataDescriptor", e)
      }

    if (descriptorResource.descriptorSet == descriptorSet) {
      return descriptorResource
    }

    return try {
      eventGroupMetadataDescriptorsStub.updateEventGroupMetadataDescriptor(
        updateEventGroupMetadataDescriptorRequest {
          eventGroupMetadataDescriptor =
            descriptorResource.copy { this.descriptorSet = descriptorSet }
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error updating EventGroupMetadataDescriptor", e)
    }
  }

  private data class Specifications(
    val measurementSpec: MeasurementSpec,
    val requisitionSpec: RequisitionSpec
  )

  private class RequisitionRefusalException(
    val justification: Requisition.Refusal.Justification,
    message: String
  ) : Exception(message)

  private class InvalidConsentSignalException(message: String? = null, cause: Throwable? = null) :
    GeneralSecurityException(message, cause)

  private class InvalidSpecException(message: String, cause: Throwable? = null) :
    Exception(message, cause)

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
        ?: throw InvalidConsentSignalException(
          "Issuer of ${measurementConsumerCertificate.name} is not trusted"
        )

    try {
      verifyMeasurementSpec(requisition.measurementSpec, x509Certificate, trustedIssuer)
    } catch (e: CertPathValidatorException) {
      throw InvalidConsentSignalException(
        "Certificate path for ${measurementConsumerCertificate.name} is invalid",
        e
      )
    } catch (e: SignatureException) {
      throw InvalidConsentSignalException("MeasurementSpec signature is invalid", e)
    }

    val measurementSpec = MeasurementSpec.parseFrom(requisition.measurementSpec.data)

    val signedRequisitionSpec: SignedData =
      try {
        decryptRequisitionSpec(requisition.encryptedRequisitionSpec, edpData.encryptionKey)
      } catch (e: GeneralSecurityException) {
        throw InvalidConsentSignalException("RequisitionSpec decryption failed", e)
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
      throw InvalidConsentSignalException(
        "Certificate path for ${measurementConsumerCertificate.name} is invalid",
        e
      )
    } catch (e: SignatureException) {
      throw InvalidConsentSignalException("RequisitionSpec signature is invalid", e)
    } catch (e: NonceMismatchException) {
      throw InvalidConsentSignalException(e.message, e)
    } catch (e: PublicKeyMismatchException) {
      throw InvalidConsentSignalException(e.message, e)
    }

    // TODO(@uakyol): Validate that collection interval is not outside of privacy landscape.

    return Specifications(measurementSpec, requisitionSpec)
  }

  private fun verifyDuchyEntry(
    duchyEntry: DuchyEntry,
    duchyCertificate: Certificate,
    protocol: ProtocolConfig.Protocol.ProtocolCase
  ) {
    require(
      protocol == ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2 ||
        protocol == ProtocolConfig.Protocol.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2
    ) {
      "Unsupported protocol $protocol"
    }

    val duchyX509Certificate: X509Certificate = readCertificate(duchyCertificate.x509Der)
    // Look up the trusted issuer certificate for this Duchy certificate. Note that this doesn't
    // confirm that this is the trusted issuer for the right Duchy. In a production environment,
    // consider having a mapping of Duchy to issuer certificate.
    val trustedIssuer =
      trustedCertificates[checkNotNull(duchyX509Certificate.authorityKeyIdentifier)]
        ?: throw InvalidConsentSignalException("Issuer of ${duchyCertificate.name} is not trusted")

    try {
      verifyElGamalPublicKey(
        duchyEntry.value.liquidLegionsV2.elGamalPublicKey,
        duchyX509Certificate,
        trustedIssuer
      )
    } catch (e: CertPathValidatorException) {
      throw InvalidConsentSignalException(
        "Certificate path for ${duchyCertificate.name} is invalid",
        e
      )
    } catch (e: SignatureException) {
      throw InvalidConsentSignalException(
        "ElGamal public key signature is invalid for Duchy ${duchyEntry.key}",
        e
      )
    }
  }

  /** Verify duchy entries' certificates. Return true for valid or false for invalid. */
  private suspend fun verifyDuchyEntries(
    requisition: Requisition,
    protocol: ProtocolConfig.Protocol.ProtocolCase
  ) {
    try {
      for (duchyEntry in requisition.duchiesList) {
        val duchyCertificate: Certificate = getCertificate(duchyEntry.value.duchyCertificate)
        verifyDuchyEntry(duchyEntry, duchyCertificate, protocol)
      }
    } catch (e: InvalidConsentSignalException) {
      logger.log(Level.WARNING, e) {
        "Consent signaling verification failed for ${requisition.name}"
      }
      throw RequisitionRefusalException(
        Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
        e.message.orEmpty()
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
        ?: throw InvalidConsentSignalException(
          "Issuer of ${measurementConsumerCertificate.name} is not trusted"
        )
    // TODO(world-federation-of-advertisers/consent-signaling-client#41): Use method from
    // DataProviders client instead of MeasurementConsumers client.
    try {
      verifyEncryptionPublicKey(signedEncryptionPublicKey, x509Certificate, trustedIssuer)
    } catch (e: CertPathValidatorException) {
      throw InvalidConsentSignalException(
        "Certificate path for ${measurementConsumerCertificate.name} is invalid",
        e
      )
    } catch (e: SignatureException) {
      throw InvalidConsentSignalException("EncryptionPublicKey signature is invalid", e)
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
    val requisitions =
      getRequisitions().filter {
        checkNotNull(MeasurementKey.fromName(it.measurement)).measurementConsumerId ==
          checkNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))
            .measurementConsumerId
      }

    if (requisitions.isEmpty()) {
      logger.fine("No unfulfilled requisition. Polling again later...")
      return
    }

    for (requisition in requisitions) {
      try {
        logger.info("Processing requisition ${requisition.name}...")
        val measurementConsumerCertificate: Certificate =
          getCertificate(requisition.measurementConsumerCertificate)

        val (measurementSpec, requisitionSpec) =
          try {
            verifySpecifications(requisition, measurementConsumerCertificate)
          } catch (e: InvalidConsentSignalException) {
            logger.log(Level.WARNING, e) {
              "Consent signaling verification failed for ${requisition.name}"
            }
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
              e.message.orEmpty()
            )
          }

        for (eventGroupEntry in requisitionSpec.events.eventGroupsList) {
          val eventGroupId = EventGroupKey.fromName(eventGroupEntry.key)!!.eventGroupId
          if (eventGroupId == CONSENT_SIGNAL_INVALID_EVENT_GROUP_ID) {
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.CONSENT_SIGNAL_INVALID,
              "consent signal invalid"
            )
          }

          if (eventGroupId == SPEC_INVALID_EVENT_GROUP_ID) {
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "spec invalid"
            )
          }

          if (eventGroupId == INSUFFICIENT_PRIVACY_BUDGET_EVENT_GROUP_ID) {
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET,
              "insufficient privacy budget"
            )
          }

          if (eventGroupId == UNFULFILLABLE_EVENT_GROUP_ID) {
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.UNFULFILLABLE,
              "unfulfillable"
            )
          }

          if (eventGroupId == DECLINED_EVENT_GROUP_ID) {
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.DECLINED,
              "declined"
            )
          }
        }

        val eventGroupSpecs: List<EventQuery.EventGroupSpec> =
          try {
            buildEventGroupSpecs(requisitionSpec)
          } catch (e: InvalidSpecException) {
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.SPEC_INVALID,
              e.message.orEmpty()
            )
          }

        val requisitionFingerprint = computeRequisitionFingerprint(requisition)

        val protocols: List<ProtocolConfig.Protocol> = requisition.protocolConfig.protocolsList

        if (protocols.any { it.hasDirect() }) {
          if (measurementSpec.hasReach() || measurementSpec.hasReachAndFrequency()) {
            fulfillDirectReachAndFrequencyMeasurement(
              requisition,
              measurementSpec,
              requisitionSpec.nonce,
              eventGroupSpecs
            )
          } else if (measurementSpec.hasDuration()) {
            fulfillDurationMeasurement(
              requisition,
              requisitionSpec,
              measurementSpec,
              eventGroupSpecs
            )
          } else if (measurementSpec.hasImpression()) {
            fulfillImpressionMeasurement(
              requisition,
              requisitionSpec,
              measurementSpec,
              eventGroupSpecs
            )
          } else {
            logger.log(
              Level.WARNING,
              "Skipping ${requisition.name}: Measurement type not supported for direct fulfillment."
            )
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Measurement type not supported for direct fulfillment."
            )
          }
        } else if (protocols.any { it.hasLiquidLegionsV2() }) {
          if (!measurementSpec.hasReach() && !measurementSpec.hasReachAndFrequency()) {
            logger.log(
              Level.WARNING,
              "Skipping ${requisition.name}: Measurement type not supported for protocol llv2."
            )
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Measurement type not supported for protocol llv2."
            )
          }
          verifyDuchyEntries(requisition, ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2)

          fulfillRequisitionForLiquidLegionsV2Measurement(
            requisition,
            measurementSpec,
            requisitionFingerprint,
            requisitionSpec.nonce,
            eventGroupSpecs
          )
        } else if (protocols.any { it.hasReachOnlyLiquidLegionsV2() }) {
          if (!measurementSpec.hasReach()) {
            logger.log(
              Level.WARNING,
              "Skipping ${requisition.name}: Measurement type not supported for protocol rollv2."
            )
            throw RequisitionRefusalException(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Measurement type not supported for protocol rollv2."
            )
          }
          verifyDuchyEntries(
            requisition,
            ProtocolConfig.Protocol.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2
          )

          fulfillRequisitionForReachOnlyLiquidLegionsV2Measurement(
            requisition,
            measurementSpec,
            requisitionFingerprint,
            requisitionSpec.nonce,
            eventGroupSpecs
          )
        } else {
          logger.log(
            Level.WARNING,
            "Skipping ${requisition.name}: Protocol not set or not supported."
          )
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Protocol not set or not supported."
          )
        }
      } catch (refusalException: RequisitionRefusalException) {
        refuseRequisition(
          requisition.name,
          refusalException.justification,
          refusalException.message ?: "Refuse to fulfill requisition."
        )
      }
    }
  }

  /**
   * Builds [EventQuery.EventGroupSpec]s from a [requisitionSpec] by fetching [EventGroup]s.
   *
   * @throws InvalidSpecException if [requisitionSpec] is found to be invalid
   */
  private suspend fun buildEventGroupSpecs(
    requisitionSpec: RequisitionSpec
  ): List<EventQuery.EventGroupSpec> {
    // TODO(@SanjayVas): Cache EventGroups.
    return requisitionSpec.events.eventGroupsList.map {
      val eventGroup =
        try {
          eventGroupsStub.getEventGroup(getEventGroupRequest { name = it.key })
        } catch (e: StatusException) {
          throw when (e.status.code) {
            Status.Code.NOT_FOUND -> InvalidSpecException("EventGroup $it not found", e)
            else -> Exception("Error retrieving EventGroup $it", e)
          }
        }

      if (!eventGroup.eventGroupReferenceId.startsWith(SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX)) {
        throw InvalidSpecException("EventGroup ${it.key} not supported by this simulator")
      }

      EventQuery.EventGroupSpec(eventGroup, it.value)
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

  private suspend fun chargeLiquidLegionsV2PrivacyBudget(
    requisitionName: String,
    measurementSpec: MeasurementSpec,
    eventSpecs: Iterable<RequisitionSpec.EventGroupEntry.Value>,
    noiseMechanism: NoiseMechanism,
    contributorCount: Int
  ) {
    logger.info(
      "chargeLiquidLegionsV2PrivacyBudget with $compositionMechanism composition mechanism for requisition with $noiseMechanism noise mechanism...",
    )

    try {
      when (compositionMechanism) {
        CompositionMechanism.DP_ADVANCED ->
          privacyBudgetManager.chargePrivacyBudget(
            getDpQuery(
              Reference(measurementConsumerName, requisitionName, false),
              measurementSpec,
              eventSpecs,
            )
          )
        CompositionMechanism.ACDP -> {
          if (noiseMechanism != NoiseMechanism.DISCRETE_GAUSSIAN) {
            throw PrivacyBudgetManagerException(
              PrivacyBudgetManagerExceptionType.INCORRECT_NOISE_MECHANISM
            )
          }

          privacyBudgetManager.chargePrivacyBudgetInAcdp(
            getLiquidLegionsV2AcdpQuery(
              Reference(measurementConsumerName, requisitionName, false),
              measurementSpec,
              eventSpecs,
              contributorCount,
            )
          )
        }
      }
    } catch (e: PrivacyBudgetManagerException) {
      logger.log(
        Level.WARNING,
        "chargeLiquidLegionsV2PrivacyBudget failed due to ${e.errorType}",
        e
      )
      when (e.errorType) {
        PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED -> {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET,
            "Privacy budget exceeded"
          )
        }
        PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER -> {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Invalid event filter"
          )
        }
        PrivacyBudgetManagerExceptionType.INCORRECT_NOISE_MECHANISM -> {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Incorrect noise mechanism. Should be DISCRETE_GAUSSIAN for ACDP composition but is $noiseMechanism"
          )
        }
        PrivacyBudgetManagerExceptionType.DATABASE_UPDATE_ERROR,
        PrivacyBudgetManagerExceptionType.UPDATE_AFTER_COMMIT,
        PrivacyBudgetManagerExceptionType.NESTED_TRANSACTION,
        PrivacyBudgetManagerExceptionType.BACKING_STORE_CLOSED -> {
          throw Exception("Unexpected PBM error", e)
        }
      }
    }
  }

  private suspend fun chargeDirectPrivacyBudget(
    requisitionName: String,
    measurementSpec: MeasurementSpec,
    eventSpecs: Iterable<RequisitionSpec.EventGroupEntry.Value>,
  ) {
    logger.info(
      "chargeDirectPrivacyBudget with $compositionMechanism composition mechanism...",
    )

    try {
      when (compositionMechanism) {
        CompositionMechanism.DP_ADVANCED ->
          privacyBudgetManager.chargePrivacyBudget(
            getDpQuery(
              Reference(measurementConsumerName, requisitionName, false),
              measurementSpec,
              eventSpecs,
            )
          )
        CompositionMechanism.ACDP -> {
          if (directNoiseMechanism != DirectNoiseMechanism.GAUSSIAN) {
            throw PrivacyBudgetManagerException(
              PrivacyBudgetManagerExceptionType.INCORRECT_NOISE_MECHANISM
            )
          }

          privacyBudgetManager.chargePrivacyBudgetInAcdp(
            getDirectAcdpQuery(
              Reference(measurementConsumerName, requisitionName, false),
              measurementSpec,
              eventSpecs,
            )
          )
        }
      }
    } catch (e: PrivacyBudgetManagerException) {
      logger.log(Level.WARNING, "chargeDirectPrivacyBudget failed due to ${e.errorType}", e)
      when (e.errorType) {
        PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED -> {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET,
            "Privacy budget exceeded"
          )
        }
        PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER -> {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Invalid event filter"
          )
        }
        PrivacyBudgetManagerExceptionType.INCORRECT_NOISE_MECHANISM -> {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Incorrect noise mechanism. Should be GAUSSIAN for ACDP composition but is $directNoiseMechanism"
          )
        }
        PrivacyBudgetManagerExceptionType.DATABASE_UPDATE_ERROR,
        PrivacyBudgetManagerExceptionType.UPDATE_AFTER_COMMIT,
        PrivacyBudgetManagerExceptionType.NESTED_TRANSACTION,
        PrivacyBudgetManagerExceptionType.BACKING_STORE_CLOSED -> {
          throw Exception("Unexpected PBM error", e)
        }
      }
    }
  }

  private fun generateSketch(
    sketchConfig: SketchConfig,
    measurementSpec: MeasurementSpec,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
  ): Sketch {
    logger.info("Generating Sketch...")
    return SketchGenerator(eventQuery, sketchConfig, measurementSpec.vidSamplingInterval)
      .generate(eventGroupSpecs)
  }

  private fun encryptLiquidLegionsV2Sketch(
    sketch: Sketch,
    combinedPublicKey: AnySketchElGamalPublicKey,
    protocol: ProtocolConfig.LiquidLegionsV2
  ): ByteString {
    logger.log(Level.INFO, "Encrypting Liquid Legions V2 Sketch...")
    return sketchEncrypter.encrypt(
      sketch,
      protocol.ellipticCurveId,
      combinedPublicKey,
      protocol.maximumFrequency
    )
  }

  private fun encryptReachOnlyLiquidLegionsV2Sketch(
    sketch: Sketch,
    combinedPublicKey: AnySketchElGamalPublicKey,
    protocol: ProtocolConfig.ReachOnlyLiquidLegionsV2
  ): ByteString {
    logger.log(Level.INFO, "Encrypting Reach-Only Liquid Legions V2 Sketch...")
    return sketchEncrypter.encrypt(sketch, protocol.ellipticCurveId, combinedPublicKey)
  }

  /**
   * Fulfill Liquid Legions V2 Measurement's Requisition by creating an encrypted sketch and send to
   * the duchy.
   */
  private suspend fun fulfillRequisitionForLiquidLegionsV2Measurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionFingerprint: ByteString,
    nonce: Long,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>
  ) {
    val llv2Protocol: ProtocolConfig.Protocol =
      requireNotNull(
        requisition.protocolConfig.protocolsList.find { protocol -> protocol.hasLiquidLegionsV2() }
      ) {
        "Protocol with LiquidLegionsV2 is missing"
      }
    val liquidLegionsV2: ProtocolConfig.LiquidLegionsV2 = llv2Protocol.liquidLegionsV2
    val combinedPublicKey = requisition.getCombinedPublicKey(liquidLegionsV2.ellipticCurveId)

    chargeLiquidLegionsV2PrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
      liquidLegionsV2.noiseMechanism,
      requisition.duchiesCount
    )

    val sketch =
      try {
        generateSketch(
          liquidLegionsV2.sketchParams.toSketchConfig(),
          measurementSpec,
          eventGroupSpecs,
        )
      } catch (e: EventFilterValidationException) {
        logger.log(
          Level.WARNING,
          "RequisitionFulfillmentWorkflow failed due to invalid event filter",
          e
        )
        throw RequisitionRefusalException(
          Requisition.Refusal.Justification.SPEC_INVALID,
          "Invalid event filter (${e.code}): ${e.code.description}"
        )
      }

    val encryptedSketch = encryptLiquidLegionsV2Sketch(sketch, combinedPublicKey, liquidLegionsV2)
    fulfillRequisition(requisition.name, requisitionFingerprint, nonce, encryptedSketch)
  }

  /**
   * Fulfill Reach-Only Liquid Legions V2 Measurement's Requisition by creating an encrypted sketch
   * and send to the duchy.
   */
  private suspend fun fulfillRequisitionForReachOnlyLiquidLegionsV2Measurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    requisitionFingerprint: ByteString,
    nonce: Long,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>
  ) {
    val roLlv2Protocol: ProtocolConfig.Protocol =
      requireNotNull(
        requisition.protocolConfig.protocolsList.find { protocol ->
          protocol.hasReachOnlyLiquidLegionsV2()
        }
      ) {
        "Protocol with ReachOnlyLiquidLegionsV2 is missing"
      }
    val reachOnlyLiquidLegionsV2: ProtocolConfig.ReachOnlyLiquidLegionsV2 =
      roLlv2Protocol.reachOnlyLiquidLegionsV2
    val combinedPublicKey =
      requisition.getCombinedPublicKey(reachOnlyLiquidLegionsV2.ellipticCurveId)

    chargeLiquidLegionsV2PrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
      reachOnlyLiquidLegionsV2.noiseMechanism,
      requisition.duchiesCount
    )

    val sketch =
      try {
        generateSketch(
          reachOnlyLiquidLegionsV2.sketchParams.toSketchConfig(),
          measurementSpec,
          eventGroupSpecs,
        )
      } catch (e: EventFilterValidationException) {
        logger.log(
          Level.WARNING,
          "RequisitionFulfillmentWorkflow failed due to invalid event filter",
          e
        )
        throw RequisitionRefusalException(
          Requisition.Refusal.Justification.SPEC_INVALID,
          "Invalid event filter (${e.code}): ${e.code.description}"
        )
      }

    val encryptedSketch =
      encryptReachOnlyLiquidLegionsV2Sketch(sketch, combinedPublicKey, reachOnlyLiquidLegionsV2)
    fulfillRequisition(requisition.name, requisitionFingerprint, nonce, encryptedSketch)
  }

  private suspend fun fulfillRequisition(
    requisitionName: String,
    requisitionFingerprint: ByteString,
    nonce: Long,
    data: ByteString,
  ) {
    logger.info("Fulfilling requisition $requisitionName...")
    val requests: Flow<FulfillRequisitionRequest> = flow {
      logger.info { "Emitting FulfillRequisitionRequests..." }
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

    return SketchEncrypter.combineElGamalPublicKeys(curveId, elGamalPublicKeys)
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
    measurementSpec: MeasurementSpec,
    nonce: Long,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>
  ) {
    chargeDirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
    )

    logger.info("Calculating direct reach and frequency...")
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

    val sampledVids: Sequence<Long> =
      try {
        eventGroupSpecs
          .asSequence()
          .flatMap { eventQuery.getUserVirtualIds(it) }
          .filter { vid ->
            VidSampling.sampler.vidIsInSamplingBucket(
              vid,
              vidSamplingIntervalStart,
              vidSamplingIntervalWidth
            )
          }
      } catch (e: EventFilterValidationException) {
        logger.log(
          Level.WARNING,
          "RequisitionFulfillmentWorkflow failed due to invalid event filter",
          e
        )
        throw RequisitionRefusalException(
          Requisition.Refusal.Justification.SPEC_INVALID,
          "Invalid event filter (${e.code}): ${e.code.description}"
        )
      }

    val measurementResult = buildDirectMeasurementResult(measurementSpec, sampledVids.asIterable())

    fulfillDirectMeasurement(requisition, measurementSpec, nonce, measurementResult)
  }

  private fun getPublisherNoiser(
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: Random
  ): AbstractNoiser =
    when (directNoiseMechanism) {
      DirectNoiseMechanism.NONE ->
        object : AbstractNoiser() {
          override val distribution = ConstantRealDistribution(0.0)
        }
      DirectNoiseMechanism.LAPLACE ->
        LaplaceNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random.asJavaRandom())
      DirectNoiseMechanism.GAUSSIAN ->
        GaussianNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random.asJavaRandom())
    }

  /**
   * Add publisher noise to calculated direct reach.
   *
   * @param reachValue Direct reach value.
   * @param privacyParams Differential privacy params for reach.
   * @return Noised reach value.
   */
  private fun addReachPublisherNoise(
    reachValue: Int,
    privacyParams: DifferentialPrivacyParams
  ): Int {
    val reachNoiser: AbstractNoiser =
      getPublisherNoiser(privacyParams, directNoiseMechanism, random)

    return reachValue + reachNoiser.sample().toInt()
  }

  /**
   * Add publisher noise to calculated direct frequency.
   *
   * @param reachValue Direct reach value.
   * @param frequencyMap Direct frequency.
   * @param privacyParams Differential privacy params for frequency map.
   * @return Noised frequency map.
   */
  private fun addFrequencyPublisherNoise(
    reachValue: Int,
    frequencyMap: Map<Int, Double>,
    privacyParams: DifferentialPrivacyParams,
  ): Map<Int, Double> {
    val frequencyNoiser: AbstractNoiser =
      getPublisherNoiser(privacyParams, directNoiseMechanism, random)

    return frequencyMap.mapValues { (_, percentage) ->
      (percentage * reachValue.toDouble() + frequencyNoiser.sample()) / reachValue.toDouble()
    }
  }

  /**
   * Build [Measurement.Result] of the measurement type specified in [MeasurementSpec].
   *
   * @param measurementSpec Measurement spec.
   * @param sampledVids sampled event VIDs
   * @return [Measurement.Result].
   */
  private fun buildDirectMeasurementResult(
    measurementSpec: MeasurementSpec,
    sampledVids: Iterable<Long>,
  ): Measurement.Result {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    return when (measurementSpec.measurementTypeCase) {
      MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
        val (sampledReachValue, frequencyMap) =
          MeasurementResults.computeReachAndFrequency(sampledVids)

        logger.info("Adding $directNoiseMechanism publisher noise to direct reach and frequency...")
        val sampledNoisedReachValue =
          addReachPublisherNoise(
            sampledReachValue,
            measurementSpec.reachAndFrequency.reachPrivacyParams
          )
        val noisedFrequencyMap =
          addFrequencyPublisherNoise(
            sampledReachValue,
            frequencyMap,
            measurementSpec.reachAndFrequency.frequencyPrivacyParams,
          )

        val scaledNoisedReachValue =
          (sampledNoisedReachValue / measurementSpec.vidSamplingInterval.width).toLong()

        MeasurementKt.result {
          reach = reach { value = scaledNoisedReachValue }
          frequency = frequency {
            relativeFrequencyDistribution.putAll(noisedFrequencyMap.mapKeys { it.key.toLong() })
          }
        }
      }
      MeasurementSpec.MeasurementTypeCase.IMPRESSION,
      MeasurementSpec.MeasurementTypeCase.DURATION,
      MeasurementSpec.MeasurementTypeCase.POPULATION -> {
        error("Measurement type not supported.")
      }
      MeasurementSpec.MeasurementTypeCase.REACH -> {
        val sampledReachValue = MeasurementResults.computeReach(sampledVids)
        logger.info("Adding $directNoiseMechanism publisher noise to direct reach...")
        val sampledNoisedReachValue =
          addReachPublisherNoise(sampledReachValue, measurementSpec.reach.privacyParams)
        val scaledNoisedReachValue =
          (sampledNoisedReachValue / measurementSpec.vidSamplingInterval.width).toLong()

        MeasurementKt.result { reach = reach { value = scaledNoisedReachValue } }
      }
      MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> {
        error("Measurement type not set.")
      }
    }
  }

  private suspend fun fulfillImpressionMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>
  ) {
    chargeDirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
    )

    val measurementResult =
      MeasurementKt.result {
        impression = impression {
          // Use externalDataProviderId since it's a known value the FrontendSimulator can verify.
          // TODO: Calculate impression from data.
          value = apiIdToExternalId(DataProviderKey.fromName(edpData.name)!!.dataProviderId)
        }
      }

    fulfillDirectMeasurement(requisition, measurementSpec, requisitionSpec.nonce, measurementResult)
  }

  private suspend fun fulfillDurationMeasurement(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec,
    measurementSpec: MeasurementSpec,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>
  ) {
    chargeDirectPrivacyBudget(
      requisition.name,
      measurementSpec,
      eventGroupSpecs.map { it.spec },
    )

    val externalDataProviderId =
      apiIdToExternalId(DataProviderKey.fromName(edpData.name)!!.dataProviderId)
    val measurementResult =
      MeasurementKt.result {
        watchDuration = watchDuration {
          value = duration {
            // Use a value based on the externalDataProviderId since it's a known value the
            // MeasurementConsumerSimulator can verify.
            seconds = log2(externalDataProviderId.toDouble()).toLong()
          }
        }
      }

    fulfillDirectMeasurement(requisition, measurementSpec, requisitionSpec.nonce, measurementResult)
  }

  private suspend fun fulfillDirectMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    nonce: Long,
    measurementResult: Measurement.Result
  ) {
    val measurementEncryptionPublicKey =
      EncryptionPublicKey.parseFrom(measurementSpec.measurementPublicKey)

    // TODO(world-federation-of-advertisers/consent-signaling-client#41): Use method from
    // DataProviders client instead of Duchies client.
    val signedData = signResult(measurementResult, edpData.signingKey)

    val encryptedData =
      measurementEncryptionPublicKey.toPublicKeyHandle().hybridEncrypt(signedData.toByteString())

    try {
      requisitionsStub.fulfillDirectRequisition(
        fulfillDirectRequisitionRequest {
          name = requisition.name
          this.encryptedData = encryptedData
          this.nonce = nonce
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error fulfilling direct requisition ${requisition.name}", e)
    }
  }

  companion object {
    private const val RPC_CHUNK_SIZE_BYTES = 32 * 1024 // 32 KiB

    private val EVENT_TEMPLATE_TYPES: List<Descriptors.Descriptor> =
      TestEvent.getDescriptor()
        .fields
        .filter { it.type == Descriptors.FieldDescriptor.Type.MESSAGE }
        .map { it.messageType }
        .filter { it.options.hasExtension(EventAnnotationsProto.eventTemplate) }
    private val EVENT_TEMPLATES: List<EventGroup.EventTemplate> =
      EVENT_TEMPLATE_TYPES.map { eventTemplate { type = it.fullName } }

    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private fun ElGamalPublicKey.toAnySketchElGamalPublicKey(): AnySketchElGamalPublicKey {
  val source = this
  return anySketchElGamalPublicKey {
    generator = source.generator
    element = source.element
  }
}
