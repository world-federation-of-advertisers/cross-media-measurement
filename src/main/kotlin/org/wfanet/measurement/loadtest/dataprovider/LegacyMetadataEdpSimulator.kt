/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import com.google.protobuf.kotlin.unpack
import io.grpc.StatusException
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlinx.coroutines.Dispatchers
import org.jetbrains.annotations.BlockingExecutor
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupKt.metadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.SettableHealth
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

/** [AbstractEdpSimulator] which sets legacy encrypted metadata on created EventGroups. */
class LegacyMetadataEdpSimulator(
  edpData: DataProviderData,
  edpDisplayName: String,
  measurementConsumerName: String,
  private val measurementConsumersStub:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub,
  certificatesStub: CertificatesGrpcKt.CertificatesCoroutineStub,
  modelLinesStub: ModelLinesGrpcKt.ModelLinesCoroutineStub,
  dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  eventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub,
  private val eventGroupMetadataDescriptorsStub:
    EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub,
  requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  requisitionFulfillmentStubsByDuchyId:
    Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
  override val eventGroupsOptions: Collection<EventGroupOptions>,
  eventQuery: SyntheticGeneratorEventQuery,
  throttler: Throttler,
  privacyBudgetManager: PrivacyBudgetManager,
  trustedCertificates: Map<ByteString, X509Certificate>,
  /**
   * Known protobuf types for [EventGroupMetadataDescriptor]s.
   *
   * This is in addition to the standard
   * [protobuf well-known types][ProtoReflection.WELL_KNOWN_TYPES].
   */
  private val knownEventGroupMetadataTypes: Iterable<Descriptors.FileDescriptor>,
  vidIndexMap: VidIndexMap? = null,
  sketchEncrypter: SketchEncrypter = SketchEncrypter.Default,
  logSketchDetails: Boolean = false,
  health: SettableHealth = SettableHealth(),
  random: Random = Random,
  blockingCoroutineContext: @BlockingExecutor CoroutineContext = Dispatchers.IO,
) :
  AbstractEdpSimulator(
    edpData,
    edpDisplayName,
    measurementConsumerName,
    certificatesStub,
    modelLinesStub,
    dataProvidersStub,
    eventGroupsStub,
    requisitionsStub,
    requisitionFulfillmentStubsByDuchyId,
    eventQuery.timeZone,
    eventGroupsOptions,
    eventQuery,
    throttler,
    privacyBudgetManager,
    trustedCertificates,
    vidIndexMap,
    sketchEncrypter,
    random,
    logSketchDetails,
    health,
    blockingCoroutineContext,
    null,
  ) {
  interface EventGroupOptions : AbstractEdpSimulator.EventGroupOptions {
    val legacyMetadata: Message
    val eventTemplates: List<EventGroup.EventTemplate>
  }

  override suspend fun ensureEventGroups(): List<EventGroup> {
    require(eventGroupsOptions.isNotEmpty())
    val metadataDescriptor: Descriptors.Descriptor =
      eventGroupsOptions.first().legacyMetadata.descriptorForType
    require(eventGroupsOptions.all { it.legacyMetadata.descriptorForType == metadataDescriptor }) {
      "All metadata messages must have the same type"
    }

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
      getCertificate(measurementConsumer.certificate),
    )

    val descriptorResource: EventGroupMetadataDescriptor =
      ensureMetadataDescriptor(metadataDescriptor)

    return eventGroupsOptions.map { ensureEventGroup(measurementConsumer, descriptorResource, it) }
  }

  private suspend fun ensureEventGroup(
    measurementConsumer: MeasurementConsumer,
    metadataDescriptor: EventGroupMetadataDescriptor,
    eventGroupOptions: EventGroupOptions,
  ): EventGroup {
    val eventGroupReferenceId = eventGroupReferenceIdPrefix + eventGroupOptions.referenceIdSuffix
    return ensureEventGroup(
      measurementConsumer,
      eventGroupReferenceId,
      eventGroupOptions.eventTemplates,
      eventGroupOptions.legacyMetadata,
      metadataDescriptor,
    )
  }

  /**
   * Ensures that an [EventGroup] exists for [measurementConsumer] with the specified
   * [eventGroupReferenceId] and [descriptorResource].
   */
  private suspend fun ensureEventGroup(
    measurementConsumer: MeasurementConsumer,
    eventGroupReferenceId: String,
    eventTemplates: Iterable<EventGroup.EventTemplate>,
    eventGroupMetadata: Message,
    descriptorResource: EventGroupMetadataDescriptor,
  ): EventGroup {
    val encryptedMetadata: EncryptedMessage =
      encryptMetadata(
        metadata {
          eventGroupMetadataDescriptor = descriptorResource.name
          metadata = eventGroupMetadata.pack()
        },
        measurementConsumer.publicKey.message.unpack(),
      )

    return ensureEventGroup(eventGroupReferenceId) {
      this.eventTemplates.clear()
      this.eventTemplates += eventTemplates
      measurementConsumerPublicKey = measurementConsumer.publicKey.message
      this.encryptedMetadata = encryptedMetadata
    }
  }

  private suspend fun ensureMetadataDescriptor(
    metadataDescriptor: Descriptors.Descriptor
  ): EventGroupMetadataDescriptor {
    val descriptorSet =
      ProtoReflection.buildFileDescriptorSet(
        metadataDescriptor,
        ProtoReflection.WELL_KNOWN_TYPES + knownEventGroupMetadataTypes,
      )
    val descriptorResource =
      try {
        eventGroupMetadataDescriptorsStub.createEventGroupMetadataDescriptor(
          createEventGroupMetadataDescriptorRequest {
            parent = edpData.name
            eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
              this.descriptorSet = descriptorSet
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

  private fun verifyEncryptionPublicKey(
    signedEncryptionPublicKey: SignedMessage,
    measurementConsumerCertificate: Certificate,
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
      org.wfanet.measurement.consent.client.measurementconsumer.verifyEncryptionPublicKey(
        signedEncryptionPublicKey,
        x509Certificate,
        trustedIssuer,
      )
    } catch (e: CertPathValidatorException) {
      throw InvalidConsentSignalException(
        "Certificate path for ${measurementConsumerCertificate.name} is invalid",
        e,
      )
    } catch (e: SignatureException) {
      throw InvalidConsentSignalException("EncryptionPublicKey signature is invalid", e)
    }
  }

  companion object {
    /** Builds [EventGroup.EventTemplate] messages for all templates in [eventMessageDescriptor]. */
    fun buildEventTemplates(
      eventMessageDescriptor: Descriptors.Descriptor
    ): List<EventGroup.EventTemplate> {
      val eventTemplateTypes: List<Descriptors.Descriptor> =
        eventMessageDescriptor.fields
          .filter { it.type == Descriptors.FieldDescriptor.Type.MESSAGE }
          .map { it.messageType }
          .filter { it.options.hasExtension(EventAnnotationsProto.eventTemplate) }
      return eventTemplateTypes.map { EventGroupKt.eventTemplate { type = it.fullName } }
    }
  }
}
