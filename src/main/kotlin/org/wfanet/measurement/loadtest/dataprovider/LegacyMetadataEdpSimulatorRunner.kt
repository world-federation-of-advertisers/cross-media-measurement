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
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import io.grpc.ManagedChannel
import java.io.File
import java.security.cert.X509Certificate
import kotlin.random.Random
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.SettableHealth
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder as TrusTeeFulfillRequisitionRequestBuilder
import org.wfanet.measurement.loadtest.config.PrivacyBudgets
import picocli.CommandLine

@CommandLine.Command(
  name = "LegacyMetadataEdpSimulatorRunner",
  description = ["EdpSimulator Daemon"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
/** Implementation of [AbstractEdpSimulatorRunner] using [LegacyMetadataEdpSimulator]. */
class LegacyMetadataEdpSimulatorRunner : AbstractEdpSimulatorRunner() {
  private data class EventGroupOptions(
    override val referenceIdSuffix: String,
    override val syntheticDataSpec: SyntheticEventGroupSpec,
    override val eventTemplates: List<EventGroup.EventTemplate>,
    override val legacyMetadata: Message,
  ) : LegacyMetadataEdpSimulator.EventGroupOptions

  @CommandLine.Option(
    names = ["--known-event-group-metadata-type"],
    description =
      [
        "File path to FileDescriptorSet containing known EventGroup metadata types.",
        "This is in addition to standard protobuf well-known types.",
        "Can be specified multiple times.",
      ],
    required = false,
    defaultValue = "",
  )
  private fun setKnownEventGroupMetadataTypes(files: List<File>) {
    knownEventGroupMetadataTypes =
      ProtoReflection.buildFileDescriptors(loadFileDescriptorSets(files), COMPILED_PROTOBUF_TYPES)
    additionalMessageTypes = knownEventGroupMetadataTypes.flatMap { it.messageTypes }
  }

  private lateinit var knownEventGroupMetadataTypes: List<Descriptors.FileDescriptor>
  override lateinit var additionalMessageTypes: Collection<Descriptors.Descriptor>

  @CommandLine.Option(
    names = ["--event-group-spec"],
    description =
      [
        "Key-value pair of EventGroup reference ID suffix and file path of " +
          "SyntheticEventGroupSpec message in text format. This can be specified multiple times."
      ],
    required = true,
  )
  private fun setEventGroupSpecByReferenceIdSuffix(
    eventGroupSpecFileByReferenceIdSuffix: Map<String, File>
  ) {
    eventGroupSpecByReferenceIdSuffix =
      eventGroupSpecFileByReferenceIdSuffix.mapValues {
        parseTextProto(it.value, SyntheticEventGroupSpec.getDefaultInstance())
      }
  }

  private lateinit var eventGroupSpecByReferenceIdSuffix: Map<String, SyntheticEventGroupSpec>

  @CommandLine.Option(
    names = ["--event-group-metadata"],
    description =
      [
        "Key-value pair of EventGroup reference ID suffix and file path of " +
          "EventGroup metadata message in protobuf text format.",
        "This can be specified multiple times.",
      ],
    required = true,
  )
  private lateinit var eventGroupMetadataFileByReferenceIdSuffix: Map<String, File>

  @CommandLine.Option(
    names = ["--event-group-metadata-type-url"],
    description =
      [
        "Type URL of the EventGroup metadata message type.",
        "This must be a known EventGroup metadata type.",
      ],
    required = false,
    defaultValue = DEFAULT_METADATA_TYPE_URL,
  )
  private lateinit var eventGroupMetadataTypeUrl: String

  override val eventGroupsOptions: List<LegacyMetadataEdpSimulator.EventGroupOptions> by lazy {
    val metadataDescriptor: Descriptors.Descriptor =
      typeRegistry.getNonNullDescriptorForTypeUrl(eventGroupMetadataTypeUrl)
    val eventTemplates = LegacyMetadataEdpSimulator.buildEventTemplates(eventMessageDescriptor)

    eventGroupSpecByReferenceIdSuffix.map { (referenceIdSuffix, eventGroupSpec) ->
      val legacyMetadata =
        parseTextProto(
          eventGroupMetadataFileByReferenceIdSuffix.getValue(referenceIdSuffix),
          DynamicMessage.getDefaultInstance(metadataDescriptor),
        )
      EventGroupOptions(referenceIdSuffix, eventGroupSpec, eventTemplates, legacyMetadata)
    }
  }

  override fun buildEdpSimulator(
    edpDisplayName: String,
    measurementConsumerName: String,
    kingdomPublicApiChannel: ManagedChannel,
    requisitionFulfillmentStubsByDuchyId:
      Map<String, RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub>,
    trustedCertificates: Map<ByteString, X509Certificate>,
    eventQuery: SyntheticGeneratorEventQuery,
    vidIndexMap: InMemoryVidIndexMap?,
    logSketchDetails: Boolean,
    throttler: MinimumIntervalThrottler,
    health: SettableHealth,
    random: Random,
    trusTeeEncryptionParams: TrusTeeFulfillRequisitionRequestBuilder.EncryptionParams?,
  ): AbstractEdpSimulator {
    return LegacyMetadataEdpSimulator(
      edpData,
      edpDisplayName,
      measurementConsumerName,
      MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(kingdomPublicApiChannel),
      CertificatesGrpcKt.CertificatesCoroutineStub(kingdomPublicApiChannel),
      ModelLinesGrpcKt.ModelLinesCoroutineStub(kingdomPublicApiChannel),
      DataProvidersGrpcKt.DataProvidersCoroutineStub(kingdomPublicApiChannel),
      EventGroupsGrpcKt.EventGroupsCoroutineStub(kingdomPublicApiChannel),
      EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
        kingdomPublicApiChannel
      ),
      RequisitionsGrpcKt.RequisitionsCoroutineStub(kingdomPublicApiChannel),
      requisitionFulfillmentStubsByDuchyId,
      eventGroupsOptions,
      eventQuery,
      throttler,
      PrivacyBudgets.createNoOpPrivacyBudgetManager(),
      trustedCertificates,
      knownEventGroupMetadataTypes,
      vidIndexMap = vidIndexMap,
      logSketchDetails = logSketchDetails,
      health = health,
      random = random,
    )
  }

  companion object {
    private const val DEFAULT_METADATA_TYPE_URL =
      "${ProtoReflection.DEFAULT_TYPE_URL_PREFIX}/$SYNTHETIC_EVENT_GROUP_SPEC_MESSAGE_TYPE"
  }
}

fun main(args: Array<String>) = commandLineMain(LegacyMetadataEdpSimulatorRunner(), args)
