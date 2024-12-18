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

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.InMemoryVidIndexMap
import picocli.CommandLine

@CommandLine.Command(
  name = "SyntheticGeneratorEdpSimulatorRunner",
  description = ["EdpSimulator Daemon"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
/** Implementation of [EdpSimulatorRunner] using [SyntheticGeneratorEventQuery]. */
class SyntheticGeneratorEdpSimulatorRunner : EdpSimulatorRunner() {
  @CommandLine.Option(
    names = ["--event-message-descriptor-set"],
    description =
      [
        "Serialized FileDescriptorSet for the event message and its dependencies.",
        "This can be specified multiple times.",
        "It need not be specified if the event message type is $TEST_EVENT_MESSAGE_TYPE.",
      ],
    required = false,
  )
  private lateinit var eventMessageDescriptorSetFiles: List<File>

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
  }

  private lateinit var knownEventGroupMetadataTypes: List<Descriptors.FileDescriptor>

  @CommandLine.Option(
    names = ["--population-spec"],
    description = ["Path to SyntheticPopulationSpec message in text format."],
    required = true,
  )
  private lateinit var populationSpecFile: File

  @CommandLine.Option(
    names = ["--event-group-spec"],
    description =
      [
        "Key-value pair of EventGroup reference ID suffix and file path of " +
          "SyntheticEventGroupSpec message in text format. This can be specified multiple times."
      ],
    required = true,
  )
  private lateinit var eventGroupSpecFileByReferenceIdSuffix: Map<String, File>

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

  @CommandLine.Option(
    names = ["--support-hmss"],
    description = ["Whether to support HMSS protocol requisitions."],
  )
  private var supportHmss: Boolean = false

  override fun run() {
    val typeRegistry: TypeRegistry = buildTypeRegistry()
    val syntheticPopulationSpec =
      parseTextProto(populationSpecFile, SyntheticPopulationSpec.getDefaultInstance())
    val eventGroupSpecByReferenceIdSuffix =
      eventGroupSpecFileByReferenceIdSuffix.mapValues {
        parseTextProto(it.value, SyntheticEventGroupSpec.getDefaultInstance())
      }
    val metadataByReferenceIdSuffix: Map<String, Message> =
      eventGroupMetadataFileByReferenceIdSuffix.mapValues {
        val descriptor: Descriptors.Descriptor =
          typeRegistry.getNonNullDescriptorForTypeUrl(eventGroupMetadataTypeUrl)
        parseTextProto(it.value, DynamicMessage.getDefaultInstance(descriptor), typeRegistry)
      }
    val eventMessageDescriptor: Descriptors.Descriptor =
      typeRegistry.getNonNullDescriptorForTypeUrl(syntheticPopulationSpec.eventMessageTypeUrl)

    val eventQuery =
      object : SyntheticGeneratorEventQuery(syntheticPopulationSpec, typeRegistry) {
        override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
          val suffix =
            EdpSimulator.getEventGroupReferenceIdSuffix(eventGroup, flags.dataProviderDisplayName)
          return eventGroupSpecByReferenceIdSuffix.getValue(suffix)
        }
      }
    val populationSpec = syntheticPopulationSpec.toPopulationSpec()
    val hmssVidIndexMap = if (supportHmss) InMemoryVidIndexMap.build(populationSpec) else null

    run(
      eventQuery,
      EdpSimulator.buildEventTemplates(eventMessageDescriptor),
      metadataByReferenceIdSuffix,
      knownEventGroupMetadataTypes,
      hmssVidIndexMap,
    )
  }

  private fun buildTypeRegistry(): TypeRegistry {
    return TypeRegistry.newBuilder()
      .apply {
        add(COMPILED_PROTOBUF_TYPES.flatMap { it.messageTypes })
        add(knownEventGroupMetadataTypes.flatMap { it.messageTypes })
        if (::eventMessageDescriptorSetFiles.isInitialized) {
          add(
            ProtoReflection.buildDescriptors(
              loadFileDescriptorSets(eventMessageDescriptorSetFiles),
              COMPILED_PROTOBUF_TYPES,
            )
          )
        }
      }
      .build()
  }

  private fun loadFileDescriptorSets(
    files: Iterable<File>
  ): List<DescriptorProtos.FileDescriptorSet> {
    return files.map { file ->
      file.inputStream().use { input ->
        DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
      }
    }
  }

  companion object {
    private const val TEST_EVENT_MESSAGE_TYPE =
      "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
    private const val SYNTHETIC_EVENT_GROUP_SPEC_MESSAGE_TYPE =
      "wfa.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec"
    private const val DEFAULT_METADATA_TYPE_URL =
      "${ProtoReflection.DEFAULT_TYPE_URL_PREFIX}/$SYNTHETIC_EVENT_GROUP_SPEC_MESSAGE_TYPE"

    /**
     * [Descriptors.FileDescriptor]s of protobuf types known at compile-time that may be loaded from
     * a [DescriptorProtos.FileDescriptorSet].
     */
    private val COMPILED_PROTOBUF_TYPES: Iterable<Descriptors.FileDescriptor> =
      (ProtoReflection.WELL_KNOWN_TYPES.asSequence() +
          SyntheticEventGroupSpec.getDescriptor().file +
          EventAnnotationsProto.getDescriptor() +
          TestEvent.getDescriptor().file)
        .asIterable()

    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    init {
      check(TestEvent.getDescriptor().fullName == TEST_EVENT_MESSAGE_TYPE)
      check(
        SyntheticEventGroupSpec.getDescriptor().fullName == SYNTHETIC_EVENT_GROUP_SPEC_MESSAGE_TYPE
      )
    }
  }
}

fun main(args: Array<String>) = commandLineMain(SyntheticGeneratorEdpSimulatorRunner(), args)

private fun TypeRegistry.getNonNullDescriptorForTypeUrl(typeUrl: String): Descriptors.Descriptor =
  checkNotNull(getDescriptorForTypeUrl(typeUrl)) { "Descriptor not found for type URL $typeUrl" }
