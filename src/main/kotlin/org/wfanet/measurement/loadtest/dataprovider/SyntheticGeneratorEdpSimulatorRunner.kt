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
import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
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

  override fun run() {
    val populationSpec =
      parseTextProto(populationSpecFile, SyntheticPopulationSpec.getDefaultInstance())
    val eventGroupSpecByReferenceIdSuffix =
      eventGroupSpecFileByReferenceIdSuffix.mapValues {
        parseTextProto(it.value, SyntheticEventGroupSpec.getDefaultInstance())
      }
    val eventMessageRegistry: TypeRegistry = buildEventMessageRegistry()

    val eventQuery =
      object : SyntheticGeneratorEventQuery(populationSpec, eventMessageRegistry) {
        override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
          val suffix =
            EdpSimulator.getEventGroupReferenceIdSuffix(eventGroup, flags.dataProviderDisplayName)
          return eventGroupSpecByReferenceIdSuffix.getValue(suffix)
        }
      }

    run(eventQuery, eventGroupSpecByReferenceIdSuffix)
  }

  private fun buildEventMessageRegistry(): TypeRegistry {
    val builder = TypeRegistry.newBuilder().add(TestEvent.getDescriptor())
    if (::eventMessageDescriptorSetFiles.isInitialized) {
      val fileDescriptorSets: List<DescriptorProtos.FileDescriptorSet> =
        eventMessageDescriptorSetFiles.map {
          parseTextProto(it, DescriptorProtos.FileDescriptorSet.getDefaultInstance())
        }
      builder.add(ProtoReflection.buildDescriptors(fileDescriptorSets))
    }
    return builder.build()
  }

  companion object {
    private const val TEST_EVENT_MESSAGE_TYPE =
      "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
  }
}

fun main(args: Array<String>) = commandLineMain(SyntheticGeneratorEdpSimulatorRunner(), args)
