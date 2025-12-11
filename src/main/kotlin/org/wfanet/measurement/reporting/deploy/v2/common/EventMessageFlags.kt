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

package org.wfanet.measurement.reporting.deploy.v2.common

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.MediaTypeProto
import org.wfanet.measurement.common.ProtoReflection
import picocli.CommandLine

class EventMessageFlags {
  @CommandLine.Option(
    names = ["--event-message-type-url"],
    description = ["Fully qualified name of the event message type."],
    required = false,
    defaultValue = "",
  )
  lateinit var eventMessageTypeUrl: String
    private set

  @CommandLine.Option(
    names = ["--event-message-descriptor-set"],
    description =
      [
        "Path to a serialized FileDescriptorSet containing an event message type and/or its " +
          "dependencies.",
        "This can be specified multiple times.",
      ],
    required = false,
  )
  var eventMessageDescriptorSetFiles: List<File> = emptyList()
    private set

  val eventDescriptor: EventMessageDescriptor? by lazy {
    // TODO(@tristanvuong2021): Flags will be required once BasicReports Phase 2 is completed.
    // When the flags are required, this will be moved to a common location that covers
    // PopulationRequisitionFulfillerDaemon as well.
    if (eventMessageTypeUrl.isNotEmpty() && eventMessageDescriptorSetFiles.isNotEmpty()) {
      val eventDescriptor: Descriptors.Descriptor =
        checkNotNull(
          buildTypeRegistry(eventMessageDescriptorSetFiles)
            .getDescriptorForTypeUrl(eventMessageTypeUrl)
        ) {
          "--event-message-type-url is invalid"
        }

      EventMessageDescriptor(eventDescriptor)
    } else {
      null
    }
  }

  private fun buildTypeRegistry(descriptorSetFiles: List<File>): TypeRegistry {
    val descriptorSets: List<DescriptorProtos.FileDescriptorSet> =
      descriptorSetFiles.map {
        it.inputStream().use { input ->
          DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
        }
      }

    return TypeRegistry.newBuilder()
      .add(ProtoReflection.buildDescriptors(descriptorSets, KNOWN_TYPES))
      .build()
  }

  companion object {
    private val KNOWN_TYPES =
      ProtoReflection.WELL_KNOWN_TYPES +
        EventAnnotationsProto.getDescriptor() +
        MediaTypeProto.getDescriptor()

    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .apply {
          add(EventAnnotationsProto.eventTemplate)
          add(EventAnnotationsProto.templateField)
        }
        .unmodifiable
  }
}
