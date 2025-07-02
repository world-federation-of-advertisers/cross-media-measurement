/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha.tools

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.Duration
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import java.io.File
import java.lang.Exception
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.EventTemplateDescriptor
import org.wfanet.measurement.api.v2alpha.MediaTypeProto
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine
import picocli.CommandLine.Model.CommandSpec

@CommandLine.Command(
  name = "EventTemplateValidator",
  description = ["Validates event templates used in an event message type."],
  mixinStandardHelpOptions = true,
)
class EventTemplateValidator : Runnable {
  @CommandLine.Spec private lateinit var spec: CommandSpec

  @CommandLine.Option(
    names = ["--event-proto"],
    description = ["Fully qualified name of the event message type."],
  )
  private lateinit var eventProto: String

  @CommandLine.Option(
    names = ["--descriptor-set"],
    description =
      [
        "Path to a serialized FileDescriptorSet containing an event message type and/or its " +
          "dependencies.",
        "This can be specified multiple times.",
      ],
  )
  private lateinit var descriptorSetFiles: List<File>

  private class ValidationException(message: String, val status: Int = 1) : Exception(message)

  override fun run() {
    spec.commandLine().executionExceptionHandler =
      CommandLine.IExecutionExceptionHandler { e: Exception, cmd: CommandLine, _ ->
        if (e !is ValidationException) {
          throw e
        }

        cmd.out.println(cmd.colorScheme.errorText(e.message))
        e.status
      }

    val typeRegistry = buildTypeRegistry()
    val eventDescriptor: Descriptors.Descriptor =
      typeRegistry.find(eventProto) ?: throw ValidationException("Type not found: $eventProto")

    val templates = mutableMapOf<String, Descriptors.Descriptor>()
    for (field: Descriptors.FieldDescriptor in eventDescriptor.fields) {
      if (
        field.type != Descriptors.FieldDescriptor.Type.MESSAGE ||
          !field.messageType.options.hasExtension(EventAnnotationsProto.eventTemplate)
      ) {
        throw ValidationException("${field.name} does not correspond to an event template")
      }

      val templateAnnotation: EventTemplateDescriptor =
        field.messageType.options.getExtension(EventAnnotationsProto.eventTemplate)
      if (field.name != templateAnnotation.name) {
        throw ValidationException(
          "Field name `${field.name}` does not match template name `${templateAnnotation.name}`"
        )
      }

      templates[field.messageType.fullName] = field.messageType
    }

    for (template in templates.values) {
      validateTemplateFields(template)
    }
  }

  private fun validateTemplateFields(template: Descriptors.Descriptor) {
    for (field: Descriptors.FieldDescriptor in template.fields) {
      val fieldRef = "${template.name}.${field.name}"

      if (!field.options.hasExtension(EventAnnotationsProto.templateField)) {
        throw ValidationException("$fieldRef does not have a template_field annotation")
      }

      if (field.isRepeated) {
        throw ValidationException("$fieldRef is repeated")
      }

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
      when (field.type) {
        Descriptors.FieldDescriptor.Type.STRING,
        Descriptors.FieldDescriptor.Type.BOOL,
        Descriptors.FieldDescriptor.Type.ENUM,
        Descriptors.FieldDescriptor.Type.DOUBLE,
        Descriptors.FieldDescriptor.Type.FLOAT,
        Descriptors.FieldDescriptor.Type.INT32,
        Descriptors.FieldDescriptor.Type.INT64 -> {}
        Descriptors.FieldDescriptor.Type.MESSAGE ->
          when (val messageName = field.messageType.fullName) {
            Duration.getDescriptor().fullName,
            Timestamp.getDescriptor().fullName -> {}
            else -> throw ValidationException("$fieldRef has unsupported type $messageName")
          }
        Descriptors.FieldDescriptor.Type.UINT64,
        Descriptors.FieldDescriptor.Type.FIXED64,
        Descriptors.FieldDescriptor.Type.FIXED32,
        Descriptors.FieldDescriptor.Type.GROUP,
        Descriptors.FieldDescriptor.Type.BYTES,
        Descriptors.FieldDescriptor.Type.UINT32,
        Descriptors.FieldDescriptor.Type.SFIXED32,
        Descriptors.FieldDescriptor.Type.SFIXED64,
        Descriptors.FieldDescriptor.Type.SINT32,
        Descriptors.FieldDescriptor.Type.SINT64 ->
          throw ValidationException("$fieldRef has unsupported type ${field.type.name.lowercase()}")
      }
    }
  }

  private fun buildTypeRegistry(): TypeRegistry {
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

    @JvmStatic fun main(args: Array<String>) = commandLineMain(EventTemplateValidator(), args)
  }
}
