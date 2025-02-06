/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.tools

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import java.io.File
import kotlin.system.exitProcess
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.readByteString
import picocli.CommandLine

/** Command line utility for interacting with protocol buffers. */
@CommandLine.Command(name = "ProtoUtil")
class ProtoUtil : Runnable {
  @CommandLine.Command(
    name = "print-types",
    description = ["Prints all types defined in the FileDescriptorSet"],
  )
  fun printTypes(
    @CommandLine.Option(
      names = ["--descriptor-set"],
      description = ["Path to serialized FileDescriptorSet"],
    )
    descriptorSet: File
  ) {
    val fileDescriptorSet =
      DescriptorProtos.FileDescriptorSet.parseFrom(descriptorSet.readByteString())
    for (typeDescriptor in getTypeDescriptors(fileDescriptorSet)) {
      println(typeDescriptor.fullName)
    }
  }

  private fun getTypeDescriptors(
    descriptorSet: DescriptorProtos.FileDescriptorSet
  ): Sequence<Descriptors.GenericDescriptor> = sequence {
    val fileDescriptors: List<Descriptors.FileDescriptor> =
      ProtoReflection.buildFileDescriptors(listOf(descriptorSet), emptyList())
    for (fileDescriptor in fileDescriptors) {
      yieldAll(fileDescriptor.enumTypes)
      yieldAll(fileDescriptor.messageTypes)
      for (messageType: Descriptors.Descriptor in fileDescriptor.messageTypes) {
        yieldAll(getNestedTypeDescriptors(messageType))
      }
    }
  }

  private fun getNestedTypeDescriptors(
    descriptor: Descriptors.Descriptor
  ): Sequence<Descriptors.GenericDescriptor> = sequence {
    yieldAll(descriptor.enumTypes)
    for (nestedMessageType in descriptor.nestedTypes) {
      yield(nestedMessageType)
      yieldAll(getNestedTypeDescriptors(nestedMessageType))
    }
  }

  override fun run() {
    CommandLine.usage(this, System.err)
    exitProcess(1)
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(ProtoUtil(), args)
  }
}
