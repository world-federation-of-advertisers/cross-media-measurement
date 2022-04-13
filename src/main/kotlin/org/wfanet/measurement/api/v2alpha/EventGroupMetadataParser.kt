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

package org.wfanet.measurement.api.v2alpha

import com.google.protobuf.Any
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry

class EventGroupMetadataParser() {
  private lateinit var typeRegistry: TypeRegistry
  private lateinit var fileDescriptorProtoMap: Map<String, FileDescriptorProto>
  private var fileDescriptorMap: MutableMap<String, FileDescriptor> = mutableMapOf()
  private var registryBuilder = TypeRegistry.newBuilder()

  private fun initializeTypeRegistry(fileDescriptorSet: FileDescriptorSet) {
    fileDescriptorProtoMap = fileDescriptorSet.getFileList().map { it.name to it }.toMap()
    for (proto in fileDescriptorSet.getFileList()) {
      buildFileDescriptors(proto)
    }

    typeRegistry = registryBuilder.build()
  }

  private fun buildFileDescriptors(proto: FileDescriptorProto): FileDescriptor {
    if (fileDescriptorMap[proto.getName()] != null) return fileDescriptorMap[proto.getName()]!!

    var dependenciesList: MutableList<FileDescriptor> = mutableListOf()
    for (dependencyByteName in proto.getDependencyList().asByteStringList()) {
      val dependencyName = dependencyByteName.toStringUtf8()
      if (fileDescriptorMap[dependencyName] != null) {
        dependenciesList.add(fileDescriptorMap[dependencyName]!!)
      } else {
        dependenciesList.add(buildFileDescriptors(fileDescriptorProtoMap[dependencyName]!!))
      }
    }

    val builtDescriptor = FileDescriptor.buildFrom(proto, dependenciesList.toTypedArray())
    fileDescriptorMap[proto.getName()] = builtDescriptor
    addDescriptorMessageTypesToRegistry(builtDescriptor)
    return builtDescriptor
  }

  private fun addDescriptorMessageTypesToRegistry(fileDescriptor: FileDescriptor) {
    for (descriptor in fileDescriptor.getMessageTypes()) {
      registryBuilder.add(descriptor)
    }
  }

  /**
   * Returns the DynamicMessage from a com.google.protobuf.Any message and corresponding
   * FileDescriptorSet.
   */
  fun convertToDynamicMessage(
    message: com.google.protobuf.Any,
    fileDescriptorSet: FileDescriptorSet
  ): DynamicMessage? {
    initializeTypeRegistry(fileDescriptorSet)
    // A packed Any message appends a "type.googleapis.com/" prefix to the typeUrl.
    val messageName = message.typeUrl.split("/")[1]

    return DynamicMessage.parseFrom(typeRegistry.find(messageName), message.value)
  }
}
