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

import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import org.wfanet.measurement.api.v2alpha.EventGroup.Metadata
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor

class EventGroupMetadataParser(eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor>) {
  private var typeRegistry: TypeRegistry

  init {
    val registryBuilder: TypeRegistry.Builder = TypeRegistry.newBuilder()
    for (eventGroupMetadataDescriptor in eventGroupMetadataDescriptors) {
      val fileList: List<FileDescriptorProto> =
        eventGroupMetadataDescriptor.details.descriptorSet.fileList
      val fileDescriptorProtoMap = fileList.map { it.name to it }.toMap()
      val fileDescriptorMap: MutableMap<String, FileDescriptor> = mutableMapOf()
      for (proto in fileList) {
        buildFileDescriptors(proto, fileDescriptorMap, fileDescriptorProtoMap, registryBuilder)
      }
    }

    typeRegistry = registryBuilder.build()
  }

  private fun buildFileDescriptors(
    proto: FileDescriptorProto,
    fileDescriptorMap: MutableMap<String, FileDescriptor>,
    fileDescriptorProtoMap: Map<String, FileDescriptorProto>,
    registryBuilder: TypeRegistry.Builder
  ): FileDescriptor {
    if (fileDescriptorMap[proto.getName()] != null) return fileDescriptorMap[proto.getName()]!!

    var dependenciesList: MutableList<FileDescriptor> = mutableListOf()
    for (dependencyByteName in proto.getDependencyList().asByteStringList()) {
      val dependencyName = dependencyByteName.toStringUtf8()
      if (fileDescriptorMap[dependencyName] != null) {
        dependenciesList.add(fileDescriptorMap[dependencyName]!!)
      } else {
        dependenciesList.add(
          buildFileDescriptors(
            fileDescriptorProtoMap[dependencyName]!!,
            fileDescriptorMap,
            fileDescriptorProtoMap,
            registryBuilder
          )
        )
      }
    }

    val builtDescriptor = FileDescriptor.buildFrom(proto, dependenciesList.toTypedArray())
    fileDescriptorMap[proto.getName()] = builtDescriptor
    for (descriptor in builtDescriptor.getMessageTypes()) {
      registryBuilder.add(descriptor)
    }
    return builtDescriptor
  }

  /** Returns the DynamicMessage from an EventGroup.Metadata message. */
  fun convertToDynamicMessage(eventGroupMetadata: EventGroup.Metadata): DynamicMessage? {
    return DynamicMessage.parseFrom(
      typeRegistry.getDescriptorForTypeUrl(eventGroupMetadata.metadata.typeUrl),
      eventGroupMetadata.metadata.value
    )
  }
}
