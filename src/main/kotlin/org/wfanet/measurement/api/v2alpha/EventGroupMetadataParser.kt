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

class EventGroupMetadataParser(eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor>) {
  private val typeRegistry: TypeRegistry

  init {
    val registryBuilder: TypeRegistry.Builder = TypeRegistry.newBuilder()
    eventGroupMetadataDescriptors
      .flatMap { buildFileDescriptors(it) }
      .flatMap { it.messageTypes }
      .forEach { registryBuilder.add(it) }
    typeRegistry = registryBuilder.build()
  }

  private fun buildFileDescriptors(
    eventGroupMetadataDescriptor: EventGroupMetadataDescriptor
  ): List<FileDescriptor> {
    val fileList: List<FileDescriptorProto> = eventGroupMetadataDescriptor.descriptorSet.fileList
    val fileDescriptorProtoMap = fileList.associateBy { it.name }
    val fileDescriptorList: MutableList<FileDescriptor> = mutableListOf()
    val builtFileDescriptorMap: MutableMap<String, FileDescriptor> = mutableMapOf()
    for (proto in fileList) {
      fileDescriptorList.add(
        buildFileDescriptors(
          proto = proto,
          builtFileDescriptorMap = builtFileDescriptorMap,
          fileDescriptorProtoMap = fileDescriptorProtoMap
        )
      )
    }

    return fileDescriptorList
  }

  private fun buildFileDescriptors(
    proto: FileDescriptorProto,
    builtFileDescriptorMap: MutableMap<String, FileDescriptor>,
    fileDescriptorProtoMap: Map<String, FileDescriptorProto>
  ): FileDescriptor {
    if (proto.name in builtFileDescriptorMap) return builtFileDescriptorMap.getValue(proto.name)

    var dependenciesList: MutableList<FileDescriptor> = mutableListOf()
    for (dependencyName in proto.dependencyList) {
      if (dependencyName in builtFileDescriptorMap) {
        dependenciesList.add(builtFileDescriptorMap.getValue(dependencyName))
      } else {
        dependenciesList.add(
          buildFileDescriptors(
            fileDescriptorProtoMap.getValue(dependencyName),
            builtFileDescriptorMap,
            fileDescriptorProtoMap
          )
        )
      }
    }

    val builtDescriptor = FileDescriptor.buildFrom(proto, dependenciesList.toTypedArray())
    builtFileDescriptorMap[proto.name] = builtDescriptor
    return builtDescriptor
  }

  /** Returns the [DynamicMessage] from an [EventGroup.Metadata] message. */
  fun convertToDynamicMessage(eventGroupMetadata: EventGroup.Metadata): DynamicMessage? {
    val descriptor =
      typeRegistry.getDescriptorForTypeUrl(eventGroupMetadata.metadata.typeUrl) ?: return null
    return DynamicMessage.parseFrom(descriptor, eventGroupMetadata.metadata.value)
  }
}
