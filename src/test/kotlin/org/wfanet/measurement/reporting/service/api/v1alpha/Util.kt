// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors

fun Descriptors.Descriptor.getFileDescriptorSet(): DescriptorProtos.FileDescriptorSet {
  val fileDescriptors = mutableSetOf<Descriptors.FileDescriptor>()
  val toVisit = mutableListOf<Descriptors.FileDescriptor>(file)
  while (toVisit.isNotEmpty()) {
    val fileDescriptor = toVisit.removeLast()
    if (!fileDescriptors.contains(fileDescriptor)) {
      fileDescriptors.add(fileDescriptor)
      fileDescriptor.dependencies.forEach {
        if (!fileDescriptors.contains(it)) {
          toVisit.add(it)
        }
      }
    }
  }
  return DescriptorProtos.FileDescriptorSet.newBuilder()
    .addAllFile(fileDescriptors.map { it.toProto() })
    .build()
}
