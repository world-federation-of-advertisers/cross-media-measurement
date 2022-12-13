/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.fileDescriptorSet

/**
 * Utility object for protobuf reflection.
 *
 * TODO(@SanjayVas): Move to common-jvm.
 */
object ProtoReflection {
  /**
   * Builds a [DescriptorProtos.FileDescriptorSet] from [descriptor], including direct and
   * transitive dependencies.
   */
  fun buildFileDescriptorSet(
    descriptor: Descriptors.Descriptor
  ): DescriptorProtos.FileDescriptorSet {
    val fileDescriptors = mutableSetOf<Descriptors.FileDescriptor>()
    val rootFileDescriptor: Descriptors.FileDescriptor = descriptor.file
    fileDescriptors.addDeps(rootFileDescriptor)
    fileDescriptors.add(rootFileDescriptor)

    return fileDescriptorSet {
      for (fileDescriptor in fileDescriptors) {
        this.file += fileDescriptor.toProto()
      }
    }
  }

  /** Adds all direct and transitive dependencies of [fileDescriptor] to this [MutableSet]. */
  private fun MutableSet<Descriptors.FileDescriptor>.addDeps(
    fileDescriptor: Descriptors.FileDescriptor
  ) {
    for (dep in fileDescriptor.dependencies) {
      if (contains(dep)) {
        continue
      }
      addDeps(dep)
      add(dep)
    }
  }
}
