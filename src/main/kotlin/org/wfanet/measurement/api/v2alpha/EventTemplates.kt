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

import com.google.common.reflect.ClassPath
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry

object EventTemplates {
  private val typeRegistry: TypeRegistry

  init {
    val registryBuilder = TypeRegistry.newBuilder()
    // TODO(chipingyeh): Filter out messages causing exceptions and/or use ClassGraph instead.
    val classes = ClassPath.from(ClassLoader.getSystemClassLoader()).getTopLevelClasses()
    for (c in classes) {
      try {
        val clazz = c.load()
        if (!Message::class.java.isAssignableFrom(clazz)) {
          continue
        }

        val descriptor: Descriptor = clazz.getMethod("getDescriptor").invoke(null) as Descriptor
        if (descriptor.options.hasExtension(EventAnnotations.eventTemplate)) {
          registryBuilder.add(descriptor)
        }
      } catch (e: Exception) {} catch (e: NoClassDefFoundError) {} catch (
        e: UnsupportedClassVersionError) {}
    }

    typeRegistry = registryBuilder.build()
  }

  /**
   * Returns the EventTemplate for a fully qualified message type [messageName]. Returns null if
   * message is not found.
   */
  fun getEventTemplateForType(messageName: String): EventTemplate? {
    val descriptor = typeRegistry.find(messageName) ?: return null

    return EventTemplate(descriptor)
  }
}
