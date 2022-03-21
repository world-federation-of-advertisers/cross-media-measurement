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
import java.lang.reflect.Constructor

class EventTemplateTypeRegistry(private val registry: TypeRegistry) {

  /**
   * Returns the Descriptor for a fully qualified message type. Returns null if message is not
   * found.
   */
  fun getDescriptorForType(messageType: String): Descriptor? {
    return registry.find(messageType)
  }

  companion object {
    /**
     * Creates a Type Registry for all Messages in a classpath @prefix. All utilized templates
     * should be included in this classpath.
     */
    fun createRegistryForPackagePrefix(prefix: String): EventTemplateTypeRegistry {
      val registryBuilder = TypeRegistry.newBuilder()
      val classes =
        ClassPath.from(ClassLoader.getSystemClassLoader()).getTopLevelClassesRecursive(prefix)
      for (c in classes) {
        try {
          val constructor = c.load().getDeclaredConstructor() as Constructor<out Message>
          constructor.isAccessible = true
          val descriptor: Descriptor = constructor.newInstance().descriptorForType
          if (descriptor.options.hasExtension(EventAnnotations.eventTemplate)) {
            registryBuilder.add(descriptor)
          }
        } catch (e: NoSuchMethodException) {} catch (e: ClassCastException) {}
      }
      return EventTemplateTypeRegistry(registryBuilder.build())
    }
  }
}
