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

class EventTemplateTypeRegistry() {
  companion object {
    lateinit var classPath: String
    lateinit var registry: TypeRegistry
    /**
     * Creates a Type Registry for all [Message]s in a package [packageName] with class path
     * [classPath]. Note that [packageName] refers to the package of the proto file, while
     * [classPath] refers to the path of the generated Java files (classes).
     */
    fun createRegistryForPackagePrefix(packageName: String, classPath: String): TypeRegistry {
      this.classPath = classPath
      val registryBuilder = TypeRegistry.newBuilder()
      val classes =
        ClassPath.from(ClassLoader.getSystemClassLoader()).getTopLevelClassesRecursive(packageName)
      for (c in classes) {
        val clazz = c.load()
        if (!Message::class.java.isAssignableFrom(clazz)) {
          continue
        }
        val descriptor: Descriptor = clazz.getMethod("getDescriptor").invoke(null) as Descriptor
        if (descriptor.options.hasExtension(EventAnnotations.eventTemplate)) {
          registryBuilder.add(descriptor)
        }
      }
      registry = registryBuilder.build()

      return registry
    }

    /**
     * Returns the Descriptor for a fully qualified message type. Returns null if message is not
     * found.
     */
    fun getDescriptorForType(messageName: String): Descriptor? {
      return registry.find(classPath + "." + messageName)
    }
  }
}
