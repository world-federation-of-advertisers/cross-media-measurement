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

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import org.reflections.ReflectionUtils
import org.reflections.Reflections
import org.reflections.scanners.Scanners.SubTypes

class EventTemplateTypeRegistry(private val packageName: String) {
  private lateinit var registry: TypeRegistry

  init {
    loadTemplates()
  }

  /** Reloads templates into memory, i.e. if a new template was added. */
  public fun reloadTemplates() {
    loadTemplates()
  }

  /** Returns the current TypeRegistry. */
  fun getDescriptorForType(messageType: String): Descriptor {
    // return templates
    return registry.find(messageType)
  }

  private fun loadTemplates() {
    val registryBuilder = TypeRegistry.newBuilder()
    val reflections = Reflections(packageName, SubTypes.filterResultsBy { true })
    val classes: Set<Class<out Message>> = reflections.getSubTypesOf(Message::class.java)
    for (c in classes) {
      try {
        val descriptor = ReflectionUtils.invoke(c.getMethod("getDescriptor"), c) as Descriptor
        if (descriptor.options.hasExtension(EventAnnotations.eventTemplate)) {
          registryBuilder.add(descriptor)
        }
      } catch (e: NoSuchMethodException) {}
    }

    registry = registryBuilder.build()
  }

  companion object {
    fun createRegistryForPackagePrefix(prefix: String): EventTemplateTypeRegistry {
      return EventTemplateTypeRegistry(prefix)
    }
  }
}
