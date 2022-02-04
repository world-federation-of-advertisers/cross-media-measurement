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
import org.reflections.ReflectionUtils
import org.reflections.Reflections
import org.reflections.scanners.Scanners.SubTypes

class EventTemplateTypeRegistry(private val packageName: String) {
  private var templates: Map<String, Descriptor> = mapOf<String, Descriptor>()

  init {
    loadTemplates()
  }

  /** Reloads templates into memory, i.e. if a new template was added. */
  public fun reloadTemplates() {
    loadTemplates()
  }

  /** Returns the current templates map. */
  fun getTemplates(): Map<String, Descriptor> {
    return templates
  }

  private fun loadTemplates() {
    val tempMap = mutableMapOf<String, Descriptor>()
    val reflections = Reflections(packageName, SubTypes.filterResultsBy { true })
    val classes: Set<Class<*>> = reflections.getSubTypesOf(Object::class.java)
    for (c in classes.filter { c -> c.simpleName.endsWith("Template") }) {
      tempMap[c.simpleName] = ReflectionUtils.invoke(c.getMethod("getDescriptor"), c) as Descriptor
    }

    templates = tempMap.toMap()
  }
}
