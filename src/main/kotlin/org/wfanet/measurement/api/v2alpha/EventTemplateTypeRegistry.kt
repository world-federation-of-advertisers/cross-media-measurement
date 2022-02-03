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
