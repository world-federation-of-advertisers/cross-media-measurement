/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.mcp.tools

import com.google.gson.JsonArray
import com.google.gson.JsonObject

class InputSchemaBuilder {
  private val properties = JsonObject()
  private val requiredList = JsonArray()

  fun stringProperty(name: String, description: String) {
    properties.add(name, JsonObject().apply {
      addProperty("type", "string")
      addProperty("description", description)
    })
  }

  fun intProperty(name: String, description: String) {
    properties.add(name, JsonObject().apply {
      addProperty("type", "integer")
      addProperty("description", description)
    })
  }

  fun objectProperty(name: String, description: String) {
    properties.add(name, JsonObject().apply {
      addProperty("type", "object")
      addProperty("description", description)
    })
  }

  fun required(vararg names: String) {
    names.forEach { requiredList.add(it) }
  }

  fun build(): JsonObject = JsonObject().apply {
    addProperty("type", "object")
    add("properties", properties)
    if (requiredList.size() > 0) {
      add("required", requiredList)
    }
  }
}

inline fun inputSchema(block: InputSchemaBuilder.() -> Unit): JsonObject =
  InputSchemaBuilder().apply(block).build()
