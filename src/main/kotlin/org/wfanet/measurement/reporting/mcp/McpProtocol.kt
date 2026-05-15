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

package org.wfanet.measurement.reporting.mcp

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.JsonPrimitive

private const val JSONRPC_VERSION = "2.0"
private const val PROTOCOL_VERSION = "2025-03-26"
private const val SERVER_NAME = "HaloReportingMcpServer"
private const val SERVER_VERSION = "0.1.0"

data class McpTool(
  val name: String,
  val description: String,
  val inputSchema: JsonObject,
  val handler: suspend (args: JsonObject) -> String,
)

data class McpPromptArgument(
  val name: String,
  val description: String,
  val required: Boolean = false,
)

data class McpPrompt(
  val name: String,
  val description: String,
  val arguments: List<McpPromptArgument> = emptyList(),
  val handler: (args: Map<String, String>) -> List<McpPromptMessage>,
)

data class McpPromptMessage(
  val role: String,
  val text: String,
)

class McpServer {
  private val tools = mutableMapOf<String, McpTool>()
  private val prompts = mutableMapOf<String, McpPrompt>()

  fun addTool(tool: McpTool) {
    tools[tool.name] = tool
  }

  fun addPrompt(prompt: McpPrompt) {
    prompts[prompt.name] = prompt
  }

  suspend fun handleRequest(requestBody: String): String {
    val request = JsonParser.parseString(requestBody).asJsonObject
    val id = request.get("id")
    val method = request.get("method")?.asString ?: return errorResponse(id, -32600, "Invalid Request")

    return when (method) {
      "initialize" -> handleInitialize(id)
      "notifications/initialized" -> ""
      "ping" -> successResponse(id, JsonObject())
      "tools/list" -> handleToolsList(id)
      "tools/call" -> handleToolsCall(id, request.getAsJsonObject("params"))
      "prompts/list" -> handlePromptsList(id)
      "prompts/get" -> handlePromptsGet(id, request.getAsJsonObject("params"))
      else -> errorResponse(id, -32601, "Method not found: $method")
    }
  }

  private fun handleInitialize(id: JsonElement?): String {
    val result = JsonObject().apply {
      addProperty("protocolVersion", PROTOCOL_VERSION)
      add("capabilities", JsonObject().apply {
        add("tools", JsonObject().apply { addProperty("listChanged", false) })
        add("prompts", JsonObject().apply { addProperty("listChanged", false) })
      })
      add("serverInfo", JsonObject().apply {
        addProperty("name", SERVER_NAME)
        addProperty("version", SERVER_VERSION)
      })
    }
    return successResponse(id, result)
  }

  private fun handleToolsList(id: JsonElement?): String {
    val toolsArray = JsonArray()
    for (tool in tools.values) {
      toolsArray.add(JsonObject().apply {
        addProperty("name", tool.name)
        addProperty("description", tool.description)
        add("inputSchema", tool.inputSchema)
      })
    }
    val result = JsonObject().apply { add("tools", toolsArray) }
    return successResponse(id, result)
  }

  private suspend fun handleToolsCall(id: JsonElement?, params: JsonObject?): String {
    val toolName = params?.get("name")?.asString
      ?: return errorResponse(id, -32602, "Missing tool name")
    val tool = tools[toolName]
      ?: return errorResponse(id, -32602, "Unknown tool: $toolName")

    val args = params.getAsJsonObject("arguments") ?: JsonObject()

    return try {
      val output = tool.handler(args)
      val result = JsonObject().apply {
        add("content", JsonArray().apply {
          add(JsonObject().apply {
            addProperty("type", "text")
            addProperty("text", output)
          })
        })
      }
      successResponse(id, result)
    } catch (e: io.grpc.StatusRuntimeException) {
      val result = JsonObject().apply {
        add("content", JsonArray().apply {
          add(JsonObject().apply {
            addProperty("type", "text")
            addProperty("text", "${e.status.code}: ${e.status.description ?: e.message}")
          })
        })
        addProperty("isError", true)
      }
      successResponse(id, result)
    } catch (e: Exception) {
      val result = JsonObject().apply {
        add("content", JsonArray().apply {
          add(JsonObject().apply {
            addProperty("type", "text")
            addProperty("text", "Error: ${e.message}")
          })
        })
        addProperty("isError", true)
      }
      successResponse(id, result)
    }
  }

  private fun handlePromptsList(id: JsonElement?): String {
    val promptsArray = JsonArray()
    for (prompt in prompts.values) {
      promptsArray.add(JsonObject().apply {
        addProperty("name", prompt.name)
        addProperty("description", prompt.description)
        if (prompt.arguments.isNotEmpty()) {
          add("arguments", JsonArray().apply {
            for (arg in prompt.arguments) {
              add(JsonObject().apply {
                addProperty("name", arg.name)
                addProperty("description", arg.description)
                addProperty("required", arg.required)
              })
            }
          })
        }
      })
    }
    val result = JsonObject().apply { add("prompts", promptsArray) }
    return successResponse(id, result)
  }

  private fun handlePromptsGet(id: JsonElement?, params: JsonObject?): String {
    val promptName = params?.get("name")?.asString
      ?: return errorResponse(id, -32602, "Missing prompt name")
    val prompt = prompts[promptName]
      ?: return errorResponse(id, -32602, "Unknown prompt: $promptName")

    val args = mutableMapOf<String, String>()
    params.getAsJsonObject("arguments")?.entrySet()?.forEach { (key, value) ->
      args[key] = value.asString
    }

    val messages = prompt.handler(args)
    val result = JsonObject().apply {
      addProperty("description", prompt.description)
      add("messages", JsonArray().apply {
        for (msg in messages) {
          add(JsonObject().apply {
            addProperty("role", msg.role)
            add("content", JsonObject().apply {
              addProperty("type", "text")
              addProperty("text", msg.text)
            })
          })
        }
      })
    }
    return successResponse(id, result)
  }

  companion object {
    fun successResponse(id: JsonElement?, result: JsonObject): String {
      return JsonObject().apply {
        addProperty("jsonrpc", JSONRPC_VERSION)
        add("id", id ?: JsonNull.INSTANCE)
        add("result", result)
      }.toString()
    }

    fun errorResponse(id: JsonElement?, code: Int, message: String): String {
      return JsonObject().apply {
        addProperty("jsonrpc", JSONRPC_VERSION)
        add("id", id ?: JsonNull.INSTANCE)
        add("error", JsonObject().apply {
          addProperty("code", code)
          addProperty("message", message)
        })
      }.toString()
    }
  }
}
