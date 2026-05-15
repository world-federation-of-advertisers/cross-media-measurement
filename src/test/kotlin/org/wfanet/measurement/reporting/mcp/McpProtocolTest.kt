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

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class McpProtocolTest {

  @Test
  fun initializeReturnsServerInfoAndCapabilities() {
    val response = handle(jsonRpcRequest("initialize", id = 1))

    assertEquals("2.0", response.get("jsonrpc").asString)
    assertEquals(1, response.get("id").asInt)
    val result = response.getAsJsonObject("result")
    assertNotNull(result.get("protocolVersion"))
    assertNotNull(result.getAsJsonObject("capabilities").getAsJsonObject("tools"))
    assertNotNull(result.getAsJsonObject("capabilities").getAsJsonObject("prompts"))
    assertEquals(
      "HaloReportingMcpServer",
      result.getAsJsonObject("serverInfo").get("name").asString,
    )
  }

  @Test
  fun toolsListReturnsRegisteredTools() {
    val server = McpServer()
    server.addTool(
      McpTool(
        name = "test_tool",
        description = "A test tool",
        inputSchema = JsonObject().apply { addProperty("type", "object") },
      ) { _ -> "result" },
    )

    val response = handle(server, jsonRpcRequest("tools/list", id = 2))

    val tools = response.getAsJsonObject("result").getAsJsonArray("tools")
    assertEquals(1, tools.size())
    assertEquals("test_tool", tools[0].asJsonObject.get("name").asString)
    assertEquals("A test tool", tools[0].asJsonObject.get("description").asString)
  }

  @Test
  fun toolsCallInvokesHandlerAndReturnsContent() {
    val server = McpServer()
    server.addTool(
      McpTool(
        name = "echo",
        description = "Echoes input",
        inputSchema = JsonObject().apply { addProperty("type", "object") },
      ) { args -> "echo: ${args.get("msg").asString}" },
    )

    val params = JsonObject().apply {
      addProperty("name", "echo")
      add("arguments", JsonObject().apply { addProperty("msg", "hello") })
    }
    val response = handle(server, jsonRpcRequest("tools/call", id = 3, params = params))

    val content = response.getAsJsonObject("result").getAsJsonArray("content")
    assertEquals(1, content.size())
    assertEquals("text", content[0].asJsonObject.get("type").asString)
    assertEquals("echo: hello", content[0].asJsonObject.get("text").asString)
  }

  @Test
  fun toolsCallReturnsErrorForUnknownTool() {
    val params = JsonObject().apply { addProperty("name", "nonexistent") }
    val response = handle(jsonRpcRequest("tools/call", id = 4, params = params))

    val error = response.getAsJsonObject("error")
    assertNotNull(error)
    assertEquals(-32602, error.get("code").asInt)
    assertTrue(error.get("message").asString.contains("nonexistent"))
  }

  @Test
  fun toolsCallReturnsIsErrorForHandlerException() {
    val server = McpServer()
    server.addTool(
      McpTool(
        name = "failing_tool",
        description = "Always fails",
        inputSchema = JsonObject().apply { addProperty("type", "object") },
      ) { _ -> throw IllegalStateException("something broke") },
    )

    val params = JsonObject().apply { addProperty("name", "failing_tool") }
    val response = handle(server, jsonRpcRequest("tools/call", id = 5, params = params))

    val result = response.getAsJsonObject("result")
    assertTrue(result.get("isError").asBoolean)
    assertTrue(
      result.getAsJsonArray("content")[0].asJsonObject.get("text").asString
        .contains("something broke"),
    )
  }

  @Test
  fun promptsListReturnsRegisteredPrompts() {
    val server = McpServer()
    server.addPrompt(
      McpPrompt(
        name = "test-prompt",
        description = "A test prompt",
        arguments = listOf(McpPromptArgument("arg1", "First arg", required = true)),
      ) { args -> listOf(McpPromptMessage("user", "Hello ${args["arg1"]}")) },
    )

    val response = handle(server, jsonRpcRequest("prompts/list", id = 6))

    val prompts = response.getAsJsonObject("result").getAsJsonArray("prompts")
    assertEquals(1, prompts.size())
    assertEquals("test-prompt", prompts[0].asJsonObject.get("name").asString)
    val promptArgs = prompts[0].asJsonObject.getAsJsonArray("arguments")
    assertEquals(1, promptArgs.size())
    assertTrue(promptArgs[0].asJsonObject.get("required").asBoolean)
  }

  @Test
  fun promptsGetInvokesHandlerWithArguments() {
    val server = McpServer()
    server.addPrompt(
      McpPrompt(
        name = "greet",
        description = "Greeting prompt",
        arguments = listOf(McpPromptArgument("name", "Name to greet", required = true)),
      ) { args -> listOf(McpPromptMessage("user", "Hi ${args["name"]}!")) },
    )

    val params = JsonObject().apply {
      addProperty("name", "greet")
      add("arguments", JsonObject().apply { addProperty("name", "Alice") })
    }
    val response = handle(server, jsonRpcRequest("prompts/get", id = 7, params = params))

    val messages = response.getAsJsonObject("result").getAsJsonArray("messages")
    assertEquals(1, messages.size())
    assertEquals("user", messages[0].asJsonObject.get("role").asString)
    assertEquals(
      "Hi Alice!",
      messages[0].asJsonObject.getAsJsonObject("content").get("text").asString,
    )
  }

  @Test
  fun unknownMethodReturnsMethodNotFoundError() {
    val response = handle(jsonRpcRequest("bogus/method", id = 8))
    assertEquals(-32601, response.getAsJsonObject("error").get("code").asInt)
  }

  @Test
  fun pingReturnsResult() {
    val response = handle(jsonRpcRequest("ping", id = 9))
    assertNotNull(response.getAsJsonObject("result"))
  }

  @Test
  fun notificationsInitializedReturnsEmpty() {
    val raw = runBlocking {
      McpServer().handleRequest(jsonRpcRequest("notifications/initialized"))
    }
    assertEquals("", raw)
  }

  private fun handle(request: String): JsonObject = handle(McpServer(), request)

  private fun handle(server: McpServer, request: String): JsonObject {
    val raw = runBlocking { server.handleRequest(request) }
    return JsonParser.parseString(raw).asJsonObject
  }

  private fun jsonRpcRequest(
    method: String,
    id: Int? = null,
    params: JsonObject? = null,
  ): String =
    JsonObject().apply {
      addProperty("jsonrpc", "2.0")
      addProperty("method", method)
      if (id != null) addProperty("id", id)
      if (params != null) add("params", params)
    }.toString()
}
