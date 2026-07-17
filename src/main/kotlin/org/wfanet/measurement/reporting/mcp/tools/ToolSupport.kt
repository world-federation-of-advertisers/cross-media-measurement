/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import io.grpc.Status
import io.grpc.StatusException
import io.modelcontextprotocol.kotlin.sdk.types.CallToolResult
import io.modelcontextprotocol.kotlin.sdk.types.McpJson
import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import java.text.ParseException
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive

object ToolSupport {
  val PROTO_JSON_PRINTER: JsonFormat.Printer = JsonFormat.printer()

  val PROTO_JSON_PARSER: JsonFormat.Parser = JsonFormat.parser()

  fun getString(args: JsonObject, key: String): String = args.getValue(key).jsonPrimitive.content

  fun getStringOrNull(args: JsonObject, key: String): String? = args[key]?.jsonPrimitive?.content

  fun getIntOrNull(args: JsonObject, key: String): Int? = args[key]?.jsonPrimitive?.int

  /** Encodes a [JsonElement] to a JSON string using [McpJson]. */
  fun encodeJsonElement(element: JsonElement): String =
    McpJson.encodeToString(JsonElement.serializer(), element)

  /** Wraps a tool call with error handling for gRPC and input parsing exceptions. */
  suspend fun handleToolCall(block: suspend () -> String): CallToolResult {
    return try {
      CallToolResult(content = listOf(TextContent(block())))
    } catch (e: StatusException) {
      // Map authentication failures to actionable guidance: the bearer token is forwarded
      // unverified, so an expired/invalid token surfaces only here as UNAUTHENTICATED, and a bare
      // "gRPC error: UNAUTHENTICATED" doesn't tell the caller to refresh it.
      val message =
        when (e.status.code) {
          Status.Code.UNAUTHENTICATED ->
            "Authentication failed: the bearer token is missing, expired, or invalid. " +
              "Re-authenticate and retry."
          Status.Code.PERMISSION_DENIED ->
            "Access denied: the authenticated principal does not have permission for this " +
              "operation. Check that the MeasurementConsumer has granted access."
          else -> "gRPC error: ${e.status.code}: ${e.status.description}"
        }
      CallToolResult(content = listOf(TextContent(message)), isError = true)
    } catch (e: InvalidProtocolBufferException) {
      CallToolResult(
        content = listOf(TextContent("Invalid proto JSON: ${e.message}")),
        isError = true,
      )
    } catch (e: ParseException) {
      CallToolResult(
        content = listOf(TextContent("Invalid timestamp format: ${e.message}")),
        isError = true,
      )
    } catch (e: IllegalArgumentException) {
      CallToolResult(
        content = listOf(TextContent("Invalid argument: ${e.message}")),
        isError = true,
      )
    }
  }
}
