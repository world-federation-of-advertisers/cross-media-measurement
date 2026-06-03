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

import com.google.protobuf.util.JsonFormat
import io.grpc.StatusException
import io.modelcontextprotocol.kotlin.sdk.types.CallToolResult
import io.modelcontextprotocol.kotlin.sdk.types.TextContent

val PROTO_JSON_PRINTER: JsonFormat.Printer =
  JsonFormat.printer().omittingInsignificantWhitespace()

val PROTO_JSON_PARSER: JsonFormat.Parser = JsonFormat.parser()

/** Wraps a gRPC tool call with [StatusException] error handling. */
suspend fun handleGrpcToolCall(block: suspend () -> String): CallToolResult {
  return try {
    CallToolResult(content = listOf(TextContent(block())))
  } catch (e: StatusException) {
    CallToolResult(
      content = listOf(TextContent("gRPC error: ${e.status.code}: ${e.status.description}")),
      isError = true,
    )
  }
}
