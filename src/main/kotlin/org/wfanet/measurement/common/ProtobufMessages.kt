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

package org.wfanet.measurement.common

import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import com.google.protobuf.util.JsonFormat
import java.io.File
import java.io.IOException
import java.nio.file.Files
import kotlin.jvm.Throws

object ProtobufMessages {
  private enum class ProtoMessageFormat {
    UNKNOWN,
    BINARY,
    JSON,
    TEXT;

    companion object {
      val DEFAULT = BINARY
    }
  }

  /**
   * Parses a [Message] from [file] in binary, JSON, or text format.
   *
   * This uses heuristics to determine the format.
   */
  @Throws(IOException::class)
  fun <T : Message> parseMessage(
    file: File,
    messageInstance: T,
    typeRegistry: TypeRegistry = TypeRegistry.getEmptyTypeRegistry(),
  ): T {
    val formatFromExtension: ProtoMessageFormat =
      if (file.name.endsWith(".binpb")) {
        ProtoMessageFormat.BINARY
      } else if (file.name.endsWith(".textproto") || file.name.endsWith(".txtpb")) {
        ProtoMessageFormat.TEXT
      } else if (file.name.endsWith(".json")) {
        ProtoMessageFormat.JSON
      } else {
        ProtoMessageFormat.UNKNOWN
      }

    val format =
      if (formatFromExtension == ProtoMessageFormat.UNKNOWN) {
        // Attempt to detect content type.
        val contentType: String? = Files.probeContentType(file.toPath())
        if (contentType == null) {
          // Default.
          ProtoMessageFormat.DEFAULT
        } else {
          val type = contentType.split(";", limit = 2).first()
          if (type == "application/protobuf") {
            ProtoMessageFormat.BINARY
          } else if (type == "application/protobuf+json" || type == "application/json") {
            ProtoMessageFormat.JSON
          } else if (type.startsWith("text/")) {
            ProtoMessageFormat.TEXT
          } else {
            ProtoMessageFormat.DEFAULT
          }
        }
      } else {
        formatFromExtension
      }

    return when (format) {
      ProtoMessageFormat.BINARY ->
        @Suppress("UNCHECKED_CAST") // Safe per protobuf API contracts.
        file.inputStream().use { input ->
          return messageInstance.parserForType.parseFrom(input) as T
        }
      ProtoMessageFormat.JSON ->
        @Suppress("UNCHECKED_CAST") // Safe per protobuf API contracts.
        messageInstance
          .toBuilder()
          .also { builder ->
            file.bufferedReader().use { reader ->
              JsonFormat.parser().usingTypeRegistry(typeRegistry).merge(reader, builder)
            }
          }
          .build() as T
      ProtoMessageFormat.TEXT -> parseTextProto(file, messageInstance, typeRegistry)
      ProtoMessageFormat.UNKNOWN -> error("Unreachable")
    }
  }
}
