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

package org.wfanet.measurement.common

import com.google.protobuf.Descriptors.FieldDescriptor
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.util.Collections
import java.util.IdentityHashMap

/**
 * Emits exception types, code locations and approved validation metadata without exception text.
 */
object ExceptionDiagnostics {
  enum class Category {
    UNKNOWN_LOOKUP_VALUE,
    UNKNOWN_ENUM_VALUE,
    OUT_OF_RANGE,
    INCOMPATIBLE_TYPE,
    INVALID_CONFIGURATION,
  }

  enum class Type {
    UNKNOWN,
    SCHEMA_FIELD,
    UNIQUE_FIELD_PATH,
    COMPOSITE_IDENTITY_COLUMNS,
    MAPPING_SOURCE,
    SINGULAR_MESSAGE,
    SINGULAR_SCALAR_OR_ENUM,
    REPEATED,
    NON_NEGATIVE_AGE,
    UINT32_AGE,
    INTEGRAL,
    FLOATING_POINT,
    ENUM,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    STRING,
    BYTE_STRING,
    MESSAGE,
    INT32,
    INT64,
    UINT32,
    UINT64,
    BOOL,
    STRING_VALUE,
    BYTES_VALUE,
    BOOL_VALUE,
    INT32_VALUE,
    INT64_VALUE,
    UINT32_VALUE,
    UINT64_VALUE,
    FLOAT_VALUE,
    DOUBLE_VALUE,
    TIMESTAMP_VALUE,
    KIND_NOT_SET,
    SOURCE_NOT_SET;

    companion object {
      /** Maps only closed type identifiers, never arbitrary diagnostic text. */
      fun fromName(name: String): Type = entries.firstOrNull { it.name == name } ?: UNKNOWN
    }
  }

  /** Approved schema, type, or explicitly configured mapping-failure metadata. */
  class ValidationFailure
  private constructor(
    val category: Category,
    val field: FieldDescriptor?,
    val expectedType: Type?,
    val actualType: Type?,
    private val configurationDetails: String = "",
  ) :
    IllegalArgumentException(
      validationMetadata(category, field, expectedType, actualType, configurationDetails)
    ) {
    internal fun diagnosticMetadata(): String =
      validationMetadata(category, field, expectedType, actualType, configurationDetails)

    companion object {
      /** Rejects duplicate paths from the configured mappings, in deterministic order. */
      fun duplicateFieldPaths(configuredPaths: Collection<String>): ValidationFailure {
        val duplicates =
          configuredPaths.groupingBy { it }.eachCount().filterValues { it > 1 }.keys.sorted()
        require(duplicates.isNotEmpty()) { "No duplicate configured field paths" }
        val retained = duplicates.take(MAX_CONFIGURED_PATHS)
        val details =
          "duplicate_paths=" +
            retained.joinToString(",", "[", "]", transform = ::quoteConfiguration) +
            if (duplicates.size > retained.size)
              "; additional_duplicate_paths=" + (duplicates.size - retained.size)
            else ""
        return ValidationFailure(
          Category.INVALID_CONFIGURATION,
          null,
          Type.UNIQUE_FIELD_PATH,
          null,
          details,
        )
      }

      /** Rejects a target symbol from enum lookup configuration, never its lookup key. */
      fun invalidEnumTarget(field: FieldDescriptor, configuredTarget: String): ValidationFailure {
        require(field.javaType == FieldDescriptor.JavaType.ENUM) { "Target field must be an enum" }
        val validIdentifier = PROTO_IDENTIFIER.matches(configuredTarget)
        val target =
          if (validIdentifier) quoteConfiguration(configuredTarget)
          else "<invalid proto identifier>"
        return ValidationFailure(
          Category.INVALID_CONFIGURATION,
          field,
          Type.ENUM,
          null,
          "enum=" +
            quoteConfiguration(field.enumType.fullName) +
            "; configured_enum_target=" +
            target +
            "; identifier_valid=" +
            validIdentifier,
        )
      }

      /** Rejects an unresolved path from mapping configuration, retaining the missing segment. */
      fun missingConfiguredField(
        configuredPath: String,
        missingSegment: String,
      ): ValidationFailure =
        ValidationFailure(
          Category.INVALID_CONFIGURATION,
          null,
          Type.SCHEMA_FIELD,
          null,
          "configured_path=" +
            quoteConfiguration(configuredPath) +
            "; missing_segment=" +
            quoteConfiguration(missingSegment),
        )

      fun unknownLookup(field: FieldDescriptor? = null): ValidationFailure =
        ValidationFailure(Category.UNKNOWN_LOOKUP_VALUE, field, null, null)

      fun unknownEnum(field: FieldDescriptor): ValidationFailure =
        ValidationFailure(Category.UNKNOWN_ENUM_VALUE, field, Type.ENUM, null)

      fun outOfRange(expected: Type, field: FieldDescriptor? = null): ValidationFailure =
        ValidationFailure(Category.OUT_OF_RANGE, field, expected, null)

      fun incompatibleType(
        expected: Type,
        actual: Type,
        field: FieldDescriptor? = null,
      ): ValidationFailure = ValidationFailure(Category.INCOMPATIBLE_TYPE, field, expected, actual)

      fun invalidConfiguration(
        expected: Type? = null,
        actual: Type? = null,
        field: FieldDescriptor? = null,
      ): ValidationFailure =
        ValidationFailure(Category.INVALID_CONFIGURATION, field, expected, actual)
    }
  }

  /** A bounded failure description for storage or transmission outside a processing boundary. */
  fun summary(error: Throwable): String {
    return try {
      val root = forLogging(error)
      val nodes = ArrayDeque<Throwable>()
      nodes.add(root)
      val retained = mutableListOf<Throwable>()
      while (nodes.isNotEmpty() && retained.size < MAX_EXCEPTIONS) {
        val node = nodes.removeFirst()
        retained += node
        node.cause?.let(nodes::addLast)
        node.suppressed.forEach(nodes::addLast)
      }
      val location = retained.firstNotNullOfOrNull { it.stackTrace.firstOrNull() }
      val omitted = if (nodes.isNotEmpty()) "; additional exceptions omitted" else ""
      val capacity = MAX_SUMMARY_LENGTH - MAX_LOCATION_LENGTH - 5 - omitted.length
      val names = retained.map(::typeName)
      val types =
        if (names.joinToString(" <- ").length <= capacity) names
        else names.map { it.substringAfterLast('.') }
      val partLimit = (capacity - 4 * (retained.size - 1)) / retained.size
      val descriptions =
        retained
          .mapIndexed { index, node ->
            val type = types[index].take(partLimit)
            val fields = metadata(node)
            if (fields.isEmpty() || type.length + 2 >= partLimit) type
            else type + ": " + fields.take(partLimit - type.length - 2)
          }
          .joinToString(" <- ")
      descriptions +
        omitted +
        location?.let { "; at ${it.toString().take(MAX_LOCATION_LENGTH)}" }.orEmpty()
    } catch (_: Throwable) {
      typeName(error).take(MAX_SUMMARY_LENGTH)
    }
  }

  /**
   * Returns a detached Throwable safe for standard logging formatters or outward propagation.
   * Original errors remain available to the caller for internal classification.
   */
  fun forLogging(error: Throwable): Throwable {
    return try {
      Snapshot().copy(error, 0)
    } catch (_: Throwable) {
      diagnostic(error, "Diagnostic details unavailable").apply { stackTrace = emptyArray() }
    }
  }

  private class Snapshot {
    private val visited = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
    private var remainingFrames = MAX_STACK_FRAMES

    fun copy(error: Throwable, depth: Int): Throwable {
      if (depth >= MAX_DEPTH || visited.size >= MAX_EXCEPTIONS || !visited.add(error)) {
        return DiagnosticException("Throwable", "Exception graph omitted").apply {
          stackTrace = emptyArray()
        }
      }
      val result = diagnostic(error, metadata(error))
      val frames =
        error.stackTrace
          .take(minOf(remainingFrames, MAX_FRAMES_PER_EXCEPTION))
          .map { frame ->
            StackTraceElement(
              escapeText(frame.className, MAX_FRAME_FIELD_LENGTH),
              escapeText(frame.methodName, MAX_FRAME_FIELD_LENGTH),
              frame.fileName?.let { escapeText(it, MAX_FRAME_FIELD_LENGTH) },
              frame.lineNumber,
            )
          }
          .toTypedArray()
      remainingFrames -= frames.size
      result.stackTrace = frames
      val cause = error.cause
      if (cause != null) result.initCause(copy(cause, depth + 1))
      for (suppressed in error.suppressed.take(MAX_EXCEPTIONS - visited.size)) {
        result.addSuppressed(copy(suppressed, depth + 1))
      }
      return result
    }
  }

  private fun diagnostic(error: Throwable, fields: String): Throwable =
    if (error is Error) DiagnosticError(typeName(error), fields)
    else DiagnosticException(typeName(error), fields)

  private class DiagnosticException(val originalType: String, val fields: String) :
    RuntimeException(fields) {
    override fun toString(): String = description(originalType, fields)
  }

  private class DiagnosticError(val originalType: String, val fields: String) : Error(fields) {
    override fun toString(): String = description(originalType, fields)
  }

  private fun typeName(error: Throwable): String =
    escapeText(
      when (error) {
        is DiagnosticException -> error.originalType
        is DiagnosticError -> error.originalType
        else -> error.javaClass.name
      },
      MAX_FIELD_LENGTH,
    )

  private fun description(type: String, fields: String): String =
    if (fields.isEmpty()) type else "$type: $fields"

  private fun metadata(error: Throwable): String =
    when (error) {
      is DiagnosticException -> error.fields
      is DiagnosticError -> error.fields
      is ValidationFailure -> error.diagnosticMetadata()
      is StatusException -> "grpc.${error.status.code.name}"
      is StatusRuntimeException -> "grpc.${error.status.code.name}"
      else -> ""
    }

  private fun validationMetadata(
    category: Category,
    field: FieldDescriptor?,
    expectedType: Type?,
    actualType: Type?,
    configurationDetails: String,
  ): String =
    buildList {
        add(category.name)
        field?.let { add("field=" + escapeText(it.fullName, MAX_FIELD_LENGTH)) }
        expectedType?.let { add("expected=" + it.name) }
        actualType?.let { add("actual=" + it.name) }
        if (configurationDetails.isNotEmpty()) add(configurationDetails)
      }
      .joinToString("; ")

  private fun quoteConfiguration(text: String): String =
    "\"" + escapeText(text, MAX_FIELD_LENGTH - 2, escapeQuotes = true) + "\""

  private fun escapeText(text: String, limit: Int, escapeQuotes: Boolean = false): String =
    buildString {
      for (character in text) {
        val escaped =
          when (character) {
            '\\' -> if (escapeQuotes) "\\\\" else "\\"
            '"' -> if (escapeQuotes) "\\\"" else "\""
            '\n' -> "\\n"
            '\r' -> "\\r"
            '\t' -> "\\t"
            else ->
              if (
                character.code < 32 ||
                  character.code in 127..159 ||
                  character == '\u2028' ||
                  character == '\u2029'
              ) {
                "\\u" + character.code.toString(16).padStart(4, '0')
              } else character.toString()
          }
        if (length + escaped.length > limit) break
        append(escaped)
      }
    }

  private val PROTO_IDENTIFIER = Regex("[A-Za-z_][A-Za-z0-9_]*")
  private const val MAX_CONFIGURED_PATHS = 5
  private const val MAX_SUMMARY_LENGTH = 1024
  private const val MAX_FIELD_LENGTH = 200
  private const val MAX_DEPTH = 20
  private const val MAX_EXCEPTIONS = 32
  private const val MAX_STACK_FRAMES = 128
  private const val MAX_FRAMES_PER_EXCEPTION = 32
  private const val MAX_FRAME_FIELD_LENGTH = 100
  private const val MAX_LOCATION_LENGTH = 200
}
