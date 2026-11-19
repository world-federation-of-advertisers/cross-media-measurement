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

  /**
   * A validation failure containing schema/configuration field names and type names only.
   *
   * @param column the source column name, never its value
   * @param fieldPath the target schema field path
   * @param expectedType the expected schema type or kind
   * @param actualType the actual schema type or kind
   */
  class ValidationFailure(
    val category: Category,
    val column: String? = null,
    val fieldPath: String? = null,
    val expectedType: String? = null,
    val actualType: String? = null,
  ) :
    IllegalArgumentException(
      validationMetadata(category, column, fieldPath, expectedType, actualType)
    )

  /** A bounded failure description for storage or transmission outside a processing boundary. */
  fun summary(error: Throwable): String {
    return try {
      val root = forLogging(error)
      val nodes = ArrayDeque<Throwable>()
      nodes.add(root)
      val details = mutableListOf<String>()
      var location: StackTraceElement? = null
      while (nodes.isNotEmpty()) {
        val node = nodes.removeFirst()
        val fields = metadata(node)
        if (node === root || fields.isNotEmpty()) {
          details += description(typeName(node), fields).take(MAX_SUMMARY_PART_LENGTH)
          if (node.stackTrace.isNotEmpty()) location = node.stackTrace.first()
        }
        node.cause?.let(nodes::addLast)
        node.suppressed.forEach(nodes::addLast)
      }
      val descriptions =
        details.distinct().joinToString(" <- ").take(MAX_SUMMARY_LENGTH - MAX_LOCATION_LENGTH - 5)
      descriptions + location?.let { "; at ${it.toString().take(MAX_LOCATION_LENGTH)}" }.orEmpty()
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
              frame.className.take(MAX_FRAME_FIELD_LENGTH),
              frame.methodName.take(MAX_FRAME_FIELD_LENGTH),
              frame.fileName?.take(MAX_FRAME_FIELD_LENGTH),
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
    (when (error) {
        is DiagnosticException -> error.originalType
        is DiagnosticError -> error.originalType
        else -> error.javaClass.name
      })
      .take(MAX_FIELD_LENGTH)

  private fun description(type: String, fields: String): String =
    if (fields.isEmpty()) type else "$type: $fields"

  private fun metadata(error: Throwable): String =
    when (error) {
      is DiagnosticException -> error.fields
      is DiagnosticError -> error.fields
      is ValidationFailure ->
        validationMetadata(
          error.category,
          error.column,
          error.fieldPath,
          error.expectedType,
          error.actualType,
        )
      is StatusException -> "grpc.${error.status.code.name}"
      is StatusRuntimeException -> "grpc.${error.status.code.name}"
      else -> ""
    }

  private fun validationMetadata(
    category: Category,
    column: String?,
    fieldPath: String?,
    expectedType: String?,
    actualType: String?,
  ): String =
    buildList {
        add(category.name)
        column?.let { add("column=${it.take(MAX_FIELD_LENGTH)}") }
        fieldPath?.let { add("field=${it.take(MAX_FIELD_LENGTH)}") }
        expectedType?.let { add("expected=${it.take(MAX_FIELD_LENGTH)}") }
        actualType?.let { add("actual=${it.take(MAX_FIELD_LENGTH)}") }
      }
      .joinToString("; ")

  private const val MAX_SUMMARY_LENGTH = 1024
  private const val MAX_FIELD_LENGTH = 200
  private const val MAX_DEPTH = 20
  private const val MAX_EXCEPTIONS = 32
  private const val MAX_STACK_FRAMES = 128
  private const val MAX_FRAMES_PER_EXCEPTION = 32
  private const val MAX_FRAME_FIELD_LENGTH = 100
  private const val MAX_SUMMARY_PART_LENGTH = 300
  private const val MAX_LOCATION_LENGTH = 200
}
