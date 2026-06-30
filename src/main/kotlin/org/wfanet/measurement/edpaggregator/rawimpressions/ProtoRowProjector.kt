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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Message
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.ParquetValue.KindCase

/**
 * Shared machinery for projecting a flat Parquet row (column name -> [ParquetValue]) onto a target
 * protobuf message via a `field path -> column name` mapping.
 *
 * A field path is a dot-separated path of proto field names into the target descriptor, e.g.
 * `event_id.id` or `person.gender`. Intermediate segments must be singular (non-repeated) message
 * fields; the final segment must be a scalar or enum field. Columns absent from the row, or whose
 * [ParquetValue] is NULL (`KIND_NOT_SET`), are skipped, leaving the corresponding proto field
 * unset.
 *
 * Field paths are resolved to descriptor chains once (via [bind]); [setMappedFields] then does no
 * string-keyed descriptor lookups per row. Stateless and safe to share across threads.
 *
 * [resolvePath], [convert], [asLong], and [setLeaf] are exposed so callers with richer mappings
 * (e.g. [LabelerInputMapper]'s structured sources) can reuse the same descriptor resolution and
 * value coercion instead of duplicating it.
 */
object ProtoRowProjector {
  /** A resolved field path bound to the [column] it reads from. */
  class Binding(val path: List<FieldDescriptor>, val column: String)

  /** Parquet kinds coercible by [asLong] (integral / timestamp leaves). */
  val INTEGRAL_KINDS =
    setOf(
      KindCase.INT32_VALUE,
      KindCase.INT64_VALUE,
      KindCase.UINT32_VALUE,
      KindCase.UINT64_VALUE,
      KindCase.TIMESTAMP_VALUE,
    )

  /** Parquet kinds coercible by [asDouble] (floating leaves). */
  val FLOATING_KINDS =
    setOf(KindCase.FLOAT_VALUE, KindCase.DOUBLE_VALUE, KindCase.INT32_VALUE, KindCase.INT64_VALUE)

  /** The [ParquetValue.KindCase]s [convert] accepts for the leaf field [fd]. */
  fun acceptedKinds(fd: FieldDescriptor): Set<KindCase> =
    when (fd.javaType) {
      JavaType.INT,
      JavaType.LONG -> INTEGRAL_KINDS
      JavaType.FLOAT,
      JavaType.DOUBLE -> FLOATING_KINDS
      JavaType.BOOLEAN -> setOf(KindCase.BOOL_VALUE)
      JavaType.STRING -> setOf(KindCase.STRING_VALUE)
      JavaType.BYTE_STRING -> setOf(KindCase.BYTES_VALUE)
      JavaType.ENUM -> setOf(KindCase.STRING_VALUE, KindCase.INT32_VALUE, KindCase.UINT32_VALUE)
      else ->
        throw IllegalArgumentException(
          "Unsupported leaf field type ${fd.javaType} for ${fd.fullName}"
        )
    }

  /**
   * Resolves [fieldMapping] (field path -> column name) against [root] into [Binding]s, validating
   * each path against the descriptor up front.
   */
  fun bind(root: Descriptor, fieldMapping: Map<String, String>): List<Binding> =
    fieldMapping.map { (fieldPath, column) -> Binding(resolvePath(root, fieldPath), column) }

  /** Sets each mapped, non-NULL [bindings] field on [builder] from [row]. */
  fun setMappedFields(
    builder: Message.Builder,
    bindings: List<Binding>,
    row: Map<String, ParquetValue>,
  ) {
    for (binding in bindings) {
      val value = row[binding.column] ?: continue
      if (value.kindCase == KindCase.KIND_NOT_SET) continue
      val leaf = binding.path.last()
      setLeaf(builder, binding.path, convert(value, leaf, binding.column))
    }
  }

  /**
   * Walks [builder] to the message that directly owns the leaf of [path] (lazily creating
   * intermediate sub-builders) and sets the leaf field to [value].
   */
  fun setLeaf(builder: Message.Builder, path: List<FieldDescriptor>, value: Any) {
    var owner: Message.Builder = builder
    for (i in 0 until path.size - 1) {
      owner = owner.getFieldBuilder(path[i])
    }
    owner.setField(path.last(), value)
  }

  /**
   * Resolves a dot-separated [fieldPath] against [root] into an ordered descriptor chain.
   * Intermediate segments must be singular message fields. The leaf must be a singular scalar or
   * enum field, unless [allowMessageLeaf] is true (then the leaf must be a singular message field,
   * e.g. an age sub-message the caller fills itself).
   */
  fun resolvePath(
    root: Descriptor,
    fieldPath: String,
    allowMessageLeaf: Boolean = false,
  ): List<FieldDescriptor> {
    require(fieldPath.isNotEmpty()) { "Empty field path" }
    val segments = fieldPath.split('.')
    val path = ArrayList<FieldDescriptor>(segments.size)
    var current: Descriptor = root
    for ((index, name) in segments.withIndex()) {
      val field =
        requireNotNull(current.findFieldByName(name)) {
          "Unknown field '$name' in field path '$fieldPath'"
        }
      val isLast = index == segments.size - 1
      if (!isLast) {
        require(field.javaType == JavaType.MESSAGE && !field.isRepeated) {
          "Intermediate segment '$name' in '$fieldPath' must be a singular message field"
        }
        current = field.messageType
      } else if (allowMessageLeaf) {
        require(field.javaType == JavaType.MESSAGE && !field.isRepeated) {
          "Leaf segment '$name' in '$fieldPath' must be a singular message field"
        }
      } else {
        require(!field.isRepeated && field.javaType != JavaType.MESSAGE) {
          "Leaf segment '$name' in '$fieldPath' must be a singular scalar or enum field"
        }
      }
      path.add(field)
    }
    return path
  }

  /**
   * Converts a [ParquetValue] to the Java value expected by [Message.Builder.setField] for [fd].
   */
  fun convert(value: ParquetValue, fd: FieldDescriptor, column: String): Any {
    return when (fd.javaType) {
      JavaType.INT -> {
        val longValue = asLong(value, column)
        // Unsigned 32-bit proto fields are stored as a two's-complement Java `int`, so values in
        // [2^31, 2^32) are valid and must be bit-truncated rather than range-rejected.
        if (fd.type == FieldDescriptor.Type.UINT32 || fd.type == FieldDescriptor.Type.FIXED32) {
          require(longValue in 0L..0xFFFFFFFFL) {
            "Column '$column' value $longValue is out of range for unsigned 32-bit field " +
              fd.fullName
          }
          longValue.toInt()
        } else {
          longValue.toIntExact(column)
        }
      }
      JavaType.LONG -> asLong(value, column)
      JavaType.FLOAT -> asDouble(value, column).toFloat()
      JavaType.DOUBLE -> asDouble(value, column)
      JavaType.BOOLEAN -> requireKind(value, column, KindCase.BOOL_VALUE) { it.boolValue }
      JavaType.STRING -> requireKind(value, column, KindCase.STRING_VALUE) { it.stringValue }
      JavaType.BYTE_STRING -> requireKind(value, column, KindCase.BYTES_VALUE) { it.bytesValue }
      JavaType.ENUM -> {
        when (value.kindCase) {
          KindCase.STRING_VALUE ->
            requireNotNull(fd.enumType.findValueByName(value.stringValue)) {
              "Column '$column' value '${value.stringValue}' is not a ${fd.enumType.name} enum name"
            }
          KindCase.INT32_VALUE,
          KindCase.UINT32_VALUE ->
            requireNotNull(fd.enumType.findValueByNumber(asLong(value, column).toIntExact(column))) {
              "Column '$column' has no ${fd.enumType.name} enum value for number"
            }
          else ->
            throw IllegalArgumentException(
              "Column '$column' (${value.kindCase}) cannot map to enum field ${fd.fullName}"
            )
        }
      }
      else ->
        throw IllegalArgumentException(
          "Unsupported leaf field type ${fd.javaType} for ${fd.fullName} (column '$column')"
        )
    }
  }

  /** Coerces an integral / timestamp [ParquetValue] to a [Long]. */
  fun asLong(value: ParquetValue, column: String): Long =
    when (value.kindCase) {
      KindCase.INT32_VALUE -> value.int32Value.toLong()
      KindCase.INT64_VALUE -> value.int64Value
      KindCase.UINT32_VALUE -> value.uint32Value.toLong() and 0xFFFFFFFFL
      KindCase.UINT64_VALUE -> value.uint64Value
      // Timestamps map to microseconds-since-epoch, the unit LabelerInput.timestamp_usec uses.
      KindCase.TIMESTAMP_VALUE ->
        value.timestampValue.seconds * 1_000_000L + value.timestampValue.nanos / 1_000L
      else ->
        throw IllegalArgumentException(
          "Column '$column' (${value.kindCase}) is not an integral value"
        )
    }

  private fun asDouble(value: ParquetValue, column: String): Double =
    when (value.kindCase) {
      KindCase.FLOAT_VALUE -> value.floatValue.toDouble()
      KindCase.DOUBLE_VALUE -> value.doubleValue
      KindCase.INT32_VALUE -> value.int32Value.toDouble()
      KindCase.INT64_VALUE -> value.int64Value.toDouble()
      else ->
        throw IllegalArgumentException(
          "Column '$column' (${value.kindCase}) is not a floating-point value"
        )
    }

  private inline fun <T> requireKind(
    value: ParquetValue,
    column: String,
    expected: KindCase,
    extract: (ParquetValue) -> T,
  ): T {
    require(value.kindCase == expected) {
      "Column '$column' (${value.kindCase}) does not match expected $expected"
    }
    return extract(value)
  }

  private fun Long.toIntExact(column: String): Int {
    require(this in Int.MIN_VALUE.toLong()..Int.MAX_VALUE.toLong()) {
      "Column '$column' value $this overflows a 32-bit int field"
    }
    return toInt()
  }
}
