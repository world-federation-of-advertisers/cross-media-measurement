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
import org.wfanet.virtualpeople.common.LabelerInput

/**
 * Maps a flat parquet row (column name -> [ParquetValue]) into a [LabelerInput] using the per-(EDP,
 * model line) mapping carried on `SubpoolAssignerParams.labeler_input_field_mapping`.
 *
 * The mapping is `labeler-input field path -> raw-impression column name`. A field path is a
 * dot-separated path of proto field names into [LabelerInput], e.g.:
 * * `timestamp_usec`
 * * `event_id.id`
 * * `profile_info.email_user_info.user_id`
 * * `geo.country_id`
 *
 * Intermediate path segments must be singular (non-repeated) message fields; the final segment must
 * be a scalar or enum field. Columns absent from the row, or whose [ParquetValue] is NULL
 * (`KIND_NOT_SET`), are skipped, leaving the corresponding proto field unset.
 *
 * Stateless and therefore safe to share across threads. Field paths are resolved to descriptors
 * once at construction, so per-row mapping does no string-keyed descriptor lookups.
 *
 * Note: `SubpoolAssignerParams.event_template_field_mapping` is intentionally NOT handled here.
 * Those fields target the model's event-template message (the EDP's event proto), whose descriptor
 * is only known once the compiled model is loaded by the labeler; event-template projection is
 * therefore the labeler-loading layer's responsibility.
 */
// TODO(world-federation-of-advertisers/cross-media-measurement#3992): support enum lookup (e.g.
//   gender M/F -> MALE/FEMALE) and age-range mapping (single int, min/max columns, or string
//   buckets) by reshaping the config from map<String, String> to repeated LabelerInputFieldMapping.
// TODO(world-federation-of-advertisers/cross-media-measurement#3993): identity composition (first
//   non-null of several columns), required-field validation (e.g. event_id.id), and startup
//   schema-drift detection against the EDP parquet schema. Depends on #3992's reshaped config.
class LabelerInputMapper(labelerInputFieldMapping: Map<String, String>) {
  private class Binding(val path: List<FieldDescriptor>, val column: String)

  private val bindings: List<Binding> =
    labelerInputFieldMapping.map { (fieldPath, column) ->
      Binding(resolvePath(LabelerInput.getDescriptor(), fieldPath), column)
    }

  /** Builds a [LabelerInput] from [row], setting only the mapped, non-NULL columns. */
  fun project(row: Map<String, ParquetValue>): LabelerInput {
    val builder = LabelerInput.newBuilder()
    for (binding in bindings) {
      val value = row[binding.column] ?: continue
      if (value.kindCase == KindCase.KIND_NOT_SET) continue

      // Walk to the builder that directly owns the leaf field, lazily creating
      // intermediate message sub-builders.
      var owner: Message.Builder = builder
      for (i in 0 until binding.path.size - 1) {
        owner = owner.getFieldBuilder(binding.path[i])
      }
      val leaf = binding.path.last()
      owner.setField(leaf, convert(value, leaf, binding.column))
    }
    return builder.build()
  }

  companion object {
    /** Resolves a dot-separated [fieldPath] against [root] into an ordered descriptor chain. */
    private fun resolvePath(root: Descriptor, fieldPath: String): List<FieldDescriptor> {
      require(fieldPath.isNotEmpty()) { "Empty labeler-input field path" }
      val segments = fieldPath.split('.')
      val path = ArrayList<FieldDescriptor>(segments.size)
      var current: Descriptor = root
      for ((index, name) in segments.withIndex()) {
        val field =
          requireNotNull(current.findFieldByName(name)) {
            "Unknown field '$name' in labeler-input field path '$fieldPath'"
          }
        val isLast = index == segments.size - 1
        if (!isLast) {
          require(field.javaType == JavaType.MESSAGE && !field.isRepeated) {
            "Intermediate segment '$name' in '$fieldPath' must be a singular message field"
          }
          current = field.messageType
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
    private fun convert(value: ParquetValue, fd: FieldDescriptor, column: String): Any {
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
              requireNotNull(
                fd.enumType.findValueByNumber(asLong(value, column).toIntExact(column))
              ) {
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
    private fun asLong(value: ParquetValue, column: String): Long =
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
}
