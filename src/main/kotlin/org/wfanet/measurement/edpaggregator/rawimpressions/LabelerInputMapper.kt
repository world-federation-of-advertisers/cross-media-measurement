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
import org.wfanet.measurement.edpaggregator.v1alpha.AgeRange
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.ParquetValue.KindCase
import org.wfanet.virtualpeople.common.LabelerInput

/**
 * Maps a flat parquet row (column name -> [ParquetValue]) into a [LabelerInput] using the per-(EDP,
 * model line) `labeler_input_field_mapping` (a list of [LabelerInputFieldMapping]).
 *
 * Each entry targets a dot-separated `field_path` into [LabelerInput] and picks a source:
 * * [LabelerInputFieldMapping.getScalar]: 1:1 column -> leaf.
 * * [LabelerInputFieldMapping.getEnumLookup]: read a string column and translate it to a target
 *   enum value name via a lookup table (e.g. "M" -> "GENDER_MALE").
 * * [LabelerInputFieldMapping.getAgeRange]: fill an age `{min_age, max_age}` sub-message from a
 *   single int column, a min/max column pair, or a string bucket looked up in an operator-defined
 *   `bucket_table` (any label, e.g. "18-24" or "65 and over" -> `{min_age, max_age}`).
 * * [LabelerInputFieldMapping.getCompositeIdentity]: set the leaf from the first non-null of
 *   several columns (identity fallback).
 *
 * Scalar path resolution and value coercion are delegated to [ProtoRowProjector] (shared with the
 * event-template and entity-key mappers); this class adds the enum/age/composite sources on top.
 * Columns absent from the row, or whose [ParquetValue] is NULL (`KIND_NOT_SET`), are skipped,
 * leaving the corresponding proto field unset. Field paths and sources are resolved to descriptors
 * once at construction, and required field paths (e.g. `event_id.id`) are enforced there so a
 * misconfigured model line fails fast rather than emitting empty-id impressions.
 *
 * Note: `event_template_field_mapping` is intentionally NOT handled here; those fields target the
 * EDP's event-template message, whose descriptor is only known once the compiled model is loaded
 * (see [org.wfanet.measurement.edpaggregator.vidlabeler.EventMessageMapper]).
 */
class LabelerInputMapper(mappings: List<LabelerInputFieldMapping>) {
  private fun interface Applier {
    fun apply(row: Map<String, ParquetValue>, builder: LabelerInput.Builder)
  }

  private val appliers: List<Applier>
  private val referencedColumnKinds: Map<String, Set<KindCase>>

  init {
    val fieldPaths = mappings.map { it.fieldPath }
    require(fieldPaths.size == fieldPaths.toSet().size) {
      "duplicate field_path in labeler_input_field_mapping: " +
        fieldPaths.groupingBy { it }.eachCount().filterValues { it > 1 }.keys
    }
    for (requiredPath in REQUIRED_FIELD_PATHS) {
      require(requiredPath in fieldPaths) {
        "labeler_input_field_mapping must map required field path '$requiredPath'"
      }
    }
    val columnKinds = LinkedHashMap<String, Set<KindCase>>()
    appliers = mappings.map { buildApplier(it, columnKinds) }
    referencedColumnKinds = columnKinds
  }

  /** Builds a [LabelerInput] from [row], setting only the mapped, non-NULL columns. */
  fun project(row: Map<String, ParquetValue>): LabelerInput {
    val builder = LabelerInput.newBuilder()
    for (applier in appliers) {
      applier.apply(row, builder)
    }
    return builder.build()
  }

  /**
   * Every raw-impression column this mapping reads, mapped to the set of [ParquetValue] kinds the
   * mapper accepts for it. Used for first-file schema-drift validation (see
   * [validateColumnsAgainstSchema]) so a renamed/typo'd column (missing) or a retyped column
   * (incompatible kind) fails loud at file open rather than silently unsetting the target field —
   * or throwing on every row — during labeling.
   */
  fun referencedColumnKinds(): Map<String, Set<KindCase>> = referencedColumnKinds

  companion object {
    // Field paths a model line MUST map. event_id.id feeds EventIdDigestExtractor; without it the
    // digest is over empty bytes and every row collides into one fingerprint.
    private val REQUIRED_FIELD_PATHS = listOf("event_id.id")

    private val ROOT: Descriptor = LabelerInput.getDescriptor()

    private fun buildApplier(
      mapping: LabelerInputFieldMapping,
      columnKinds: MutableMap<String, Set<KindCase>>,
    ): Applier {
      val fieldPath = mapping.fieldPath
      require(fieldPath.isNotEmpty()) { "Empty labeler-input field path" }
      return when (mapping.sourceCase) {
        LabelerInputFieldMapping.SourceCase.SCALAR -> {
          val column = mapping.scalar.column
          val path = ProtoRowProjector.resolvePath(ROOT, fieldPath)
          record(columnKinds, column, ProtoRowProjector.acceptedKinds(path.last()))
          Applier { row, builder ->
            val value = presentValue(row, column) ?: return@Applier
            ProtoRowProjector.setLeaf(
              builder,
              path,
              ProtoRowProjector.convert(value, path.last(), column),
            )
          }
        }
        LabelerInputFieldMapping.SourceCase.ENUM_LOOKUP -> {
          val lookup = mapping.enumLookup
          val column = lookup.column
          val path = ProtoRowProjector.resolvePath(ROOT, fieldPath)
          val leaf = path.last()
          require(leaf.javaType == JavaType.ENUM) {
            "enum_lookup target '$fieldPath' must be an enum field"
          }
          record(columnKinds, column, setOf(KindCase.STRING_VALUE))
          Applier { row, builder ->
            val value = presentValue(row, column) ?: return@Applier
            require(value.kindCase == KindCase.STRING_VALUE) {
              "enum_lookup column '$column' (${value.kindCase}) must be a string"
            }
            val raw = value.stringValue
            if (raw.isEmpty()) return@Applier
            val enumName =
              lookup.lookupTableMap[raw]
                ?: lookup.defaultEnumValue.takeIf { it.isNotEmpty() }
                ?: throw IllegalArgumentException(
                  "Column '$column' value '$raw' has no entry in enum_lookup.lookup_table for " +
                    "'$fieldPath' (and no default_enum_value)"
                )
            val enumValue =
              requireNotNull(leaf.enumType.findValueByName(enumName)) {
                "enum_lookup maps '$raw' to '$enumName', which is not a ${leaf.enumType.name} " +
                  "value ($fieldPath)"
              }
            ProtoRowProjector.setLeaf(builder, path, enumValue)
          }
        }
        LabelerInputFieldMapping.SourceCase.AGE_RANGE -> {
          val ageRange = mapping.ageRange
          val path = ProtoRowProjector.resolvePath(ROOT, fieldPath, allowMessageLeaf = true)
          for (column in ageRangeColumns(ageRange, fieldPath)) {
            record(columnKinds, column, ageColumnKinds(ageRange))
          }
          Applier { row, builder -> applyAgeRange(builder, path, ageRange, row) }
        }
        LabelerInputFieldMapping.SourceCase.COMPOSITE_IDENTITY -> {
          val composite = mapping.compositeIdentity
          require(composite.columnsList.isNotEmpty()) {
            "composite_identity for '$fieldPath' must list at least one column"
          }
          val path = ProtoRowProjector.resolvePath(ROOT, fieldPath)
          for (column in composite.columnsList) {
            record(columnKinds, column, ProtoRowProjector.acceptedKinds(path.last()))
          }
          Applier { row, builder ->
            for (column in composite.columnsList) {
              val value = presentValue(row, column) ?: continue
              ProtoRowProjector.setLeaf(
                builder,
                path,
                ProtoRowProjector.convert(value, path.last(), column),
              )
              return@Applier
            }
          }
        }
        LabelerInputFieldMapping.SourceCase.SOURCE_NOT_SET ->
          throw IllegalArgumentException(
            "labeler_input_field_mapping entry for '$fieldPath' has no source set"
          )
      }
    }

    /**
     * Records [column] -> the kinds accepted for it, intersecting when a column is mapped twice.
     */
    private fun record(
      columnKinds: MutableMap<String, Set<KindCase>>,
      column: String,
      kinds: Set<KindCase>,
    ) {
      columnKinds[column] = columnKinds[column]?.let { it intersect kinds } ?: kinds
    }

    /** The [ParquetValue.KindCase]s accepted for an [ageRange]'s source column(s). */
    private fun ageColumnKinds(ageRange: AgeRange): Set<KindCase> =
      when (ageRange.sourceCase) {
        AgeRange.SourceCase.BUCKET_LOOKUP -> setOf(KindCase.STRING_VALUE)
        else -> ProtoRowProjector.INTEGRAL_KINDS
      }

    /** The row's [ParquetValue] for [column], or null when the column is absent or SQL NULL. */
    private fun presentValue(row: Map<String, ParquetValue>, column: String): ParquetValue? =
      row[column]?.takeIf { it.kindCase != KindCase.KIND_NOT_SET }

    private fun ageRangeColumns(ageRange: AgeRange, fieldPath: String): List<String> =
      when (ageRange.sourceCase) {
        AgeRange.SourceCase.SINGLE_AGE_COLUMN -> listOf(ageRange.singleAgeColumn)
        AgeRange.SourceCase.MIN_MAX_COLUMNS ->
          listOf(ageRange.minMaxColumns.minAgeColumn, ageRange.minMaxColumns.maxAgeColumn)
        AgeRange.SourceCase.BUCKET_LOOKUP -> listOf(ageRange.bucketLookup.column)
        AgeRange.SourceCase.SOURCE_NOT_SET ->
          throw IllegalArgumentException("age_range for '$fieldPath' has no source set")
      }

    private fun applyAgeRange(
      builder: LabelerInput.Builder,
      path: List<FieldDescriptor>,
      ageRange: AgeRange,
      row: Map<String, ParquetValue>,
    ) {
      val (minAge, maxAge) =
        when (ageRange.sourceCase) {
          AgeRange.SourceCase.SINGLE_AGE_COLUMN -> {
            val column = ageRange.singleAgeColumn
            val value = presentValue(row, column) ?: return
            val age = age(value, column)
            age to age
          }
          AgeRange.SourceCase.MIN_MAX_COLUMNS -> {
            val minColumn = ageRange.minMaxColumns.minAgeColumn
            val maxColumn = ageRange.minMaxColumns.maxAgeColumn
            val minValue = presentValue(row, minColumn)
            val maxValue = presentValue(row, maxColumn)
            if (minValue == null && maxValue == null) return
            val min = minValue?.let { age(it, minColumn) } ?: 0
            val max = maxValue?.let { age(it, maxColumn) } ?: 0
            min to max
          }
          AgeRange.SourceCase.BUCKET_LOOKUP -> {
            val lookup = ageRange.bucketLookup
            val value = presentValue(row, lookup.column) ?: return
            require(value.kindCase == KindCase.STRING_VALUE) {
              "age bucket column '${lookup.column}' (${value.kindCase}) must be a string"
            }
            val key = value.stringValue
            if (key.isEmpty()) return
            val bucket =
              lookup.bucketTableMap[key]
                ?: throw IllegalArgumentException(
                  "Column '${lookup.column}' value '$key' has no entry in " +
                    "age_range.bucket_lookup.bucket_table"
                )
            require(bucket.minAge >= 0 && bucket.maxAge >= 0) {
              "age_range.bucket_lookup bucket '$key' has negative min_age/max_age"
            }
            bucket.minAge to bucket.maxAge
          }
          AgeRange.SourceCase.SOURCE_NOT_SET -> return
        }
      var owner: com.google.protobuf.Message.Builder = builder
      for (i in 0 until path.size - 1) {
        owner = owner.getFieldBuilder(path[i])
      }
      val ageBuilder = owner.getFieldBuilder(path.last())
      val ageDescriptor = ageBuilder.descriptorForType
      ageBuilder.setField(requireField(ageDescriptor, "min_age"), minAge)
      ageBuilder.setField(requireField(ageDescriptor, "max_age"), maxAge)
      owner.setField(path.last(), ageBuilder.build())
    }

    /** Reads a non-negative age from an integral [value], via the shared coercion. */
    private fun age(value: ParquetValue, column: String): Int {
      val longValue = ProtoRowProjector.asLong(value, column)
      require(longValue in 0L..Int.MAX_VALUE.toLong()) {
        "Column '$column' age $longValue is out of range for a uint32 age field"
      }
      return longValue.toInt()
    }

    private fun requireField(descriptor: Descriptor, name: String): FieldDescriptor =
      requireNotNull(descriptor.findFieldByName(name)) {
        "age message ${descriptor.fullName} has no '$name' field"
      }
  }
}
