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

package org.wfanet.measurement.reporting.tools

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import com.google.protobuf.UnknownFieldSet
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.copy

/** Lossless conversion between BasicReports and the draft compact file format. */
object BasicReportConverter {
  const val FORMAT_VERSION = 1

  /** Packs metadata and metrics without changing their values. */
  fun compact(report: BasicReport): CompactBasicReport {
    val units = MessageTable<ResultGroup.MetricMetadata.ReportingUnitSummary>()
    val windows = MessageTable<ResultGroup.MetricMetadata>()
    val dimensions = MessageTable<ResultGroup.MetricMetadata.DimensionSpecSummary>()
    val filters = MessageTable<ReportingImpressionQualificationFilter>()
    val strings = StringTable()
    val encoder = ColumnEncoder(strings)
    val groups =
      report.resultGroupsList.map { group ->
        CompactBasicReportKt.group {
          header = group.copy { results.clear() }
          rowCount = group.resultsCount
          val windowIds = mutableListOf<Int>()
          val unitIds = mutableListOf<Int>()
          val dimensionIds = mutableListOf<Int>()
          val filterIds = mutableListOf<Int>()
          for (result in group.resultsList) {
            val metadata = result.metadata
            windowIds +=
              if (result.hasMetadata()) {
                windows.index(
                  metadata.copy {
                    clearReportingUnitSummary()
                    clearDimensionSpecSummary()
                    clearFilter()
                  }
                )
              } else 0
            unitIds +=
              if (metadata.hasReportingUnitSummary()) units.index(metadata.reportingUnitSummary)
              else 0
            dimensionIds +=
              if (metadata.hasDimensionSpecSummary())
                dimensions.index(metadata.dimensionSpecSummary)
              else 0
            filterIds += if (metadata.hasFilter()) filters.index(metadata.filter) else 0
          }
          if (windowIds.any { it != 0 }) windowIndices += windowIds
          if (unitIds.any { it != 0 }) reportingUnitIndices += unitIds
          if (dimensionIds.any { it != 0 }) dimensionIndices += dimensionIds
          if (filterIds.any { it != 0 }) filterIndices += filterIds
          val columns =
            encoder.encode(
              group.resultsList.map { if (it.hasMetricSet()) it.metricSet else null },
              MetricSetColumns.getDescriptor(),
            )
          if (columns != null) metrics = MetricSetColumns.parseFrom(columns.toByteString())
          val unknowns = group.resultsList.map { it.unknownFields.toByteString() }
          if (unknowns.any { !it.isEmpty }) resultUnknownFields += unknowns
        }
      }
    return compactBasicReport {
      formatVersion = FORMAT_VERSION
      header = report.copy { resultGroups.clear() }
      reportingUnits += units.values
      this.windows += windows.values
      this.dimensions += dimensions.values
      this.filters += filters.values
      componentNames += strings.values
      this.groups += groups
    }
  }

  /**
   * Reconstructs the original report.
   *
   * @param maxResultRows maximum total rows to allocate from the compact input.
   * @throws IllegalArgumentException for unsupported versions or inconsistent columns.
   */
  fun expand(report: CompactBasicReport, maxResultRows: Int): BasicReport {
    require(maxResultRows >= 0) { "maxResultRows must not be negative" }
    require(report.formatVersion == FORMAT_VERSION) { "Unsupported compact format version" }
    requireNoUnknownFields(report)
    require(report.hasHeader() && report.header.resultGroupsCount == 0) {
      "Header must not contain result groups"
    }
    require(report.componentNamesList.all { it.isNotEmpty() }) {
      "Component dictionary must not contain empty names"
    }
    var totalRows = 0L
    for (group in report.groupsList) {
      require(group.rowCount >= 0) { "Invalid row count" }
      totalRows += group.rowCount.toLong()
      require(totalRows <= maxResultRows) { "Compact report exceeds maxResultRows" }
    }
    for (window in report.windowsList) {
      require(
        !window.hasReportingUnitSummary() &&
          !window.hasDimensionSpecSummary() &&
          !window.hasFilter()
      ) {
        "Window dictionary contains separately indexed metadata"
      }
    }
    return report.header.copy {
      for (group in report.groupsList) {
        requireNoUnknownFields(group)
        require(group.header.resultsCount == 0) { "Group header must not contain results" }
        val rows = group.rowCount
        val windowIds = indices(group.windowIndicesList, rows)
        val unitIds = indices(group.reportingUnitIndicesList, rows)
        val dimensionIds = indices(group.dimensionIndicesList, rows)
        val filterIds = indices(group.filterIndicesList, rows)
        val unknowns = unknownFields(group.resultUnknownFieldsList, rows)
        val metrics =
          if (group.hasMetrics()) {
            decodeColumns(
              group.metrics,
              ResultGroup.MetricSet.getDescriptor(),
              rows,
              report.componentNamesList,
            )
          } else List<Message?>(rows) { null }
        resultGroups +=
          group.header.copy {
            for (i in 0 until rows) {
              val result = ResultGroup.Result.newBuilder().setUnknownFields(unknowns[i])
              if (windowIds[i] != 0) {
                val window = entry(report.windowsList, windowIds[i], "window")
                val metadata =
                  window.copy {
                    if (unitIds[i] != 0)
                      reportingUnitSummary =
                        entry(report.reportingUnitsList, unitIds[i], "reporting unit")
                    if (dimensionIds[i] != 0)
                      dimensionSpecSummary =
                        entry(report.dimensionsList, dimensionIds[i], "dimension")
                    if (filterIds[i] != 0)
                      filter = entry(report.filtersList, filterIds[i], "filter")
                  }
                result.metadata = metadata
              } else {
                require(unitIds[i] == 0 && dimensionIds[i] == 0 && filterIds[i] == 0) {
                  "Absent metadata has dictionary references"
                }
              }
              val metricSet = metrics[i]
              if (metricSet != null)
                result.metricSet = ResultGroup.MetricSet.parseFrom(metricSet.toByteString())
              results += result.build()
            }
          }
      }
    }
  }

  private class MessageTable<T : Message> {
    val values = mutableListOf<T>()
    private val indices = mutableMapOf<T, Int>()

    fun index(message: T): Int =
      indices.getOrPut(message) {
        values += message
        values.size
      }
  }

  private class StringTable {
    val values = mutableListOf<String>()
    private val indices = mutableMapOf<String, Int>()

    fun index(value: String): Int {
      if (value.isEmpty()) return 0
      return indices.getOrPut(value) {
        values += value
        values.size
      }
    }
  }

  // Reflection shares the packing logic; the file schema exposes typed metric columns.
  private class ColumnEncoder(private val strings: StringTable) {
    fun encode(messages: List<Message?>, descriptor: Descriptor): Message? {
      val source = messages.firstOrNull { it != null } ?: return null
      val builder = DynamicMessage.newBuilder(descriptor)
      val presence = DynamicMessage.newBuilder(field(descriptor, "presence").messageType)
      if (messages.all { it != null }) {
        presence.setField(field(presence.descriptorForType, "all_present"), true)
      } else {
        val bitmap = ByteArray((messages.size + 7) / 8)
        for ((i, message) in messages.withIndex()) {
          if (message != null) bitmap[i / 8] = (bitmap[i / 8].toInt() or (1 shl (i % 8))).toByte()
        }
        presence.setField(
          field(presence.descriptorForType, "present_rows"),
          ByteString.copyFrom(bitmap),
        )
      }
      val unknowns = messages.map { it?.unknownFields?.toByteString() ?: ByteString.EMPTY }
      if (unknowns.any { !it.isEmpty })
        presence.setField(field(presence.descriptorForType, "unknown_fields"), unknowns)
      builder.setField(field(descriptor, "presence"), presence.build())
      for (sourceField in source.descriptorForType.fields) {
        val targetField = columnField(descriptor, sourceField)
        when {
          sourceField.javaType == FieldDescriptor.JavaType.MESSAGE -> {
            if (sourceField.isRepeated) {
              val width = messages.maxOf { it?.getRepeatedFieldCount(sourceField) ?: 0 }
              for (position in 0 until width) {
                val children =
                  messages.map {
                    if (it != null && it.getRepeatedFieldCount(sourceField) > position)
                      it.getRepeatedField(sourceField, position) as Message
                    else null
                  }
                builder.addRepeatedField(
                  targetField,
                  requireNotNull(encode(children, targetField.messageType)),
                )
              }
            } else {
              val children =
                messages.map {
                  if (it != null && it.hasField(sourceField)) it.getField(sourceField) as Message
                  else null
                }
              val child = encode(children, targetField.messageType)
              if (child != null) builder.setField(targetField, child)
            }
          }
          sourceField.isRepeated -> {
            val vectors =
              messages.map { message ->
                val values =
                  if (message == null) emptyList<Any>()
                  else message.getField(sourceField) as List<*>
                values.map { value ->
                  if (sourceField.javaType == FieldDescriptor.JavaType.STRING)
                    strings.index(value as String)
                  else requireNotNull(value)
                }
              }
            val vector = encodeVector(vectors, targetField.messageType)
            if (vector != null) builder.setField(targetField, vector)
          }
          else -> {
            require(!sourceField.hasPresence()) { "Unsupported optional scalar metric field" }
            val values =
              messages
                .map { it?.getField(sourceField) ?: sourceField.defaultValue }
                .map {
                  if (sourceField.javaType == FieldDescriptor.JavaType.STRING)
                    strings.index(it as String)
                  else it
                }
            if (values.any { nonzero(it) }) builder.setField(targetField, values)
          }
        }
      }
      return builder.build()
    }

    private fun encodeVector(vectors: List<List<Any>>, descriptor: Descriptor): Message? {
      if (vectors.all { it.isEmpty() }) return null
      val builder = DynamicMessage.newBuilder(descriptor)
      builder.setField(field(descriptor, "values"), vectors.flatten())
      val lengths = vectors.map { it.size }
      if (lengths.all { it == lengths.first() })
        builder.setField(field(descriptor, "fixed_length"), lengths.first())
      else builder.setField(field(descriptor, "lengths"), lengths)
      return builder.build()
    }
  }

  private fun decodeColumns(
    columns: Message,
    descriptor: Descriptor,
    rows: Int,
    strings: List<String>,
  ): List<Message?> {
    requireNoUnknownFields(columns)
    val presenceField = field(columns.descriptorForType, "presence")
    require(columns.hasField(presenceField)) { "Metric column requires message presence" }
    val presence = columns.getField(presenceField) as Message
    requireNoUnknownFields(presence)
    val present = presenceRows(presence, rows)
    val unknowns =
      unknownFields(
        presence.getField(field(presence.descriptorForType, "unknown_fields")) as List<*>,
        rows,
      )
    val builders =
      List(rows) { DynamicMessage.newBuilder(descriptor).setUnknownFields(unknowns[it]) }
    for (sourceField in descriptor.fields) {
      val targetField = columnField(columns.descriptorForType, sourceField)
      when {
        sourceField.javaType == FieldDescriptor.JavaType.MESSAGE -> {
          if (sourceField.isRepeated) {
            for (position in 0 until columns.getRepeatedFieldCount(targetField)) {
              val children =
                decodeColumns(
                  columns.getRepeatedField(targetField, position) as Message,
                  sourceField.messageType,
                  rows,
                  strings,
                )
              for (i in 0 until rows) {
                val child = children[i] ?: continue
                require(builders[i].getRepeatedFieldCount(sourceField) == position) {
                  "Component presence must form a prefix"
                }
                builders[i].addRepeatedField(sourceField, child)
              }
            }
          } else if (columns.hasField(targetField)) {
            val children =
              decodeColumns(
                columns.getField(targetField) as Message,
                sourceField.messageType,
                rows,
                strings,
              )
            for (i in 0 until rows) if (children[i] != null)
              builders[i].setField(sourceField, children[i])
          }
        }
        sourceField.isRepeated -> {
          if (columns.hasField(targetField)) {
            val vectors = decodeVector(columns.getField(targetField) as Message, rows)
            for (i in 0 until rows) {
              val values =
                vectors[i].map {
                  if (sourceField.javaType == FieldDescriptor.JavaType.STRING)
                    componentName(strings, it as Int)
                  else it
                }
              if (values.isNotEmpty()) builders[i].setField(sourceField, values)
            }
          }
        }
        else -> {
          require(!sourceField.hasPresence()) { "Unsupported optional scalar metric field" }
          val values = columns.getField(targetField) as List<*>
          require(values.isEmpty() || values.size == rows) {
            "Scalar column length does not match row count"
          }
          for (i in values.indices) {
            val value =
              if (sourceField.javaType == FieldDescriptor.JavaType.STRING)
                componentName(strings, values[i] as Int)
              else requireNotNull(values[i])
            if (if (value is String) value.isNotEmpty() else nonzero(value)) {
              builders[i].setField(sourceField, value)
            }
          }
        }
      }
    }
    return builders.mapIndexed { i, builder ->
      val value = builder.build()
      if (present[i]) value
      else {
        require(value.allFields.isEmpty() && value.unknownFields.asMap().isEmpty()) {
          "Absent message contains metric values"
        }
        null
      }
    }
  }

  private fun decodeVector(column: Message, rows: Int): List<List<Any>> {
    requireNoUnknownFields(column)
    val descriptor = column.descriptorForType
    val values = column.getField(field(descriptor, "values")) as List<*>
    val lengths = column.getField(field(descriptor, "lengths")) as List<*>
    val fixedField = field(descriptor, "fixed_length")
    val sizes =
      if (column.hasField(fixedField)) {
        require(lengths.isEmpty()) { "Vector cannot specify both fixed and variable lengths" }
        val length = column.getField(fixedField) as Int
        require(length >= 0) { "Invalid vector length" }
        List(rows) { length }
      } else {
        require(lengths.size == rows) { "Vector lengths do not match row count" }
        lengths.map {
          (it as Int).also { length -> require(length >= 0) { "Invalid vector length" } }
        }
      }
    require(sizes.sumOf { it.toLong() } == values.size.toLong()) {
      "Vector values do not match lengths"
    }
    var offset = 0
    return sizes.map { length ->
      values.subList(offset, offset + length).map { requireNotNull(it) }.also { offset += length }
    }
  }

  private fun presenceRows(presence: Message, rows: Int): List<Boolean> {
    val descriptor = presence.descriptorForType
    val selected = presence.getOneofFieldDescriptor(descriptor.oneofs.single())
    return when (selected?.name) {
      "all_present" -> {
        require(presence.getField(selected) == true) { "all_present must be true" }
        List(rows) { true }
      }
      "present_rows" -> {
        val bitmap = presence.getField(selected) as ByteString
        require(bitmap.size() == (rows + 7) / 8) {
          "Presence bitmap length does not match row count"
        }
        for (i in rows until bitmap.size() * 8) require(
          (bitmap.byteAt(i / 8).toInt() and (1 shl (i % 8))) == 0
        ) {
          "Presence bitmap has trailing bits set"
        }
        List(rows) { (bitmap.byteAt(it / 8).toInt() and (1 shl (it % 8))) != 0 }
      }
      else -> throw IllegalArgumentException("Missing message presence")
    }
  }

  private fun unknownFields(values: List<*>, rows: Int): List<UnknownFieldSet> {
    require(values.isEmpty() || values.size == rows) {
      "Unknown-field column length does not match row count"
    }
    return List(rows) {
      if (values.isEmpty()) UnknownFieldSet.getDefaultInstance()
      else UnknownFieldSet.parseFrom(values[it] as ByteString)
    }
  }

  private fun indices(values: List<Int>, rows: Int): List<Int> {
    require(values.isEmpty() || values.size == rows) {
      "Metadata column length does not match row count"
    }
    return if (values.isEmpty()) List(rows) { 0 } else values
  }

  private fun <T> entry(values: List<T>, index: Int, description: String): T {
    require(index > 0 && index <= values.size) { "Invalid $description dictionary index" }
    return values[index - 1]
  }

  private fun componentName(strings: List<String>, index: Int): String =
    if (index == 0) "" else entry(strings, index, "component")

  private fun field(descriptor: Descriptor, name: String): FieldDescriptor =
    requireNotNull(descriptor.findFieldByName(name)) { "Unsupported metric field: $name" }

  private fun columnField(descriptor: Descriptor, source: FieldDescriptor): FieldDescriptor {
    val name =
      when {
        source.javaType == FieldDescriptor.JavaType.MESSAGE -> source.name
        source.javaType == FieldDescriptor.JavaType.STRING -> source.name + "_indices"
        source.javaType == FieldDescriptor.JavaType.LONG ||
          source.javaType == FieldDescriptor.JavaType.FLOAT -> source.name + "_values"
        else -> throw IllegalArgumentException("Unsupported metric type: ${source.javaType}")
      }
    return field(descriptor, name)
  }

  private fun nonzero(value: Any): Boolean =
    when (value) {
      is Float -> value.toRawBits() != 0
      is Long -> value != 0L
      is Int -> value != 0
      else -> throw IllegalArgumentException("Unsupported scalar column")
    }

  private fun requireNoUnknownFields(message: Message) {
    require(message.unknownFields.asMap().isEmpty()) { "Unsupported compact-format fields" }
  }
}
