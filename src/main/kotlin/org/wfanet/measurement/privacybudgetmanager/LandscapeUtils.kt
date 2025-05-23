/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.privacybudgetmanager

import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.ceil
import kotlin.math.floor
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters.compileProgram
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters.matches

// We divide the [0,1) interval to 300 equal pieces and sample from that.
// PBM is opinionated about this.
const val NUM_VID_INTERVALS = 300

/** Wraps utilities to filter and map [PrivacyLandscapes]. */
object LandscapeUtils {

  // Cache the landscape based event message generation so its not recomputed for same inputs.
  private val landscapeCache = ConcurrentHashMap<LandscapeNode, List<DynamicMessage>>()

  // Cache the Mapping action so that its not recomputed for same inputs.
  private val mappingCache =
    ConcurrentHashMap<
      Triple<PrivacyLandscapeMapping, PrivacyLandscape, PrivacyLandscape>,
      Map<Int, Set<Int>>,
    >()

  /** Wraps the PrivacyLandscape along with the event template descriptor it references */
  data class LandscapeNode(
    val landscape: PrivacyLandscape,
    val eventTemplateDescriptor: Descriptors.Descriptor,
  )

  /** Wraps a landscape to be mapped from and its mapping to another landscape. */
  data class MappingNode(
    val fromLandscape: PrivacyLandscape,
    val mapping: PrivacyLandscapeMapping?,
  )

  private fun navigateAndSetEnumValue(
    messageBuilder: DynamicMessage.Builder,
    eventTemplateDescriptor: Descriptors.Descriptor,
    fieldPathParts: List<String>,
    enumValue: String,
  ) {
    var currentBuilder = messageBuilder
    var currentDescriptor: Descriptors.Descriptor = eventTemplateDescriptor

    // Navigate to the leaf on the field path
    for (j in 0 until fieldPathParts.size - 1) {
      val fieldName = fieldPathParts[j]

      val fieldDescriptor =
        currentDescriptor.findFieldByName(fieldName)
          ?: throw IllegalArgumentException(
            "Field '$fieldName' not found in message '${currentDescriptor.fullName}'."
          )
      val nestedDescriptor =
        fieldDescriptor.messageType
          ?: throw IllegalArgumentException("Field '$fieldName' is not a message.")

      val nextBuilder = currentBuilder.getFieldBuilder(fieldDescriptor)
      currentBuilder = nextBuilder as DynamicMessage.Builder
      currentDescriptor = nestedDescriptor
    }

    val lastFieldName = fieldPathParts.last()
    val lastFieldDescriptor =
      currentDescriptor.findFieldByName(lastFieldName)
        ?: throw IllegalArgumentException(
          "Field '$lastFieldName' not found in message '${currentDescriptor.fullName}'."
        )

    // Set the leaf to the desired enum.
    if (lastFieldDescriptor.type == Descriptors.FieldDescriptor.Type.ENUM) {
      val enumValueDescriptor =
        lastFieldDescriptor.enumType.findValueByName(enumValue)
          ?: throw IllegalArgumentException(
            "Enum value '$enumValue' not found in enum '${lastFieldDescriptor.enumType.fullName}'."
          )
      currentBuilder.setField(lastFieldDescriptor, enumValueDescriptor)
    } else {
      throw IllegalArgumentException(
        "Field '$lastFieldName' is not an enum. Expected enum for dimension."
      )
    }
  }

  fun getFieldValueCombinations(
    dimensions: List<PrivacyLandscape.Dimension>,
    withFieldPath: Boolean,
  ): List<List<String>> {
    val dimensionInfo =
      dimensions
        .map { dimension ->
          val fieldPath = dimension.fieldPath
          val fieldValues = dimension.fieldValuesList.map { it.enumValue }
          Triple(fieldPath, fieldValues, dimension.order)
        }
        .sortedBy { it.third }

    val combinations = mutableListOf<List<String>>()
    fun generateCombinationsRecursive(index: Int, currentCombination: List<String>) {
      if (index == dimensionInfo.size) {
        combinations.add(currentCombination.toList())
        return
      }

      val (fieldPath, fieldValues, _) = dimensionInfo[index]
      for (value in fieldValues) {
        val valueToAdd =
          if (withFieldPath) {
            "${fieldPath}.$value"
          } else {
            value
          }
        val newCombination = currentCombination + valueToAdd
        generateCombinationsRecursive(index + 1, newCombination)
      }
    }
    generateCombinationsRecursive(0, emptyList())
    return combinations
  }

  private fun generateEventTemplateProtosFromDescriptors(
    landscapeNode: LandscapeNode
  ): List<DynamicMessage> {
    return landscapeCache.getOrPut(landscapeNode) {
      val eventTemplateName = landscapeNode.landscape.eventTemplateName
      val dimensions = landscapeNode.landscape.dimensionsList

      val eventTemplateDescriptor: Descriptors.Descriptor? =
        landscapeNode.eventTemplateDescriptor.file.findMessageTypeByName(
          eventTemplateName.substringAfterLast('.')
        )

      if (eventTemplateDescriptor == null) {
        throw IllegalArgumentException(
          "Event template '${landscapeNode.landscape.eventTemplateName}' not found in the fileDescriptor."
        )
      }

      val dimensionInfo =
        dimensions
          .map { dimension ->
            val fieldPathParts = dimension.fieldPath.split('.')
            val fieldValues = dimension.fieldValuesList.map { it.enumValue }
            Triple(fieldPathParts, fieldValues, dimension.order)
          }
          .sortedBy { it.third }

      val combinations = getFieldValueCombinations(dimensions, false)
      val generatedProtos = mutableListOf<DynamicMessage>()
      for (combination in combinations) {
        val messageBuilder = DynamicMessage.newBuilder(eventTemplateDescriptor)
        for (i in dimensionInfo.indices) {
          val (fieldPathParts, _, _) = dimensionInfo[i]
          val value = combination[i]
          navigateAndSetEnumValue(messageBuilder, eventTemplateDescriptor, fieldPathParts, value)
        }
        generatedProtos.add(messageBuilder.build())
      }

      generatedProtos
    }
  }

  private fun getPopulationIndices(
    eventFilter: String,
    privacyLandscape: PrivacyLandscape,
    eventTemplateDescriptor: Descriptors.Descriptor,
  ): List<Int> {
    val generatedProtos =
      generateEventTemplateProtosFromDescriptors(
        LandscapeNode(privacyLandscape, eventTemplateDescriptor)
      )
    val operativeFields = privacyLandscape.dimensionsList.map { it.fieldPath }.toSet()
    val program = compileProgram(eventTemplateDescriptor, eventFilter, operativeFields)

    return generatedProtos
      .withIndex()
      .filter { (index, generatedProto) ->
        if (matches(generatedProto, program)) {
          true
        } else {
          false
        }
      }
      .map { (index, _) -> index }
      .toList()
  }

  private fun getVidIntervalIndices(vidSampleStart: Float, vidSampleWidth: Float): List<Int> {
    val vidSampleEnd = vidSampleStart + vidSampleWidth

    val startIndex = floor(vidSampleStart * NUM_VID_INTERVALS).toInt()
    val endIndexExclusive = ceil(vidSampleEnd * NUM_VID_INTERVALS).toInt()
    return (startIndex until endIndexExclusive).toList()
  }

  private fun getLedgerRowKeys(
    measurementConsumerId: String,
    eventGroupId: String,
    dateRange: DateRange,
  ): List<LedgerRowKey> {
    val startDate = dateRange.start.toLocalDate()
    val endDateExclusive = dateRange.endExclusive.toLocalDate()

    return generateSequence(startDate) { it.plusDays(1) }
      .takeWhile { it < endDateExclusive }
      .map { LedgerRowKey(measurementConsumerId, eventGroupId, it) }
      .toList()
  }

  /**
   * Filters a list of [PrivacyBucket]s given a [privacyLandscape] and a [eventGroupLandscapeMask]
   * mask to filter it.
   *
   * @param measurementConsumerId: Specifies the Measurement Consumer buckets belong to.
   * @param eventGroupLandscapeMasks: Specifies the filters for each event group.
   * @param privacyLandscape: The landscape to be filtered.
   * @param eventTemplateDescriptor: Event template descriptor for the top level event message.
   * @returns filtered [PrivacyBucket]s.
   */
  fun getBuckets(
    measurementConsumerId: String,
    eventGroupLandscapeMasks: List<EventGroupLandscapeMask>,
    privacyLandscape: PrivacyLandscape,
    eventTemplateDescriptor: Descriptors.Descriptor,
  ): List<PrivacyBucket> {
    val privacyBuckets = mutableListOf<PrivacyBucket>()
    for (eventGroupLandscapeMask in eventGroupLandscapeMasks) {

      val populationIndices =
        getPopulationIndices(
          eventGroupLandscapeMask.eventFilter,
          privacyLandscape,
          eventTemplateDescriptor,
        )

      val vidIntervalIndices =
        getVidIntervalIndices(
          eventGroupLandscapeMask.vidSampleStart,
          eventGroupLandscapeMask.vidSampleWidth,
        )

      val ledgerRowKeys =
        getLedgerRowKeys(
          measurementConsumerId,
          eventGroupLandscapeMask.eventGroupId,
          eventGroupLandscapeMask.dateRange,
        )

      // Create PrivacyBuckets by taking the Cartesian product
      for (ledgerRowKey in ledgerRowKeys) {
        for (populationIndex in populationIndices) {
          for (vidIntervalIndex in vidIntervalIndices) {
            privacyBuckets.add(PrivacyBucket(ledgerRowKey, populationIndex, vidIntervalIndex))
          }
        }
      }
    }
    return privacyBuckets
  }

  fun getIndexToFieldMapping(privacyLandscape: PrivacyLandscape): Map<Int, Set<String>> {
    val indextoFieldMap = mutableMapOf<Int, MutableSet<String>>()
    val combinations = getFieldValueCombinations(privacyLandscape.dimensionsList, true)
    for ((index, combination) in combinations.withIndex()) {
      for (fieldPath in combination) {
        indextoFieldMap.getOrPut(index) { mutableSetOf() }.add(fieldPath)
      }
    }
    return indextoFieldMap
  }

  fun getFieldToIndexMapping(privacyLandscape: PrivacyLandscape): Map<String, Set<Int>> {
    val fieldToIndexMap = mutableMapOf<String, MutableSet<Int>>()
    val combinations = getFieldValueCombinations(privacyLandscape.dimensionsList, true)
    for ((index, combination) in combinations.withIndex()) {
      for (fieldPath in combination) {
        fieldToIndexMap.getOrPut(fieldPath) { mutableSetOf() }.add(index)
      }
    }
    return fieldToIndexMap
  }

  private fun getDimIndices(
    fromFieldValue: String,
    mapping: Map<String, List<String>>,
    toLandscapeIndexMapping: Map<String, Set<Int>>,
  ): List<Int> {
    val targetIndices = mutableListOf<Int>()

    // Find all "to" field values associated with the given "fromFieldValue"
    val mappedToValues = mapping[fromFieldValue]

    if (mappedToValues != null) {
      for (toValue in mappedToValues) {
        // For each "to" field value, find the corresponding indices in toLandscapeIndexMapping
        val indices = toLandscapeIndexMapping[toValue]
        if (indices != null) {
          targetIndices.addAll(indices)
        }
      }
    }

    return targetIndices.distinct() // Return only unique indices
  }

  private fun getMapping(
    privacyLandscapeMapping: PrivacyLandscapeMapping
  ): Map<String, List<String>> {
    val mappingResult = mutableMapOf<String, MutableList<String>>()

    for (dimensionMapping in privacyLandscapeMapping.mappingsList) {
      val fromFieldPath = dimensionMapping.fromDimensionFieldPath
      val toFieldPath = dimensionMapping.toDimensionFieldPath

      for (fieldValueMapping in dimensionMapping.fieldValueMappingsList) {
        val fromFieldValue = fieldValueMapping.fromFieldValue
        val toFieldValuesList = fieldValueMapping.toFieldValuesList

        val fromKey = "$fromFieldPath.${fromFieldValue.enumValue}"

        val toValues =
          toFieldValuesList.map { toFieldValue -> "$toFieldPath.${toFieldValue.enumValue}" }

        mappingResult.computeIfAbsent(fromKey) { mutableListOf() }.addAll(toValues)
      }
    }

    return mappingResult
  }

  fun getPopulationIndexMapping(
    privacyLandscapeMapping: PrivacyLandscapeMapping,
    from: PrivacyLandscape,
    to: PrivacyLandscape,
  ): Map<Int, Set<Int>> {
    val key = Triple(privacyLandscapeMapping, from, to)
    return mappingCache.getOrPut(key) {
      val populationIndexMapping = mutableMapOf<Int, MutableSet<Int>>()

      val fromLandscapeFieldMapping = getIndexToFieldMapping(from)
      val toLandscapeIndexMapping = getFieldToIndexMapping(to)
      val mapping = getMapping(privacyLandscapeMapping)

      // Algorithm:
      //   For a given population index (e.g. 0), find all the field values that constructs it
      //      e.g. MALE and 18_34
      //   Then find what all these field values map to
      //      e.g. MALE->MALE , 18_34->[18_24, 25_34]
      //   Then find all the indexes these new field values are part of in the new landscape
      //        e.g. MALE - [0,1,2,3]   18_24 - [0,5]   25_34 - [1,6]
      //   Then for each dimension join the lists, so for gender [0,1,2,3] for age [0,5,1,6]
      //   Then take the intersection of these lists [0,1,2,3] intersect [0,5,1,6] = [0,1]
      //      why? Because index 0 in the new landscape has both  MALE and 18_24
      //      and index 1 has both MALE and 25_34
      //   Based on MALE maps to MALE and 18_34 maps to 18_24 25_34. So population index 0
      //    should be mapped to [0,1]

      for ((index, fromFieldValues) in fromLandscapeFieldMapping.entries) {
        val allToDimIndicies = mutableListOf<List<Int>>()
        for (fromFieldValue in fromFieldValues) {
          allToDimIndicies += getDimIndices(fromFieldValue, mapping, toLandscapeIndexMapping)
        }

        val intersection =
          if (allToDimIndicies.isNotEmpty()) {
            allToDimIndicies.reduce { acc, list -> acc.intersect(list.toSet()).toList() }
          } else {
            emptyList()
          }

        val valueSet: MutableSet<Int> = populationIndexMapping.getOrPut(index) { mutableSetOf() }
        valueSet.addAll(intersection)
      }
      populationIndexMapping
    }
  }

  /**
   * Maps a list of [PrivacyBucket]s from an [fromPrivacyLandscape] to an [toPrivacyLandscape]
   *
   * @param privacyBuckets: [PrivacyBucket] list to be mapped.
   * @param privacyLandscapeMapping: Mapping from the [fromPrivacyLandscape] to
   *   [toPrivacyLandscape].
   * @param fromPrivacyLandscape: The landscape to be mapped from.
   * @param toPrivacyLandscape: The landscape to be mapped to.
   * @returns mapped [PrivacyBucket]s.
   */
  fun mapBuckets(
    buckets: List<PrivacyBucket>,
    mapping: PrivacyLandscapeMapping,
    from: PrivacyLandscape,
    to: PrivacyLandscape,
  ): List<PrivacyBucket> {
    val mappedPrivacyBuckets = mutableListOf<PrivacyBucket>()
    val populationIndexMapping = getPopulationIndexMapping(mapping, from, to)

    for (bucket in buckets) {
      val mappedPopulationIndicies =
        populationIndexMapping.getOrElse(bucket.populationIndex) {
          throw IllegalStateException(
            "Population index '${bucket.populationIndex}' not found in mapping. This should never happen."
          )
        }
      for (mappedPopulationIndex in mappedPopulationIndicies) {
        mappedPrivacyBuckets.add(
          PrivacyBucket(bucket.rowKey, mappedPopulationIndex, bucket.vidIntervalIndex)
        )
      }
    }
    return mappedPrivacyBuckets
  }
}
