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

/**
 * The `LandscapeProcessor` handles the processing and transformation of `PrivacyLandscape`
 * definitions, primarily for generating and mapping `PrivacyBucket`s.
 *
 * It provides two core functionalities:
 * 1. **Privacy Bucket Generation (`getBuckets`)**: This function creates `PrivacyBucket` instances
 *    by applying filtering criteria (defined by `EventGroupLandscapeMask`s) to a given
 *    `PrivacyLandscape`. It determines the relevant population segments, VID intervals, and
 *    date-specific ledger keys, then combines these to form the resulting buckets.
 * 2. **Privacy Bucket Mapping (`mapBuckets`)**: This function transforms a list of `PrivacyBucket`s
 *    from a source `PrivacyLandscape` to a target `PrivacyLandscape`. It uses a
 *    `PrivacyLandscapeMapping` to translate population indices between the two landscapes, allowing
 *    buckets defined in one structural context to be accurately represented in another.
 */
class LandscapeProcessor {

  // Cache the landscape based event message generation so its not recomputed for same inputs.
  private val landscapeCache = ConcurrentHashMap<LandscapeNode, List<DynamicMessage>>()

  /**
   * Defines the key for caching population index mapping results. It encapsulates the mapping
   * definition itself, and the source and target landscapes.
   */
  private data class PopulationMappingKey(
    val mapping: PrivacyLandscapeMapping,
    val source: PrivacyLandscape,
    val target: PrivacyLandscape,
  )

  /**
   * Caches the computed mappings between population indices of different privacy landscapes.
   *
   * The key, [PopulationMappingKey], specifies the context of the mapping:
   * - `mapping`: The [PrivacyLandscapeMapping] proto defining the rules for how field values in
   *   dimensions are translated.
   * - `sourceLandscape`: The [PrivacyLandscape] from which indices are being mapped.
   * - `targetLandscape`: The [PrivacyLandscape] to which indices are being mapped.
   *
   * The value is a `Map<Int, Set<Int>>`. In this map:
   * - The key (`Int`) represents a **population index from the `source Landscape`**. A population
   *   index is a unique identifier for a specific combination of dimension values within that
   *   source landscape e.g : (Age_18_24, Female) or (Age_25_34, Male).
   * - The value (`Set<Int>`) is a set of **population indices within the `target Landscape`**.
   *   These are all the combinations of dimension values in the target landscape that correspond to
   *   the source population index, according to the rules defined in the
   *   `PopulationMappingKey.mapping`.
   */
  private val mappingCache = ConcurrentHashMap<PopulationMappingKey, Map<Int, Set<Int>>>()

  /**
   * Represents a specific configuration of a privacy landscape tied to its concrete event message
   * structure.
   *
   * @property landscape The [PrivacyLandscape] proto message. This defines the dimensions, field
   *   values, and the name of the event template (e.g., `full.package.EventMessage`) that this
   *   landscape describes.
   * @property eventTemplateDescriptor The Protobuf [Descriptors.Descriptor] for the specific event
   *   message type referenced by `landscape.eventTemplateName`. This is the top level Event
   *   meassage e.g.
   *   /src/main/proto/wfa/measurement/api/v2alpha/event_templates/testing/test_event.proto
   */
  data class LandscapeNode(
    val landscape: PrivacyLandscape,
    val eventTemplateDescriptor: Descriptors.Descriptor,
  )

  /** Wraps a landscape to be mapped from and its mapping to another landscape. */
  data class MappingNode(val source: PrivacyLandscape, val mapping: PrivacyLandscapeMapping?)

  /**
   * Generates a list of [PrivacyBucket]s based on filtering criteria applied to a
   * [PrivacyLandscape].
   *
   * This function takes a set of event group masks, each specifying an event filter, VID sampling
   * parameters, and a date range. For each mask, it determines the matching population segments
   * (indices) within the `privacyLandscape`, the relevant VID intervals, and the ledger row keys
   * (derived from date ranges). It then creates `PrivacyBucket`s representing the Cartesian product
   * of these determined elements.
   *
   * @param eventDataProviderName The name of the Event Data Provider to whom the buckets belong.
   * @param measurementConsumerName The name of the Measurement Consumer to whom the buckets belong.
   * @param eventGroupLandscapeMasks A list of [EventGroupLandscapeMask]s, each defining filtering
   *   criteria for an event group.
   * @param privacyLandscape The [PrivacyLandscape] defining the population segments.
   * @param eventTemplateDescriptor The descriptor for the top-level event message template
   *   referenced in the `privacyLandscape`.
   * @return A list of [PrivacyBucket]s derived from applying the masks to the landscape.
   */
  fun getBuckets(
    eventDataProviderName: String,
    measurementConsumerName: String,
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
          eventDataProviderName,
          measurementConsumerName,
          eventGroupLandscapeMask.eventGroupId,
          eventGroupLandscapeMask.dateRange,
        )

      // Create PrivacyBuckets by taking the Cartesian product
      for (ledgerRowKey in ledgerRowKeys) {
        for (populationIndex in populationIndices) {
          for (vidIntervalIndex in vidIntervalIndices) {
            privacyBuckets.add(
              PrivacyBucket(ledgerRowKey, BucketIndex(populationIndex, vidIntervalIndex))
            )
          }
        }
      }
    }
    return privacyBuckets
  }

  /**
   * Maps a list of [PrivacyBucket]s from a source [PrivacyLandscape] to a target
   * [PrivacyLandscape].
   *
   * This function uses a [PrivacyLandscapeMapping] to determine how population indices in the
   * `source` landscape correspond to population indices in the `to` landscape. Each input bucket is
   * then transformed into one or more output buckets based on this mapping, preserving the original
   * row key and VID interval index.
   *
   * @param buckets The list of [PrivacyBucket]s to be mapped.
   * @param mapping The [PrivacyLandscapeMapping] defining the transformation rules between the
   *   source and target landscapes.
   * @param source The source [PrivacyLandscape] from which the buckets originate.
   * @param target The target [PrivacyLandscape] to which the buckets will be mapped.
   * @return A new list of [PrivacyBucket]s mapped to the target landscape.
   * @throws IllegalStateException if a population index from an input bucket cannot be found in the
   *   computed population index mapping (which indicates an inconsistency).
   */
  fun mapBuckets(
    buckets: List<PrivacyBucket>,
    mapping: PrivacyLandscapeMapping,
    source: PrivacyLandscape,
    target: PrivacyLandscape,
  ): List<PrivacyBucket> {
    val mappedPrivacyBuckets = mutableListOf<PrivacyBucket>()
    val populationIndexMapping = getPopulationIndexMapping(mapping, source, target)

    for (bucket in buckets) {
      val mappedPopulationIndices =
        populationIndexMapping.getOrElse(bucket.bucketIndex.populationIndex) {
          throw IllegalStateException(
            "Population index '${bucket.bucketIndex.populationIndex}' not found in mapping. This should never happen."
          )
        }
      for (mappedPopulationIndex in mappedPopulationIndices) {
        mappedPrivacyBuckets.add(
          PrivacyBucket(
            bucket.rowKey,
            BucketIndex(mappedPopulationIndex, bucket.bucketIndex.vidIntervalIndex),
          )
        )
      }
    }
    return mappedPrivacyBuckets
  }

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

  private fun getFieldValueCombinations(
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

  private fun generateEventProtosFromDescriptors(
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
          "Event template '${landscapeNode.landscape.eventTemplateName}' not found in the Descriptor."
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
      generateEventProtosFromDescriptors(LandscapeNode(privacyLandscape, eventTemplateDescriptor))
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
    eventDataProviderName: String,
    measurementConsumerName: String,
    eventGroupId: String,
    dateRange: DateRange,
  ): List<LedgerRowKey> {
    val startDate = dateRange.start.toLocalDate()
    val endDateExclusive = dateRange.endExclusive.toLocalDate()

    return generateSequence(startDate) { it.plusDays(1) }
      .takeWhile { it < endDateExclusive }
      .map { LedgerRowKey(eventDataProviderName, measurementConsumerName, eventGroupId, it) }
      .toList()
  }

  private fun getIndexToFieldMapping(privacyLandscape: PrivacyLandscape): Map<Int, Set<String>> {
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

  /**
   * Retrieves a list of unique target dimension indices based on a source field value and mappings.
   *
   * This function takes a [sourceFieldValue] and uses the provided [mapping] to find associated
   * target field values. For each found target field value, it then looks up its corresponding
   * indices in the [targetLandscapeIndexMapping].
   *
   * @param sourceFieldValue The specific source field value to find mappings for.
   * @param mapping A map where keys are source field values and values are lists of mapped target
   *   field values.
   * @param targetLandscapeIndexMapping A map where keys are target field values and values are sets
   *   of their corresponding landscape indices.
   * @return A [List] of unique integer indices from the target landscape, or an empty list if no
   *   mappings or indices are found for the given [sourceFieldValue].
   */
  private fun getDimIndices(
    sourceFieldValue: String,
    mapping: Map<String, List<String>>,
    targetLandscapeIndexMapping: Map<String, Set<Int>>,
  ): List<Int> {
    val targetIndices = mutableListOf<Int>()

    // Find all "target" field values associated with the given "sourceFieldValue"
    val mappedTargetValues = mapping[sourceFieldValue]

    if (mappedTargetValues != null) {
      for (targetValue in mappedTargetValues) {
        // For each "target" field value, find the corresponding indices in toLandscapeIndexMapping
        val indices = targetLandscapeIndexMapping[targetValue]
        if (indices != null) {
          targetIndices.addAll(indices)
        }
      }
    }

    return targetIndices.distinct() // Return only unique indices
  }

  /**
   * Transforms a [PrivacyLandscapeMapping] into a flattened map of source-to-target field values.
   *
   * This function iterates through the provided `privacyLandscapeMapping` to create a new map where
   * each key represents a unique combination of a source dimension's field path and its enum value.
   *
   * @param privacyLandscapeMapping The privacy landscape mapping data to be transformed.
   * @return A [Map] where keys are concatenated source field paths and enum values, and values are
   *   lists of concatenated target field paths and enum values.
   * @throws IllegalStateException if a duplicate source key is encountered, indicating an error in
   *   the `privacyLandscapeMapping` data.
   */
  private fun getMapping(
    privacyLandscapeMapping: PrivacyLandscapeMapping
  ): Map<String, List<String>> {
    val mappingResult = mutableMapOf<String, MutableList<String>>()

    for (dimensionMapping in privacyLandscapeMapping.mappingsList) {
      val sourceFieldPath = dimensionMapping.sourceDimensionFieldPath
      val targetFieldPath = dimensionMapping.targetDimensionFieldPath

      for (fieldValueMapping in dimensionMapping.fieldValueMappingsList) {
        val sourceFieldValue = fieldValueMapping.sourceFieldValue
        val targetFieldValuesList = fieldValueMapping.targetFieldValuesList

        val sourceKey = "$sourceFieldPath.${sourceFieldValue.enumValue}"

        val targetValues =
          targetFieldValuesList.map { targetFieldValue ->
            "$targetFieldPath.${targetFieldValue.enumValue}"
          }

        check(!mappingResult.containsKey(sourceKey)) {
          "Source key '$sourceKey' already exists in the landscape mapping. This should never happen."
        }
        mappingResult[sourceKey] = targetValues.toMutableList()
      }
    }

    return mappingResult
  }

  /**
   * Computes a mapping of population indices from one privacy landscape to another.
   *
   * @param privacyLandscapeMapping The mapping between dimensions and field values of the two
   *   privacy landscapes.
   * @param source The [PrivacyLandscape] whose indices are being mapped.
   * @param target The [PrivacyLandscape] to which the indices are mapped.
   * @return A map where each key is an index from the source privacy landscape, and the value is a
   *   set of indices in the target privacy landscape that correspond to the key.
   */
  private fun getPopulationIndexMapping(
    privacyLandscapeMapping: PrivacyLandscapeMapping,
    source: PrivacyLandscape,
    target: PrivacyLandscape,
  ): Map<Int, Set<Int>> {
    val key = PopulationMappingKey(privacyLandscapeMapping, source, target)
    return mappingCache.getOrPut(key) {
      val populationIndexMapping = mutableMapOf<Int, MutableSet<Int>>()

      val sourceLandscapeFieldMapping: Map<Int, Set<String>> = getIndexToFieldMapping(source)
      val targetLandscapeIndexMapping: Map<String, Set<Int>> = getFieldToIndexMapping(target)
      val mapping: Map<String, List<String>> = getMapping(privacyLandscapeMapping)

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
      for ((index, sourceFieldValues) in sourceLandscapeFieldMapping.entries) {
        val allTargetDimIndicies = mutableListOf<List<Int>>()
        for (sourceFieldValue in sourceFieldValues) {
          allTargetDimIndicies +=
            getDimIndices(sourceFieldValue, mapping, targetLandscapeIndexMapping)
        }

        val intersection =
          allTargetDimIndicies.reduce { acc, list -> acc.intersect(list.toSet()).toList() }

        val valueSet: MutableSet<Int> = populationIndexMapping.getOrPut(index) { mutableSetOf() }
        valueSet.addAll(intersection)
      }
      populationIndexMapping
    }
  }
}
