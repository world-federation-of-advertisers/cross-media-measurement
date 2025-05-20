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
import kotlin.math.floor
import kotlin.math.ceil
import org.wfanet.measurement.common.toLocalDate

// We divide the [0,1) interval to 300 equal pieces and sample from that.
// PBM is opinonated about this.
const val NUM_VID_INTERVALS = 300

/** Wraps utilities to filter and map [PrivacyLandscapes]. */
object LandscapeUtils {
  /** Wraps a landscape to be mapped from and its mapping to another landscape. */
  data class MappingNode(
    val fromLandscape: PrivacyLandscape,
    val mapping: PrivacyLandscapeMapping?,
  )

  fun generateEventTemplateProtosFromDescriptors(
    privacyLandscape: PrivacyLandscape,
    fileDescriptors: Iterable<Descriptors.FileDescriptor>,
  ): List<DynamicMessage> {
    val eventTemplateName = privacyLandscape.eventTemplateName
    val dimensions = privacyLandscape.dimensionsList

    var eventTemplateDescriptor: Descriptors.Descriptor? = null
    for (fileDescriptor in fileDescriptors) {
      val foundDescriptor =
        fileDescriptor.findMessageTypeByName(eventTemplateName.substringAfterLast('.'))
      if (foundDescriptor != null) {
        eventTemplateDescriptor = foundDescriptor
        break
      }
    }

    if (eventTemplateDescriptor == null) {
      throw IllegalArgumentException(
        "Event template '${privacyLandscape.eventTemplateName}' not found in the provided FileDescriptors."
      )
    }

    if (dimensions.isEmpty()) {
      return listOf(DynamicMessage.newBuilder(eventTemplateDescriptor).build())
    }

    val dimensionInfo =
      dimensions
        .map { dimension ->
          val fieldPathParts = dimension.fieldPath.split('.')
          val fieldValues = dimension.fieldValuesList.map { it.enumValue }
          Triple(fieldPathParts, fieldValues, dimension.order)
        }
        .sortedBy { it.third }

    val combinations = mutableListOf<List<String>>()
    fun generateCombinationsRecursive(index: Int, currentCombination: List<String>) {
      if (index == dimensionInfo.size) {
        combinations.add(currentCombination.toList())
        return
      }

      val (_, fieldValues, _) = dimensionInfo[index]
      for (value in fieldValues) {
        val newCombination = currentCombination + value
        generateCombinationsRecursive(index + 1, newCombination)
      }
    }
    generateCombinationsRecursive(0, emptyList())

    val generatedProtos = mutableListOf<DynamicMessage>()
    for (combination in combinations) {
      val messageBuilder = DynamicMessage.newBuilder(eventTemplateDescriptor)
      for (i in dimensionInfo.indices) {
        val (fieldPathParts, _, _) = dimensionInfo[i]
        val value = combination[i]

        var currentBuilder = messageBuilder
        var currentDescriptor: Descriptors.Descriptor = eventTemplateDescriptor
        for (j in 0 until fieldPathParts.size - 1) {
          val fieldName = fieldPathParts[j]
          val fieldDescriptor = currentDescriptor.findFieldByName(fieldName)
              ?: throw IllegalArgumentException("Field '$fieldName' not found in message '${currentDescriptor.fullName}'.")
          val nestedDescriptor = fieldDescriptor.messageType
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

        if (lastFieldDescriptor.type == Descriptors.FieldDescriptor.Type.ENUM) {
          val enumValueDescriptor =
            lastFieldDescriptor.enumType.findValueByName(value)
              ?: throw IllegalArgumentException(
                "Enum value '$value' not found in enum '${lastFieldDescriptor.enumType.fullName}'."
              )
          currentBuilder.setField(lastFieldDescriptor, enumValueDescriptor)
        } else {
          throw IllegalArgumentException(
            "Field '$lastFieldName' is not an enum. Expected enum for dimension."
          )
        }
      }
      generatedProtos.add(messageBuilder.build())
    }

    return generatedProtos
  }

  private fun getPopulationIndicies(
    eventFilter:String,
    privacyLandscape: PrivacyLandscape,
    eventTemplateDescriptors: Iterable<Descriptors.FileDescriptor>,
  ) : List<Int>{
    generateEventTemplateProtosFromDescriptors(privacyLandscape, eventTemplateDescriptors)
    return listOf(1)
  }

  private fun getVidIntervalIndicies(vidSampleStart: Float, vidSampleWidth: Float): List<Int> {
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
   * @returns filtered [PrivacyBucket]s.
   */
  fun getBuckets(
    measurementConsumerId: String,
    eventGroupLandscapeMasks: List<EventGroupLandscapeMask>,
    privacyLandscape: PrivacyLandscape,
    eventTemplateDescriptors: Iterable<Descriptors.FileDescriptor>,
  ): List<PrivacyBucket> {
    val privacyBuckets = mutableListOf<PrivacyBucket>()
    for (eventGroupLandscapeMask in eventGroupLandscapeMasks) {

      val populationIndicies =
        getPopulationIndicies(
          eventGroupLandscapeMask.eventFilter,
          privacyLandscape,
          eventTemplateDescriptors,
        )

      val vidIntervalIndices =
        getVidIntervalIndicies(
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
        for (populationIndex in populationIndicies) {
          for (vidIntervalIndex in vidIntervalIndices) {
            privacyBuckets.add(PrivacyBucket(ledgerRowKey, populationIndex, vidIntervalIndex))
          }
        }
      }
    }
    return privacyBuckets
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
    privacyBuckets: List<PrivacyBucket>,
    privacyLandscapeMapping: PrivacyLandscapeMapping,
    fromPrivacyLandscape: PrivacyLandscape,
    toPrivacyLandscape: PrivacyLandscape,
  ): List<PrivacyBucket> = TODO("uakyol: implement this")
}
