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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.Descriptors
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import org.wfanet.measurement.api.v2alpha.EventTemplates
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator

/**
 * Writes the population attributes of an assigned VID onto that impression's EventTemplate event.
 *
 * A VID model may assign an impression a different demo than the `DataProvider` uploaded (its
 * demographic-correction matrix), and it draws the VID from the pool of the demo it assigned.
 * `ResultsFulfiller` filters and groups reports on the event's population-attribute fields, so
 * those fields have to describe the VID; otherwise an impression is counted in the bucket its VID
 * came from but filtered by the bucket the `DataProvider` uploaded.
 *
 * The mapping is taken from the model line's [PopulationSpec] rather than from the model's
 * `LabelerOutput`: a `SubPopulation` already pairs its VID ranges with the fully-populated event
 * template attributes for that demo, and it is the same artifact `ResultsFulfiller` builds its VID
 * index map from, so the two cannot drift. It also avoids inventing a translation from the model's
 * numeric `[min_age, max_age]` to the market's age-group enum, which nothing declares.
 *
 * Only fields annotated `population_attribute` are written, so event properties that share a
 * template with them (e.g. `media_type` alongside `sex`/`age_group`) keep the value projected from
 * the raw row.
 *
 * Ranges are flattened and sorted at construction, so [apply] is a binary search and no descriptor
 * lookups. Stateless after construction and therefore safe to share across threads.
 */
class PopulationAttributeWriter(
  private val eventDescriptor: Descriptors.Descriptor,
  populationSpec: PopulationSpec,
) {
  /** One population-attribute value to set: which template holds it, which field, what value. */
  private class AttributeValue(
    val templateField: FieldDescriptor,
    val populationField: FieldDescriptor,
    val value: kotlin.Any,
  )

  /** One VID range and the attribute values every VID in it carries. */
  private class VidRangeAttributes(
    val startVid: Long,
    val endVidInclusive: Long,
    val values: List<AttributeValue>,
  )

  private val eventTypeUrl: String = TYPE_URL_PREFIX + eventDescriptor.fullName

  /**
   * Event template type name -> the event field holding it, e.g. "…v1.Common" -> `Event.common`.
   */
  private val templateFieldsByTypeName: Map<String, FieldDescriptor> = buildMap {
    for (field in eventDescriptor.fields) {
      if (field.javaType != FieldDescriptor.JavaType.MESSAGE) continue
      val typeName = field.messageType.fullName
      // PopulationSpec attributes are keyed by template type, so two fields of the same template
      // type make the lookup ambiguous. SyntheticDataGeneration rejects such event messages for
      // the same reason; pick-one-arbitrarily would silently write the wrong field.
      require(!containsKey(typeName)) {
        "${eventDescriptor.fullName} has more than one field of event template type '$typeName' " +
          "('${get(typeName)?.name}' and '${field.name}'); PopulationSpec attributes are keyed by " +
          "template type and cannot be resolved unambiguously"
      }
      put(typeName, field)
    }
  }

  /** Population-attribute fields of each event template, from the `population_attribute` option. */
  private val populationFieldsByTemplate: Map<Descriptors.Descriptor, List<FieldDescriptor>> =
    EventTemplates.getPopulationFieldsByTemplateType(eventDescriptor)

  // VID ranges flattened across subpopulations and sorted by start, searched per impression.
  private val rangeStarts: LongArray
  private val rangeEnds: LongArray
  private val rangeValues: Array<List<AttributeValue>>

  init {
    // Establishes both preconditions this class depends on: the VID ranges are disjoint (so the
    // binary search in [valuesFor] resolves to exactly one subpopulation) and every population
    // attribute is set on every subpopulation (so [attributeValues] never copies a proto3 default
    // over a real value). Same check the Kingdom runs when a Population is registered.
    PopulationSpecValidator.validate(populationSpec, eventDescriptor)

    val flattened =
      populationSpec.subpopulationsList
        .flatMap { subPopulation ->
          val values = attributeValues(subPopulation)
          subPopulation.vidRangesList.map { range ->
            VidRangeAttributes(range.startVid, range.endVidInclusive, values)
          }
        }
        .sortedBy { it.startVid }

    rangeStarts = LongArray(flattened.size) { flattened[it].startVid }
    rangeEnds = LongArray(flattened.size) { flattened[it].endVidInclusive }
    rangeValues = Array(flattened.size) { flattened[it].values }
  }

  /**
   * Returns [eventMessage] packed into a `google.protobuf.Any`, with the population attributes of
   * [vid]'s subpopulation written onto it.
   *
   * Takes the built message rather than a packed `Any` so the event is serialized once here instead
   * of being packed by the converter and re-parsed by this method for every impression.
   *
   * Throws when [vid] falls outside every range in the [PopulationSpec]: the model assigned a VID
   * the spec does not describe, which is a model/spec mismatch. Writing nothing would leave the
   * `DataProvider`'s uploaded demographics on the event and report the wrong bucket.
   */
  fun apply(eventMessage: Message, vid: Long): ProtoAny {
    val values =
      requireNotNull(valuesFor(vid)) {
        "VID $vid is outside every VidRange in the model line's PopulationSpec"
      }
    val builder = eventMessage.toBuilder()
    for (attributeValue in values) {
      builder
        .getFieldBuilder(attributeValue.templateField)
        .setField(attributeValue.populationField, attributeValue.value)
    }
    // Byte-identical to ProtoAny.pack(message); the type_url is fixed by the event descriptor.
    return ProtoAny.newBuilder()
      .setTypeUrl(eventTypeUrl)
      .setValue(builder.build().toByteString())
      .build()
  }

  /** Binary search for the range containing [vid]; `null` when no range does. */
  private fun valuesFor(vid: Long): List<AttributeValue>? {
    var low = 0
    var high = rangeStarts.size - 1
    while (low <= high) {
      val mid = (low + high) ushr 1
      when {
        vid < rangeStarts[mid] -> high = mid - 1
        vid > rangeEnds[mid] -> low = mid + 1
        else -> return rangeValues[mid]
      }
    }
    return null
  }

  /**
   * Reads a subpopulation's population-attribute values, resolved against [eventDescriptor]'s own
   * descriptors so the values can be set directly on the event.
   */
  private fun attributeValues(subPopulation: PopulationSpec.SubPopulation): List<AttributeValue> =
    buildList {
      for (attribute in subPopulation.attributesList) {
        val typeName = attribute.typeUrl.substringAfterLast('/')
        val templateField =
          requireNotNull(templateFieldsByTypeName[typeName]) {
            "PopulationSpec attribute type '$typeName' is not an event template of " +
              eventDescriptor.fullName
          }
        val templateDescriptor = templateField.messageType
        // Parsed against the event's own template descriptor, so enum values below are already the
        // descriptors of the field being set. Every population field is guaranteed present by the
        // PopulationSpecValidator.validate call in init.
        val attributeMessage = DynamicMessage.parseFrom(templateDescriptor, attribute.value)
        for (populationField in populationFieldsByTemplate[templateDescriptor].orEmpty()) {
          add(
            AttributeValue(
              templateField,
              populationField,
              attributeMessage.getField(populationField),
            )
          )
        }
      }
    }

  private companion object {
    private const val TYPE_URL_PREFIX = "type.googleapis.com/"
  }
}
