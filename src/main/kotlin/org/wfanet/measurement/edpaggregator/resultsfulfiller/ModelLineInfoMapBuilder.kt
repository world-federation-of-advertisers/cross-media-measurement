/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.protobuf.Descriptors
import com.google.protobuf.TypeRegistry
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

/**
 * A model line to build [ModelLineInfo] for, decoupled from how its configuration was obtained
 * (e.g. CLI flags, Terraform-generated arguments).
 */
data class ModelLineSource(
  val modelLine: String,
  val populationSpecFileBlobUri: String,
  val eventTemplateDescriptorBlobUri: String,
  val eventTemplateTypeName: String,
)

/**
 * Builds the model line -> [ModelLineInfo] map for a set of [ModelLineSource]s.
 *
 * Model lines are grouped by population-spec blob URI: the VID index depends only on the population
 * spec, not on which event descriptor a model line selects from it, so each group's
 * [PopulationSpec] and [VidIndexMap] are downloaded, parsed, and indexed once and shared by every
 * model line in the group.
 *
 * Population-spec textproto parsing can require descriptors for message types packed in
 * google.protobuf.Any (e.g. event template attributes). Each group gets its own [TypeRegistry],
 * scoped to only the descriptor sets referenced by that group's model lines, so a population spec
 * is never resolved against message types pulled in by unrelated model lines -- which could
 * otherwise paper over a genuine type mismatch or a name collision across groups. Descriptor sets
 * are still cached process-wide by descriptor blob URI (see [EventDescriptorLoader]), so a URI
 * shared across groups is only downloaded and parsed once.
 *
 * @param loadDescriptorSet downloads and parses the descriptor set for a descriptor blob URI.
 * @param loadPopulationSpec downloads and parses the [PopulationSpec] for a population-spec blob
 *   URI, given a [TypeRegistry] for resolving Any-packed attributes.
 * @param buildVidIndexMap builds the [VidIndexMap] for a [PopulationSpec].
 */
class ModelLineInfoMapBuilder(
  private val loadDescriptorSet: suspend (String) -> List<Descriptors.Descriptor>,
  private val loadPopulationSpec: suspend (String, TypeRegistry) -> PopulationSpec,
  private val buildVidIndexMap: suspend (PopulationSpec) -> VidIndexMap,
) {
  /**
   * Loads the [Descriptors.Descriptor] for one model line's event template type, from its
   * descriptor blob URI.
   *
   * Descriptor sets are cached by blob URI: model lines sharing a descriptor blob URI only have it
   * downloaded and parsed once, regardless of how many distinct event template type names are
   * looked up within it.
   */
  private class EventDescriptorLoader(
    private val loadDescriptorSet: suspend (String) -> List<Descriptors.Descriptor>
  ) {
    private val descriptorSetsByUri = mutableMapOf<String, List<Descriptors.Descriptor>>()

    suspend fun load(
      descriptorBlobUri: String,
      eventTemplateTypeName: String,
    ): Descriptors.Descriptor {
      val descriptors =
        descriptorSetsByUri.getOrPut(descriptorBlobUri) { loadDescriptorSet(descriptorBlobUri) }
      return descriptors.firstOrNull { it.fullName == eventTemplateTypeName }
        ?: error("Descriptor not found for type: $eventTemplateTypeName")
    }

    /**
     * Descriptors from the descriptor sets at [descriptorBlobUris], which must have already been
     * loaded via [load].
     */
    fun descriptorsForUris(descriptorBlobUris: Set<String>): List<Descriptors.Descriptor> =
      descriptorBlobUris.flatMap { descriptorSetsByUri.getValue(it) }
  }

  /**
   * Builds the model line -> [ModelLineInfo] map for [modelLines].
   *
   * @throws IllegalArgumentException if [modelLines] contains duplicate model line names.
   */
  suspend fun build(modelLines: List<ModelLineSource>): Map<String, ModelLineInfo> {
    val duplicateModelLines: Set<String> =
      modelLines.groupingBy { it.modelLine }.eachCount().filterValues { it > 1 }.keys
    require(duplicateModelLines.isEmpty()) {
      "Duplicate model line(s) in configuration: $duplicateModelLines"
    }

    val eventDescriptorLoader = EventDescriptorLoader(loadDescriptorSet)
    val eventDescriptorByModelLine: Map<String, Descriptors.Descriptor> =
      modelLines.associate { source ->
        source.modelLine to
          eventDescriptorLoader.load(
            source.eventTemplateDescriptorBlobUri,
            source.eventTemplateTypeName,
          )
      }

    data class PopulationSpecResources(
      val populationSpec: PopulationSpec,
      val vidIndexMap: VidIndexMap,
    )
    val populationSpecResourcesByUri = mutableMapOf<String, PopulationSpecResources>()
    for (group in modelLines.groupBy { it.populationSpecFileBlobUri }.values) {
      val populationSpecBlobUri = group.first().populationSpecFileBlobUri
      val descriptorBlobUris: Set<String> =
        group.mapTo(mutableSetOf()) { it.eventTemplateDescriptorBlobUri }
      val groupTypeRegistry: TypeRegistry =
        TypeRegistry.newBuilder()
          .add(eventDescriptorLoader.descriptorsForUris(descriptorBlobUris))
          .build()
      val populationSpec = loadPopulationSpec(populationSpecBlobUri, groupTypeRegistry)
      populationSpecResourcesByUri[populationSpecBlobUri] =
        PopulationSpecResources(populationSpec, buildVidIndexMap(populationSpec))
    }

    return modelLines.associate { source ->
      val resources = populationSpecResourcesByUri.getValue(source.populationSpecFileBlobUri)
      source.modelLine to
        ModelLineInfo(
          populationSpec = resources.populationSpec,
          vidIndexMap = resources.vidIndexMap,
          eventDescriptor = eventDescriptorByModelLine.getValue(source.modelLine),
          localAlias = null,
        )
    }
  }
}
