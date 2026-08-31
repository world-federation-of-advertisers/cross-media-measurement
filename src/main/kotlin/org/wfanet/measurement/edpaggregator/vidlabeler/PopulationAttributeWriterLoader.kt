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

import com.google.protobuf.Descriptors
import com.google.protobuf.TypeRegistry
import java.util.concurrent.ConcurrentHashMap
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams

/**
 * Loads and caches the [PopulationAttributeWriter] for a model line.
 *
 * A `PopulationSpec` is multi-MB and its VID ranges are indexed once into the writer, so reading
 * and building it per WorkItem would be wasteful. Cached per (population-spec blob URI,
 * event-template descriptor blob URI, event-template type) for the life of the process.
 *
 * @param readBlob reads a config-storage blob's bytes by URI.
 */
class PopulationAttributeWriterLoader(
  private val readBlob: suspend (blobUri: String) -> ByteArray
) {
  private val cache = ConcurrentHashMap<CacheKey, PopulationAttributeWriter>()

  /**
   * Returns the writer for [config], loading the `PopulationSpec` at
   * [VidLabelerParams.ModelLineConfig.getPopulationSpecBlobUri] on first use. `ResultsFulfiller`
   * loads the same blob for the same model line.
   *
   * @param eventDescriptor the model line's already-resolved EventTemplate event descriptor.
   */
  suspend fun getWriter(
    config: VidLabelerParams.ModelLineConfig,
    eventDescriptor: Descriptors.Descriptor,
  ): PopulationAttributeWriter {
    val blobUri = config.populationSpecBlobUri
    require(blobUri.isNotEmpty()) {
      "population_spec_blob_uri must be set; without it the labeled output would carry the " +
        "DataProvider's uploaded demographics instead of the ones the model assigned"
    }
    // Keyed on the descriptor blob too: two model lines can share a spec URI and event type name
    // while resolving that type from different descriptor blobs, and the writer is bound to the
    // descriptor it was built against.
    val cacheKey =
      CacheKey(blobUri, config.eventTemplateDescriptorBlobUri, config.eventTemplateType)
    cache[cacheKey]?.let {
      return it
    }
    // The spec's SubPopulation.attributes are Any-packed event templates. TypeRegistry.Builder.add
    // registers the type's whole file and recurses through its dependencies, so the event type's
    // transitive closure covers every template the spec can reference.
    val typeRegistry: TypeRegistry = TypeRegistry.newBuilder().add(eventDescriptor).build()
    val populationSpec =
      readBlob(blobUri).inputStream().reader(Charsets.UTF_8).use { reader ->
        parseTextProto(reader, PopulationSpec.getDefaultInstance(), typeRegistry)
      }
    val writer = PopulationAttributeWriter(eventDescriptor, populationSpec)
    return cache.computeIfAbsent(cacheKey) { writer }
  }

  /**
   * Identifies a cached writer by every input it was built from: the population-spec blob URI, the
   * event-template descriptor blob URI, and the event-template type.
   */
  private data class CacheKey(
    val populationSpecBlobUri: String,
    val eventTemplateDescriptorBlobUri: String,
    val eventTemplateType: String,
  )
}
