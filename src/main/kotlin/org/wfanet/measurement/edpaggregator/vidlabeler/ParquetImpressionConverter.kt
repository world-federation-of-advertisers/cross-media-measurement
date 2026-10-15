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
import java.util.concurrent.ConcurrentHashMap
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetRawEvent
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams

/**
 * Production [ImpressionConverter]: projects a raw-impression Parquet row into the
 * labeling-relevant fields for one model line.
 *
 * Per [convert]:
 * * the [LabelerInput] is projected from the row via the model line's
 *   `labeler_input_field_mapping`;
 * * `event_time_micros` is read from `LabelerInput.timestamp_usec` (mapped via the same mapping);
 * * the output `event` is built as a [com.google.protobuf.Any]-packed [eventDescriptor] message
 *   projected via the model line's `event_template_field_mapping` (empty mapping -> empty event);
 * * the `entity_keys` are read per row from the model line's required/optional entity-key column
 *   mappings (see [EntityKeyMapper]).
 *
 * One instance is built per (WorkItem, model line) by the runner factory, so [config] is fixed
 * across the instance's [convert] calls; the per-config [LabelerInputMapper], [EventMessageMapper],
 * and [EntityKeyMapper] are therefore built once and memoized on first use.
 *
 * @property eventDescriptor descriptor of the model line's EventTemplate event message, resolved by
 *   the runner from `ModelLineConfig.event_template_descriptor_blob_uri` + `event_template_type`.
 */
class ParquetImpressionConverter(
  private val eventDescriptor: Descriptors.Descriptor,
  private val populationAttributeWriter: PopulationAttributeWriter,
) : ImpressionConverter {
  // A converter instance may see more than one ModelLineConfig: the non-memoized path builds ONE
  // converter and calls convert() once per bundled model line's config. Cache the (path-resolved)
  // mappers per config in a lock-free ConcurrentHashMap — computeIfAbsent builds each config's
  // mappers exactly once. The mappers are a pure function of the config value and are all
  // stateless / thread-safe once built, so reuse (even across threads and across value-equal
  // configs) is behavior-identical to rebuilding.
  private val mappersByConfig = ConcurrentHashMap<VidLabelerParams.ModelLineConfig, Mappers>()

  private fun mappersFor(config: VidLabelerParams.ModelLineConfig): Mappers =
    mappersByConfig.computeIfAbsent(config) { cfg ->
      Mappers(
        LabelerInputMapper(cfg.labelerInputFieldMappingList),
        EventMessageMapper(eventDescriptor, cfg.eventTemplateFieldMappingMap),
        EntityKeyMapper(cfg.requiredEntityKeyFieldMappingMap, cfg.optionalEntityKeyFieldMappingMap),
      )
    }

  override fun convert(
    event: ParquetRawEvent,
    config: VidLabelerParams.ModelLineConfig,
  ): ConvertedImpression? {
    val mappers = mappersFor(config)

    val labelerInput = mappers.inputMapper.project(event.row)
    // Deferred, not built: the sink projects the event only after the model has assigned a VID,
    // immediately before PopulationAttributeWriter corrects its population attributes. An event
    // holding the DataProvider's uploaded demographics therefore never exists, and a row dropped
    // upstream is never projected at all.
    val buildEvent = { mappers.messageMapper.project(event.row) }
    // entity_keys are read per row from the mapped columns.
    val entityKeys = mappers.entityKeyMapper.map(event.row)

    return ConvertedImpression(
      labelerInput = labelerInput,
      buildEvent = buildEvent,
      entityKeys = entityKeys,
      // Cannot be applied here: the VID model has not run yet, so the assigned VID -- and with it
      // the subpopulation whose attributes describe this impression -- is unknown until the sink
      // has a LabelerOutput.
      populationAttributeWriter = populationAttributeWriter,
    )
  }

  /** The per-config, path-resolved mappers, built once per config and reused for every row. */
  private class Mappers(
    val inputMapper: LabelerInputMapper,
    val messageMapper: EventMessageMapper,
    val entityKeyMapper: EntityKeyMapper,
  )
}
