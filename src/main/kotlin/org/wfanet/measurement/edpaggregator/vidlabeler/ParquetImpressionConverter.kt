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

import com.google.protobuf.Any
import com.google.protobuf.Descriptors
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
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
 * * the `entity_keys` and `event_group_reference_id` are taken from the per-file [FileEntityKeys]
 *   that the reader read from the file's plaintext Parquet footer ("Option Y").
 *
 * One instance is built per (WorkItem, model line) by the runner factory, so [config] is fixed
 * across the instance's [convert] calls; the per-config [LabelerInputMapper] and
 * [EventMessageMapper] are therefore built once and memoized on first use.
 *
 * @property eventDescriptor descriptor of the model line's EventTemplate event message, resolved by
 *   the runner from `ModelLineConfig.event_template_descriptor_blob_uri` + `event_template_type`.
 */
class ParquetImpressionConverter(private val eventDescriptor: Descriptors.Descriptor) :
  ImpressionConverter {
  // The converter is built per (WorkItem, model line), so config is fixed; build the mappers once
  // on first use and reuse them for every row (path resolution is the expensive part).
  @Volatile private var cachedConfig: VidLabelerParams.ModelLineConfig? = null
  private lateinit var labelerInputMapper: LabelerInputMapper
  private lateinit var eventMessageMapper: EventMessageMapper

  @Synchronized
  private fun mappersFor(
    config: VidLabelerParams.ModelLineConfig
  ): Pair<LabelerInputMapper, EventMessageMapper> {
    if (cachedConfig !== config) {
      labelerInputMapper = LabelerInputMapper(config.labelerInputFieldMappingMap)
      eventMessageMapper = EventMessageMapper(eventDescriptor, config.eventTemplateFieldMappingMap)
      cachedConfig = config
    }
    return labelerInputMapper to eventMessageMapper
  }

  override fun convert(
    event: ParquetDigestedEvent,
    config: VidLabelerParams.ModelLineConfig,
    fileEntityKeys: FileEntityKeys,
  ): ConvertedImpression? {
    val (inputMapper, messageMapper) = mappersFor(config)

    val labelerInput = inputMapper.project(event.row)
    val eventTimeMicros: Long = labelerInput.timestampUsec
    val eventMessage = Any.pack(messageMapper.project(event.row))

    // entity_keys + event_group_reference_id come from the file's plaintext footer (read by the
    // reader); FileEntityKeys already enforced that they are present and non-empty.
    return ConvertedImpression(
      labelerInput = labelerInput,
      eventTimeMicros = eventTimeMicros,
      eventGroupReferenceId = fileEntityKeys.eventGroupReferenceId,
      event = eventMessage,
      entityKeys = fileEntityKeys.entityKeys,
    )
  }
}
