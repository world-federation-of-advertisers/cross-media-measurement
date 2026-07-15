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
import com.google.protobuf.DynamicMessage
import org.wfanet.measurement.edpaggregator.rawimpressions.ProtoRowProjector
import org.wfanet.measurement.storage.ParquetValue

/**
 * Projects a flat Parquet row (column name -> [ParquetValue]) onto the model line's EventTemplate
 * event message, using the per-(EDP, model line)
 * `VidLabelerParams.ModelLineConfig.event_template_field_mapping` (`event field path -> column
 * name`).
 *
 * The event message type is only known at run time (it is the EDP's compiled event proto, loaded
 * from a `FileDescriptorSet`), so the message is built as a [DynamicMessage] over [eventDescriptor]
 * — the write-side mirror of `StorageEventReader`'s `DynamicMessage.parseFrom(descriptor, ...)`
 * read side. The converter then packs the result into a `google.protobuf.Any`.
 *
 * Field paths are resolved against [eventDescriptor] once at construction (via
 * [ProtoRowProjector]); per-row [project] does no string-keyed descriptor lookups. An **empty**
 * mapping is valid (some EDPs label without any event-template attributes) and yields an empty
 * message of the event type.
 *
 * Stateless and therefore safe to share across threads.
 */
class EventMessageMapper(
  private val eventDescriptor: Descriptors.Descriptor,
  eventTemplateFieldMapping: Map<String, String>,
) {
  private val bindings: List<ProtoRowProjector.Binding> =
    ProtoRowProjector.bind(eventDescriptor, eventTemplateFieldMapping)

  /** Builds the event [DynamicMessage] from [row], setting only the mapped, non-NULL columns. */
  fun project(row: Map<String, ParquetValue>): DynamicMessage {
    val builder = DynamicMessage.newBuilder(eventDescriptor)
    ProtoRowProjector.setMappedFields(builder, bindings, row)
    return builder.build()
  }
}
