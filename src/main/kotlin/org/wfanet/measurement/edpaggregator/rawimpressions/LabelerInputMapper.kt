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

package org.wfanet.measurement.edpaggregator.rawimpressions

import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.virtualpeople.common.LabelerInput

/**
 * Maps a flat parquet row (column name -> [ParquetValue]) into a [LabelerInput] using the per-(EDP,
 * model line) mapping carried on `SubpoolAssignerParams.labeler_input_field_mapping`.
 *
 * The mapping is `labeler-input field path -> raw-impression column name`. A field path is a
 * dot-separated path of proto field names into [LabelerInput], e.g.:
 * * `timestamp_usec`
 * * `event_id.id`
 * * `profile_info.email_user_info.user_id`
 * * `geo.country_id`
 *
 * Intermediate path segments must be singular (non-repeated) message fields; the final segment must
 * be a scalar or enum field. Columns absent from the row, or whose [ParquetValue] is NULL
 * (`KIND_NOT_SET`), are skipped, leaving the corresponding proto field unset.
 *
 * Stateless and therefore safe to share across threads. Field paths are resolved to descriptors
 * once at construction (via [ProtoRowProjector]), so per-row mapping does no string-keyed
 * descriptor lookups.
 *
 * Note: `SubpoolAssignerParams.event_template_field_mapping` is intentionally NOT handled here.
 * Those fields target the model's event-template message (the EDP's event proto); event-template
 * projection is the [EventMessageMapper]'s responsibility once the event descriptor is loaded.
 */
// TODO(world-federation-of-advertisers/cross-media-measurement#3992): support enum lookup (e.g.
//   gender M/F -> MALE/FEMALE) and age-range mapping (single int, min/max columns, or string
//   buckets) by reshaping the config from map<String, String> to repeated LabelerInputFieldMapping.
// TODO(world-federation-of-advertisers/cross-media-measurement#3993): identity composition (first
//   non-null of several columns), required-field validation (e.g. event_id.id), and startup
//   schema-drift detection against the EDP parquet schema. Depends on #3992's reshaped config.
class LabelerInputMapper(labelerInputFieldMapping: Map<String, String>) {
  private val bindings: List<ProtoRowProjector.Binding> =
    ProtoRowProjector.bind(LabelerInput.getDescriptor(), labelerInputFieldMapping)

  /** Builds a [LabelerInput] from [row], setting only the mapped, non-NULL columns. */
  fun project(row: Map<String, ParquetValue>): LabelerInput {
    val builder = LabelerInput.newBuilder()
    ProtoRowProjector.setMappedFields(builder, bindings, row)
    return builder.build()
  }
}
