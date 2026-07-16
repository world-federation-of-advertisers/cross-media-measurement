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

import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetRawEvent
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.virtualpeople.common.LabelerInput

/**
 * Converts a raw-impression Parquet row into the fields needed to label and emit an impression for
 * one model line.
 *
 * This is the seam between the raw-impression reader (which hands out rows keyed by **Parquet
 * column name** — see [ParquetRawEvent]) and the VirtualPeople [LabelerInput]. The conversion is
 * model-line-specific because the column→field mapping lives in
 * [VidLabelerParams.ModelLineConfig.getLabelerInputFieldMappingList].
 *
 * It is injected (rather than implemented inline) so the labeling pipeline can be built and tested
 * independently of the production projection: tests supply a fake, and the production converter
 * ([ParquetImpressionConverter]) is wired in by the runner.
 */
fun interface ImpressionConverter {
  /**
   * Converts [event]'s row for the model line described by [config].
   *
   * Per-row `entity_keys` are read from the model line's required/optional entity-key column
   * mappings via [EntityKeyMapper]; there is no file-level entity-key state (unlike the file-level
   * event date carried by [RawImpressionFileMetadata]).
   *
   * @return the [ConvertedImpression], or `null` to skip this row for this model line.
   */
  fun convert(
    event: ParquetRawEvent,
    config: VidLabelerParams.ModelLineConfig,
  ): ConvertedImpression?
}

/**
 * The labeling-relevant fields extracted from one raw-impression row for one model line.
 *
 * @property labelerInput input fed to the [VidAssigner].
 * @property eventTime impression event time — used for active-window filtering and as the output
 *   `event_time`. A typed `Timestamp` so the converter contract carries the unit instead of a bare
 *   epoch-micros `Long`.
 * @property event the Event payload to embed in the labeled output.
 * @property entityKeys entity keys for this impression, read per-row from the model line's
 *   required/optional entity-key column mappings (see [EntityKeyMapper]); propagated to the labeled
 *   output and the per-blob `BlobDetails.entity_keys` union.
 * @property eventTimeMicros [eventTime] as epoch-micros, carried alongside the typed [eventTime] so
 *   the sink's active-window check does not re-derive it per row via `Timestamps.toMicros`.
 *   Defaults to `Timestamps.toMicros(eventTime)`, so it is always exactly that value.
 */
data class ConvertedImpression(
  val labelerInput: LabelerInput,
  val eventTime: com.google.protobuf.Timestamp,
  val event: com.google.protobuf.Any,
  val entityKeys: List<org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression.EntityKey>,
  val eventTimeMicros: Long = Timestamps.toMicros(eventTime),
) {
  init {
    // Making entityKeys a required parameter only blocks accidental omission at the call site; an
    // explicit `emptyList()` would still silently drop the per-impression `LabeledImpression
    // .entity_keys` and the per-blob `BlobDetails.entity_keys` union. Guard the runtime contract.
    require(entityKeys.isNotEmpty()) {
      "entityKeys must not be empty — every impression must be attributable to at least one entity"
    }
  }
}
