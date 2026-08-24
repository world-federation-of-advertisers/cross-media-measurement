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

import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetRawEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileMetadata
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
 * @property labelerInput input fed to the [VidAssigner]. Its `timestamp_usec` is the single source
 *   of the impression's event time: the sink filters the active window on it directly and derives
 *   the output `event_time` from it via `Timestamps.fromMicros`, so no separate timestamp field can
 *   drift out of sync with it.
 * @property buildEvent projects this row onto the model line's EventTemplate event, invoked by the
 *   sink only once the assigned VID is known and immediately before
 *   [PopulationAttributeWriter.apply] corrects its population attributes. Deferred rather than
 *   built here so an event carrying the `DataProvider`'s uploaded demographics never exists, and so
 *   rows dropped for being outside the active window or unlabeled are never projected at all.
 * @property entityKeys entity keys for this impression, read per-row from the model line's
 *   required/optional entity-key column mappings (see [EntityKeyMapper]); propagated to the labeled
 *   output and the per-blob `BlobDetails.entity_keys` union.
 * @property populationAttributeWriter writes the population attributes of the VID the model assigns
 *   onto [eventMessage]. It cannot be applied here — the model has not run yet, so the VID is
 *   unknown — so the sink applies it per assigned person. Shared and stateless: the converter
 *   memoizes one per model-line config.
 *
 * Not a `data class`: [populationAttributeWriter] has no value semantics, so a generated `equals`
 * would make structural equality depend on which instance was injected.
 */
class ConvertedImpression(
  val labelerInput: LabelerInput,
  val buildEvent: () -> com.google.protobuf.Message,
  val entityKeys: List<org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression.EntityKey>,
  val populationAttributeWriter: PopulationAttributeWriter,
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
