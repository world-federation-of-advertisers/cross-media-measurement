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

import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.virtualpeople.common.LabelerInput

/**
 * Converts a raw-impression Parquet row into the fields needed to label and emit an impression for
 * one model line.
 *
 * This is the seam between the raw-impression reader (which hands out rows keyed by **Parquet
 * column name** — see [ParquetDigestedEvent]) and the VirtualPeople [LabelerInput]. The conversion
 * is model-line-specific because the column→field mapping lives in
 * [VidLabelerParams.ModelLineConfig.getLabelerInputFieldMappingMap].
 *
 * It is injected (rather than implemented inline) so the labeling pipeline can be built and tested
 * independently of the production projection: tests supply a fake, and the production converter
 * ([ParquetImpressionConverter]) is wired in by the runner.
 */
fun interface ImpressionConverter {
  /**
   * Converts [event]'s row for the model line described by [config].
   *
   * @param inputBlobUri the raw-impression file this row came from, used to resolve the file's
   *   entity keys (and legacy event group reference id) from the dispatcher-provided per-file map.
   * @return the [ConvertedImpression], or `null` to skip this row for this model line.
   */
  fun convert(
    event: ParquetDigestedEvent,
    config: VidLabelerParams.ModelLineConfig,
    inputBlobUri: String,
  ): ConvertedImpression?
}

/**
 * The labeling-relevant fields extracted from one raw-impression row for one model line.
 *
 * @property labelerInput input fed to the [VidAssigner].
 * @property eventTime impression event time — used for active-window filtering and as the output
 *   `event_time`. A typed `Timestamp` so the converter contract carries the unit instead of a bare
 *   epoch-micros `Long`.
 * @property eventGroupReferenceId event group the impression belongs to.
 * @property event the Event payload to embed in the labeled output.
 * @property entityKeys entity keys associated with this impression, propagated from the
 *   `EventGroup` metadata to the labeled output and `BlobDetails`.
 */
data class ConvertedImpression(
  val labelerInput: LabelerInput,
  val eventTime: com.google.protobuf.Timestamp,
  val eventGroupReferenceId: String,
  val event: com.google.protobuf.Any,
  val entityKeys: List<org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression.EntityKey>,
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
