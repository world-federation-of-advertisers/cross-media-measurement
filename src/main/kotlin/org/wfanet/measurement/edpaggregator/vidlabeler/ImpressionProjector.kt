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
 * Projects a raw-impression Parquet row into the fields needed to label and emit an impression for
 * one model line.
 *
 * This is the seam between the raw-impression reader (which hands out rows keyed by **Parquet
 * column name** — see [ParquetDigestedEvent]) and the VirtualPeople [LabelerInput]. The projection
 * is model-line-specific because the column→field mapping lives in
 * [VidLabelerParams.ModelLineConfig.getLabelerInputFieldMappingMap].
 *
 * It is injected (rather than implemented inline) so the labeling pipeline can be built and tested
 * before the Parquet schema is finalized: tests supply a fake, and the production projector is
 * wired in once the schema is pinned.
 *
 * TODO(world-federation-of-advertisers/cross-media-measurement#3913): provide the production
 *   implementation once the raw-impression Parquet schema (column names + types) is finalized with
 *   the reader (#3954).
 */
fun interface ImpressionProjector {
  /**
   * Projects [event]'s row for the model line described by [config].
   *
   * @return the [ProjectedImpression], or `null` to skip this row for this model line.
   */
  fun project(
    event: ParquetDigestedEvent,
    config: VidLabelerParams.ModelLineConfig,
  ): ProjectedImpression?
}

/**
 * The labeling-relevant fields extracted from one raw-impression row for one model line.
 *
 * @property labelerInput input fed to the [VidAssigner].
 * @property eventTimeMicros impression event time, epoch micros — used for active-window filtering
 *   and as the output `event_time`.
 * @property eventGroupReferenceId event group the impression belongs to.
 * @property event the Event payload to embed in the labeled output.
 */
data class ProjectedImpression(
  val labelerInput: LabelerInput,
  val eventTimeMicros: Long,
  val eventGroupReferenceId: String,
  val event: com.google.protobuf.Any,
)
