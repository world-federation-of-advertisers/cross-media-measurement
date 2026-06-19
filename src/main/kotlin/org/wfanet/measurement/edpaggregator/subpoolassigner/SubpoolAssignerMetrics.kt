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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import org.wfanet.measurement.common.Instrumentation

/**
 * Encapsulates the OpenTelemetry instruments used by [SubpoolAssignmentSink].
 *
 * These cover the Phase-0 *labeling outcomes* (a different layer from the reader's
 * `RawImpressionSourceMetrics`, which counts rows read/dropped/emitted). They integrate with
 * `EdpaTelemetry`, so labeled / dropped / unrouted rates surface on operator dashboards (the
 * component runs in the same TEE apps as the rest of the EDPA pipeline). The in-process
 * [SubpoolAssignmentSink.labeled] / [SubpoolAssignmentSink.droppedOutsideWindow] /
 * [SubpoolAssignmentSink.unrouted] tallies are kept separately because OpenTelemetry counters are
 * not readable in-process for the completion log / run summary.
 *
 * @param meter the OpenTelemetry [Meter] used to create instruments.
 */
class SubpoolAssignerMetrics(meter: Meter = Instrumentation.meter) {
  /** Counter for events that routed to at least one subpool. */
  val eventsLabeledCounter: LongCounter =
    meter
      .counterBuilder("edpa.subpool_assigner.events_labeled")
      .setDescription("Number of events that routed to at least one subpool")
      .build()

  /** Counter for events dropped because their timestamp was unset or outside the active window. */
  val eventsDroppedOutsideWindowCounter: LongCounter =
    meter
      .counterBuilder("edpa.subpool_assigner.events_dropped_outside_window")
      .setDescription("Number of events dropped by the active-window filter")
      .build()

  /** Counter for events the labeler routed to no subpool (hash-fallback path). */
  val eventsUnroutedCounter: LongCounter =
    meter
      .counterBuilder("edpa.subpool_assigner.events_unrouted")
      .setDescription("Number of events the labeler routed to no subpool")
      .build()
}
