/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.loadtest.dataprovider

import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.TimeInterval
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.timeOrNull
import org.wfanet.measurement.api.v2alpha.toRange
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/** Fulfills the query with matching events using filters. */
open class InMemoryEventQuery(private val events: Iterable<LabelledEvent>) : EventQuery {
  constructor(
    events: Map<Long, List<TestEvent>>
  ) : this(events.flatMap { (vid, events) -> events.map { event -> LabelledEvent(vid, event) } })

  data class LabelledEvent(val vid: Long, val event: TestEvent)

  override fun getUserVirtualIds(
    timeInterval: TimeInterval,
    eventFilter: RequisitionSpec.EventFilter
  ): Sequence<Long> {
    val timeRange = timeInterval.toRange()
    val program = EventQuery.compileProgram(eventFilter, TestEvent.getDescriptor())

    return events
      .asSequence()
      .filter { (_, event) ->
        checkNotNull(event.timeOrNull).toInstant() in timeRange &&
          EventFilters.matches(event, program)
      }
      .map { it.vid }
  }
}
