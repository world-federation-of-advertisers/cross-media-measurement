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

import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toRange
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

typealias LabeledTestEvent = LabeledEvent<TestEvent>

private const val DEFAULT_MAX_VID_VALUE = 10_000_000L

/** Fulfills the query with matching events using filters. */
open class InMemoryEventQuery(
  private val labeledEvents: List<LabeledTestEvent>,
  private val maxVidValue: Long = DEFAULT_MAX_VID_VALUE,
) : EventQuery<TestEvent> {
  override fun getLabeledEvents(
    eventGroupSpec: EventQuery.EventGroupSpec
  ): Sequence<LabeledTestEvent> {
    val timeRange: OpenEndTimeRange = eventGroupSpec.spec.collectionInterval.toRange()
    val program: Program =
      EventQuery.compileProgram(eventGroupSpec.spec.filter, TestEvent.getDescriptor())

    return labeledEvents.asSequence().filter {
      it.timestamp in timeRange && EventFilters.matches(it.message, program)
    }
  }

  override fun getUserVirtualIdUniverse(): Sequence<Long> {
    return (1L..maxVidValue).asSequence()
  }
}
