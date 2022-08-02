/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Message
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/** Fulfills the query with matching events using filters. */
class FilterEventQuery(val events: Map<Int, List<TestEvent>>) : EventQuery() {

  override fun getUserVirtualIds(eventFilter: RequisitionSpec.EventFilter): Sequence<Long> {
    val program =
      EventFilters.compileProgram(
        eventFilter.expression,
        testEvent {},
      )
    return sequence {
      events.forEach { (vid, testEvents) ->
        testEvents.forEach {
          if (EventFilters.matches(it as Message, program)) {
            yield(vid.toLong())
          }
        }
      }
    }
  }
}
