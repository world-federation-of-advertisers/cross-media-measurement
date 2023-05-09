// Copyright 2023 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.eventdataprovider.validation

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal.Justification
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec

private val EMPTY_REQUISITION = requisition {}

@RunWith(JUnit4::class)
class EventFilterValidatorTest {

  private val validator = EventFilterValidator(testEvent {})

  @Test
  fun validatePassesForCorrectFilter() {
    val result =
      validator.validate(
        EMPTY_REQUISITION,
        requisitionSpec {
          eventGroups += eventGroupEntry {
            value = value { filter = eventFilter { expression = "person.gender.value == 1" } }
          }
        }
      )

    assertThat(result).isNull()
  }

  @Test
  fun validateFailsForWrongFilter() {
    val result =
      validator.validate(
        EMPTY_REQUISITION,
        requisitionSpec {
          eventGroups += eventGroupEntry {
            // Using "and" is not allowed (instead use "&&").
            value = value {
              filter = eventFilter {
                expression = "person.gender.value == 1 and person.gender.value == 2"
              }
            }
          }
        }
      )

    assertThat(result)
      .isEqualTo(
        refusal {
          justification = Justification.SPECIFICATION_INVALID
          message =
            "Syntax error in Event Filter CEL Expression: person.gender.value == 1 and person.gender.value == 2."
        }
      )
  }
}
