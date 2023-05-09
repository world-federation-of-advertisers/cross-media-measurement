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
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal.Justification
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec

@RunWith(JUnit4::class)
class CompoundValidatorTest {

  @Test
  fun validatePassesWhenAllValidatorsPass() {
    val validator =
      CompoundValidator(
        listOf(
          RequisitionValidator { _, _ -> null },
          RequisitionValidator { _, _ -> null },
          RequisitionValidator { _, _ -> null },
        )
      )

    assertThat(validator.validate(requisition {}, requisitionSpec {})).isNull()
  }

  @Test
  fun validateFailsWhenOneValidatorFails() {
    val result = refusalResultOf(Justification.DECLINED)
    val validator =
      CompoundValidator(
        listOf(
          RequisitionValidator { _, _ -> null },
          RequisitionValidator { _, _ -> result },
          RequisitionValidator { _, _ -> null },
        )
      )

    assertThat(validator.validate(requisition {}, requisitionSpec {})).isEqualTo(result)
  }

  @Test
  fun validateFailsWithFirstRefusalResult() {
    val result1 = refusalResultOf(Justification.DECLINED)
    val result2 = refusalResultOf(Justification.UNFULFILLABLE)
    val validator =
      CompoundValidator(
        listOf(
          RequisitionValidator { _, _ -> null },
          RequisitionValidator { _, _ -> result1 },
          RequisitionValidator { _, _ -> result2 },
        )
      )

    assertThat(validator.validate(requisition {}, requisitionSpec {})).isEqualTo(result1)
  }

  companion object {

    private fun refusalResultOf(justification: Justification): Refusal = refusal {
      this.justification = justification
    }
  }
}
