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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec

private val REQUISITION_SPEC = requisitionSpec {
  eventFilter { expression = "gender=MALE,age>=18,age<=34,duration>2" }
}

private val MEASUREMENT_SPEC = measurementSpec {
  reachAndFrequency {
    reachPrivacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.01
    }
    frequencyPrivacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.01
    }
    vidSamplingInterval = vidSamplingInterval {
      start = 0.1f
      width = 0.2f
    }
  }
}

class PrivacyBudgetManagerTest {
  @Test
  fun `Test privacy budget manager`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(backingStore)
    pbm.chargePrivacyBudget(REQUISITION_SPEC, MEASUREMENT_SPEC)
    assertFailsWith<PrivacyBudgetManagerException> {
      pbm.chargePrivacyBudget(REQUISITION_SPEC, MEASUREMENT_SPEC)
    }
  }
}
