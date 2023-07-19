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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.type.interval
import java.time.LocalDate
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.DpCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.EventGroupSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.LandscapeMask
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Query
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper.getPrivacyQuery

private const val MEASUREMENT_CONSUMER_ID = "ACME"

private val LAST_EVENT_DATE = LocalDate.now()
private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

private const val FILTER_EXPRESSION =
  "person.gender==0 && person.age_group==0 && banner_ad.gender == 1"
private val REQUISITION_SPEC = requisitionSpec {
  eventGroups += eventGroupEntry {
    key = "eventGroups/someEventGroup"
    value =
      RequisitionSpecKt.EventGroupEntryKt.value {
        collectionInterval = interval {
          startTime = TIME_RANGE.start.toProtoTime()
          endTime = TIME_RANGE.endExclusive.toProtoTime()
        }
        filter = eventFilter { expression = FILTER_EXPRESSION }
      }
  }
}

private val REACH_AND_FREQ_MEASUREMENT_SPEC = measurementSpec {
  reachAndFrequency = reachAndFrequency {
    reachPrivacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.01
    }

    frequencyPrivacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.01
    }
  }
  vidSamplingInterval = vidSamplingInterval {
    start = 0.01f
    width = 0.02f
  }
}

private val DURATION_MEASUREMENT_SPEC = measurementSpec {
  impression = impression {
    privacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.02
    }
  }
}

private val IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
  impression = impression {
    privacyParams = differentialPrivacyParams {
      epsilon = 0.4
      delta = 0.02
    }
  }
}

private val REACH_MEASUREMENT_SPEC = measurementSpec {
  reach = reach {
    privacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.01
    }
  }
  vidSamplingInterval = vidSamplingInterval {
    start = 0.01f
    width = 0.02f
  }
}

class PrivacyQueryMapperTest {
  @Test
  fun `converts reach and Frequency measurement to privacy query`() {
    val referenceId = "RequisitioId1"

    assertThat(
        getPrivacyQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          REACH_AND_FREQ_MEASUREMENT_SPEC,
          REQUISITION_SPEC.eventGroupsList.map { it.value }
        )
      )
      .isEqualTo(
        Query(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)), 0.01f, 0.02f),
          DpCharge(0.6f, 0.02f)
        )
      )
  }

  @Test
  fun `converts duration measurement to privacy query`() {
    val referenceId = "RequisitioId1"

    assertThat(
        getPrivacyQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          DURATION_MEASUREMENT_SPEC,
          REQUISITION_SPEC.eventGroupsList.map { it.value }
        )
      )
      .isEqualTo(
        Query(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)), 0.0f, 0.0f),
          DpCharge(0.3f, 0.02f)
        )
      )
  }

  @Test
  fun `converts impression measurement to privacy query`() {
    val referenceId = "RequisitioId1"

    assertThat(
        getPrivacyQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          IMPRESSION_MEASUREMENT_SPEC,
          REQUISITION_SPEC.eventGroupsList.map { it.value }
        )
      )
      .isEqualTo(
        Query(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)), 0.0f, 0.0f),
          DpCharge(0.4f, 0.02f)
        )
      )
  }

  @Test
  fun `converts reach measurement to privacy query`() {
    val referenceId = "RequisitioId1"

    assertThat(
        getPrivacyQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          REACH_MEASUREMENT_SPEC,
          REQUISITION_SPEC.eventGroupsList.map { it.value }
        )
      )
      .isEqualTo(
        Query(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(
            listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)),
            REACH_MEASUREMENT_SPEC.vidSamplingInterval.start,
            REACH_MEASUREMENT_SPEC.vidSamplingInterval.width
          ),
          DpCharge(
            REACH_MEASUREMENT_SPEC.reach.privacyParams.epsilon.toFloat(),
            REACH_MEASUREMENT_SPEC.reach.privacyParams.delta.toFloat()
          )
        )
      )
  }
}
