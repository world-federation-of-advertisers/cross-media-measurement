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
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration
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
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpQuery
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.EventGroupSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.LandscapeMask
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper.getDirectAcdpQuery
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper.getMpcAcdpQuery

class PrivacyQueryMapperTest {

  @Test
  fun `converts hmss reach only measurement to AcdpQuery`() {
    val referenceId = "RequisitionId1"
    val dpParams =
      DpParams(
        REACH_MEASUREMENT_SPEC.reach.privacyParams.epsilon,
        REACH_MEASUREMENT_SPEC.reach.privacyParams.delta,
      )
    val expectedAcdpCharge = AcdpParamsConverter.getMpcAcdpCharge(dpParams, HMSS_CONTRIBUTOR_COUNT)

    assertThat(
        getMpcAcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          REACH_MEASUREMENT_SPEC,
          REQUISITION_SPEC.events.eventGroupsList.map { it.value },
          HMSS_CONTRIBUTOR_COUNT,
        )
      )
      .isEqualTo(
        AcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)), 0.01f, 0.02f),
          expectedAcdpCharge,
        )
      )
  }

  @Test
  fun `converts hmss reach and frequency measurement to AcdpQuery`() {
    val referenceId = "RequisitionId1"
    val acdpChargeForReach =
      AcdpParamsConverter.getMpcAcdpCharge(
        DpParams(
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.reachPrivacyParams.epsilon,
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.reachPrivacyParams.delta,
        ),
        HMSS_CONTRIBUTOR_COUNT,
      )
    val acdpChargeForFrequency =
      AcdpParamsConverter.getMpcAcdpCharge(
        DpParams(
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.frequencyPrivacyParams.epsilon,
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.frequencyPrivacyParams.delta,
        ),
        HMSS_CONTRIBUTOR_COUNT,
      )
    val expectedAcdpCharge =
      AcdpCharge(
        acdpChargeForReach.rho + acdpChargeForFrequency.rho,
        acdpChargeForReach.theta + acdpChargeForFrequency.theta,
      )

    assertThat(
        getMpcAcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          REACH_AND_FREQ_MEASUREMENT_SPEC,
          REQUISITION_SPEC.events.eventGroupsList.map { it.value },
          HMSS_CONTRIBUTOR_COUNT,
        )
      )
      .isEqualTo(
        AcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)), 0.01f, 0.02f),
          expectedAcdpCharge,
        )
      )
  }

  @Test
  fun `converts llv2 reach and frequency measurement to AcdpQuery`() {
    val referenceId = "RequisitionId1"
    val acdpChargeForReach =
      AcdpParamsConverter.getMpcAcdpCharge(
        DpParams(
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.reachPrivacyParams.epsilon,
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.reachPrivacyParams.delta,
        ),
        CONTRIBUTOR_COUNT,
      )
    val acdpChargeForFrequency =
      AcdpParamsConverter.getMpcAcdpCharge(
        DpParams(
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.frequencyPrivacyParams.epsilon,
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.frequencyPrivacyParams.delta,
        ),
        CONTRIBUTOR_COUNT,
      )
    val expectedAcdpCharge =
      AcdpCharge(
        acdpChargeForReach.rho + acdpChargeForFrequency.rho,
        acdpChargeForReach.theta + acdpChargeForFrequency.theta,
      )

    assertThat(
        getMpcAcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          REACH_AND_FREQ_MEASUREMENT_SPEC,
          REQUISITION_SPEC.events.eventGroupsList.map { it.value },
          CONTRIBUTOR_COUNT,
        )
      )
      .isEqualTo(
        AcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)), 0.01f, 0.02f),
          expectedAcdpCharge,
        )
      )
  }

  @Test
  fun `converts direct reach and frequency measurement to AcdpQuery`() {
    val referenceId = "RequisitionId1"
    val acdpChargeForReach =
      AcdpParamsConverter.getDirectAcdpCharge(
        DpParams(
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.reachPrivacyParams.epsilon,
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.reachPrivacyParams.delta,
        ),
        SENSITIVITY,
      )
    val acdpChargeForFrequency =
      AcdpParamsConverter.getDirectAcdpCharge(
        DpParams(
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.frequencyPrivacyParams.epsilon,
          REACH_AND_FREQ_MEASUREMENT_SPEC.reachAndFrequency.frequencyPrivacyParams.delta,
        ),
        SENSITIVITY,
      )
    val expectedAcdpCharge =
      AcdpCharge(
        acdpChargeForReach.rho + acdpChargeForFrequency.rho,
        acdpChargeForReach.theta + acdpChargeForFrequency.theta,
      )

    assertThat(
        getDirectAcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          REACH_AND_FREQ_MEASUREMENT_SPEC,
          REQUISITION_SPEC.events.eventGroupsList.map { it.value },
        )
      )
      .isEqualTo(
        AcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)), 0.01f, 0.02f),
          expectedAcdpCharge,
        )
      )
  }

  @Test
  fun `converts llv2 reach measurement to AcdpQuery`() {
    val referenceId = "RequisitionId1"

    val expectedAcdpCharge =
      AcdpParamsConverter.getMpcAcdpCharge(
        DpParams(
          REACH_MEASUREMENT_SPEC.reach.privacyParams.epsilon,
          REACH_MEASUREMENT_SPEC.reach.privacyParams.delta,
        ),
        CONTRIBUTOR_COUNT,
      )

    assertThat(
        getMpcAcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          REACH_MEASUREMENT_SPEC,
          REQUISITION_SPEC.events.eventGroupsList.map { it.value },
          CONTRIBUTOR_COUNT,
        )
      )
      .isEqualTo(
        AcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(
            listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)),
            REACH_MEASUREMENT_SPEC.vidSamplingInterval.start,
            REACH_MEASUREMENT_SPEC.vidSamplingInterval.width,
          ),
          expectedAcdpCharge,
        )
      )
  }

  @Test
  fun `converts direct reach measurement to AcdpQuery`() {
    val referenceId = "RequisitionId1"

    val expectedAcdpCharge =
      AcdpParamsConverter.getDirectAcdpCharge(
        DpParams(
          REACH_MEASUREMENT_SPEC.reach.privacyParams.epsilon,
          REACH_MEASUREMENT_SPEC.reach.privacyParams.delta,
        ),
        SENSITIVITY,
      )

    assertThat(
        getDirectAcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          REACH_MEASUREMENT_SPEC,
          REQUISITION_SPEC.events.eventGroupsList.map { it.value },
        )
      )
      .isEqualTo(
        AcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(
            listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)),
            REACH_MEASUREMENT_SPEC.vidSamplingInterval.start,
            REACH_MEASUREMENT_SPEC.vidSamplingInterval.width,
          ),
          expectedAcdpCharge,
        )
      )
  }

  @Test
  fun `converts impression measurement to AcdpQuery`() {
    val referenceId = "RequisitionId1"

    val expectedAcdpCharge =
      AcdpParamsConverter.getDirectAcdpCharge(
        DpParams(
          IMPRESSION_MEASUREMENT_SPEC.impression.privacyParams.epsilon,
          IMPRESSION_MEASUREMENT_SPEC.impression.privacyParams.delta,
        ),
        SENSITIVITY,
      )

    assertThat(
        getDirectAcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          IMPRESSION_MEASUREMENT_SPEC,
          REQUISITION_SPEC.events.eventGroupsList.map { it.value },
        )
      )
      .isEqualTo(
        AcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)), 0.0f, 0.0f),
          expectedAcdpCharge,
        )
      )
  }

  @Test
  fun `converts duration measurement to AcdpQuery`() {
    val referenceId = "RequisitionId1"

    val expectedAcdpCharge =
      AcdpParamsConverter.getDirectAcdpCharge(
        DpParams(
          DURATION_MEASUREMENT_SPEC.duration.privacyParams.epsilon,
          DURATION_MEASUREMENT_SPEC.duration.privacyParams.delta,
        ),
        SENSITIVITY,
      )

    assertThat(
        getDirectAcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          DURATION_MEASUREMENT_SPEC,
          REQUISITION_SPEC.events.eventGroupsList.map { it.value },
        )
      )
      .isEqualTo(
        AcdpQuery(
          Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
          LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, TIME_RANGE)), 0.0f, 0.0f),
          expectedAcdpCharge,
        )
      )
  }

  companion object {
    private const val MEASUREMENT_CONSUMER_ID = "ACME"
    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)
    private const val FILTER_EXPRESSION =
      "person.gender==0 && person.age_group==0 && banner_ad.gender == 1"
    private const val CONTRIBUTOR_COUNT = 3
    private const val HMSS_CONTRIBUTOR_COUNT = 2
    private const val SENSITIVITY = 1.0

    private val REQUISITION_SPEC = requisitionSpec {
      events =
        RequisitionSpecKt.events {
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
    }

    private val REACH_AND_FREQ_MEASUREMENT_SPEC = measurementSpec {
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = differentialPrivacyParams {
          epsilon = 0.03
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
      duration = duration {
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
  }
}
