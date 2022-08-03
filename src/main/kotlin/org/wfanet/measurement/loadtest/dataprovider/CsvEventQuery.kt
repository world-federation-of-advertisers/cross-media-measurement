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
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender as BannerGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplateKt.gender as bannerGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange as PrivacyAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.Gender as PrivacyGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt.ageRange as privacyAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt.gender as privacyGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.AgeRange as VideoAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplateKt.ageRange as videoAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testVideoTemplate
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/** Fulfill the query with data pulled in from csv source. */
// TODO(@jcorilla): replace csvEvents input with implementation of object from CSV reader
class CsvEventQuery(csvEvents: List<Map<String, Any>>) : EventQuery() {

  private val events =
    mutableMapOf<Int, MutableList<TestEvent>>().apply {
      csvEvents.forEach {
        val vid = it["VID"] ?: error("Missing CSV data for label 'VID'")
        getOrPut(vid as Int) { mutableListOf() }.add(csvEntryToTestEvent(it))
      }
    }

  /** Generates Ids by applying filter on events */
  override fun getUserVirtualIds(eventFilter: EventFilter): Sequence<Long> {
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

  private fun csvEntryToTestEvent(event: Map<String, Any>): TestEvent {

    return testEvent {
      this.privacyBudget = testPrivacyBudgetTemplate {
        when (event["Sex"]) {
          "M" -> gender = privacyGender { value = PrivacyGender.Value.GENDER_MALE }
          "F" -> gender = privacyGender { value = PrivacyGender.Value.GENDER_FEMALE }
        }
        when (event["Age_Group"]) {
          "18_34" -> age = privacyAgeRange { value = PrivacyAgeRange.Value.AGE_18_TO_34 }
          "35_54" -> age = privacyAgeRange { value = PrivacyAgeRange.Value.AGE_35_TO_54 }
          "55+" -> age = privacyAgeRange { value = PrivacyAgeRange.Value.AGE_OVER_54 }
        }
      }
      this.videoAd = testVideoTemplate {
        when (event["Age_Group"]) {
          "18_34" -> age = videoAgeRange { value = VideoAgeRange.Value.AGE_18_TO_34 }
          else -> age = videoAgeRange { value = VideoAgeRange.Value.AGE_RANGE_UNSPECIFIED }
        }
      }
      this.bannerAd = testBannerTemplate {
        when (event["Sex"]) {
          "M" -> gender = bannerGender { value = BannerGender.Value.GENDER_MALE }
          "F" -> gender = bannerGender { value = BannerGender.Value.GENDER_FEMALE }
          else -> gender = bannerGender { value = BannerGender.Value.GENDER_UNKOWN }
        }
      }
    }
  }
}
