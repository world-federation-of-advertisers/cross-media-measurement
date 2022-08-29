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

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.logging.Logger
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender as BannerGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplateKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange as PrivacyAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.Gender as PrivacyGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.AgeRange as VideoAgeRange
import com.google.type.Date
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplateKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testVideoTemplate
import org.wfanet.measurement.common.toProtoDate

private val NONMATCHING_BANNER_GENDER = BannerGender.Value.GENDER_MALE
private val MATCHING_BANNER_GENDER = BannerGender.Value.GENDER_FEMALE
private val NONMATCHING_PRIVACY_AGE_RANGE = PrivacyAgeRange.Value.AGE_18_TO_24
private val MATCHING_PRIVACY_AGE_RANGE = PrivacyAgeRange.Value.AGE_35_TO_54
private val MATCHING_EVENT_FILTER = eventFilter {
  expression = "privacy_budget.age.value == 1 || banner_ad.gender.value == 2"
}

private val SCHEMA =
  FieldList.of(
    Field.of("Publisher ID", StandardSQLTypeName.STRING),
    Field.of("Sex", StandardSQLTypeName.STRING),
    Field.of("Age Group", StandardSQLTypeName.STRING),
    Field.of("Social Grade", StandardSQLTypeName.STRING),
    Field.of("Date", StandardSQLTypeName.STRING),
    Field.of("Complete", StandardSQLTypeName.STRING),
    Field.of("VID", StandardSQLTypeName.STRING)
  )

@RunWith(JUnit4::class)
class BigQueryEventQueryTest {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  @Test
  fun `create expecteded test event from fieldValues`() {
    val fieldValues =
      FieldValueList.of(
        listOf(
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "M"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "18_34"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "ABC1"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "09/03/2021"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1003520")
        ),
        SCHEMA
      )

    val expectedEvent = testEvent {
      this.bannerAd = testBannerTemplate {
        gender = TestBannerTemplateKt.gender { value = BannerGender.Value.GENDER_MALE }
      }
      this.videoAd = testVideoTemplate {
        age = TestVideoTemplateKt.ageRange { value =  VideoAgeRange.Value.AGE_18_TO_24 }
      }
      this.privacyBudget = testPrivacyBudgetTemplate {
        age = TestPrivacyBudgetTemplateKt.ageRange { value = PrivacyAgeRange.Value.AGE_18_TO_24 }
        gender = TestPrivacyBudgetTemplateKt.gender { value = PrivacyGender.Value.GENDER_MALE }
        publisher = 1
        socialGrade = TestPrivacyBudgetTemplateKt.socialGrade { value = TestPrivacyBudgetTemplate.SocialGrade.Value.ABC1 }
        date = Date.newBuilder()
          .apply {
            year = 2021
            month = 3
            day = 9
          }
          .build()
        complete = true
      }
    }

    val testEvent = BigQueryEventQuery.fieldValuesToTestEvent(fieldValues)
    assertThat(testEvent).isEqualTo(expectedEvent)
  }

  @Test
  fun `invalid values creates testEvent with default values`() {
    val fieldValues =
      FieldValueList.of(
        listOf(
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "N/A"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "N/A"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "N/A"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "N/A"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "N/A"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "N/A"),
          FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1003520")
        ),
        SCHEMA
      )

    val expectedEvent = testEvent {
      this.bannerAd = testBannerTemplate {
        gender = TestBannerTemplateKt.gender { value = BannerGender.Value.GENDER_UNKOWN }
      }
      this.videoAd = testVideoTemplate {
        age = TestVideoTemplateKt.ageRange { value =  VideoAgeRange.Value.AGE_RANGE_UNSPECIFIED }
      }
      this.privacyBudget = testPrivacyBudgetTemplate {
        // age = TestPrivacyBudgetTemplateKt.ageRange { value = PrivacyAgeRange.Value.AGE_18_TO_24 }
        // gender = TestPrivacyBudgetTemplateKt.gender { value = PrivacyGender.Value.GENDER_MALE }
        // publisher = 0
        // socialGrade = TestPrivacyBudgetTemplateKt.socialGrade { value = TestPrivacyBudgetTemplate.SocialGrade.Value.GRADE_UNSPECIFIED }
        // date = Date.newBuilder()
        //   .apply {
        //     year = 0
        //     month = 0
        //     day = 0
        //   }
        //   .build()
        // complete = false
      }
    }

    val testEvent = BigQueryEventQuery.fieldValuesToTestEvent(fieldValues)
    assertThat(testEvent).isEqualTo(expectedEvent)
  }
  //
  // @Test
  // fun `missing values creates testEvent with default values`() {
  //   val partialSchema =
  //     FieldList.of(
  //       Field.of("Age Group", StandardSQLTypeName.STRING),
  //       Field.of("VID", StandardSQLTypeName.STRING)
  //     )
  //
  //   val fieldValues =
  //     FieldValueList.of(
  //       listOf(
  //         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "N/A"),
  //         FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1003520")
  //       ),
  //       partialSchema
  //     )
  //
  //   val expectedEvent = testEvent {
  //     this.bannerAd = testBannerTemplate {
  //       gender = TestBannerTemplateKt.gender { value = BannerGender.Value.GENDER_UNKOWN }
  //     }
  //     this.videoAd = testVideoTemplate {
  //       age = TestVideoTemplateKt.ageRange { value =  VideoAgeRange.Value.AGE_RANGE_UNSPECIFIED }
  //     }
  //     this.privacyBudget = testPrivacyBudgetTemplate {
  //       // age = TestPrivacyBudgetTemplateKt.ageRange { value = PrivacyAgeRange.Value.AGE_18_TO_24 }
  //       // gender = TestPrivacyBudgetTemplateKt.gender { value = PrivacyGender.Value.GENDER_MALE }
  //       // publisher = 0
  //       // socialGrade = TestPrivacyBudgetTemplateKt.socialGrade { value = TestPrivacyBudgetTemplate.SocialGrade.Value.GRADE_UNSPECIFIED }
  //       // date = Date.newBuilder()
  //       //   .apply {
  //       //     year = 0
  //       //     month = 0
  //       //     day = 0
  //       //   }
  //       //   .build()
  //       // complete = false
  //     }
  //   }
  //
  //   val testEvent = BigQueryEventQuery.fieldValuesToTestEvent(fieldValues)
  //   assertThat(testEvent).isEqualTo(expectedEvent)
  // }
}
