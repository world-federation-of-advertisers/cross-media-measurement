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

import com.opencsv.CSVReader
import com.opencsv.CSVReaderBuilder
import java.io.File
import java.io.IOException
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.api.v2alpha.TimeInterval
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.PersonKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender as BannerGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplateKt as TestBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange as PrivacyAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.Gender as PrivacyGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt as TestPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.AgeRange as VideoAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplateKt as TestVideoTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testVideoTemplate
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

private const val EDP_ID_INDEX = 0
private const val SEX_INDEX = 2
private const val AGE_GROUP_INDEX = 3
private const val SOCIAL_GRADE_INDEX = 4
private const val DATE_INDEX = 5
private const val VID_INDEX = 7

/** Fulfill the query with VIDs imported from CSV file. */
class CsvEventQuery(publisherId: Int, file: File) : EventQuery {
  private val labelledEvents: List<TestEvent> by lazy {
    logger.info("Reading data from CSV file: $file...")
    readCsvFile(publisherId, file).also { logger.info("Finished reading data from CSV file") }
  }

  /** Generates Ids by applying filter on events */
  override fun getUserVirtualIds(
    timeInterval: TimeInterval,
    eventFilter: EventFilter
  ): Sequence<Long> {
    logger.info("Querying and filtering VIDs from CsvEventQuery...")

    val timeRange = TimeIntervalRange(timeInterval)
    val program = EventQuery.compileProgram(eventFilter, TestEvent.getDefaultInstance())

    return labelledEvents
      .asSequence()
      .filter { it.time.toInstant() in timeRange && EventFilters.matches(it, program) }
      .map { it.person.vid }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.UK)

    @Throws(IOException::class)
    private fun readCsvFile(publisherId: Int, file: File): List<TestEvent> {
      val labelledEvents: Sequence<TestEvent> =
        file.reader().use { fileReader ->
          val csvReader: CSVReader = CSVReaderBuilder(fileReader).withSkipLines(1).build()
          csvReader
            .iterator()
            .asSequence()
            .filter { row -> row[EDP_ID_INDEX].toInt() == publisherId }
            .map { row -> parseTestEvent(row) }
        }

      return labelledEvents.toList()
    }

    private fun parseTestEvent(row: Array<String>): TestEvent {
      val vid = row[VID_INDEX].toLong()
      val sex: Person.Sex? =
        when (row[SEX_INDEX]) {
          "M" -> Person.Sex.MALE
          "F" -> Person.Sex.FEMALE
          else -> null
        }
      val ageGroup: Person.AgeGroup? =
        when (row[AGE_GROUP_INDEX]) {
          "18_34" -> Person.AgeGroup.YEARS_18_TO_34
          "35_54" -> Person.AgeGroup.YEARS_35_TO_54
          "55+" -> Person.AgeGroup.YEARS_55_PLUS
          else -> null
        }
      val socialGradeGroup: Person.SocialGradeGroup? =
        when (row[SOCIAL_GRADE_INDEX]) {
          "ABC1" -> Person.SocialGradeGroup.A_B_C1
          "C2DE" -> Person.SocialGradeGroup.C2_D_E
          else -> null
        }
      return testEvent {
        time =
          LocalDate.parse(row[DATE_INDEX], dateFormatter)
            .atStartOfDay()
            .toInstant(ZoneOffset.UTC)
            .toProtoTime()
        person = person {
          this.vid = vid
          if (sex != null) {
            this.sex = PersonKt.sexField { value = sex }
          }
          if (ageGroup != null) {
            this.ageGroup = PersonKt.ageGroupField { value = ageGroup }
          }
          if (socialGradeGroup != null) {
            socialGrade = PersonKt.socialGradeGroupField { value = socialGradeGroup }
          }
        }
        privacyBudget = testPrivacyBudgetTemplate {
          if (sex != null) {
            gender =
              TestPrivacyBudgetTemplate.gender {
                value =
                  when (sex) {
                    Person.Sex.MALE -> PrivacyGender.Value.GENDER_MALE
                    Person.Sex.FEMALE -> PrivacyGender.Value.GENDER_FEMALE
                    Person.Sex.SEX_UNSPECIFIED,
                    Person.Sex.UNRECOGNIZED -> error("Unhandled Person.Sex value")
                  }
              }
          }
          if (ageGroup != null) {
            age =
              TestPrivacyBudgetTemplate.ageRange {
                value =
                  when (ageGroup) {
                    Person.AgeGroup.YEARS_18_TO_34 -> PrivacyAgeRange.Value.AGE_18_TO_34
                    Person.AgeGroup.YEARS_35_TO_54 -> PrivacyAgeRange.Value.AGE_35_TO_54
                    Person.AgeGroup.YEARS_55_PLUS -> PrivacyAgeRange.Value.AGE_OVER_54
                    Person.AgeGroup.AGE_GROUP_UNSPECIFIED,
                    Person.AgeGroup.UNRECOGNIZED -> error("Unhandled Person.AgeGroup value")
                  }
              }
          }
        }
        videoAd = testVideoTemplate {
          if (ageGroup == Person.AgeGroup.YEARS_18_TO_34) {
            age = TestVideoTemplate.ageRange { value = VideoAgeRange.Value.AGE_18_TO_34 }
          }
        }
        bannerAd = testBannerTemplate {
          if (sex != null) {
            gender =
              TestBannerTemplate.gender {
                value =
                  when (sex) {
                    Person.Sex.MALE -> BannerGender.Value.GENDER_MALE
                    Person.Sex.FEMALE -> BannerGender.Value.GENDER_FEMALE
                    Person.Sex.SEX_UNSPECIFIED,
                    Person.Sex.UNRECOGNIZED -> error("Unhandled Person.Sex value")
                  }
              }
          }
        }
      }
    }
  }
}
