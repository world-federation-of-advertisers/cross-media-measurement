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

import com.opencsv.CSVReader
import com.opencsv.CSVReaderBuilder
import java.io.File
import java.io.IOException
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.PersonKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.VideoKt.viewedFractionField
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.video
import org.wfanet.measurement.common.toProtoTime

private const val EDP_ID_INDEX = 0
private const val GENDER_INDEX = 2
private const val AGE_GROUP_INDEX = 3
private const val SOCIAL_GRADE_INDEX = 4
private const val DATE_INDEX = 5
private const val COMPLETE_INDEX = 6
private const val VID_INDEX = 7

/** Fulfill the query with VIDs imported from CSV file. */
class CsvEventQuery(publisherId: Int, file: File) :
  InMemoryEventQuery(readCsvFile(publisherId, file)) {

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.UK)

    @Throws(IOException::class)
    private fun readCsvFile(publisherId: Int, file: File): List<LabelledEvent> {
      logger.info("Reading data from CSV file: $file...")

      return file.reader().use { fileReader ->
        val csvReader: CSVReader = CSVReaderBuilder(fileReader).withSkipLines(1).build()
        csvReader
          .iterator()
          .asSequence()
          .filter { row -> row[EDP_ID_INDEX].toInt() == publisherId }
          .map { row ->
            val event: TestEvent = parseTestEvent(row)
            LabelledEvent(event.person.vid, event)
          }
          .toList()
      }
    }

    private fun parseTestEvent(row: Array<String>): TestEvent {
      val vid = row[VID_INDEX].toLong()
      val gender: Person.Gender? =
        when (row[GENDER_INDEX]) {
          "M" -> Person.Gender.MALE
          "F" -> Person.Gender.FEMALE
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
      val complete: Boolean? =
        when (row[COMPLETE_INDEX].toIntOrNull()) {
          0 -> false
          1 -> true
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
          if (gender != null) {
            this.gender = PersonKt.genderField { value = gender }
          }
          if (ageGroup != null) {
            this.ageGroup = PersonKt.ageGroupField { value = ageGroup }
          }
          if (socialGradeGroup != null) {
            socialGrade = PersonKt.socialGradeGroupField { value = socialGradeGroup }
          }
        }
        videoAd = video {
          if (complete != null) {
            viewedFraction = viewedFractionField {
              value =
                if (complete) {
                  1.0
                } else {
                  0.0
                }
            }
          }
        }
      }
    }
  }
}
