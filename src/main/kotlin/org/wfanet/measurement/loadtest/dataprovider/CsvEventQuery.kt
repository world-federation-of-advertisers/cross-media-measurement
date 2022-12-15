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
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender as BannerGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplateKt as TestBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange as PrivacyAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.Gender as PrivacyGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt as TestPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.AgeRange as VideoAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplateKt as TestVideoTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testVideoTemplate
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

private const val SEX = "Sex"
private const val AGE_GROUP = "Age_Group"

private const val EDP_ID_INDEX = 0
private const val SEX_INDEX = 2
private const val AGE_GROUP_INDEX = 3
private const val DATE_INDEX = 5
private const val VID_INDEX = 7

data class LabelledEvent(val vid: Long, val event: TestEvent, val date: LocalDate)

/** Fulfill the query with VIDs imported from CSV file. */
class CsvEventQuery(private val publisherId: Int, private val file: File) : EventQuery {
  private val labelledEventList: List<LabelledEvent> by lazy { readCsvFile() }

  @Throws(IOException::class)
  private fun readCsvFile(): List<LabelledEvent> {
    val vidsAndEvents: MutableList<LabelledEvent> = mutableListOf()

    logger.info("Reading data from CSV file: $file...")
    file.reader().use { fileReader ->
      val csvReader = CSVReaderBuilder(fileReader).withSkipLines(1).build()
      csvReader.forEach { row ->
        if (row[EDP_ID_INDEX].toInt() == publisherId) {
          val csvEventMap = mapOf(SEX to row[SEX_INDEX], AGE_GROUP to row[AGE_GROUP_INDEX])
          vidsAndEvents.add(
            LabelledEvent(
              row[VID_INDEX].toLong(),
              csvEntryToTestEvent(csvEventMap),
              LocalDate.parse(row[DATE_INDEX], dateFormatter)
            )
          )
        }
      }
    }

    logger.info("Finished reading data from CSV file")
    return vidsAndEvents
  }

  /** Generates Ids by applying filter on events */
  override fun getUserVirtualIds(
    timeInterval: TimeInterval,
    eventFilter: EventFilter
  ): Sequence<Long> {
    logger.info("Querying and filtering VIDs from CsvEventQuery...")

    val timeRange = TimeIntervalRange(timeInterval)
    val program = EventQuery.compileProgram(eventFilter, TestEvent.getDefaultInstance())

    return labelledEventList
      .asSequence()
      .filter {
        it.date.atStartOfDay().toInstant(ZoneOffset.UTC) in timeRange &&
          EventFilters.matches(it.event, program)
      }
      .map { it.vid }
  }

  private fun csvEntryToTestEvent(event: Map<String, Any>): TestEvent {
    return testEvent {
      this.privacyBudget = testPrivacyBudgetTemplate {
        when (event[SEX]) {
          "M" ->
            gender = TestPrivacyBudgetTemplate.gender { value = PrivacyGender.Value.GENDER_MALE }
          "F" ->
            gender = TestPrivacyBudgetTemplate.gender { value = PrivacyGender.Value.GENDER_FEMALE }
          else ->
            gender =
              TestPrivacyBudgetTemplate.gender { value = PrivacyGender.Value.GENDER_UNSPECIFIED }
        }
        when (event[AGE_GROUP]) {
          "18_34" ->
            age = TestPrivacyBudgetTemplate.ageRange { value = PrivacyAgeRange.Value.AGE_18_TO_34 }
          "35_54" ->
            age = TestPrivacyBudgetTemplate.ageRange { value = PrivacyAgeRange.Value.AGE_35_TO_54 }
          "55+" ->
            age = TestPrivacyBudgetTemplate.ageRange { value = PrivacyAgeRange.Value.AGE_OVER_54 }
          else ->
            age =
              TestPrivacyBudgetTemplate.ageRange {
                value = PrivacyAgeRange.Value.AGE_RANGE_UNSPECIFIED
              }
        }
      }
      this.videoAd = testVideoTemplate {
        when (event[AGE_GROUP]) {
          "18_34" -> age = TestVideoTemplate.ageRange { value = VideoAgeRange.Value.AGE_18_TO_34 }
          else ->
            age = TestVideoTemplate.ageRange { value = VideoAgeRange.Value.AGE_RANGE_UNSPECIFIED }
        }
      }
      this.bannerAd = testBannerTemplate {
        when (event[SEX]) {
          "M" -> gender = TestBannerTemplate.gender { value = BannerGender.Value.GENDER_MALE }
          "F" -> gender = TestBannerTemplate.gender { value = BannerGender.Value.GENDER_FEMALE }
          else ->
            gender = TestBannerTemplate.gender { value = BannerGender.Value.GENDER_UNSPECIFIED }
        }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.UK)
  }
}
