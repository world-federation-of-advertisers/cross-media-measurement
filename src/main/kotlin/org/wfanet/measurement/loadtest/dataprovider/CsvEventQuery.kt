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
import com.opencsv.CSVReaderBuilder
import java.io.FileReader
import java.nio.file.Paths
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
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
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

private const val SEX = "Sex"
private const val AGE_GROUP = "Age_Group"

/** Fulfill the query with VIDs imported from CSV file. */
class CsvEventQuery(
  private val edpDisplayName: String,
) : EventQuery() {
  private val edpIdIndex = 0
  private val sexIndex = 2
  private val ageGroupIndex = 3
  private val vidIndex = 7

  private val vidsList: MutableList<Int> = mutableListOf()
  private val eventsList: MutableList<TestEvent> = mutableListOf()

  /** Import VIDs from CSV file and creates events list. */
  init {
    // Get path for CSV files in //src/main/k8s/testing/data
    val directoryPath =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "k8s",
        "testing",
        "data",
      )

    val fileName = "synthetic-labelled-events.csv"

    val fileRuntimePath = getRuntimePath(directoryPath.resolve(fileName)).toString()
    logger.info("Reading data from CSV file...")
    val fileReader = FileReader(fileRuntimePath)

    fileReader.use {
      val csvReader = CSVReaderBuilder(fileReader).withSkipLines(1).build()
      csvReader.use { reader ->
        var row = reader.readNext()
        while (row != null) {
          if (row[edpIdIndex] == edpDisplayName.last().toString()) {
            val csvEventMap = mapOf(SEX to row[sexIndex], AGE_GROUP to row[ageGroupIndex])
            vidsList.add(row[vidIndex].toInt())
            eventsList.add(csvEntryToTestEvent(csvEventMap))
          }

          row = reader.readNext()
        }
      }
    }
    logger.info("Finished reading data from CSV file")
  }

  /** Generates Ids by applying filter on events */
  override fun getUserVirtualIds(eventFilter: EventFilter): Sequence<Long> {
    logger.info("Querying and filtering VIDs from CsvEventQuery...")
    val program =
      EventFilters.compileProgram(
        eventFilter.expression,
        testEvent {},
      )

    return sequence {
      this@CsvEventQuery.eventsList.zip(this@CsvEventQuery.vidsList) { event, vid ->
        if (EventFilters.matches(event as Message, program)) {
          yield(vid.toLong())
        }
      }
    }
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

  /** This function is to simulate reading CSV data in the unit test */
  fun readCSVData(csvEvents: List<Map<String, Any>>) {
    this.eventsList.clear()
    this.vidsList.clear()
    csvEvents.forEach {
      this.vidsList.add(it["VID"] as Int)
      this.eventsList.add(csvEntryToTestEvent(it))
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
