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

import com.google.common.truth.Truth.assertThat
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneOffset
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.toProtoTime

private val directoryPath: Path =
  Paths.get(
    "wfa_measurement_system",
    "src",
    "test",
    "kotlin",
    "org",
    "wfanet",
    "measurement",
    "loadtest",
    "dataprovider",
  )
private const val FILE_NAME = "CsvEventQueryTestEvents.csv"
private val FILE: File = getRuntimePath(directoryPath.resolve(FILE_NAME))!!.toFile()

private const val PUBLISHER_ID_1 = 1
private const val PERSON_35_to_54 = Person.AgeGroup.YEARS_35_TO_54_VALUE
private const val PERSON_MALE = Person.Gender.MALE_VALUE
private const val PERSON_FEMALE = Person.Gender.FEMALE_VALUE

private val EMPTY_EVENT_FILTER = RequisitionSpec.EventFilter.getDefaultInstance()

private val FIRST_EVENT_DATE = LocalDate.of(2021, 4, 20)
private val LAST_EVENT_DATE = FIRST_EVENT_DATE.plusDays(1)
private val FULL_TIME_INTERVAL = timeInterval {
  startTime = FIRST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
  // end_time is exclusive, so we specify the start of the day after LAST_EVENT_DATE.
  endTime = LAST_EVENT_DATE.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
}

private val ALL_VIDS: List<Long> =
  listOf(1000077, 1000650, 1000694, 1000759, 1000840, 1000997, 1001028, 1001096, 1001096, 1001289)

@RunWith(JUnit4::class)
class CsvEventQueryTest {
  @Test
  fun `getUserVirtualIds excludes events outside of time interval`() {
    val userVids: Sequence<Long> =
      eventQuery.getUserVirtualIds(
        FULL_TIME_INTERVAL.copy {
          // end_time is exclusive, so this should exclude all events on or after LAST_EVENT_DATE.
          endTime = LAST_EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
        },
        EMPTY_EVENT_FILTER
      )

    assertThat(userVids.toList())
      .containsExactly(1000077L, 1000650L, 1000759L, 1000997L, 1001096L, 1001096L)
  }

  @Test
  fun `getUserVirtualIds return empty when no events match filter expression`() {
    val userVids: Sequence<Long> =
      eventQuery.getUserVirtualIds(
        FULL_TIME_INTERVAL,
        eventFilter {
          expression =
            "person.gender.value == $PERSON_MALE && person.gender.value == $PERSON_FEMALE"
        }
      )

    assertThat(userVids.toList()).isEmpty()
  }

  @Test
  fun `getUserVirtualIds returns VIDs for matching events`() {
    val userVids: Sequence<Long> =
      eventQuery.getUserVirtualIds(
        FULL_TIME_INTERVAL,
        eventFilter {
          expression =
            "person.age_group.value == $PERSON_35_to_54 && person.gender.value == $PERSON_FEMALE"
        }
      )

    assertThat(userVids.toList()).containsExactly(1001096L, 1001096L)
  }

  @Test
  fun `getUserVirtualIds returns VIDs for all events when filter is empty`() {
    val vids = eventQuery.getUserVirtualIds(FULL_TIME_INTERVAL, EMPTY_EVENT_FILTER)

    assertThat(vids.toList()).containsExactlyElementsIn(ALL_VIDS)
  }

  companion object {
    private val eventQuery = CsvEventQuery(PUBLISHER_ID_1, FILE)
  }
}
