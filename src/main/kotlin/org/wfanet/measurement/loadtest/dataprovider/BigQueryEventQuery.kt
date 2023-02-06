// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.dataprovider

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryError
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.QueryParameterValue
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.UUID
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.api.v2alpha.TimeInterval
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.PersonKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.VideoKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.video
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/** Fulfill the query by querying the specified BigQuery table. */
class BigQueryEventQuery(
  private val bigQuery: BigQuery,
  private val datasetName: String,
  private val tableName: String,
  private val publisherId: Int,
) : EventQuery {

  override fun getUserVirtualIds(
    timeInterval: TimeInterval,
    eventFilter: EventFilter
  ): Sequence<Long> {
    val queryConfig =
      buildQueryConfig(
        publisherId = publisherId,
        startTime = timeInterval.startTime.toInstant(),
        endTimeExclusive = timeInterval.endTime.toInstant(),
      )

    bigQuery.query(queryConfig)

    val jobId: JobId = JobId.of(UUID.randomUUID().toString())
    val queryJob: Job = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
    logger.info("Connected to BigQuery Successfully.")

    val resultJob: Job = queryJob.waitFor() ?: error("Query job no longer exists")
    val queryError: BigQueryError? = resultJob.status.error
    if (queryError != null) {
      error("Query job failed with $queryError")
    }
    logger.info("Running query on BigQuery table.")

    val program = EventQuery.compileProgram(eventFilter, TestEvent.getDescriptor())

    return resultJob
      .getQueryResults()
      .iterateAll()
      .asSequence()
      .map { it.toTestEvent() }
      .filter { EventFilters.matches(it, program) }
      .map { it.person.vid }
  }

  /** Builds a query based on the parameters given. */
  private fun buildQueryConfig(
    publisherId: Int,
    startTime: Instant,
    endTimeExclusive: Instant
  ): QueryJobConfiguration {
    val query =
      """
      SELECT *, CAST(date AS TIMESTAMP) AS time
      FROM `$datasetName.$tableName`
      WHERE publisher_id = @publisher_id
        AND CAST(date AS TIMESTAMP) >= @start_time AND CAST(date AS TIMESTAMP) < @end_time_exclusive
      """
        .trimIndent()

    return QueryJobConfiguration.newBuilder(query)
      .apply {
        addNamedParameter("publisher_id", QueryParameterValue.int64(publisherId))
        addNamedParameter(
          "start_time",
          QueryParameterValue.timestamp(timestampFormatter.format(startTime))
        )
        addNamedParameter(
          "end_time_exclusive",
          QueryParameterValue.timestamp(timestampFormatter.format(endTimeExclusive))
        )
      }
      .build()
  }

  private fun FieldValueList.toTestEvent(): TestEvent {
    val gender: Person.Gender? =
      when (get("sex").stringValue) {
        "M" -> Person.Gender.MALE
        "F" -> Person.Gender.FEMALE
        else -> null
      }
    val ageGroup: Person.AgeGroup? =
      when (get("age_group").stringValue) {
        "18_34" -> Person.AgeGroup.YEARS_18_TO_34
        "35_54" -> Person.AgeGroup.YEARS_35_TO_54
        "55+" -> Person.AgeGroup.YEARS_55_PLUS
        else -> null
      }
    val socialGradeGroup: Person.SocialGradeGroup? =
      when (get("social_grade").stringValue) {
        "ABC1" -> Person.SocialGradeGroup.A_B_C1
        "C2DE" -> Person.SocialGradeGroup.C2_D_E
        else -> null
      }
    val complete: Boolean =
      when (get("complete").longValue) {
        0L -> false
        else -> true
      }
    return testEvent {
      time = timestampFormatter.parse(get("time").stringValue, Instant::from).toProtoTime()
      person = person {
        vid = get("vid").longValue
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
        viewedFraction =
          VideoKt.viewedFractionField {
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val timestampFormatter: DateTimeFormatter =
      DateTimeFormatterBuilder()
        .parseLenient()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .appendLiteral(' ')
        .appendValue(ChronoField.HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
        .optionalStart()
        .appendFraction(ChronoField.NANO_OF_SECOND, 6, 9, true)
        .optionalStart()
        .appendOffset("+HHMM", "+00:00")
        .optionalEnd()
        .toFormatter()
        .withZone(ZoneOffset.UTC)
  }
}
