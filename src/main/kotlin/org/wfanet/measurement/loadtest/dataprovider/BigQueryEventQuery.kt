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

import com.google.cloud.Timestamp
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryError
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.QueryParameterValue
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.video
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toRange
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.gcloud.common.toInstant

/** Fulfill the query by querying the specified BigQuery table. */
abstract class BigQueryEventQuery(
  private val bigQuery: BigQuery,
  private val datasetName: String,
  private val tableName: String,
) : EventQuery<TestEvent> {
  protected abstract fun getPublisherId(eventGroup: EventGroup): Int

  override fun getLabeledEvents(
    eventGroupSpec: EventQuery.EventGroupSpec
  ): Sequence<LabeledEvent<TestEvent>> {
    val timeRange: OpenEndTimeRange = eventGroupSpec.spec.collectionInterval.toRange()
    val queryConfig: QueryJobConfiguration =
      buildQueryConfig(getPublisherId(eventGroupSpec.eventGroup), timeRange)

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

    val program = EventQuery.compileProgram(eventGroupSpec.spec.filter, TestEvent.getDescriptor())

    return resultJob
      .getQueryResults()
      .iterateAll()
      .asSequence()
      .map { it.toLabeledEvent() }
      .filter { EventFilters.matches(it.message, program) }
  }

  /** Builds a query based on the parameters given. */
  private fun buildQueryConfig(
    publisherId: Int,
    timeRange: OpenEndTimeRange
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
        addNamedParameter("start_time", QueryParameterValue.timestamp(timeRange.start.epochMicros))
        addNamedParameter(
          "end_time_exclusive",
          QueryParameterValue.timestamp(timeRange.endExclusive.epochMicros)
        )
      }
      .build()
  }

  private fun FieldValueList.toLabeledEvent(): LabeledEvent<TestEvent> {
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
    val message = testEvent {
      person = person {
        if (gender != null) {
          this.gender = gender
        }
        if (ageGroup != null) {
          this.ageGroup = ageGroup
        }
        if (socialGradeGroup != null) {
          this.socialGradeGroup = socialGradeGroup
        }
      }
      videoAd = video {
        viewedFraction =
          if (complete) {
            1.0
          } else {
            0.0
          }
      }
    }
    return LabeledEvent(
      Timestamp.ofTimeMicroseconds(get("time").timestampValue).toInstant(),
      get("vid").longValue,
      message
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

class SinglePublisherBigQueryEventQuery(
  bigQuery: BigQuery,
  datasetName: String,
  tableName: String,
  private val publisherId: Int
) : BigQueryEventQuery(bigQuery, datasetName, tableName) {
  override fun getPublisherId(eventGroup: EventGroup): Int {
    return publisherId
  }
}

private val Instant.epochMicros: Long
  get() = ChronoUnit.MICROS.between(Instant.EPOCH, this)
