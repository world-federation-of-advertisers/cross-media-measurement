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
import org.halo_cmm.uk.pilot.Event
import org.halo_cmm.uk.pilot.display
import org.halo_cmm.uk.pilot.event
import org.halo_cmm.uk.pilot.video
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toRange
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.gcloud.common.toInstant

/** Fulfill the query by querying the specified BigQuery table. */
abstract class BigQueryEventQuery(
  private val bigQuery: BigQuery,
  private val datasetName: String,
  private val tableName: String,
) : EventQuery<Event> {
  protected abstract fun getPublisherId(eventGroup: EventGroup): Int

  override fun getLabeledEvents(
    eventGroupSpec: EventQuery.EventGroupSpec
  ): Sequence<LabeledEvent<Event>> {
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

    val program = EventQuery.compileProgram(eventGroupSpec.spec.filter, Event.getDescriptor())

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
    timeRange: OpenEndTimeRange,
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
          QueryParameterValue.timestamp(timeRange.endExclusive.epochMicros),
        )
      }
      .build()
  }

  private fun FieldValueList.toLabeledEvent(): LabeledEvent<Event> {
    val viewability = get("viewability").stringValue
    val completionStatus = get("digital_video_completion_status").stringValue
    val message = event {
      video = video {
        when (completionStatus) {
          "0% - 25%" -> completed0PercentPlus = true
          "25% - 50%" -> {
            completed0PercentPlus = true
            completed25PercentPlus = true
          }
          "50% - 75%" -> {
            completed0PercentPlus = true
            completed25PercentPlus = true
            completed50PercentPlus = true
          }
          "75% - 100%" -> {
            completed0PercentPlus = true
            completed25PercentPlus = true
            completed50PercentPlus = true
            completed75PercentPlus = true
          }
          "100%" -> {
            completed0PercentPlus = true
            completed25PercentPlus = true
            completed50PercentPlus = true
            completed75PercentPlus = true
            completed100Percent = true
          }
          else -> error("Unknown video completion status :  $completionStatus")
        }
        when (viewability) {
          "viewable_0_percent_to_50_percent" -> viewable0PercentPlus = true
          "viewable_50_percent_to_100_percent" -> {
            viewable0PercentPlus = true
            viewable50PercentPlus = true
          }
          "viewable_100_percent" -> {
            viewable0PercentPlus = true
            viewable50PercentPlus = true
            viewable100Percent = true
          }
          else -> error("Unknown video viewability :  $viewability")
        }
      }
      display = display {
        when (viewability) {
          "viewable_0_percent_to_50_percent" -> viewable0PercentPlus = true
          "viewable_50_percent_to_100_percent" -> {
            viewable0PercentPlus = true
            viewable50PercentPlus = true
          }
          "viewable_100_percent" -> {
            viewable0PercentPlus = true
            viewable50PercentPlus = true
            viewable100Percent = true
          }
          else -> throw Exception("Unknown display viewability :  $viewability")
        }
      }
    }

    return LabeledEvent(
      Timestamp.ofTimeMicroseconds(get("time").timestampValue).toInstant(),
      get("vid").longValue,
      message,
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
  private val publisherId: Int,
) : BigQueryEventQuery(bigQuery, datasetName, tableName) {
  override fun getPublisherId(eventGroup: EventGroup): Int {
    return publisherId
  }
}

private val Instant.epochMicros: Long
  get() = ChronoUnit.MICROS.between(Instant.EPOCH, this)
