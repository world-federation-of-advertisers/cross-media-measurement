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
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.logging.Logger
import org.halo_cmm.uk.pilot.Display.Viewability as DisplayViewability
import org.halo_cmm.uk.pilot.DisplayKt.viewability as displayViewability
import org.halo_cmm.uk.pilot.Event
import org.halo_cmm.uk.pilot.Video.DigitalVideoCompletionStatus
import org.halo_cmm.uk.pilot.Video.Viewability as VideoViewability
import org.halo_cmm.uk.pilot.VideoKt.digitalVideoCompletionStatus
import org.halo_cmm.uk.pilot.VideoKt.viewability as videoViewability
import org.halo_cmm.uk.pilot.display
import org.halo_cmm.uk.pilot.event
import org.halo_cmm.uk.pilot.video
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toRange
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/** Fulfill the query by querying the specified BigQuery table. */
class BigQueryEventQuery(
  private val bigQuery: BigQuery,
  private val datasetName: String,
  private val tableName: String,
  private val publisherId: Int,
) : EventQuery {

  override fun getUserVirtualIds(eventGroupSpec: EventQuery.EventGroupSpec): Sequence<Long> {
    val timeRange: OpenEndTimeRange = eventGroupSpec.spec.collectionInterval.toRange()
    val queryConfig: QueryJobConfiguration = buildQueryConfig(publisherId, timeRange)

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
      .map { it.toVidAndEvent() }
      .filter { EventFilters.matches(it.event, program) }
      .map { it.vid }
  }

  /** Builds a query based on the parameters given. */
  private fun buildQueryConfig(
    publisherId: Int,
    timeRange: OpenEndTimeRange
  ): QueryJobConfiguration {
    // Please make sure to correctly cast and query date column. The filtering on date column is
    // done here and not in the event templates.
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

  private fun getDigitalVideoCompletionStatus(
    completionStatus: String
  ): DigitalVideoCompletionStatus? {
    return when (completionStatus) {
      "0% - 25%" -> digitalVideoCompletionStatus { completed0PercentPlus = true }
      "25% - 50%" ->
        digitalVideoCompletionStatus {
          completed0PercentPlus = true
          completed25PercentPlus = true
        }
      "50% - 75%" ->
        digitalVideoCompletionStatus {
          completed0PercentPlus = true
          completed25PercentPlus = true
          completed50PercentPlus = true
        }
      "75% - 100%" ->
        digitalVideoCompletionStatus {
          completed0PercentPlus = true
          completed25PercentPlus = true
          completed50PercentPlus = true
          completed75PercentPlus = true
        }
      "100%" ->
        digitalVideoCompletionStatus {
          completed0PercentPlus = true
          completed25PercentPlus = true
          completed50PercentPlus = true
          completed75PercentPlus = true
          completed100Percent = true
        }
      else -> null
    }
  }

  private fun getVideoViewability(viewability: String): VideoViewability? {
    return when (viewability) {
      "viewable_0_percent_to_50_percent" -> videoViewability { viewable0PercentPlus = true }
      "viewable_50_percent_to_100_percent" ->
        videoViewability {
          viewable0PercentPlus = true
          viewable50PercentPlus = true
        }
      "viewable_100_percent" ->
        videoViewability {
          viewable0PercentPlus = true
          viewable50PercentPlus = true
          viewable100Percent = true
        }
      else -> null
    }
  }

  private fun getDisplayViewability(viewability: String): DisplayViewability? {
    return when (viewability) {
      "viewable_0_percent_to_50_percent" -> displayViewability { viewable0PercentPlus = true }
      "viewable_50_percent_to_100_percent" ->
        displayViewability {
          viewable0PercentPlus = true
          viewable50PercentPlus = true
        }
      "viewable_100_percent" ->
        displayViewability {
          viewable0PercentPlus = true
          viewable50PercentPlus = true
          viewable100Percent = true
        }
      else -> null
    }
  }

  private fun FieldValueList.toVidAndEvent(): VidAndEvent {
    val digitalVideoCompletion =
      getDigitalVideoCompletionStatus(get("digital_video_completion_status").stringValue)
    val videoViewability = getVideoViewability(get("viewability").stringValue)
    val displayViewability = getDisplayViewability(get("viewability").stringValue)
    val event = event {
      video = video {
        if (digitalVideoCompletion != null) {
          digitalVideoCompletionStatus = digitalVideoCompletion
        }
        if (videoViewability != null) {
          viewability = videoViewability
        }
      }
      display = display {
        if (displayViewability != null) {
          viewability = displayViewability
        }
      }
    }
    return VidAndEvent(get("vid").longValue, event)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

data class VidAndEvent(val vid: Long, val event: Event)

private val Instant.epochMicros: Long
  get() = ChronoUnit.MICROS.between(Instant.EPOCH, this)
