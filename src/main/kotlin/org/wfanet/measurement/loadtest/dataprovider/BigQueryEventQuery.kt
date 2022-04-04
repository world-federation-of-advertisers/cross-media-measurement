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
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.QueryParameterValue
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.yield

/** The mapping from the EDP Display name to the Publisher Id in the synthetic data. */
private val DISPLAY_NAME_TO_PUBLISHER_MAP = (1..6).associateBy { "edp$it" }

/** Fulfill the query by querying the specified BigQuery table. */
class BiqQueryEventQuery(
  private val bigQuery: BigQuery,
  private val tableName: String,
) : EventQuery() {

  lateinit var edpDisplayName : String
    public set

  override fun getUserVirtualIds(parameter: QueryParameter): Sequence<Long> {
    val publisher =
      DISPLAY_NAME_TO_PUBLISHER_MAP[parameter.edpDisplayName]
        ?: error("EDP ${parameter.edpDisplayName} not in the test data.")
    val queryConfig =
      buildQueryConfig(
        publisher = publisher,
        beginDate = parameter.beginDate,
        endDate = parameter.endDate,
        sex = parameter.sex,
        ageGroup = parameter.ageGroup,
        socialGrade = parameter.socialGrade,
        complete = parameter.complete,
      )

    bigQuery.query(queryConfig)

    val jobId: JobId = JobId.of(UUID.randomUUID().toString())
    var queryJob: Job = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
    logger.info("Connected to BigQuery Successfully.")

    queryJob = queryJob.waitFor()
    if (queryJob == null || queryJob.status.error != null) {
      throw RuntimeException("Error running query.")
    }
    logger.info("Running query on BigQuery table.")

    return sequence {
      queryJob.getQueryResults().iterateAll().forEach { yield(it.get("vid").longValue) }
    }
  }

  // Builds a query based on the parameters given.
  private fun buildQueryConfig(
    publisher: Int,
    beginDate: String,
    endDate: String,
    sex: Sex?,
    ageGroup: AgeGroup?,
    socialGrade: SocialGrade?,
    complete: Complete?
  ): QueryJobConfiguration {
    var query =
      """
      SELECT vid
      FROM `$tableName`
      WHERE publisher_id = $publisher
      AND date BETWEEN @begin_date AND @end_date
      """.trimIndent()
    if (sex != null) {
      query += " AND sex = \"${sex.string}\" "
    }
    if (ageGroup != null) {
      query += " AND age_group = \"${ageGroup.string}\" "
    }
    if (socialGrade != null) {
      query += " AND social_grade = \"${socialGrade.string}\" "
    }
    if (complete != null) {
      query += " AND complete = ${complete.integer} "
    }

    val queryConfig: QueryJobConfiguration =
      QueryJobConfiguration.newBuilder(query)
        .apply {
          addNamedParameter("begin_date", QueryParameterValue.date(beginDate))
          addNamedParameter("end_date", QueryParameterValue.date(endDate))
        }
        .build()
    return queryConfig
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
