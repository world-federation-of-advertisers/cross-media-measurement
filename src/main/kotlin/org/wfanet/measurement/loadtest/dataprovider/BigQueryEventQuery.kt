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
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import com.google.protobuf.Message
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender as BannerGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplateKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange as PrivacyAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.Gender as PrivacyGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.SocialGrade as PrivacySocialGrade
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.AgeRange as VideoAgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplateKt
import com.google.cloud.bigquery.FieldValueList
import com.google.type.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlin.math.log
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testVideoTemplate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/** The mapping from the EDP Display name to the Publisher Id in the synthetic data. */
private val DISPLAY_NAME_TO_PUBLISHER_MAP = (1..6).associateBy { "edp$it" }

/** TODO(@uakyol): Delete once the GCS correctness test supports [EventFilter]s */
private val DEFAULT_QUERY_PARAMETER =
  QueryParameter(
    beginDate = "2021-03-01",
    endDate = "2021-03-28",
    sex = Sex.FEMALE,
    ageGroup = null,
    socialGrade = SocialGrade.ABC1,
    complete = Complete.COMPLETE
  )

/** Fulfill the query by querying the specified BigQuery table. */
class BigQueryEventQuery(
  private val edpDisplayName: String,
  private val bigQuery: BigQuery,
  private val tableName: String,
) : EventQuery() {

  /**
   * Converts [eventFilter] to a select statement and fetches the virtual ids from [BigQuery],
   *
   * TODO(@uakyol): Use [eventFiltbiger] rather than DEFAULT_QUERY_PARAMETER once the GCS
   * correctness test supports [EventFilter]s
   */
  private val eventsList: MutableList<TestEvent> = mutableListOf()
  private val vidsList: MutableList<Int> = mutableListOf()
  private val valueMap: Map<String, String> = mutableMapOf()

  override fun getUserVirtualIds(eventFilter: EventFilter): Sequence<Long> {
    val publisher =
      DISPLAY_NAME_TO_PUBLISHER_MAP[edpDisplayName]
        ?: error("EDP $edpDisplayName not in the test data.")
    // val queryConfig =
    //   buildQueryConfig(
    //     publisher = publisher,
    //     beginDate = DEFAULT_QUERY_PARAMETER.beginDate,
    //     endDate = DEFAULT_QUERY_PARAMETER.endDate,
    //     sex = DEFAULT_QUERY_PARAMETER.sex,
    //     ageGroup = DEFAULT_QUERY_PARAMETER.ageGroup,
    //     socialGrade = DEFAULT_QUERY_PARAMETER.socialGrade,
    //     complete = DEFAULT_QUERY_PARAMETER.complete,
    //   )
    val queryConfig = buildGeneralQueryConfig(eventFilter, publisher)

    bigQuery.query(queryConfig)

    val jobId: JobId = JobId.of(UUID.randomUUID().toString())
    var queryJob: Job = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
    logger.info("Connected to BigQuery Successfully.")

    queryJob = queryJob.waitFor()
    if (queryJob == null || queryJob.status.error != null) {
      throw RuntimeException("Error running query.")
    }
    logger.info("Running query on BigQuery table.")

    queryJob.getQueryResults().iterateAll().forEach {
      this.vidsList.add(it.get("VID").stringValue.toInt())
      this.eventsList.add(fieldValuesToTestEvent(it))
    }

    val program =
      EventFilters.compileProgram(
        eventFilter.expression,
        testEvent {},
      )

    return sequence {
      this@BigQueryEventQuery.eventsList.zip(this@BigQueryEventQuery.vidsList) { event, vid ->
        if (EventFilters.matches(event as Message, program)) {
          yield(vid.toLong())
        }
      }
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

  private fun buildGeneralQueryConfig(
    eventFilter: EventFilter,
    publisher : Int
  ): QueryJobConfiguration {
    var query =
      """
      SELECT vid
      FROM `$tableName`
      WHERE publisher_id = $publisher
      """.trimIndent()

    return QueryJobConfiguration.newBuilder(query).build()
  }


  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    fun fieldValuesToTestEvent(fieldValues: FieldValueList): TestEvent {
      return testEvent {
        this.privacyBudget = testPrivacyBudgetTemplate {
          when (fieldValues.get("Sex").stringValue) {
            "M" -> gender = TestPrivacyBudgetTemplateKt.gender { value = PrivacyGender.Value.GENDER_MALE }
            "F" -> gender = TestPrivacyBudgetTemplateKt.gender { value = PrivacyGender.Value.GENDER_FEMALE }
          }
          when (fieldValues.get("Age Group").stringValue) {
            "18_34" -> age = TestPrivacyBudgetTemplateKt.ageRange { value = PrivacyAgeRange.Value.AGE_18_TO_24 }
            "35_54" -> age = TestPrivacyBudgetTemplateKt.ageRange { value = PrivacyAgeRange.Value.AGE_35_TO_54 }
            "55+" -> age = TestPrivacyBudgetTemplateKt.ageRange { value = PrivacyAgeRange.Value.AGE_OVER_54 }
          }
          when (val publisherId = fieldValues.get("Publisher ID").stringValue.toIntOrNull()) {
            is Int -> publisher = publisherId
          }
          when (fieldValues.get("Social Grade").stringValue) {
            "ABC1" -> socialGrade = TestPrivacyBudgetTemplateKt.socialGrade { value = PrivacySocialGrade.Value.ABC1 }
            "C2DE" -> socialGrade = TestPrivacyBudgetTemplateKt.socialGrade { value = PrivacySocialGrade.Value.C2DE }
          }
          val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
          var eventDate : LocalDate?
          eventDate = try {
            LocalDate.parse(fieldValues.get("Date").stringValue, formatter)
          } catch (e: Exception) {
            null
          }
          when (eventDate) {
            is LocalDate -> date = eventDate.toProtoDate()
          }
          when (fieldValues.get("Complete").stringValue) {
            "1" -> complete = true
            "0" -> complete = false
          }
        }
        this.videoAd = testVideoTemplate {
          when (fieldValues.get("Age Group").stringValue) {
            "18_34" -> age = TestVideoTemplateKt.ageRange { value = VideoAgeRange.Value.AGE_18_TO_24 }
            else -> age = TestVideoTemplateKt.ageRange { value = VideoAgeRange.Value.AGE_RANGE_UNSPECIFIED }
          }
        }
        this.bannerAd = testBannerTemplate {
          when (fieldValues.get("Sex").stringValue) {
            "M" -> gender = TestBannerTemplateKt.gender { value = BannerGender.Value.GENDER_MALE }
            "F" -> gender = TestBannerTemplateKt.gender { value = BannerGender.Value.GENDER_FEMALE }
            else -> gender = TestBannerTemplateKt.gender { value = BannerGender.Value.GENDER_UNKOWN }
          }
        }
      }
    }
  }
}
