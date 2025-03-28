/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.SpannerException
import com.google.protobuf.Timestamp
import io.grpc.Status
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.postgres.SerializableErrors.withSerializableErrorRetries
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.GetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.InsertBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.BasicReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.MeasurementConsumerResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.basicReportExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getBasicReportByExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getMeasurementConsumerByCmmsMeasurementConsumerId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertBasicReport
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertMeasurementConsumer
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.measurementConsumerExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readBasicReports
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.BasicReportAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.BasicReportNotFoundException
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.service.internal.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException
import org.wfanet.measurement.reporting.service.internal.RequiredFieldNotSetException

class SpannerBasicReportsService(
  private val spannerClient: AsyncDatabaseClient,
  private val postgresClient: DatabaseClient,
  private val impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : BasicReportsCoroutineImplBase() {
  override suspend fun getBasicReport(request: GetBasicReportRequest): BasicReport {
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.externalBasicReportId.isEmpty()) {
      throw RequiredFieldNotSetException("external_basic_report_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val basicReportResult: BasicReportResult =
      try {
        spannerClient.singleUse().use { txn ->
          txn.getBasicReportByExternalId(
            cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId,
            externalBasicReportId = request.externalBasicReportId,
          )
        }
      } catch (e: BasicReportNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    val reportingSetResult: ReportingSetReader.Result =
      try {
        getReportingSets(
            request.cmmsMeasurementConsumerId,
            listOf(basicReportResult.basicReport.externalCampaignGroupId),
          )
          .first()
      } catch (e: ReportingSetNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.INTERNAL)
      }

    return basicReportResult.basicReport.copy {
      campaignGroupDisplayName = reportingSetResult.reportingSet.displayName
    }
  }

  override suspend fun listBasicReports(
    request: ListBasicReportsRequest
  ): ListBasicReportsResponse {
    if (request.filter.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("filter.cmms_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      if (request.pageSize > 0) {
        if (request.pageSize > MAX_PAGE_SIZE) {
          MAX_PAGE_SIZE
        } else {
          request.pageSize
        }
      } else if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        throw InvalidFieldValueException("page_size")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }

    val pageToken: ListBasicReportsPageToken? =
      if (request.hasPageToken()) {
        request.pageToken
      } else {
        null
      }

    val basicReports =
      spannerClient.singleUse().use { txn ->
        txn
          .readBasicReports(pageSize + 1, request.filter, pageToken)
          .map { it.basicReport }
          .toList()
      }

    val reportingSetsByExternalId: Map<String, ReportingSetReader.Result> = buildMap {
      putAll(
        getReportingSets(
            request.filter.cmmsMeasurementConsumerId,
            basicReports.map { it.externalCampaignGroupId }.distinct(),
          )
          .associateBy { it.reportingSet.externalReportingSetId }
      )
    }

    return listBasicReportsResponse {
      if (basicReports.size == pageSize + 1) {
        this.basicReports +=
          basicReports.subList(0, basicReports.lastIndex).map {
            it.copy {
              campaignGroupDisplayName =
                reportingSetsByExternalId
                  .getValue(it.externalCampaignGroupId)
                  .reportingSet
                  .displayName
            }
          }

        nextPageToken = listBasicReportsPageToken {
          cmmsMeasurementConsumerId = request.filter.cmmsMeasurementConsumerId
          if (request.filter.hasCreateTimeAfter()) {
            filter =
              ListBasicReportsPageTokenKt.filter {
                createTimeAfter = request.filter.createTimeAfter
              }
          }
          lastBasicReport =
            ListBasicReportsPageTokenKt.previousPageEnd {
              createTime = basicReports[basicReports.lastIndex - 1].createTime
              externalBasicReportId = basicReports[basicReports.lastIndex - 1].externalBasicReportId
            }
        }
      } else {
        this.basicReports +=
          basicReports.map {
            it.copy {
              campaignGroupDisplayName =
                reportingSetsByExternalId
                  .getValue(it.externalCampaignGroupId)
                  .reportingSet
                  .displayName
            }
          }
      }
    }
  }

  override suspend fun insertBasicReport(request: InsertBasicReportRequest): BasicReport {
    try {
      validateBasicReport(request.basicReport)
    } catch (e: RequiredFieldNotSetException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (e: ImpressionQualificationFilterNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val transactionRunner = spannerClient.readWriteTransaction()

    try {
      transactionRunner.run { txn ->
        checkReportingSet(request.basicReport)

        val measurementConsumerResult =
          try {
            txn.getMeasurementConsumerByCmmsMeasurementConsumerId(
              request.basicReport.cmmsMeasurementConsumerId
            )
          } catch (e: MeasurementConsumerNotFoundException) {
            val measurementConsumerId =
              idGenerator.generateNewId { id -> txn.measurementConsumerExists(id) }

            val measurementConsumer = measurementConsumer {
              cmmsMeasurementConsumerId = request.basicReport.cmmsMeasurementConsumerId
            }

            // If the Reporting Set exists, then the Measurement Consumer exists so it should be
            // in the Spanner database as well.
            txn.insertMeasurementConsumer(
              measurementConsumerId = measurementConsumerId,
              measurementConsumer = measurementConsumer,
            )

            MeasurementConsumerResult(
              measurementConsumerId = measurementConsumerId,
              measurementConsumer = measurementConsumer,
            )
          }

        val basicReportId =
          idGenerator.generateNewId { id ->
            txn.basicReportExists(measurementConsumerResult.measurementConsumerId, id)
          }

        txn.insertBasicReport(
          basicReportId = basicReportId,
          measurementConsumerId = measurementConsumerResult.measurementConsumerId,
          basicReport = request.basicReport,
        )
      }
    } catch (e: SpannerException) {
      if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
        throw BasicReportAlreadyExistsException(
            request.basicReport.cmmsMeasurementConsumerId,
            request.basicReport.externalBasicReportId,
          )
          .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
      } else {
        throw e
      }
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: ReportingSetNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

    return request.basicReport.copy { createTime = commitTimestamp }
  }

  /**
   * Checks whether the basic report is valid.
   *
   * @throws RequiredFieldNotSetException
   * @throws ImpressionQualificationFilterNotFoundException
   */
  private fun validateBasicReport(basicReport: BasicReport) {
    if (basicReport.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
    }

    if (basicReport.externalBasicReportId.isEmpty()) {
      throw RequiredFieldNotSetException("external_basic_report_id")
    }

    if (basicReport.externalCampaignGroupId.isEmpty()) {
      throw RequiredFieldNotSetException("external_campaign_group_id")
    }

    if (!basicReport.hasDetails()) {
      throw RequiredFieldNotSetException("details")
    }

    if (!basicReport.hasResultDetails()) {
      throw RequiredFieldNotSetException("result_details")
    }

    for (impressionQualificationFilter in basicReport.details.impressionQualificationFiltersList) {
      if (
        impressionQualificationFilter.externalImpressionQualificationFilterId.isNotEmpty() &&
          impressionQualificationFilterMapping.getImpressionQualificationByExternalId(
            impressionQualificationFilter.externalImpressionQualificationFilterId
          ) == null
      ) {
        throw ImpressionQualificationFilterNotFoundException(
          impressionQualificationFilter.externalImpressionQualificationFilterId
        )
      }
    }
  }

  /**
   * Reads a [ReportingSet] using Postgres.
   *
   * @throws ReportingSetNotFoundException
   */
  private suspend fun checkReportingSet(basicReport: BasicReport) {
    getReportingSets(
      basicReport.cmmsMeasurementConsumerId,
      listOf(basicReport.externalCampaignGroupId),
    )
  }

  /**
   * Reads [ReportingSet]s using Postgres.
   *
   * @throws ReportingSetNotFoundException
   */
  private suspend fun getReportingSets(
    cmmsMeasurementConsumerId: String,
    externalReportingSetIds: List<String>,
  ): List<ReportingSetReader.Result> {
    var postgresReadContext: ReadContext? = null
    return try {
      if (externalReportingSetIds.isEmpty()) {
        return emptyList()
      }

      postgresReadContext = postgresClient.singleUse()

      ReportingSetReader(postgresReadContext)
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalReportingSetIds += externalReportingSetIds
          }
        )
        .withSerializableErrorRetries()
        .toList()
    } finally {
      postgresReadContext?.close()
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 25
  }
}
