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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
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
import org.wfanet.measurement.internal.reporting.v2.CreateBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.FailBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.GetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.InsertBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ResultGroup
import org.wfanet.measurement.internal.reporting.v2.SetExternalReportIdRequest
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.basicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.BasicReportProcessedResultsTransformation.buildResultGroups
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.BasicReportResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.MeasurementConsumerResult
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.basicReportExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getBasicReportByExternalId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getBasicReportByRequestId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.getMeasurementConsumerByCmmsMeasurementConsumerId
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertBasicReport
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.insertMeasurementConsumer
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.measurementConsumerExists
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readBasicReports
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.readFullReportingSetResults
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.setBasicReportStateToFailed
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db.setExternalReportId
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.BasicReportAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.BasicReportNotFoundException
import org.wfanet.measurement.reporting.service.internal.GroupingDimensions
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
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : BasicReportsCoroutineImplBase(coroutineContext) {
  private val groupingDimensions =
    GroupingDimensions(impressionQualificationFilterMapping.eventMessageDescriptor)

  private data class ReportingSetExternalKey(
    val cmmsMeasurementConsumerId: String,
    val externalReportingSetId: String,
  )

  private sealed class ReportingSetKey {
    data class Composite(val setExpression: ReportingSet.SetExpression) : ReportingSetKey()

    data class Primitive(val eventGroupKeys: Set<ReportingSet.Primitive.EventGroupKey>) :
      ReportingSetKey()
  }

  override suspend fun getBasicReport(request: GetBasicReportRequest): BasicReport {
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.externalBasicReportId.isEmpty()) {
      throw RequiredFieldNotSetException("external_basic_report_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val (basicReport: BasicReport, reportingSetResults: List<ReportingSetResult>) =
      try {
        spannerClient.readOnlyTransaction().use { txn ->
          val basicReportResult =
            txn.getBasicReportByExternalId(
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId,
              externalBasicReportId = request.externalBasicReportId,
            )

          val reportingSetResults: List<ReportingSetResult> =
            txn.getReportingSetResults(basicReportResult)

          basicReportResult.basicReport to reportingSetResults
        }
      } catch (e: BasicReportNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    val campaignGroup =
      getCampaignGroup(request.cmmsMeasurementConsumerId, basicReport.externalCampaignGroupId)

    return if (basicReport.state != BasicReport.State.SUCCEEDED) {
      basicReport.copy { campaignGroupDisplayName = campaignGroup.displayName }
    } else {
      val campaignGroupReportingSets =
        listReportingSetsByCampaignGroup(
            basicReport.cmmsMeasurementConsumerId,
            basicReport.externalCampaignGroupId,
          )
          .filter { it.filter.isEmpty() }

      basicReport.withResults(campaignGroup, campaignGroupReportingSets, reportingSetResults)
    }
  }

  override suspend fun listBasicReports(
    request: ListBasicReportsRequest
  ): ListBasicReportsResponse {
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
        request.pageToken.also {
          // Handle "expired" tokens in old format.
          if (it.lastBasicReport.cmmsMeasurementConsumerId.isEmpty()) {
            throw RequiredFieldNotSetException(
                "page_token.last_basic_report.cmms_measurement_consumer_id"
              )
              .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
          }
        }
      } else {
        null
      }

    return listBasicReportsResponse {
      spannerClient.readOnlyTransaction().use { txn ->
        val basicReportResults: List<BasicReportResult> =
          txn
            .readBasicReports(filter = request.filter, limit = pageSize + 1, pageToken = pageToken)
            .toList()

        val externalCampaignGroupIdsByCmmsMeasurementConsumerId: Map<String, List<String>> =
          basicReportResults.groupBy({ it.basicReport.cmmsMeasurementConsumerId }) {
            it.basicReport.externalCampaignGroupId
          }
        val campaignGroupByExternalKey: Map<ReportingSetExternalKey, ReportingSet> = buildMap {
          for ((cmmsMeasurementConsumerId, externalCampaignGroupIds) in
            externalCampaignGroupIdsByCmmsMeasurementConsumerId) {
            val campaignGroups: List<ReportingSet> =
              getReportingSets(cmmsMeasurementConsumerId, externalCampaignGroupIds.distinct()).map {
                it.reportingSet
              }
            for (campaignGroup in campaignGroups) {
              put(
                ReportingSetExternalKey(
                  cmmsMeasurementConsumerId,
                  campaignGroup.externalReportingSetId,
                ),
                campaignGroup,
              )
            }
          }
        }

        val reportingSetsByCampaignGroupExternalKey:
          MutableMap<ReportingSetExternalKey, List<ReportingSet>> =
          mutableMapOf()

        basicReports +=
          basicReportResults.subList(0, minOf(basicReportResults.size, pageSize)).map {
            basicReportResult ->
            val campaignGroupExternalKey =
              ReportingSetExternalKey(
                basicReportResult.basicReport.cmmsMeasurementConsumerId,
                basicReportResult.basicReport.externalCampaignGroupId,
              )
            val campaignGroup = campaignGroupByExternalKey.getValue(campaignGroupExternalKey)

            if (basicReportResult.basicReport.state != BasicReport.State.SUCCEEDED) {
              basicReportResult.basicReport.copy {
                campaignGroupDisplayName = campaignGroup.displayName
              }
            } else {
              val campaignGroupReportingSets: List<ReportingSet> =
                reportingSetsByCampaignGroupExternalKey.getOrPut(campaignGroupExternalKey) {
                  listReportingSetsByCampaignGroup(
                      basicReportResult.basicReport.cmmsMeasurementConsumerId,
                      basicReportResult.basicReport.externalCampaignGroupId,
                    )
                    .filter { it.filter.isEmpty() }
                }

              val reportingSetResults: List<ReportingSetResult> =
                txn.getReportingSetResults(basicReportResult)

              basicReportResult.basicReport.withResults(
                campaignGroup,
                campaignGroupReportingSets,
                reportingSetResults,
              )
            }
          }

        if (basicReportResults.size == pageSize + 1) {
          val lastBasicReportResult = basicReportResults[basicReportResults.lastIndex - 1]

          nextPageToken = listBasicReportsPageToken {
            lastBasicReport =
              ListBasicReportsPageTokenKt.previousPageEnd {
                createTime = lastBasicReportResult.basicReport.createTime
                this.cmmsMeasurementConsumerId =
                  lastBasicReportResult.basicReport.cmmsMeasurementConsumerId
                externalBasicReportId = lastBasicReportResult.basicReport.externalBasicReportId
              }
          }
        }
      }
    }
  }

  override suspend fun createBasicReport(request: CreateBasicReportRequest): BasicReport {
    val transactionRunner = spannerClient.readWriteTransaction()

    val (existingBasicReport: BasicReport?, reportingSetResults: List<ReportingSetResult>) =
      try {
        transactionRunner.run { txn ->
          checkReportingSet(request.basicReport)

          val measurementConsumerResult =
            try {
              txn.getMeasurementConsumerByCmmsMeasurementConsumerId(
                request.basicReport.cmmsMeasurementConsumerId
              )
            } catch (_: MeasurementConsumerNotFoundException) {
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

          val existingBasicReportResult: BasicReportResult? =
            txn.getBasicReportByRequestId(
              measurementConsumerId = measurementConsumerResult.measurementConsumerId,
              createRequestId = request.requestId,
            )

          if (existingBasicReportResult != null) {
            val reportingSetResults: List<ReportingSetResult> =
              txn.getReportingSetResults(existingBasicReportResult)

            existingBasicReportResult.basicReport to reportingSetResults
          } else {
            val basicReportId =
              idGenerator.generateNewId { id ->
                txn.basicReportExists(measurementConsumerResult.measurementConsumerId, id)
              }

            txn.insertBasicReport(
              basicReportId = basicReportId,
              measurementConsumerId = measurementConsumerResult.measurementConsumerId,
              basicReport = request.basicReport,
              state = BasicReport.State.CREATED,
              requestId = request.requestId.ifEmpty { null },
            )

            null to emptyList()
          }
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
        // run doesn't support a NULL return value
      } catch (_: NullPointerException) {
        null to emptyList()
      }

    val campaignGroup =
      getCampaignGroup(
        request.basicReport.cmmsMeasurementConsumerId,
        request.basicReport.externalCampaignGroupId,
      )

    if (existingBasicReport != null) {
      if (existingBasicReport.state != BasicReport.State.SUCCEEDED) {
        return existingBasicReport.copy { campaignGroupDisplayName = campaignGroup.displayName }
      } else {
        val campaignGroupReportingSets =
          listReportingSetsByCampaignGroup(
              existingBasicReport.cmmsMeasurementConsumerId,
              existingBasicReport.externalCampaignGroupId,
            )
            .filter { it.filter.isEmpty() }

        return existingBasicReport.withResults(
          campaignGroup,
          campaignGroupReportingSets,
          reportingSetResults,
        )
      }
    } else {
      val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

      return request.basicReport.copy {
        campaignGroupDisplayName = campaignGroup.displayName
        createTime = commitTimestamp
        state = BasicReport.State.CREATED
      }
    }
  }

  override suspend fun setExternalReportId(request: SetExternalReportIdRequest): BasicReport {
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.externalBasicReportId.isEmpty()) {
      throw RequiredFieldNotSetException("external_basic_report_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.externalReportId.isEmpty()) {
      throw RequiredFieldNotSetException("external_report_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner = spannerClient.readWriteTransaction()

    val basicReportResult: BasicReportResult =
      try {
        transactionRunner.run { txn ->
          txn
            .getBasicReportByExternalId(
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId,
              externalBasicReportId = request.externalBasicReportId,
            )
            .also {
              txn.setExternalReportId(
                it.measurementConsumerId,
                it.basicReportId,
                request.externalReportId,
              )
            }
        }
      } catch (e: BasicReportNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    val campaignGroup =
      getCampaignGroup(
        request.cmmsMeasurementConsumerId,
        basicReportResult.basicReport.externalCampaignGroupId,
      )

    return basicReportResult.basicReport.copy {
      campaignGroupDisplayName = campaignGroup.displayName
      externalReportId = request.externalReportId
      state = BasicReport.State.REPORT_CREATED
    }
  }

  override suspend fun failBasicReport(request: FailBasicReportRequest): BasicReport {
    if (request.cmmsMeasurementConsumerId.isEmpty()) {
      throw RequiredFieldNotSetException("cmms_measurement_consumer_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.externalBasicReportId.isEmpty()) {
      throw RequiredFieldNotSetException("external_basic_report_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner = spannerClient.readWriteTransaction()

    val basicReportResult: BasicReportResult =
      try {
        transactionRunner.run { txn ->
          txn
            .getBasicReportByExternalId(
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId,
              externalBasicReportId = request.externalBasicReportId,
            )
            .also { txn.setBasicReportStateToFailed(it.measurementConsumerId, it.basicReportId) }
        }
      } catch (e: BasicReportNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    val campaignGroup =
      getCampaignGroup(
        request.cmmsMeasurementConsumerId,
        basicReportResult.basicReport.externalCampaignGroupId,
      )

    return basicReportResult.basicReport.copy {
      campaignGroupDisplayName = campaignGroup.displayName
      state = BasicReport.State.FAILED
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
          } catch (_: MeasurementConsumerNotFoundException) {
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
          state = BasicReport.State.SUCCEEDED,
          requestId = null,
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

    val campaignGroup =
      getCampaignGroup(
        request.basicReport.cmmsMeasurementConsumerId,
        request.basicReport.externalCampaignGroupId,
      )

    return request.basicReport.copy {
      campaignGroupDisplayName = campaignGroup.displayName
      createTime = commitTimestamp
      state = BasicReport.State.SUCCEEDED
    }
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

  /** Reads CampaignGroup */
  private suspend fun getCampaignGroup(
    cmmsMeasurementConsumerId: String,
    externalCampaignGroupId: String,
  ): ReportingSet {
    val reportingSetResult: ReportingSetReader.Result =
      getReportingSets(cmmsMeasurementConsumerId, listOf(externalCampaignGroupId)).single()

    return reportingSetResult.reportingSet
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

  private suspend fun listReportingSetsByCampaignGroup(
    cmmsMeasurementConsumerId: String,
    externalCampaignGroupId: String,
  ): List<ReportingSet> {
    var postgresReadContext: ReadContext? = null
    return try {
      postgresReadContext = postgresClient.singleUse()

      ReportingSetReader(postgresReadContext)
        .readReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                this.externalCampaignGroupId = externalCampaignGroupId
              }
            limit = Int.MAX_VALUE
          }
        )
        .withSerializableErrorRetries()
        .map { it.reportingSet }
        .toList()
    } catch (e: Exception) {
      postgresReadContext?.close()
      throw e
    }
  }

  private suspend fun AsyncDatabaseClient.ReadContext.getReportingSetResults(
    basicReportResult: BasicReportResult
  ): List<ReportingSetResult> {
    val reportResultId: Long? = basicReportResult.reportResultId
    return if (
      reportResultId != null && basicReportResult.basicReport.state == BasicReport.State.SUCCEEDED
    ) {
      readFullReportingSetResults(
          impressionQualificationFilterMapping,
          groupingDimensions,
          basicReportResult.measurementConsumerId,
          reportResultId,
        )
        .map { it.reportingSetResult }
        .toList()
    } else {
      emptyList()
    }
  }

  private fun BasicReport.withResults(
    campaignGroup: ReportingSet,
    campaignGroupReportingSets: List<ReportingSet>,
    reportingSetResults: List<ReportingSetResult>,
  ): BasicReport {
    val source = this
    return source.copy {
      campaignGroupDisplayName = campaignGroup.displayName

      if (reportingSetResults.isNotEmpty()) {
        resultDetails = basicReportResultDetails {
          resultGroups +=
            transformReportResultIntoResultGroups(
              source,
              reportingSetResults,
              campaignGroup,
              campaignGroupReportingSets,
            )
        }
      }
    }
  }

  private fun transformReportResultIntoResultGroups(
    basicReport: BasicReport,
    reportingSetResults: Iterable<ReportingSetResult>,
    campaignGroup: ReportingSet,
    campaignGroupReportingSets: List<ReportingSet>,
  ): List<ResultGroup> {
    val campaignGroupReportingSetIdByReportingSetKey: Map<ReportingSetKey, String> =
      campaignGroupReportingSets.associate {
        if (it.hasComposite()) {
          ReportingSetKey.Composite(setExpression = it.composite) to it.externalReportingSetId
        } else {
          ReportingSetKey.Primitive(eventGroupKeys = it.primitive.eventGroupKeysList.toSet()) to
            it.externalReportingSetId
        }
      }

    val eventGroupKeysByDataProviderId: Map<String, List<ReportingSet.Primitive.EventGroupKey>> =
      campaignGroup.primitive.eventGroupKeysList.groupBy { it.cmmsDataProviderId }

    val primitiveInfoByDataProviderId:
      Map<String, BasicReportProcessedResultsTransformation.PrimitiveInfo> =
      buildMap {
        for (dataProviderId in eventGroupKeysByDataProviderId.keys) {
          val primitiveEventGroupKeys =
            eventGroupKeysByDataProviderId.getValue(dataProviderId).toSet()
          val reportingSetKey = ReportingSetKey.Primitive(eventGroupKeys = primitiveEventGroupKeys)

          if (campaignGroupReportingSetIdByReportingSetKey.containsKey(reportingSetKey)) {
            put(
              dataProviderId,
              BasicReportProcessedResultsTransformation.PrimitiveInfo(
                eventGroupKeys = primitiveEventGroupKeys,
                externalReportingSetId =
                  campaignGroupReportingSetIdByReportingSetKey.getValue(reportingSetKey),
              ),
            )
          }
        }
      }

    val compositeReportingSetIdBySetExpression: Map<ReportingSet.SetExpression, String> = buildMap {
      campaignGroupReportingSetIdByReportingSetKey
        .filter { it.key is ReportingSetKey.Composite }
        .forEach { put((it.key as ReportingSetKey.Composite).setExpression, it.value) }
    }

    return buildResultGroups(
      basicReport,
      reportingSetResults,
      primitiveInfoByDataProviderId,
      compositeReportingSetIdBySetExpression,
    )
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 25
  }
}
