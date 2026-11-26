/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.job

import com.google.protobuf.Any as ProtoAny
import io.grpc.Status
import io.grpc.StatusException
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportComputationsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.setMeasurementsCreatedRequest
import org.wfanet.measurement.internal.reporting.v2.setReportComputationMeasurementIdRequest
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.MultimeasurementCompiler
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey

/**
 * Creates the CMMS Measurements of [BasicReport]s on the multimeasurement path.
 *
 * Each [BasicReport] in state `CREATED_MULTIMEASUREMENT` is compiled into one multimeasurement per
 * impression qualification filter, each of which is created in the CMMS, after which the
 * [BasicReport] moves to state `MEASUREMENTS_CREATED`. A [BasicReport] that cannot be compiled or
 * that the CMMS rejects is FAILED. Any other error leaves the [BasicReport] for the next execution,
 * which reuses the stored `CreateMeasurement` request IDs.
 *
 * @param impressionQualificationFilterIds map of external impression qualification filter ID to its
 *   internal ID
 */
class BasicReportsMeasurementsJob(
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val internalBasicReportsStub: InternalBasicReportsCoroutineStub,
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
  private val compiler: MultimeasurementCompiler,
  private val impressionQualificationFilterIds: Map<String, Long>,
) {
  suspend fun execute() {
    for ((measurementConsumerName, measurementConsumerConfig) in
      measurementConsumerConfigs.configsMap) {
      if (measurementConsumerConfig.apiKey.isEmpty()) {
        continue
      }
      val cmmsMeasurementConsumerId =
        requireNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))
          .measurementConsumerId

      val resourceLists =
        internalBasicReportsStub.listResources { pageToken: ListBasicReportsPageToken? ->
          val response =
            internalBasicReportsStub.listBasicReports(
              listBasicReportsRequest {
                pageSize = BATCH_SIZE
                if (pageToken != null) {
                  this.pageToken = pageToken
                }
                filter =
                  ListBasicReportsRequestKt.filter {
                    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                    state = BasicReport.State.CREATED_MULTIMEASUREMENT
                  }
              }
            )
          val nextPageToken = if (response.hasNextPageToken()) response.nextPageToken else null
          ResourceList(response.basicReportsList, nextPageToken)
        }

      resourceLists.collect { resourceList ->
        for (basicReport in resourceList.resources) {
          try {
            createMeasurements(basicReport, measurementConsumerName, measurementConsumerConfig)
          } catch (e: CancellationException) {
            throw e
          } catch (e: Exception) {
            logger.log(
              Level.WARNING,
              "Failed to create Measurements for BasicReport ${basicReport.name}",
              e,
            )
          }
        }
      }
    }
  }

  private suspend fun createMeasurements(
    basicReport: BasicReport,
    measurementConsumerName: String,
    measurementConsumerConfig: MeasurementConsumerConfig,
  ) {
    val components: Map<String, MultimeasurementCompiler.ReportingUnitComponent> =
      resolveComponents(basicReport)
    val compiledReportComputations: List<MultimeasurementCompiler.CompiledReportComputation> =
      try {
        compiler.compile(basicReport, components, impressionQualificationFilterIds)
      } catch (e: IllegalArgumentException) {
        logger.log(Level.WARNING, "BasicReport ${basicReport.name} cannot be compiled", e)
        failBasicReport(basicReport)
        return
      }
    val multimeasurementByFilterId: Map<Long, Measurement.Multimeasurement> =
      compiledReportComputations.associate {
        it.reportComputation.impressionQualificationFilterId to it.multimeasurement
      }

    val pendingReportComputations: List<ReportComputation> =
      batchCreateReportComputations(basicReport, compiledReportComputations).filter {
        it.cmmsMeasurementId.isEmpty()
      }
    if (pendingReportComputations.isNotEmpty()) {
      val measurementConsumer: MeasurementConsumer =
        getMeasurementConsumer(measurementConsumerName, measurementConsumerConfig.apiKey)
      for (reportComputation in pendingReportComputations) {
        val request: CreateMeasurementRequest =
          buildCreateMeasurementRequest(
            basicReport,
            reportComputation,
            multimeasurementByFilterId.getValue(reportComputation.impressionQualificationFilterId),
            measurementConsumer,
            measurementConsumerConfig,
          )
        val measurement: Measurement =
          try {
            measurementsStub
              .withAuthenticationKey(measurementConsumerConfig.apiKey)
              .createMeasurement(request)
          } catch (e: StatusException) {
            if (
              e.status.code == Status.Code.INVALID_ARGUMENT ||
                e.status.code == Status.Code.FAILED_PRECONDITION
            ) {
              logger.log(Level.WARNING, "CMMS rejected a Measurement of ${basicReport.name}", e)
              failBasicReport(basicReport)
              return
            }
            throw Exception("Unable to create Measurement for ${basicReport.name}", e)
          }
        setReportComputationMeasurementId(
          reportComputation,
          checkNotNull(MeasurementKey.fromName(measurement.name)).measurementId,
        )
      }
    }

    try {
      internalBasicReportsStub.setMeasurementsCreated(
        setMeasurementsCreatedRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
        }
      )
    } catch (e: StatusException) {
      throw Exception("Unable to set Measurements created for ${basicReport.name}", e)
    }
  }

  /**
   * Resolves the `ReportingUnit` components of [basicReport], keyed by resource name as in the
   * public `ReportingUnit.components`.
   */
  private suspend fun resolveComponents(
    basicReport: BasicReport
  ): Map<String, MultimeasurementCompiler.ReportingUnitComponent> {
    val reportingUnits = basicReport.details.resultGroupSpecsList.map { it.reportingUnit }
    val dataProviderIds: Set<String> =
      reportingUnits
        .flatMap { it.dataProviderKeys.dataProviderKeysList }
        .map { it.cmmsDataProviderId }
        .toSet()
    val reportingSetIds: Set<String> =
      reportingUnits
        .flatMap { it.reportingSetKeys.reportingSetKeysList }
        .map { it.externalReportingSetId }
        .toSet()

    return buildMap {
      if (dataProviderIds.isNotEmpty()) {
        putAll(resolveDataProviderComponents(basicReport, dataProviderIds))
      }
      if (reportingSetIds.isNotEmpty()) {
        for (reportingSet in
          batchGetReportingSets(basicReport.cmmsMeasurementConsumerId, reportingSetIds)) {
          val key =
            ReportingSetKey(
              reportingSet.cmmsMeasurementConsumerId,
              reportingSet.externalReportingSetId,
            )
          put(
            key.toName(),
            MultimeasurementCompiler.ReportingUnitComponent(
              reportingSet.externalReportingSetId,
              reportingSet.cmmsEventGroups,
            ),
          )
        }
      }
    }
  }

  /**
   * Resolves DataProvider components to the primitive [InternalReportingSet] under the Campaign
   * Group of [basicReport] whose EventGroups are the Campaign Group's EventGroups of that
   * DataProvider.
   */
  private suspend fun resolveDataProviderComponents(
    basicReport: BasicReport,
    dataProviderIds: Set<String>,
  ): Map<String, MultimeasurementCompiler.ReportingUnitComponent> {
    val cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
    val campaignGroup: InternalReportingSet =
      batchGetReportingSets(cmmsMeasurementConsumerId, listOf(basicReport.externalCampaignGroupId))
        .single()
    val eventGroupKeysByDataProviderId:
      Map<String, List<InternalReportingSet.Primitive.EventGroupKey>> =
      campaignGroup.primitive.eventGroupKeysList.groupBy { it.cmmsDataProviderId }

    val campaignGroupReportingSets: List<InternalReportingSet> =
      try {
        internalReportingSetsStub
          .streamReportingSets(
            streamReportingSetsRequest {
              filter =
                StreamReportingSetsRequestKt.filter {
                  this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                  externalCampaignGroupId = basicReport.externalCampaignGroupId
                }
              limit = STREAM_REPORTING_SETS_LIMIT
            }
          )
          .toList()
      } catch (e: StatusException) {
        throw Exception("Unable to stream ReportingSets for ${basicReport.name}", e)
      }
    // Mirrors BasicReportsService, which reuses or mints one unfiltered primitive ReportingSet per
    // DataProvider of the Campaign Group.
    val primitiveIdByEventGroupKeys:
      Map<Set<InternalReportingSet.Primitive.EventGroupKey>, String> =
      campaignGroupReportingSets
        .filter { it.hasPrimitive() && it.filter.isEmpty() }
        .associate { it.primitive.eventGroupKeysList.toSet() to it.externalReportingSetId }

    return dataProviderIds.associate { dataProviderId ->
      val eventGroupKeys: List<InternalReportingSet.Primitive.EventGroupKey> =
        eventGroupKeysByDataProviderId[dataProviderId].orEmpty()
      val externalReportingSetId: String =
        checkNotNull(primitiveIdByEventGroupKeys[eventGroupKeys.toSet()]) {
          "No primitive ReportingSet for DataProvider $dataProviderId in ${basicReport.name}"
        }
      DataProviderKey(dataProviderId).toName() to
        MultimeasurementCompiler.ReportingUnitComponent(
          externalReportingSetId,
          eventGroupKeys.map {
            CmmsEventGroupKey(it.cmmsDataProviderId, it.cmmsEventGroupId).toName()
          },
        )
    }
  }

  private suspend fun batchGetReportingSets(
    cmmsMeasurementConsumerId: String,
    externalReportingSetIds: Collection<String>,
  ): List<InternalReportingSet> {
    return try {
      internalReportingSetsStub
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalReportingSetIds += externalReportingSetIds
          }
        )
        .reportingSetsList
    } catch (e: StatusException) {
      throw Exception("Unable to get ReportingSets", e)
    }
  }

  /**
   * Creates the [ReportComputation]s of [basicReport], each with a fresh `CreateMeasurement`
   * request ID, and returns the stored ones, which keep the request ID they were first created
   * with.
   */
  private suspend fun batchCreateReportComputations(
    basicReport: BasicReport,
    compiledReportComputations: List<MultimeasurementCompiler.CompiledReportComputation>,
  ): List<ReportComputation> {
    return try {
      internalBasicReportsStub
        .batchCreateReportComputations(
          batchCreateReportComputationsRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
            reportComputations +=
              compiledReportComputations.map {
                it.reportComputation.copy {
                  cmmsCreateMeasurementRequestId = UUID.randomUUID().toString()
                }
              }
          }
        )
        .reportComputationsList
    } catch (e: StatusException) {
      throw Exception("Unable to create ReportComputations for ${basicReport.name}", e)
    }
  }

  private suspend fun getMeasurementConsumer(
    measurementConsumerName: String,
    apiKey: String,
  ): MeasurementConsumer {
    return try {
      measurementConsumersStub
        .withAuthenticationKey(apiKey)
        .getMeasurementConsumer(getMeasurementConsumerRequest { name = measurementConsumerName })
    } catch (e: StatusException) {
      throw Exception("Unable to get $measurementConsumerName", e)
    }
  }

  private fun buildCreateMeasurementRequest(
    basicReport: BasicReport,
    reportComputation: ReportComputation,
    multimeasurement: Measurement.Multimeasurement,
    measurementConsumer: MeasurementConsumer,
    measurementConsumerConfig: MeasurementConsumerConfig,
  ): CreateMeasurementRequest {
    val measurementPublicKey: ProtoAny = measurementConsumer.publicKey.message
    val requestId: String = reportComputation.cmmsCreateMeasurementRequestId
    val basicReportName: String = basicReport.name
    return createMeasurementRequest {
      parent = measurementConsumer.name
      measurement = measurement {
        measurementConsumerCertificate = measurementConsumerConfig.signingCertificateName
        unsignedMeasurementSpec = measurementSpec {
          this.measurementPublicKey = measurementPublicKey
          multi = MeasurementSpecKt.multiMeasurementSpec {}
          vidSamplingInterval =
            MeasurementSpecKt.vidSamplingInterval {
              start = 0.0f
              width = 1.0f
            }
          if (basicReport.hasModelLineKey()) {
            modelLine =
              ModelLineKey(
                  basicReport.modelLineKey.cmmsModelProviderId,
                  basicReport.modelLineKey.cmmsModelSuiteId,
                  basicReport.modelLineKey.cmmsModelLineId,
                )
                .toName()
          }
          reportingMetadata =
            MeasurementSpecKt.reportingMetadata {
              // A multimeasurement BasicReport has no Report. DataProviders group requisitions by
              // `report`, so it names one under the BasicReport's ID.
              report =
                ReportKey(basicReport.cmmsMeasurementConsumerId, basicReport.externalBasicReportId)
                  .toName()
              this.basicReport = basicReportName
            }
        }
        this.multimeasurement = multimeasurement.withRequisitionSpecKeys(measurementPublicKey)
        measurementReferenceId = requestId
      }
      this.requestId = requestId
    }
  }

  /**
   * Returns this multimeasurement with [measurementPublicKey] in each `RequisitionSpec`.
   *
   * A multimeasurement `RequisitionSpec` is plaintext, so it has no nonce.
   */
  private fun Measurement.Multimeasurement.withRequisitionSpecKeys(
    measurementPublicKey: ProtoAny
  ): Measurement.Multimeasurement {
    val source = this
    return source.copy {
      dataProviderVectors.clear()
      dataProviderVectors +=
        source.dataProviderVectorsList.map { entry ->
          entry.copy {
            vectors.clear()
            vectors +=
              entry.vectorsList.map { vector ->
                vector.copy {
                  requisitionSpec =
                    vector.requisitionSpec.copy { this.measurementPublicKey = measurementPublicKey }
                }
              }
          }
        }
    }
  }

  private suspend fun setReportComputationMeasurementId(
    reportComputation: ReportComputation,
    cmmsMeasurementId: String,
  ) {
    try {
      internalBasicReportsStub.setReportComputationMeasurementId(
        setReportComputationMeasurementIdRequest {
          cmmsMeasurementConsumerId = reportComputation.cmmsMeasurementConsumerId
          externalBasicReportId = reportComputation.externalBasicReportId
          impressionQualificationFilterId = reportComputation.impressionQualificationFilterId
          this.cmmsMeasurementId = cmmsMeasurementId
        }
      )
    } catch (e: StatusException) {
      throw Exception("Unable to set Measurement ID of a ReportComputation", e)
    }
  }

  private suspend fun failBasicReport(basicReport: BasicReport) {
    try {
      internalBasicReportsStub.failBasicReport(
        failBasicReportRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
        }
      )
    } catch (e: StatusException) {
      throw Exception("Unable to fail ${basicReport.name}", e)
    }
  }

  private val BasicReport.name: String
    get() = BasicReportKey(cmmsMeasurementConsumerId, externalBasicReportId).toName()

  private val InternalReportingSet.cmmsEventGroups: List<String>
    get() =
      primitive.eventGroupKeysList.map {
        CmmsEventGroupKey(it.cmmsDataProviderId, it.cmmsEventGroupId).toName()
      }

  companion object {
    private val logger: Logger = Logger.getLogger(BasicReportsMeasurementsJob::class.java.name)
    private const val BATCH_SIZE = 10
    private const val STREAM_REPORTING_SETS_LIMIT = 1000
  }
}
