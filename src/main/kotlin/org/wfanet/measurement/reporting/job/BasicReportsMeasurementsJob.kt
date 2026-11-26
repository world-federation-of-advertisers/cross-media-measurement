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
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.cancelMeasurementRequest
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
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listReportComputationsRequest
import org.wfanet.measurement.internal.reporting.v2.setMeasurementsCreatedRequest
import org.wfanet.measurement.internal.reporting.v2.setReportComputationMeasurementIdRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey

/**
 * Creates the CMMS Measurements of [BasicReport]s on the multimeasurement path.
 *
 * Each [BasicReport] in state `CREATED_MULTIMEASUREMENT` has one compiled multimeasurement per
 * impression qualification filter, each of which is created in the CMMS, after which the
 * [BasicReport] moves to state `MEASUREMENTS_CREATED`. A [BasicReport] whose Measurement the CMMS
 * rejects is FAILED, and its Measurements already created are cancelled. Any other error leaves the
 * [BasicReport] for the next execution, which reuses the stored `CreateMeasurement` request IDs.
 */
class BasicReportsMeasurementsJob(
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  private val internalBasicReportsStub: InternalBasicReportsCoroutineStub,
  private val measurementConsumersStub: MeasurementConsumersCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
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
    val reportComputations: List<ReportComputation> = listReportComputations(basicReport)
    val pendingReportComputations: List<ReportComputation> =
      reportComputations.filter { it.cmmsMeasurementId.isEmpty() }
    if (pendingReportComputations.isNotEmpty()) {
      val measurementConsumer: MeasurementConsumer =
        getMeasurementConsumer(measurementConsumerName, measurementConsumerConfig.apiKey)
      val createdMeasurementIds: MutableList<String> =
        reportComputations.mapNotNull { it.cmmsMeasurementId.ifEmpty { null } }.toMutableList()
      for (reportComputation in pendingReportComputations) {
        val request: CreateMeasurementRequest =
          buildCreateMeasurementRequest(
            basicReport,
            reportComputation,
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
              cancelMeasurements(
                measurementConsumerName,
                measurementConsumerConfig,
                createdMeasurementIds,
              )
              failBasicReport(basicReport)
              return
            }
            throw Exception("Unable to create Measurement for ${basicReport.name}", e)
          }
        val measurementId: String =
          checkNotNull(MeasurementKey.fromName(measurement.name)).measurementId
        createdMeasurementIds += measurementId
        setReportComputationMeasurementId(reportComputation, measurementId)
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

  private suspend fun listReportComputations(basicReport: BasicReport): List<ReportComputation> {
    return try {
      internalBasicReportsStub
        .listReportComputations(
          listReportComputationsRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
          }
        )
        .reportComputationsList
    } catch (e: StatusException) {
      throw Exception("Unable to list ReportComputations of ${basicReport.name}", e)
    }
  }

  /**
   * Cancels the CMMS Measurements with [measurementIds], so that none of a failed BasicReport's
   * Measurements keep running.
   */
  private suspend fun cancelMeasurements(
    measurementConsumerName: String,
    measurementConsumerConfig: MeasurementConsumerConfig,
    measurementIds: List<String>,
  ) {
    val measurementConsumerKey =
      checkNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))
    for (measurementId in measurementIds) {
      val name =
        MeasurementKey(measurementConsumerKey.measurementConsumerId, measurementId).toName()
      try {
        measurementsStub
          .withAuthenticationKey(measurementConsumerConfig.apiKey)
          .cancelMeasurement(cancelMeasurementRequest { this.name = name })
      } catch (e: StatusException) {
        throw Exception("Unable to cancel $name", e)
      }
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
        multimeasurement =
          Measurement.Multimeasurement.parseFrom(reportComputation.multimeasurement)
            .withRequisitionSpecKeys(measurementPublicKey)
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
          externalImpressionQualificationFilterId =
            reportComputation.externalImpressionQualificationFilterId
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

  companion object {
    private val logger: Logger = Logger.getLogger(BasicReportsMeasurementsJob::class.java.name)
    private const val BATCH_SIZE = 10
  }
}
