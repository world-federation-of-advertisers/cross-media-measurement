/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.Date
import com.google.type.Interval
import com.google.type.copy
import com.google.type.date
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.time.DateTimeException
import java.time.LocalDate
import java.time.Period
import java.time.temporal.ChronoField
import java.time.temporal.ChronoUnit
import java.time.temporal.Temporal
import java.time.temporal.TemporalAdjusters
import java.time.zone.ZoneRulesException
import kotlin.math.min
import kotlin.random.Random
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.projectnessie.cel.Env
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequest as InternalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec as InternalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportRequest as internalGetReportRequest
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.reporting.service.api.submitBatchRequests
import org.wfanet.measurement.reporting.service.api.v2alpha.MetadataPrincipalServerInterceptor.Companion.withPrincipalName
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportScheduleInfoServerInterceptor.Companion.reportScheduleInfoFromCurrentContext
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.BatchGetMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.CreateReportRequest
import org.wfanet.measurement.reporting.v2alpha.GetReportRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportsPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.ListReportsRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportsResponse
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.batchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listReportsPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportsResponse
import org.wfanet.measurement.reporting.v2alpha.report

import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import com.google.protobuf.Any
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.stub.StreamObserver
import java.nio.charset.StandardCharsets

class WorkItemsService(
  private val internalReportsStub: ReportsCoroutineStub,
  private val internalMetricCalculationSpecsStub: MetricCalculationSpecsCoroutineStub,
  private val metricsStub: MetricsCoroutineStub,
  private val metricSpecConfig: MetricSpecConfig,
  private val secureRandom: Random,
) : WorkItemsCoroutineImplBase() {
  private data class CreateReportInfo(
    val parent: String,
    val requestId: String,
    val timeIntervals: List<Interval>?,
    val reportingInterval: Report.ReportingInterval?,
  )

  override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    val listReportsPageToken = request.toListReportsPageToken()

    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list Reports belonging to other MeasurementConsumers."
          }
        }
      }
    }

    val streamInternalReportsRequest: StreamReportsRequest =
      listReportsPageToken.toStreamReportsRequest()

    val results: List<InternalReport> =
      try {
        internalReportsStub.streamReports(streamInternalReportsRequest).toList()
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          Status.Code.CANCELLED -> Status.CANCELLED
          else -> Status.UNKNOWN
        }
          .withCause(e)
          .withDescription("Unable to list Reports.")
          .asRuntimeException()
      }

    if (results.isEmpty()) {
      return ListReportsResponse.getDefaultInstance()
    }

    val nextPageToken: ListReportsPageToken? =
      if (results.size > listReportsPageToken.pageSize) {
        listReportsPageToken.copy {
          lastReport =
            ListReportsPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = results[results.lastIndex - 1].cmmsMeasurementConsumerId
              createTime = results[results.lastIndex - 1].createTime
              externalReportId = results[results.lastIndex - 1].externalReportId
            }
        }
      } else {
        null
      }

    val subResults: List<InternalReport> =
      results.subList(0, min(results.size, listReportsPageToken.pageSize))

    // Get metrics.
    val metricNames: Flow<String> = flow {
      buildSet {
        for (internalReport in subResults) {
          for (reportingMetricEntry in internalReport.reportingMetricEntriesMap) {
            for (metricCalculationSpecReportingMetrics in
            reportingMetricEntry.value.metricCalculationSpecReportingMetricsList) {
              for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
                val name =
                  MetricKey(
                    internalReport.cmmsMeasurementConsumerId,
                    reportingMetric.externalMetricId,
                  )
                    .toName()

                if (!contains(name)) {
                  emit(name)
                  add(name)
                }
              }
            }
          }
        }
      }
    }

    val callRpc: suspend (List<String>) -> BatchGetMetricsResponse = { items ->
      batchGetMetrics(principal.resourceKey.toName(), items)
    }
    val externalIdToMetricMap: Map<String, Metric> = buildMap {
      submitBatchRequests(metricNames, BATCH_GET_METRICS_LIMIT, callRpc) { response ->
        response.metricsList
      }
        .collect { metrics: List<Metric> ->
          for (metric in metrics) {
            computeIfAbsent(checkNotNull(MetricKey.fromName(metric.name)).metricId) { metric }
          }
        }
    }

    return listReportsResponse {
      reports +=
        filterReports(
          subResults.map { internalReport ->
            convertInternalReportToPublic(internalReport, externalIdToMetricMap)
          },
          request.filter,
        )

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }


}

class WorkItemsService : WorkItemsCoroutineImplBase() {

  private val rabbitMqHost: String = System.getenv("RABBIT_HOST") ?: "localhost"
  private val connectionFactory: ConnectionFactory = ConnectionFactory().apply {
    host = rabbitMqHost
  }

  override suspend fun createWorkItem(request: CreateWorkItemRequest): WorkItem {
  override fun createWorkItem(
    request: CreateWorkItemRequest,
  ) {
    try {

      grpcRequireNotNull(ModelLineKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

      // Validate the WorkItem request
      val workItem = request.workItem
      val queueName = workItem.queue
      if (queueName.isEmpty()) {
        throw StatusException(Status.INVALID_ARGUMENT.withDescription("Queue name is required."))
      }

      // Serialize the WorkItem if necessary (convert to byte array)
      val serializedWorkItem = workItem.toByteArray()

      // Enqueue the WorkItem into the RabbitMQ queue
      enqueueToQueue(queueName, serializedWorkItem)


    } catch (e: Exception) {

    }
  }

  // Enqueues the serialized WorkItem to the specified RabbitMQ queue
  private fun enqueueToQueue(queueName: String, serializedWorkItem: ByteArray) {
    val connection: Connection = connectionFactory.newConnection()
    val channel: Channel = connection.createChannel()

    try {
      // Check if the queue exists without creating it
      channel.queueDeclarePassive(queueName)

      // Publish the WorkItem to the queue
      channel.basicPublish("", queueName, null, serializedWorkItem)
    } catch (e: java.io.IOException) {
      // Handle the case where the queue doesn't exist
      throw StatusException(Status.NOT_FOUND.withDescription("Queue $queueName does not exist").withCause(e))
    } finally {
      // Ensure the channel and connection are closed
      channel.close()
      connection.close()
    }
  }
}
