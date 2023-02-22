// Copyright 2023 The Cross-Media Measurement Authors/*
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import java.security.PrivateKey
import java.security.SecureRandom
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2alpha.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSet.SetExpression as InternalSetExpression
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2alpha.getMetricByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.v2alpha.getReportingSetRequest as getInternalReportingSetRequest
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.CreateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportingSet.SetExpression
import org.wfanet.measurement.reporting.v2alpha.TimeInterval
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.reportingSet

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

private const val NUMBER_VID_BUCKETS = 300
private const val REACH_ONLY_VID_SAMPLING_WIDTH = 3.0f / NUMBER_VID_BUCKETS
private const val NUMBER_REACH_ONLY_BUCKETS = 16
private val REACH_ONLY_VID_SAMPLING_START_LIST =
  (0 until NUMBER_REACH_ONLY_BUCKETS).map { it * REACH_ONLY_VID_SAMPLING_WIDTH }
private const val REACH_ONLY_REACH_EPSILON = 0.0041
private const val REACH_ONLY_FREQUENCY_EPSILON = 0.0001
private const val REACH_ONLY_MAXIMUM_FREQUENCY_PER_USER = 1

private const val REACH_FREQUENCY_VID_SAMPLING_WIDTH = 5.0f / NUMBER_VID_BUCKETS
private const val NUMBER_REACH_FREQUENCY_BUCKETS = 19
private val REACH_FREQUENCY_VID_SAMPLING_START_LIST =
  (0 until NUMBER_REACH_FREQUENCY_BUCKETS).map {
    REACH_ONLY_VID_SAMPLING_START_LIST.last() +
      REACH_ONLY_VID_SAMPLING_WIDTH +
      it * REACH_FREQUENCY_VID_SAMPLING_WIDTH
  }
private const val REACH_FREQUENCY_REACH_EPSILON = 0.0033
private const val REACH_FREQUENCY_FREQUENCY_EPSILON = 0.115

private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / NUMBER_VID_BUCKETS
private const val NUMBER_IMPRESSION_BUCKETS = 1
private val IMPRESSION_VID_SAMPLING_START_LIST =
  (0 until NUMBER_IMPRESSION_BUCKETS).map {
    REACH_FREQUENCY_VID_SAMPLING_START_LIST.last() +
      REACH_FREQUENCY_VID_SAMPLING_WIDTH +
      it * IMPRESSION_VID_SAMPLING_WIDTH
  }
private const val IMPRESSION_EPSILON = 0.0011

private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / NUMBER_VID_BUCKETS
private const val NUMBER_WATCH_DURATION_BUCKETS = 1
private val WATCH_DURATION_VID_SAMPLING_START_LIST =
  (0 until NUMBER_WATCH_DURATION_BUCKETS).map {
    IMPRESSION_VID_SAMPLING_START_LIST.last() +
      IMPRESSION_VID_SAMPLING_WIDTH +
      it * WATCH_DURATION_VID_SAMPLING_WIDTH
  }
private const val WATCH_DURATION_EPSILON = 0.001

private const val DIFFERENTIAL_PRIVACY_DELTA = 1e-12

private val REACH_ONLY_MEASUREMENT_SPEC =
  MeasurementSpecKt.reachAndFrequency {
    reachPrivacyParams = differentialPrivacyParams {
      epsilon = REACH_ONLY_REACH_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DELTA
    }
    frequencyPrivacyParams = differentialPrivacyParams {
      epsilon = REACH_ONLY_FREQUENCY_EPSILON
      delta = DIFFERENTIAL_PRIVACY_DELTA
    }
    maximumFrequencyPerUser = REACH_ONLY_MAXIMUM_FREQUENCY_PER_USER
  }

data class MeasurementInfo(
  val measurementConsumerReferenceId: String,
  val idempotencyKey: String,
  val eventGroupFilters: Map<String, String>,
)

data class SigningConfig(
  val signingCertificateName: String,
  val signingCertificateDer: ByteString,
  val signingPrivateKey: PrivateKey,
)

data class WeightedMeasurementInfo(
  val reportingMeasurementId: String,
  val weightedSubSetUnion: WeightedSubSetUnion,
  val timeInterval: TimeInterval,
  var kingdomMeasurementId: String? = null,
)

data class SetOperationResult(
  val weightedMeasurementInfoList: List<WeightedMeasurementInfo>,
)

class MetricsService(
  private val internalReportingSetsStub: InternalReportingSetsCoroutineStub,
  private val internalMeasurementsStub: InternalMeasurementsCoroutineStub,
  private val internalMetricsStub: InternalMetricsCoroutineStub,
  private val dataProvidersStub: DataProvidersCoroutineStub,
  private val measurementsStub: MeasurementsCoroutineStub,
  private val certificateStub: CertificatesCoroutineStub,
  private val measurementConsumer: MeasurementConsumer,
  private val apiAuthenticationKey: String,
  private val secureRandom: SecureRandom,
) : MetricsCoroutineImplBase() {
  private val setExpressionCompiler = SetExpressionCompiler()

  override suspend fun createMetric(request: CreateMetricRequest): Metric {
    grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }

    val principal: ReportingPrincipal = principalFromCurrentContext

    when (principal) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a Metric for another MeasurementConsumer."
          }
        }
      }
    }

    val resourceKey = principal.resourceKey
    val apiAuthenticationKey: String = principal.config.apiKey

    grpcRequire(request.hasMetric()) { "Metric is not specified." }
    grpcRequire(request.metricId.isNotBlank()) { "Metric unique ID is not specified." }
    grpcRequire(request.metric.reportingSet.isNotBlank()) {
      "Reporting set in metric is not specified."
    }
    grpcRequire(request.metric.hasTimeInterval()) { "Time interval in metric is not specified." }
    grpcRequire(request.metric.hasMetricSpec()) { "Metric spec in metric is not specified." }

    val existingIntervalMetric: InternalMetric? =
      getInternalMetricByIdempotencyKey(resourceKey.measurementConsumerId, request.metricId)

    if (existingIntervalMetric != null) return existingIntervalMetric.toMetric()

    // Get the internal reporting set.
    val internalReportingSet: InternalReportingSet =
      getInternalReportingSet(resourceKey.measurementConsumerId, request.metric.reportingSet)
    grpcRequireNotNull(internalReportingSet) {
      "Unable to retrieve a reporting set from the reporting database using the provided reportingSet [${request.metric.reportingSet}]."
    }

    // Get measurementConsumer and signingConfig

    // Create a unique internal Metric ID based on metricIdempotencyKey, reportingSet,
    // metricType, timeInterval, and the list of additionalFilters.

    /**
     * Measurement Supplier - getInternalMeasurementIds For each WeightedSubsetUnion,
     * 1. Create a unique internal measurement ID for each WeightedSubsetUnion based on the internal
     *    metric ID and the WeightedSubsetUnionId.
     * 2. Create an internal measurement resource if not exist in the reporting server with the
     *    internal IDs. At this stage, cmmsMeasurementId is NULL, and the state is NOT_REQUESTED.
     *    Return Map<internal MeasurementId, weight>
     */

    // Create an internal metric resource with the list of internal measurement Ids and weights.

    /**
     * Measurement Supplier - createMeasurements
     * 1. For each internal measurement, a. call createMeasurement
     *     - request a corresponding kingdom measurement
     *     - update the cmmsMeasurementId in the internal measurement.
     */

    // Convert the internal metric to public and return it.

    return metric {}
  }

  /** Gets an [InternalMetric]. */
  private suspend fun getInternalMetricByIdempotencyKey(
    measurementConsumerReferenceId: String,
    metricIdempotencyKey: String,
  ): InternalMetric? {
    return try {
      internalMetricsStub.getMetricByIdempotencyKey(
        getMetricByIdempotencyKeyRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          this.metricIdempotencyKey = metricIdempotencyKey
        }
      )
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.NOT_FOUND) {
        throw Exception(
          "Unable to retrieve a metric from the reporting database using the provided " +
            "metricIdempotencyKey [$metricIdempotencyKey].",
          e
        )
      }
      null
    }
  }

  /** Gets an [InternalMetric]. */
  private suspend fun getInternalReportingSet(
    measurementConsumerReferenceId: String,
    reportingSetName: String,
  ): InternalReportingSet {
    val reportingSetKey =
      grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
        "Invalid reporting set name $reportingSetName."
      }

    grpcRequire(reportingSetKey.measurementConsumerId == measurementConsumerReferenceId) {
      "No access to the reporting set [$reportingSetName]."
    }

    return try {
      internalReportingSetsStub.getReportingSet(
        getInternalReportingSetRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          this.externalReportingSetId = apiIdToExternalId(reportingSetKey.reportingSetId)
        }
      )
    } catch (e: StatusException) {
      throw Exception(
        "Unable to retrieve a reporting set from the reporting database using the provided " +
          "reportingSet [$reportingSetName].",
        e
      )
    }
  }
}

private fun InternalSetExpression.toSetExpression(): SetExpression {
  TODO("Not yet implemented")
}

private fun InternalMetric.toMetric(): Metric {
  val source = this
  // val metricResourceName = MetricKey(
  //   measurementConsumerId = source.measurementConsumerReferenceId,
  //
  // )
  return metric {}
}
