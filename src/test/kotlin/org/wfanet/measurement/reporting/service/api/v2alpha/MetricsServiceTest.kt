/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Empty
import com.google.protobuf.duration
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import com.google.rpc.errorInfo
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Paths
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.Instant
import kotlin.math.ceil
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.random.Random
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.BatchCreateMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.BatchGetMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodology
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodologyKt
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.DeterministicCount
import org.wfanet.measurement.api.v2alpha.DeterministicCountDistinct
import org.wfanet.measurement.api.v2alpha.DeterministicSum
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.GetMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultOutput
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reportingMetadata
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.batchCreateMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.batchCreateMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.customDirectMethodology
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.deterministicCount
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.liquidLegionsDistribution
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.reachOnlyLiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.grpcStatusCode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toInterval
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricsRequest as InternalBatchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequestKt.measurementIds
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequestKt.measurementFailure
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequestKt.measurementResult
import org.wfanet.measurement.internal.reporting.v2.CustomDirectMethodology as InternalCustomDirectMethodology
import org.wfanet.measurement.internal.reporting.v2.CustomDirectMethodologyKt as InternalCustomDirectMethodologyKt
import org.wfanet.measurement.internal.reporting.v2.DeterministicCount as InternalDeterministicCount
import org.wfanet.measurement.internal.reporting.v2.DeterministicCountDistinct as InternalDeterministicCountDistinct
import org.wfanet.measurement.internal.reporting.v2.DeterministicSum as InternalDeterministicSum
import org.wfanet.measurement.internal.reporting.v2.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt as InternalMeasurementsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineImplBase as InternalMeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt as InternalMetricsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.NoiseMechanism
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSet.SetExpression as InternalSetExpression
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt as InternalReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt.primitiveReportingSetBasis
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt.weightedSubsetUnion
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt as InternalReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequestKt.filter
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsRequest as internalBatchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsResponse as internalBatchCreateMetricsResponse
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest as internalBatchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsResponse as internalBatchGetMetricsResponse
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest as internalCreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.customDirectMethodology as internalCustomDirectMethodology
import org.wfanet.measurement.internal.reporting.v2.deterministicCount as internalDeterministicCount
import org.wfanet.measurement.internal.reporting.v2.invalidateMetricRequest as internalInvalidateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.liquidLegionsDistribution as internalLiquidLegionsDistribution
import org.wfanet.measurement.internal.reporting.v2.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2.metric as internalMetric
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.reachOnlyLiquidLegionsSketchParams as internalReachOnlyLiquidLegionsSketchParams
import org.wfanet.measurement.internal.reporting.v2.reachOnlyLiquidLegionsV2
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.v2.streamMetricsRequest
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.FrequencyMetricVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.FrequencyVariances
import org.wfanet.measurement.measurementconsumer.stats.ImpressionMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.ImpressionMetricVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.Methodology
import org.wfanet.measurement.measurementconsumer.stats.ReachMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.ReachMetricVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.Variances
import org.wfanet.measurement.measurementconsumer.stats.WatchDurationMeasurementVarianceParams
import org.wfanet.measurement.measurementconsumer.stats.WatchDurationMetricVarianceParams
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.internal.InvalidMetricStateTransitionException
import org.wfanet.measurement.reporting.service.internal.MetricNotFoundException
import org.wfanet.measurement.reporting.v2alpha.ListMetricsPageTokenKt.previousPageEnd
import org.wfanet.measurement.reporting.v2alpha.ListMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricKt
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.impressionCountParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.reachAndFrequencyParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.reachParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.watchDurationParams
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.batchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.batchGetMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.getMetricRequest
import org.wfanet.measurement.reporting.v2alpha.invalidateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricsPageToken
import org.wfanet.measurement.reporting.v2alpha.listMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.univariateStatistics

private const val MAX_BATCH_SIZE = 1000
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000
private const val BATCH_GET_REPORTING_SETS_LIMIT = 1000
private const val BATCH_SET_CMMS_MEASUREMENT_IDS_LIMIT = 1000
private const val BATCH_SET_MEASUREMENT_RESULTS_LIMIT = 1000
private const val BATCH_SET_MEASUREMENT_FAILURES_LIMIT = 1000
private const val BATCH_KINGDOM_MEASUREMENTS_LIMIT = 50

private const val NUMBER_VID_BUCKETS = 300
private const val REACH_ONLY_VID_SAMPLING_WIDTH = 3.0f / NUMBER_VID_BUCKETS
private const val REACH_ONLY_VID_SAMPLING_START = 0.0f
private const val REACH_ONLY_REACH_EPSILON = 0.0041
private const val SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_WIDTH = 4.0f / NUMBER_VID_BUCKETS
private const val SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_START = 0.01f
private const val SINGLE_DATA_PROVIDER_REACH_ONLY_REACH_EPSILON = 0.0042

private const val REACH_FREQUENCY_VID_SAMPLING_WIDTH = 5.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_VID_SAMPLING_START = 48.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_REACH_EPSILON = 0.0033
private const val REACH_FREQUENCY_FREQUENCY_EPSILON = 0.115
private const val REACH_FREQUENCY_MAXIMUM_FREQUENCY = 5
private const val SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_WIDTH =
  5.1f / NUMBER_VID_BUCKETS
private const val SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_START =
  48.1f / NUMBER_VID_BUCKETS
private const val SINGLE_DATA_PROVIDER_REACH_FREQUENCY_REACH_EPSILON = 0.0034
private const val SINGLE_DATA_PROVIDER_REACH_FREQUENCY_FREQUENCY_EPSILON = 0.116

private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_VID_SAMPLING_START = 143.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_EPSILON = 0.0011
private const val IMPRESSION_MAXIMUM_FREQUENCY_PER_USER = 60
private const val IMPRESSION_CUSTOM_MAXIMUM_FREQUENCY_PER_USER = 100

private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_VID_SAMPLING_START = 205.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_EPSILON = 0.001
private val MAXIMUM_WATCH_DURATION_PER_USER = Durations.fromSeconds(4000)

private const val DIFFERENTIAL_PRIVACY_DELTA = 1e-12

private const val RANDOM_OUTPUT_INT = 0
private const val RANDOM_OUTPUT_LONG = 0L

private val METRIC_SPEC_CONFIG = metricSpecConfig {
  reachParams =
    MetricSpecConfigKt.reachParams {
      multipleDataProviderParams =
        MetricSpecConfigKt.samplingAndPrivacyParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          vidSamplingInterval =
            MetricSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = REACH_ONLY_VID_SAMPLING_START
                  width = REACH_ONLY_VID_SAMPLING_WIDTH
                }
            }
        }

      singleDataProviderParams =
        MetricSpecConfigKt.samplingAndPrivacyParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = SINGLE_DATA_PROVIDER_REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          vidSamplingInterval =
            MetricSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_START
                  width = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_WIDTH
                }
            }
        }
    }

  reachAndFrequencyParams =
    MetricSpecConfigKt.reachAndFrequencyParams {
      multipleDataProviderParams =
        MetricSpecConfigKt.reachAndFrequencySamplingAndPrivacyParams {
          reachPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          frequencyPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          vidSamplingInterval =
            MetricSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = REACH_FREQUENCY_VID_SAMPLING_START
                  width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
                }
            }
        }

      singleDataProviderParams =
        MetricSpecConfigKt.reachAndFrequencySamplingAndPrivacyParams {
          reachPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          frequencyPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_FREQUENCY_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          vidSamplingInterval =
            MetricSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_START
                  width = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_WIDTH
                }
            }
        }
      maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
    }

  impressionCountParams =
    MetricSpecConfigKt.impressionCountParams {
      params =
        MetricSpecConfigKt.samplingAndPrivacyParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = IMPRESSION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          vidSamplingInterval =
            MetricSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = IMPRESSION_VID_SAMPLING_START
                  width = IMPRESSION_VID_SAMPLING_WIDTH
                }
            }
        }
      maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
    }

  watchDurationParams =
    MetricSpecConfigKt.watchDurationParams {
      params =
        MetricSpecConfigKt.samplingAndPrivacyParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = WATCH_DURATION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          vidSamplingInterval =
            MetricSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = WATCH_DURATION_VID_SAMPLING_START
                  width = WATCH_DURATION_VID_SAMPLING_WIDTH
                }
            }
        }
      maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
    }

  populationCountParams = MetricSpecConfig.PopulationCountParams.getDefaultInstance()
}

private val SECRETS_DIR =
  getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )!!
    .toFile()

// Authentication key
private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"

// Aggregator certificate

private val AGGREGATOR_SIGNING_KEY: SigningKeyHandle by lazy {
  loadSigningKey(
    SECRETS_DIR.resolve("aggregator_cs_cert.der"),
    SECRETS_DIR.resolve("aggregator_cs_private.der"),
  )
}
private val AGGREGATOR_CERTIFICATE = certificate {
  name = "duchies/aggregator/certificates/abc123"
  x509Der = AGGREGATOR_SIGNING_KEY.certificate.encoded.toByteString()
}
private val AGGREGATOR_ROOT_CERTIFICATE: X509Certificate =
  readCertificate(SECRETS_DIR.resolve("aggregator_root.pem"))

// Measurement consumer crypto

private val TRUSTED_MEASUREMENT_CONSUMER_ISSUER: X509Certificate =
  readCertificate(SECRETS_DIR.resolve("mc_root.pem"))
private val MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE =
  loadSigningKey(SECRETS_DIR.resolve("mc_cs_cert.der"), SECRETS_DIR.resolve("mc_cs_private.der"))
private val MEASUREMENT_CONSUMER_CERTIFICATE = MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE.certificate
private val MEASUREMENT_CONSUMER_PRIVATE_KEY_HANDLE: PrivateKeyHandle =
  loadPrivateKey(SECRETS_DIR.resolve("mc_enc_private.tink"))
private val MEASUREMENT_CONSUMER_PUBLIC_KEY = encryptionPublicKey {
  format = EncryptionPublicKey.Format.TINK_KEYSET
  data = SECRETS_DIR.resolve("mc_enc_public.tink").readByteString()
}

private val MEASUREMENT_CONSUMERS: Map<MeasurementConsumerKey, MeasurementConsumer> =
  (1L..2L).associate {
    val measurementConsumerKey = MeasurementConsumerKey(ExternalId(it + 110L).apiId.value)
    val certificateKey =
      MeasurementConsumerCertificateKey(
        measurementConsumerKey.measurementConsumerId,
        ExternalId(it + 120L).apiId.value,
      )
    measurementConsumerKey to
      measurementConsumer {
        name = measurementConsumerKey.toName()
        certificate = certificateKey.toName()
        certificateDer = MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE.certificate.encoded.toByteString()
        publicKey =
          signEncryptionPublicKey(
            MEASUREMENT_CONSUMER_PUBLIC_KEY,
            MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE,
          )
      }
  }

private val CONFIG = measurementConsumerConfig {
  apiKey = API_AUTHENTICATION_KEY
  signingCertificateName = MEASUREMENT_CONSUMERS.values.first().certificate
  signingPrivateKeyPath = "mc_cs_private.der"
}

private val MEASUREMENT_CONSUMER_CONFIGS = measurementConsumerConfigs {
  configs[MEASUREMENT_CONSUMERS.values.first().name] = CONFIG
}

// InMemoryEncryptionKeyPairStore
private val ENCRYPTION_KEY_PAIR_STORE =
  InMemoryEncryptionKeyPairStore(
    MEASUREMENT_CONSUMERS.values.associateBy(
      { it.name },
      {
        listOf(
          it.publicKey.unpack<EncryptionPublicKey>().data to MEASUREMENT_CONSUMER_PRIVATE_KEY_HANDLE
        )
      },
    )
  )

private const val DEFAULT_VID_MODEL_LINE = "modelProviders/mp-1/modelSuites/ms-1/modelLines/default"
private const val MC_MODEL_LINE = "modelProviders/mp-1/modelSuites/ms-1/modelLines/mc-model-line"
private val MEASUREMENT_CONSUMER_MODEL_LINES: Map<String, String> =
  mapOf(MEASUREMENT_CONSUMERS.values.first().name to MC_MODEL_LINE)

private val DATA_PROVIDER_PUBLIC_KEY = encryptionPublicKey {
  format = EncryptionPublicKey.Format.TINK_KEYSET
  data = SECRETS_DIR.resolve("edp1_enc_public.tink").readByteString()
}
private val DATA_PROVIDER_PRIVATE_KEY_HANDLE =
  loadPrivateKey(SECRETS_DIR.resolve("edp1_enc_private.tink"))
private val DATA_PROVIDER_SIGNING_KEY =
  loadSigningKey(
    SECRETS_DIR.resolve("edp1_cs_cert.der"),
    SECRETS_DIR.resolve("edp1_cs_private.der"),
  )
private val DATA_PROVIDER_ROOT_CERTIFICATE = readCertificate(SECRETS_DIR.resolve("edp1_root.pem"))

// Data providers

private val DATA_PROVIDERS =
  (1L..3L).associate {
    val dataProviderKey = DataProviderKey(ExternalId(it + 550L).apiId.value)
    val certificateKey =
      DataProviderCertificateKey(dataProviderKey.dataProviderId, ExternalId(it + 560L).apiId.value)
    dataProviderKey to
      dataProvider {
        name = dataProviderKey.toName()
        certificate = certificateKey.toName()
        publicKey = signEncryptionPublicKey(DATA_PROVIDER_PUBLIC_KEY, DATA_PROVIDER_SIGNING_KEY)
      }
  }

private const val EXTERNAL_POPULATION_DATA_PROVIDER_ID = 100L
private val POPULATION_DATA_PROVIDER_ID =
  ExternalId(EXTERNAL_POPULATION_DATA_PROVIDER_ID).apiId.value
private val POPULATION_DATA_PROVIDER_NAME = "dataProviders/$POPULATION_DATA_PROVIDER_ID"
private val POPULATION_DATA_PROVIDER = dataProvider {
  name = POPULATION_DATA_PROVIDER_NAME
  certificate =
    DataProviderCertificateKey(POPULATION_DATA_PROVIDER_ID, ExternalId(1000L).apiId.value).toName()
  publicKey = signEncryptionPublicKey(DATA_PROVIDER_PUBLIC_KEY, DATA_PROVIDER_SIGNING_KEY)
}

// Event group keys

private val CMMS_EVENT_GROUP_KEYS =
  DATA_PROVIDERS.keys.mapIndexed { index, dataProviderKey ->
    CmmsEventGroupKey(dataProviderKey.dataProviderId, ExternalId(index + 660L).apiId.value)
  }

// Event filters
private const val INCREMENTAL_REPORTING_SET_FILTER = "AGE>18"
private const val METRIC_FILTER = "media_type==video"
private const val PRIMITIVE_REPORTING_SET_FILTER = "gender==male"
private val ALL_FILTERS =
  listOf(INCREMENTAL_REPORTING_SET_FILTER, METRIC_FILTER, PRIMITIVE_REPORTING_SET_FILTER)

// Containing Report
private const val CONTAINING_REPORT = "report X"

// Metric ID and Name
private const val METRIC_ID = "metric-id"
private val METRIC_NAME =
  MetricKey(MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId, METRIC_ID).toName()

// Internal reporting sets

private val INTERNAL_UNION_ALL_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = "220L"
  this.primitive =
    InternalReportingSetKt.primitive {
      eventGroupKeys += CMMS_EVENT_GROUP_KEYS.map { it.toInternal() }
    }
  filter = PRIMITIVE_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
  weightedSubsetUnions += weightedSubsetUnion {
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId = this@internalReportingSet.externalReportingSetId
      filters += this@internalReportingSet.filter
    }
    weight = 1
    binaryRepresentation = 1
  }
}
private val INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId + "1"
  this.primitive =
    InternalReportingSetKt.primitive {
      (0 until CMMS_EVENT_GROUP_KEYS.size - 1).map { i ->
        eventGroupKeys += CMMS_EVENT_GROUP_KEYS[i].toInternal()
      }
    }
  filter = PRIMITIVE_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
  weightedSubsetUnions += weightedSubsetUnion {
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId = this@internalReportingSet.externalReportingSetId
      filters += this@internalReportingSet.filter
    }
    weight = 1
    binaryRepresentation = 1
  }
}
private val INTERNAL_SINGLE_PUBLISHER_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId =
    INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId + "1"
  this.primitive =
    InternalReportingSetKt.primitive {
      eventGroupKeys +=
        (0L until 3L)
          .map { index ->
            CmmsEventGroupKey(
              DATA_PROVIDERS.keys.first().dataProviderId,
              ExternalId(index + 670L).apiId.value,
            )
          }
          .map { it.toInternal() }
    }
  filter = PRIMITIVE_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
  weightedSubsetUnions += weightedSubsetUnion {
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId = this@internalReportingSet.externalReportingSetId
      filters += this@internalReportingSet.filter
    }
    weight = 1
    binaryRepresentation = 1
  }
}

private val INTERNAL_INCREMENTAL_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId + "1"
  this.composite =
    InternalReportingSetKt.setExpression {
      operation = InternalSetExpression.Operation.DIFFERENCE
      lhs =
        InternalReportingSetKt.SetExpressionKt.operand {
          externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
        }
      rhs =
        InternalReportingSetKt.SetExpressionKt.operand {
          externalReportingSetId =
            INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
        }
    }
  filter = INCREMENTAL_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
  weightedSubsetUnions += weightedSubsetUnion {
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
      filters += INCREMENTAL_REPORTING_SET_FILTER
      filters += INTERNAL_UNION_ALL_REPORTING_SET.filter
    }
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId =
        INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
      filters += INCREMENTAL_REPORTING_SET_FILTER
      filters += INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.filter
    }
    weight = 1
    binaryRepresentation = 3
  }
  weightedSubsetUnions += weightedSubsetUnion {
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId =
        INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
      filters += INCREMENTAL_REPORTING_SET_FILTER
      filters += INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.filter
    }
    weight = -1
    binaryRepresentation = 2
  }
}
private val INTERNAL_POPULATION_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId + "1"
  this.primitive =
    InternalReportingSetKt.primitive {
      eventGroupKeys += CMMS_EVENT_GROUP_KEYS.first().toInternal()
    }
  filter = INCREMENTAL_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
  weightedSubsetUnions += weightedSubsetUnion {
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId = this@internalReportingSet.externalReportingSetId
    }
    weight = 1
    binaryRepresentation = 1
  }
}

// Time intervals

private val START_INSTANT = Instant.now()
private val TIME_RANGE = OpenEndTimeRange(START_INSTANT, START_INSTANT.plus(Duration.ofDays(1)))
private val TIME_INTERVAL: Interval = TIME_RANGE.toInterval()

// Requisition specs
private val REQUISITION_SPECS: Map<DataProviderKey, RequisitionSpec> =
  CMMS_EVENT_GROUP_KEYS.groupBy(
      { it.parentKey },
      {
        RequisitionSpecKt.eventGroupEntry {
          key = it.toName()
          value =
            RequisitionSpecKt.EventGroupEntryKt.value {
              collectionInterval = TIME_INTERVAL
              filter =
                RequisitionSpecKt.eventFilter {
                  expression =
                    "($INCREMENTAL_REPORTING_SET_FILTER) AND ($METRIC_FILTER) AND ($PRIMITIVE_REPORTING_SET_FILTER)"
                }
            }
        }
      },
    )
    .mapValues {
      requisitionSpec {
        events = RequisitionSpecKt.events { eventGroups += it.value }
        measurementPublicKey = MEASUREMENT_CONSUMERS.values.first().publicKey.message
        nonce = RANDOM_OUTPUT_LONG
      }
    }

// Data provider entries
private val DATA_PROVIDER_ENTRIES =
  REQUISITION_SPECS.mapValues { (dataProviderKey, requisitionSpec) ->
    val dataProvider = DATA_PROVIDERS.getValue(dataProviderKey)
    MeasurementKt.dataProviderEntry {
      key = dataProvider.name
      value =
        MeasurementKt.DataProviderEntryKt.value {
          dataProviderCertificate = dataProvider.certificate
          dataProviderPublicKey = dataProvider.publicKey.message
          encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signRequisitionSpec(requisitionSpec, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE),
              dataProvider.publicKey.unpack(),
            )
          nonceHash = Hashing.hashSha256(requisitionSpec.nonce)
        }
    }
  }

// Measurements

private val BASE_MEASUREMENT = measurement {
  measurementConsumerCertificate = MEASUREMENT_CONSUMERS.values.first().certificate
}

private const val LL_DISTRIBUTION_DECAY_RATE = 2e-2
private const val LL_DISTRIBUTION_SKETCH_SIZE = 20000L
private const val REACH_ONLY_LLV2_DECAY_RATE = 1e-2
private const val REACH_ONLY_LLV2_SKETCH_SIZE = 10000L

// Measurement values
private const val UNION_ALL_REACH_VALUE = 100_000L
private const val UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE = 70_000L
private const val INCREMENTAL_REACH_VALUE =
  UNION_ALL_REACH_VALUE - UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE
private const val REACH_FREQUENCY_REACH_VALUE = 100_000L
private val REACH_FREQUENCY_FREQUENCY_VALUE = mapOf(1L to 0.1, 2L to 0.2, 3L to 0.3, 4L to 0.4)
private const val IMPRESSION_VALUE = 1_000_000L
private val WATCH_DURATION_SECOND_LIST = listOf(100L, 200L, 300L)
private val WATCH_DURATION_LIST = WATCH_DURATION_SECOND_LIST.map { duration { seconds = it } }
private val TOTAL_WATCH_DURATION = duration { seconds = WATCH_DURATION_SECOND_LIST.sum() }
private const val TOTAL_POPULATION_VALUE = 1000L

// Internal incremental reach measurements

private val INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  cmmsCreateMeasurementRequestId = "UNION_ALL_REACH_MEASUREMENT"
  cmmsMeasurementId = externalIdToApiId(401L)
  timeInterval = TIME_INTERVAL
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
    filters += ALL_FILTERS
  }
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId =
      INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
    filters += ALL_FILTERS
  }
  state = InternalMeasurement.State.PENDING
  details = InternalMeasurementKt.details { dataProviderCount = 3 }
}

private val INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  cmmsCreateMeasurementRequestId = "UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT"
  cmmsMeasurementId = externalIdToApiId(402L)
  timeInterval = TIME_INTERVAL
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId =
      INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
    filters += ALL_FILTERS
  }
  state = InternalMeasurement.State.PENDING
  details = InternalMeasurementKt.details { dataProviderCount = 2 }
}

private val INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT =
  INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    details =
      details.copy {
        results +=
          InternalMeasurementKt.result {
            reach =
              InternalMeasurementKt.ResultKt.reach {
                value = UNION_ALL_REACH_VALUE
                noiseMechanism = NoiseMechanism.DISCRETE_GAUSSIAN
                reachOnlyLiquidLegionsV2 = reachOnlyLiquidLegionsV2 {
                  sketchParams = internalReachOnlyLiquidLegionsSketchParams {
                    decayRate = REACH_ONLY_LLV2_DECAY_RATE
                    maxSize = REACH_ONLY_LLV2_SKETCH_SIZE
                  }
                }
              }
          }
      }
  }

private val INTERNAL_SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    details =
      details.copy {
        results +=
          InternalMeasurementKt.result {
            reach =
              InternalMeasurementKt.ResultKt.reach {
                value = UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE
                noiseMechanism = NoiseMechanism.DISCRETE_GAUSSIAN
                reachOnlyLiquidLegionsV2 = reachOnlyLiquidLegionsV2 {
                  sketchParams = internalReachOnlyLiquidLegionsSketchParams {
                    decayRate = REACH_ONLY_LLV2_DECAY_RATE
                    maxSize = REACH_ONLY_LLV2_SKETCH_SIZE
                  }
                }
              }
          }
      }
  }

// Internal single publisher reach-frequency measurements
private val INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  cmmsCreateMeasurementRequestId = "SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT"
  cmmsMeasurementId = externalIdToApiId(443L)
  timeInterval = TIME_INTERVAL
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
    filters += METRIC_FILTER
    filters += PRIMITIVE_REPORTING_SET_FILTER
  }
  state = InternalMeasurement.State.PENDING
  details = InternalMeasurementKt.details { dataProviderCount = 1 }
}

private val INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT =
  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    details =
      details.copy {
        results +=
          InternalMeasurementKt.result {
            reach =
              InternalMeasurementKt.ResultKt.reach {
                value = REACH_FREQUENCY_REACH_VALUE
                noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                deterministicCountDistinct = InternalDeterministicCountDistinct.getDefaultInstance()
              }
            frequency =
              InternalMeasurementKt.ResultKt.frequency {
                relativeFrequencyDistribution.putAll(REACH_FREQUENCY_FREQUENCY_VALUE)
                noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                liquidLegionsDistribution = internalLiquidLegionsDistribution {
                  decayRate = LL_DISTRIBUTION_DECAY_RATE
                  maxSize = LL_DISTRIBUTION_SKETCH_SIZE
                }
              }
          }
      }
  }

// Internal single publisher impression measurements

private val INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  cmmsCreateMeasurementRequestId = "SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT"
  cmmsMeasurementId = externalIdToApiId(403L)
  timeInterval = TIME_INTERVAL
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
    filters += METRIC_FILTER
    filters += PRIMITIVE_REPORTING_SET_FILTER
  }
  state = InternalMeasurement.State.PENDING
  details = InternalMeasurementKt.details { dataProviderCount = 1 }
}

private val INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    state = InternalMeasurement.State.FAILED
    details =
      details.copy {
        failure =
          InternalMeasurementKt.failure {
            reason = InternalMeasurement.Failure.Reason.REQUISITION_REFUSED
            message = "Privacy budget exceeded."
          }
      }
  }

private val INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    details =
      details.copy {
        results +=
          InternalMeasurementKt.result {
            impression =
              InternalMeasurementKt.ResultKt.impression {
                value = IMPRESSION_VALUE
                noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                deterministicCount = InternalDeterministicCount.getDefaultInstance()
              }
          }
      }
  }

private val INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_CUSTOM_CAP =
  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    details =
      details.copy {
        results +=
          InternalMeasurementKt.result {
            impression =
              InternalMeasurementKt.ResultKt.impression {
                value = IMPRESSION_VALUE
                noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                deterministicCount = internalDeterministicCount {
                  customMaximumFrequencyPerUser = IMPRESSION_CUSTOM_MAXIMUM_FREQUENCY_PER_USER
                }
              }
          }
      }
  }

// Internal cross-publisher watch duration measurements
private val INTERNAL_REQUESTING_UNION_ALL_WATCH_DURATION_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  timeInterval = TIME_INTERVAL
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
    filters += listOf(METRIC_FILTER, PRIMITIVE_REPORTING_SET_FILTER)
  }
  details = InternalMeasurementKt.details { dataProviderCount = 3 }
}

private val INTERNAL_PENDING_NOT_CREATED_UNION_ALL_WATCH_DURATION_MEASUREMENT =
  INTERNAL_REQUESTING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
    cmmsMeasurementId = externalIdToApiId(414L)
    cmmsCreateMeasurementRequestId = "UNION_ALL_WATCH_DURATION_MEASUREMENT"
    state = InternalMeasurement.State.PENDING
  }

private val INTERNAL_PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT =
  INTERNAL_PENDING_NOT_CREATED_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
    cmmsMeasurementId = externalIdToApiId(404L)
  }

private val INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT =
  INTERNAL_PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    details =
      details.copy {
        results +=
          WATCH_DURATION_LIST.map { duration ->
            InternalMeasurementKt.result {
              watchDuration =
                InternalMeasurementKt.ResultKt.watchDuration {
                  value = duration
                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                  deterministicSum = InternalDeterministicSum.getDefaultInstance()
                }
            }
          }
      }
  }

// Internal population measurements

val INTERNAL_PENDING_POPULATION_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  cmmsCreateMeasurementRequestId = "POPULATION_MEASUREMENT"
  cmmsMeasurementId = externalIdToApiId(443L)
  timeInterval = TIME_INTERVAL
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_POPULATION_REPORTING_SET.externalReportingSetId
    filters += INCREMENTAL_REPORTING_SET_FILTER
  }
  state = InternalMeasurement.State.PENDING
  details = InternalMeasurementKt.details { dataProviderCount = 1 }
}

val INTERNAL_SUCCEEDED_POPULATION_MEASUREMENT =
  INTERNAL_PENDING_POPULATION_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    details =
      details.copy {
        results +=
          InternalMeasurementKt.result {
            population =
              InternalMeasurementKt.ResultKt.population { value = TOTAL_POPULATION_VALUE }
          }
      }
  }

// CMMS measurements

private val BASE_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()
  modelLine = MEASUREMENT_CONSUMER_MODEL_LINES[MEASUREMENT_CONSUMERS.values.first().name]!!
  reportingMetadata = reportingMetadata {
    report = CONTAINING_REPORT
    metric = METRIC_NAME
  }
}

// CMMS incremental reach measurements
private val UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC =
  BASE_MEASUREMENT_SPEC.copy {
    measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

    nonceHashes +=
      listOf(Hashing.hashSha256(RANDOM_OUTPUT_LONG), Hashing.hashSha256(RANDOM_OUTPUT_LONG))

    reach =
      MeasurementSpecKt.reach {
        privacyParams = differentialPrivacyParams {
          epsilon = REACH_ONLY_REACH_EPSILON
          delta = DIFFERENTIAL_PRIVACY_DELTA
        }
      }
    vidSamplingInterval =
      MeasurementSpecKt.vidSamplingInterval {
        start = REACH_ONLY_VID_SAMPLING_START
        width = REACH_ONLY_VID_SAMPLING_WIDTH
      }
  }

private val REACH_PROTOCOL_CONFIG: ProtocolConfig = protocolConfig {
  measurementType = ProtocolConfig.MeasurementType.REACH
  protocols +=
    ProtocolConfigKt.protocol {
      reachOnlyLiquidLegionsV2 =
        ProtocolConfigKt.reachOnlyLiquidLegionsV2 {
          sketchParams = reachOnlyLiquidLegionsSketchParams {
            decayRate = REACH_ONLY_LLV2_DECAY_RATE
            maxSize = REACH_ONLY_LLV2_SKETCH_SIZE
          }
          noiseMechanism = ProtocolConfig.NoiseMechanism.DISCRETE_GAUSSIAN
        }
    }
}

private val REQUESTING_UNION_ALL_REACH_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDERS.keys.map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(
        UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
          nonceHashes += Hashing.hashSha256(RANDOM_OUTPUT_LONG)
        },
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE,
      )
    measurementReferenceId =
      INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
  }
private val REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDERS.keys.take(2).map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(
        UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC,
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE,
      )
    measurementReferenceId =
      INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_UNION_ALL_REACH_MEASUREMENT =
  REQUESTING_UNION_ALL_REACH_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId,
        )
        .toName()
    protocolConfig = REACH_PROTOCOL_CONFIG
    state = Measurement.State.COMPUTING
  }
private val PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId,
        )
        .toName()
    protocolConfig = REACH_PROTOCOL_CONFIG
    state = Measurement.State.COMPUTING
  }

private val SUCCEEDED_UNION_ALL_REACH_MEASUREMENT =
  PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
    state = Measurement.State.SUCCEEDED

    results += resultOutput {
      val result =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = UNION_ALL_REACH_VALUE }
        }
      encryptedResult =
        encryptResult(signResult(result, AGGREGATOR_SIGNING_KEY), MEASUREMENT_CONSUMER_PUBLIC_KEY)
      certificate = AGGREGATOR_CERTIFICATE.name
    }
  }
private val SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
    state = Measurement.State.SUCCEEDED

    results += resultOutput {
      val result =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE }
        }
      encryptedResult =
        encryptResult(signResult(result, AGGREGATOR_SIGNING_KEY), MEASUREMENT_CONSUMER_PUBLIC_KEY)
      certificate = AGGREGATOR_CERTIFICATE.name
    }
  }

// CMMS single publisher reach-frequency measurements
private val SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT_SPEC =
  BASE_MEASUREMENT_SPEC.copy {
    measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

    nonceHashes.add(Hashing.hashSha256(RANDOM_OUTPUT_LONG))

    reachAndFrequency =
      MeasurementSpecKt.reachAndFrequency {
        reachPrivacyParams = differentialPrivacyParams {
          epsilon = REACH_FREQUENCY_REACH_EPSILON
          delta = DIFFERENTIAL_PRIVACY_DELTA
        }
        frequencyPrivacyParams = differentialPrivacyParams {
          epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
          delta = DIFFERENTIAL_PRIVACY_DELTA
        }
        maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
      }
    vidSamplingInterval =
      MeasurementSpecKt.vidSamplingInterval {
        start = REACH_FREQUENCY_VID_SAMPLING_START
        width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
      }
  }

private val REACH_FREQUENCY_PROTOCOL_CONFIG: ProtocolConfig = protocolConfig {
  measurementType = ProtocolConfig.MeasurementType.REACH_AND_FREQUENCY
  protocols +=
    ProtocolConfigKt.protocol {
      direct =
        ProtocolConfigKt.direct {
          noiseMechanisms +=
            listOf(
              ProtocolConfig.NoiseMechanism.NONE,
              ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE,
              ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
            )
          deterministicCount = ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
          liquidLegionsDistribution =
            ProtocolConfig.Direct.LiquidLegionsDistribution.getDefaultInstance()
        }
    }
}

private val REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDER_ENTRIES.getValue(DATA_PROVIDERS.keys.first())

    measurementSpec =
      signMeasurementSpec(
        SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT_SPEC,
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE,
      )
    measurementReferenceId =
      INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT =
  REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.cmmsMeasurementId,
        )
        .toName()
    protocolConfig = REACH_FREQUENCY_PROTOCOL_CONFIG
    state = Measurement.State.COMPUTING
  }

private val SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT =
  PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
    state = Measurement.State.SUCCEEDED

    results += resultOutput {
      val result =
        MeasurementKt.result {
          reach =
            MeasurementKt.ResultKt.reach {
              value = REACH_FREQUENCY_REACH_VALUE
              noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
              deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
            }
          frequency =
            MeasurementKt.ResultKt.frequency {
              relativeFrequencyDistribution.putAll(REACH_FREQUENCY_FREQUENCY_VALUE)
              noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
              liquidLegionsDistribution = liquidLegionsDistribution {
                decayRate = LL_DISTRIBUTION_DECAY_RATE
                maxSize = LL_DISTRIBUTION_SKETCH_SIZE
              }
            }
        }
      encryptedResult =
        encryptResult(signResult(result, AGGREGATOR_SIGNING_KEY), MEASUREMENT_CONSUMER_PUBLIC_KEY)
      certificate = AGGREGATOR_CERTIFICATE.name
    }
  }

// CMMS single publisher impression measurements
private val SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC =
  BASE_MEASUREMENT_SPEC.copy {
    measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

    nonceHashes.add(Hashing.hashSha256(RANDOM_OUTPUT_LONG))

    impression =
      MeasurementSpecKt.impression {
        privacyParams = differentialPrivacyParams {
          epsilon = IMPRESSION_EPSILON
          delta = DIFFERENTIAL_PRIVACY_DELTA
        }
        maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
      }
    vidSamplingInterval =
      MeasurementSpecKt.vidSamplingInterval {
        start = IMPRESSION_VID_SAMPLING_START
        width = IMPRESSION_VID_SAMPLING_WIDTH
      }
  }

private val IMPRESSION_PROTOCOL_CONFIG: ProtocolConfig = protocolConfig {
  measurementType = ProtocolConfig.MeasurementType.IMPRESSION
  protocols +=
    ProtocolConfigKt.protocol {
      direct =
        ProtocolConfigKt.direct {
          noiseMechanisms +=
            listOf(
              ProtocolConfig.NoiseMechanism.NONE,
              ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE,
              ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
            )
          deterministicCount = ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
        }
    }
}

private val REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDER_ENTRIES.getValue(DATA_PROVIDERS.keys.first())

    measurementSpec =
      signMeasurementSpec(
        SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC,
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE,
      )
    measurementReferenceId =
      INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId,
        )
        .toName()
    protocolConfig = IMPRESSION_PROTOCOL_CONFIG
    state = Measurement.State.COMPUTING
  }

private val SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    state = Measurement.State.SUCCEEDED
    results += resultOutput {
      val result =
        MeasurementKt.result {
          impression =
            MeasurementKt.ResultKt.impression {
              value = IMPRESSION_VALUE
              noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
              deterministicCount = DeterministicCount.getDefaultInstance()
            }
        }
      encryptedResult =
        encryptResult(signResult(result, AGGREGATOR_SIGNING_KEY), MEASUREMENT_CONSUMER_PUBLIC_KEY)
      certificate = AGGREGATOR_CERTIFICATE.name
    }
  }

private val SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_CUSTOM_CAP =
  PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    state = Measurement.State.SUCCEEDED
    results += resultOutput {
      val result =
        MeasurementKt.result {
          impression =
            MeasurementKt.ResultKt.impression {
              value = IMPRESSION_VALUE
              noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
              deterministicCount = deterministicCount {
                customMaximumFrequencyPerUser = IMPRESSION_CUSTOM_MAXIMUM_FREQUENCY_PER_USER
              }
            }
        }
      encryptedResult =
        encryptResult(signResult(result, AGGREGATOR_SIGNING_KEY), MEASUREMENT_CONSUMER_PUBLIC_KEY)
      certificate = AGGREGATOR_CERTIFICATE.name
    }
  }

// CMMS cross publisher watch duration measurements
private val UNION_ALL_WATCH_DURATION_MEASUREMENT_SPEC =
  BASE_MEASUREMENT_SPEC.copy {
    measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

    nonceHashes +=
      listOf(
        Hashing.hashSha256(RANDOM_OUTPUT_LONG),
        Hashing.hashSha256(RANDOM_OUTPUT_LONG),
        Hashing.hashSha256(RANDOM_OUTPUT_LONG),
      )

    duration =
      MeasurementSpecKt.duration {
        privacyParams = differentialPrivacyParams {
          epsilon = WATCH_DURATION_EPSILON
          delta = DIFFERENTIAL_PRIVACY_DELTA
        }
        privacyParams = differentialPrivacyParams {
          epsilon = WATCH_DURATION_EPSILON
          delta = DIFFERENTIAL_PRIVACY_DELTA
        }
        maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
      }
    vidSamplingInterval =
      MeasurementSpecKt.vidSamplingInterval {
        start = WATCH_DURATION_VID_SAMPLING_START
        width = WATCH_DURATION_VID_SAMPLING_WIDTH
      }
  }

private val WATCH_DURATION_PROTOCOL_CONFIG: ProtocolConfig = protocolConfig {
  measurementType = ProtocolConfig.MeasurementType.DURATION
  protocols +=
    ProtocolConfigKt.protocol {
      direct =
        ProtocolConfigKt.direct {
          noiseMechanisms +=
            listOf(
              ProtocolConfig.NoiseMechanism.NONE,
              ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE,
              ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
            )
          deterministicSum = ProtocolConfig.Direct.DeterministicSum.getDefaultInstance()
        }
    }
}

private val REQUESTING_UNION_ALL_WATCH_DURATION_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDERS.keys.map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(
        UNION_ALL_WATCH_DURATION_MEASUREMENT_SPEC.copy {
          nonceHashes += Hashing.hashSha256(RANDOM_OUTPUT_LONG)
        },
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE,
      )
  }

private val PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT =
  REQUESTING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.cmmsMeasurementId,
        )
        .toName()
    protocolConfig = WATCH_DURATION_PROTOCOL_CONFIG
    state = Measurement.State.COMPUTING
  }

private val SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT =
  PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
    state = Measurement.State.SUCCEEDED

    results +=
      DATA_PROVIDERS.keys.zip(WATCH_DURATION_LIST).map { (dataProviderKey, watchDuration) ->
        val dataProvider = DATA_PROVIDERS.getValue(dataProviderKey)
        resultOutput {
          val result =
            MeasurementKt.result {
              this.watchDuration =
                MeasurementKt.ResultKt.watchDuration {
                  value = watchDuration
                  noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
                  deterministicSum = DeterministicSum.getDefaultInstance()
                }
            }
          encryptedResult =
            encryptResult(
              signResult(result, DATA_PROVIDER_SIGNING_KEY),
              MEASUREMENT_CONSUMER_PUBLIC_KEY,
            )
          certificate = dataProvider.certificate
        }
      }
  }

// CMMS population measurements
private val POPULATION_MEASUREMENT_SPEC =
  BASE_MEASUREMENT_SPEC.copy {
    measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

    nonceHashes +=
      listOf(
        Hashing.hashSha256(RANDOM_OUTPUT_LONG),
        Hashing.hashSha256(RANDOM_OUTPUT_LONG),
        Hashing.hashSha256(RANDOM_OUTPUT_LONG),
      )

    population = MeasurementSpec.Population.getDefaultInstance()
  }

private val REQUESTING_POPULATION_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    dataProviders += dataProviderEntry {
      val requisitionSpec = requisitionSpec {
        population =
          RequisitionSpecKt.population {
            filter = RequisitionSpecKt.eventFilter { expression = "(($METRIC_FILTER))" }
            interval = TIME_INTERVAL
          }
        measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()
        nonce = RANDOM_OUTPUT_LONG
      }
      val encryptRequisitionSpec =
        encryptRequisitionSpec(
          signRequisitionSpec(requisitionSpec, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE),
          POPULATION_DATA_PROVIDER.publicKey.unpack(),
        )

      key = POPULATION_DATA_PROVIDER_NAME
      value =
        MeasurementKt.DataProviderEntryKt.value {
          dataProviderCertificate = POPULATION_DATA_PROVIDER.certificate
          dataProviderPublicKey = POPULATION_DATA_PROVIDER.publicKey.message
          this.encryptedRequisitionSpec = encryptRequisitionSpec
          nonceHash = Hashing.hashSha256(requisitionSpec.nonce)
        }
    }

    measurementSpec =
      signMeasurementSpec(POPULATION_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)

    measurementReferenceId = INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_POPULATION_MEASUREMENT =
  REQUESTING_POPULATION_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsMeasurementId,
        )
        .toName()
    state = Measurement.State.COMPUTING
  }

// Metric Specs

private val REACH_METRIC_SPEC: MetricSpec = metricSpec {
  reach = reachParams { privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance() }
}
private val REACH_FREQUENCY_METRIC_SPEC: MetricSpec = metricSpec {
  reachAndFrequency = reachAndFrequencyParams {
    reachPrivacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
    frequencyPrivacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
  }
}
private val IMPRESSION_COUNT_METRIC_SPEC: MetricSpec = metricSpec {
  impressionCount = impressionCountParams {
    privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
  }
}
private val WATCH_DURATION_METRIC_SPEC: MetricSpec = metricSpec {
  watchDuration = watchDurationParams {
    privacyParams = MetricSpec.DifferentialPrivacyParams.getDefaultInstance()
  }
}

private val POPULATION_METRIC_SPEC: MetricSpec = metricSpec {
  populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
}

// Metrics

// Metric idempotency keys
private const val INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY = "TEST_INCREMENTAL_REACH_METRIC"

// Internal Incremental Metrics
private val INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC = internalMetric {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId
  timeInterval = TIME_INTERVAL
  metricSpec = internalMetricSpec {
    reach =
      InternalMetricSpecKt.reachParams {
        multipleDataProviderParams =
          InternalMetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              InternalMetricSpecKt.differentialPrivacyParams {
                epsilon = REACH_ONLY_REACH_EPSILON
                delta = DIFFERENTIAL_PRIVACY_DELTA
              }
            vidSamplingInterval =
              InternalMetricSpecKt.vidSamplingInterval {
                start = REACH_ONLY_VID_SAMPLING_START
                width = REACH_ONLY_VID_SAMPLING_WIDTH
              }
          }
      }
  }
  weightedMeasurements += weightedMeasurement {
    weight = 1
    binaryRepresentation = 3
    measurement =
      INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
        clearCmmsCreateMeasurementRequestId()
        clearCmmsMeasurementId()
        clearState()
      }
  }
  weightedMeasurements += weightedMeasurement {
    weight = -1
    binaryRepresentation = 2
    measurement =
      INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
        clearCmmsCreateMeasurementRequestId()
        clearCmmsMeasurementId()
        clearState()
      }
  }
  details =
    InternalMetricKt.details {
      filters += listOf(METRIC_FILTER)
      containingReport = CONTAINING_REPORT
    }
}

private val INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC =
  INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC.copy {
    externalMetricId = "331L"
    createTime = Instant.now().toProtoTime()
    state = InternalMetric.State.RUNNING
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 3
      measurement = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy { clearCmmsMeasurementId() }
    }
    weightedMeasurements += weightedMeasurement {
      weight = -1
      binaryRepresentation = 2
      measurement =
        INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
          clearCmmsMeasurementId()
        }
    }
  }

private val INTERNAL_PENDING_INCREMENTAL_REACH_METRIC =
  INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC.copy {
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 3
      measurement = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT
    }
    weightedMeasurements += weightedMeasurement {
      weight = -1
      binaryRepresentation = 2
      measurement = INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
    }
  }

private val INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC =
  INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.copy {
    state = InternalMetric.State.SUCCEEDED
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 3
      measurement = INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT
    }
    weightedMeasurements += weightedMeasurement {
      weight = -1
      binaryRepresentation = 2
      measurement = INTERNAL_SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
    }
  }

// Internal Single publisher reach-frequency metrics
private val INTERNAL_REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC = internalMetric {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
  timeInterval = TIME_INTERVAL
  metricSpec = internalMetricSpec {
    reachAndFrequency =
      InternalMetricSpecKt.reachAndFrequencyParams {
        multipleDataProviderParams =
          InternalMetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
            reachPrivacyParams =
              InternalMetricSpecKt.differentialPrivacyParams {
                epsilon = REACH_FREQUENCY_REACH_EPSILON
                delta = DIFFERENTIAL_PRIVACY_DELTA
              }
            frequencyPrivacyParams =
              InternalMetricSpecKt.differentialPrivacyParams {
                epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
                delta = DIFFERENTIAL_PRIVACY_DELTA
              }
            vidSamplingInterval =
              InternalMetricSpecKt.vidSamplingInterval {
                start = REACH_FREQUENCY_VID_SAMPLING_START
                width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
              }
          }
        maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
      }
  }
  weightedMeasurements += weightedMeasurement {
    weight = 1
    binaryRepresentation = 1
    measurement =
      INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
        clearCmmsCreateMeasurementRequestId()
        clearCmmsMeasurementId()
        clearState()
      }
  }
  details =
    InternalMetricKt.details {
      filters += listOf(METRIC_FILTER)
      containingReport = CONTAINING_REPORT
    }
}

private val INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC =
  INTERNAL_REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
    externalMetricId = "332L"
    createTime = Instant.now().toProtoTime()
    state = InternalMetric.State.RUNNING
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement =
        INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
          clearCmmsMeasurementId()
        }
    }
  }

private val INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC =
  INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
    }
  }

private val INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC =
  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
    state = InternalMetric.State.SUCCEEDED
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
    }
  }

// Internal Single publisher impression metrics
private val INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC = internalMetric {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
  timeInterval = TIME_INTERVAL
  metricSpec = internalMetricSpec {
    impressionCount =
      InternalMetricSpecKt.impressionCountParams {
        params =
          InternalMetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              InternalMetricSpecKt.differentialPrivacyParams {
                epsilon = IMPRESSION_EPSILON
                delta = DIFFERENTIAL_PRIVACY_DELTA
              }
            vidSamplingInterval =
              InternalMetricSpecKt.vidSamplingInterval {
                start = IMPRESSION_VID_SAMPLING_START
                width = IMPRESSION_VID_SAMPLING_WIDTH
              }
          }
        maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
      }
  }
  weightedMeasurements += weightedMeasurement {
    weight = 1
    binaryRepresentation = 1
    measurement =
      INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
        clearCmmsCreateMeasurementRequestId()
        clearCmmsMeasurementId()
        clearState()
      }
  }
  details =
    InternalMetricKt.details {
      filters += listOf(METRIC_FILTER)
      containingReport = CONTAINING_REPORT
    }
}

private val INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    externalMetricId = "333L"
    createTime = Instant.now().toProtoTime()
    state = InternalMetric.State.RUNNING
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement =
        INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy { clearCmmsMeasurementId() }
    }
  }

private val INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
    }
  }

private val INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    state = InternalMetric.State.FAILED
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
    }
  }

private val INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    state = InternalMetric.State.SUCCEEDED
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
    }
  }

private val INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC_CUSTOM_CAP =
  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    state = InternalMetric.State.SUCCEEDED
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_CUSTOM_CAP
    }
  }

// Internal Cross Publisher Watch Duration Metrics
private val INTERNAL_REQUESTING_CROSS_PUBLISHER_WATCH_DURATION_METRIC = internalMetric {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
  timeInterval = TIME_INTERVAL
  metricSpec = internalMetricSpec {
    watchDuration =
      InternalMetricSpecKt.watchDurationParams {
        params =
          InternalMetricSpecKt.samplingAndPrivacyParams {
            privacyParams =
              InternalMetricSpecKt.differentialPrivacyParams {
                epsilon = WATCH_DURATION_EPSILON
                delta = DIFFERENTIAL_PRIVACY_DELTA
              }
            vidSamplingInterval =
              InternalMetricSpecKt.vidSamplingInterval {
                start = WATCH_DURATION_VID_SAMPLING_START
                width = WATCH_DURATION_VID_SAMPLING_WIDTH
              }
          }

        maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
      }
  }
  weightedMeasurements += weightedMeasurement {
    weight = 1
    binaryRepresentation = 1
    measurement = INTERNAL_REQUESTING_UNION_ALL_WATCH_DURATION_MEASUREMENT
  }
  details =
    InternalMetricKt.details {
      filters += listOf(METRIC_FILTER)
      containingReport = CONTAINING_REPORT
    }
}

private val INTERNAL_PENDING_INITIAL_CROSS_PUBLISHER_WATCH_DURATION_METRIC =
  INTERNAL_REQUESTING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
    externalMetricId = "334L"
    createTime = Instant.now().toProtoTime()
    state = InternalMetric.State.RUNNING
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_PENDING_NOT_CREATED_UNION_ALL_WATCH_DURATION_MEASUREMENT
    }
  }

private val INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC =
  INTERNAL_PENDING_INITIAL_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT
    }
  }

private val INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC =
  INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
    state = InternalMetric.State.SUCCEEDED
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT
    }
  }

// Internal population metric
val INTERNAL_REQUESTING_POPULATION_METRIC = internalMetric {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_POPULATION_REPORTING_SET.externalReportingSetId
  timeInterval = TIME_INTERVAL
  metricSpec = internalMetricSpec {
    populationCount = InternalMetricSpec.PopulationCountParams.getDefaultInstance()
  }

  weightedMeasurements += weightedMeasurement {
    weight = 1
    binaryRepresentation = 1
    measurement =
      INTERNAL_PENDING_POPULATION_MEASUREMENT.copy {
        clearCmmsCreateMeasurementRequestId()
        clearCmmsMeasurementId()
        clearState()
      }
  }
  details =
    InternalMetricKt.details {
      filters += listOf(INCREMENTAL_REPORTING_SET_FILTER)
      containingReport = CONTAINING_REPORT
    }
}

private val INTERNAL_PENDING_INITIAL_POPULATION_METRIC =
  INTERNAL_REQUESTING_POPULATION_METRIC.copy {
    externalMetricId = "331L"
    createTime = Instant.now().toProtoTime()
    state = InternalMetric.State.RUNNING
    weightedMeasurements.clear()

    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_PENDING_POPULATION_MEASUREMENT.copy { clearCmmsMeasurementId() }
    }
  }

val INTERNAL_PENDING_POPULATION_METRIC =
  INTERNAL_PENDING_INITIAL_POPULATION_METRIC.copy {
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_PENDING_POPULATION_MEASUREMENT
    }
  }

val INTERNAL_SUCCEEDED_POPULATION_METRIC =
  INTERNAL_PENDING_POPULATION_METRIC.copy {
    state = InternalMetric.State.SUCCEEDED
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_SUCCEEDED_POPULATION_MEASUREMENT
    }
  }

// Public Metrics

// Incremental reach metrics
private val REQUESTING_INCREMENTAL_REACH_METRIC = metric {
  reportingSet = INTERNAL_INCREMENTAL_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = REACH_METRIC_SPEC
  filters += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.details.filtersList
  containingReport = CONTAINING_REPORT
}

private val PENDING_INCREMENTAL_REACH_METRIC =
  REQUESTING_INCREMENTAL_REACH_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId,
        )
        .toName()
    state = Metric.State.RUNNING
    metricSpec = metricSpec {
      reach = reachParams {
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = REACH_ONLY_REACH_EPSILON
            delta = DIFFERENTIAL_PRIVACY_DELTA
          }
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = REACH_ONLY_VID_SAMPLING_START
          width = REACH_ONLY_VID_SAMPLING_WIDTH
        }
    }
    createTime = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.createTime
  }

private const val VARIANCE_VALUE = 4.0
private const val SINGLE_DATA_PROVIDER_VARIANCE_VALUE = 5.0

private val FREQUENCY_VARIANCE: Map<Int, Double> =
  (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).associateWith { it.toDouble().pow(2.0) }
private val FREQUENCY_VARIANCES =
  FrequencyVariances(FREQUENCY_VARIANCE, FREQUENCY_VARIANCE, FREQUENCY_VARIANCE, FREQUENCY_VARIANCE)
private val SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCE: Map<Int, Double> =
  (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).associateWith { it.toDouble().pow(3.0) }
private val SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCES =
  FrequencyVariances(
    SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCE,
    SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCE,
    SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCE,
    SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCE,
  )

private val SUCCEEDED_INCREMENTAL_REACH_METRIC =
  PENDING_INCREMENTAL_REACH_METRIC.copy {
    state = Metric.State.SUCCEEDED

    result = metricResult {
      reach =
        MetricResultKt.reachResult {
          value = INCREMENTAL_REACH_VALUE
          univariateStatistics = univariateStatistics { standardDeviation = sqrt(VARIANCE_VALUE) }
        }
      cmmsMeasurements += PENDING_UNION_ALL_REACH_MEASUREMENT.name
      cmmsMeasurements += PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name
    }
  }

// Single publisher reach-frequency metrics
private val REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC = metric {
  reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = REACH_FREQUENCY_METRIC_SPEC
  filters += INTERNAL_REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.details.filtersList
  containingReport = CONTAINING_REPORT
}

private val PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC =
  REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.externalMetricId,
        )
        .toName()
    metricSpec = metricSpec {
      reachAndFrequency = reachAndFrequencyParams {
        reachPrivacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = REACH_FREQUENCY_REACH_EPSILON
            delta = DIFFERENTIAL_PRIVACY_DELTA
          }
        frequencyPrivacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
            delta = DIFFERENTIAL_PRIVACY_DELTA
          }
        maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = REACH_FREQUENCY_VID_SAMPLING_START
          width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
        }
    }
    state = Metric.State.RUNNING
    createTime = INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.createTime
  }

private val SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC =
  PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
    state = Metric.State.SUCCEEDED
    result = metricResult {
      cmmsMeasurements += PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.name
      reachAndFrequency =
        MetricResultKt.reachAndFrequencyResult {
          reach =
            MetricResultKt.reachResult {
              value = REACH_FREQUENCY_REACH_VALUE
              univariateStatistics = univariateStatistics {
                standardDeviation = sqrt(VARIANCE_VALUE)
              }
            }
          frequencyHistogram =
            MetricResultKt.histogramResult {
              bins +=
                (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).map { frequency ->
                  MetricResultKt.HistogramResultKt.bin {
                    label = frequency.toString()
                    binResult =
                      MetricResultKt.HistogramResultKt.binResult {
                        value =
                          REACH_FREQUENCY_REACH_VALUE *
                            REACH_FREQUENCY_FREQUENCY_VALUE.getOrDefault(frequency.toLong(), 0.0)
                      }
                    resultUnivariateStatistics = univariateStatistics {
                      standardDeviation =
                        sqrt(FREQUENCY_VARIANCES.countVariances.getValue(frequency))
                    }
                    relativeUnivariateStatistics = univariateStatistics {
                      standardDeviation =
                        sqrt(FREQUENCY_VARIANCES.relativeVariances.getValue(frequency))
                    }
                    kPlusUnivariateStatistics = univariateStatistics {
                      standardDeviation =
                        sqrt(FREQUENCY_VARIANCES.kPlusCountVariances.getValue(frequency))
                    }
                    relativeKPlusUnivariateStatistics = univariateStatistics {
                      standardDeviation =
                        sqrt(FREQUENCY_VARIANCES.kPlusRelativeVariances.getValue(frequency))
                    }
                  }
                }
            }
        }
    }
  }

// Single publisher impression metrics
private val REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC = metric {
  reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = IMPRESSION_COUNT_METRIC_SPEC
  filters += INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.details.filtersList
  containingReport = CONTAINING_REPORT
}

private val PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId,
        )
        .toName()
    metricSpec = metricSpec {
      impressionCount = impressionCountParams {
        params =
          params.copy {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = IMPRESSION_EPSILON
                delta = DIFFERENTIAL_PRIVACY_DELTA
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start = IMPRESSION_VID_SAMPLING_START
                width = IMPRESSION_VID_SAMPLING_WIDTH
              }
          }
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = IMPRESSION_EPSILON
            delta = DIFFERENTIAL_PRIVACY_DELTA
          }
        maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = IMPRESSION_VID_SAMPLING_START
          width = IMPRESSION_VID_SAMPLING_WIDTH
        }
    }
    state = Metric.State.RUNNING
    createTime = INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.createTime
  }

private val FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    state = Metric.State.FAILED
    result = metricResult {
      cmmsMeasurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name
    }
  }

private val SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    state = Metric.State.SUCCEEDED
    result = metricResult {
      impressionCount =
        MetricResultKt.impressionCountResult {
          value = IMPRESSION_VALUE
          univariateStatistics = univariateStatistics { standardDeviation = sqrt(VARIANCE_VALUE) }
        }
      cmmsMeasurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name
    }
  }

// Cross publisher watch duration metrics
private val REQUESTING_CROSS_PUBLISHER_WATCH_DURATION_METRIC = metric {
  reportingSet = INTERNAL_UNION_ALL_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = WATCH_DURATION_METRIC_SPEC
  filters += INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.details.filtersList
  containingReport = CONTAINING_REPORT
}

private val PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC =
  REQUESTING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.externalMetricId,
        )
        .toName()
    metricSpec = metricSpec {
      watchDuration = watchDurationParams {
        params =
          params.copy {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = WATCH_DURATION_EPSILON
                delta = DIFFERENTIAL_PRIVACY_DELTA
              }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start = WATCH_DURATION_VID_SAMPLING_START
                width = WATCH_DURATION_VID_SAMPLING_WIDTH
              }
          }
        privacyParams =
          MetricSpecKt.differentialPrivacyParams {
            epsilon = WATCH_DURATION_EPSILON
            delta = DIFFERENTIAL_PRIVACY_DELTA
          }
        maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
      }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = WATCH_DURATION_VID_SAMPLING_START
          width = WATCH_DURATION_VID_SAMPLING_WIDTH
        }
    }
    state = Metric.State.RUNNING
    createTime = INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.createTime
  }

private val SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC =
  PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
    state = Metric.State.SUCCEEDED
    result = metricResult {
      watchDuration =
        MetricResultKt.watchDurationResult {
          value = TOTAL_WATCH_DURATION.seconds.toDouble()
          univariateStatistics = univariateStatistics {
            standardDeviation = sqrt(WATCH_DURATION_LIST.sumOf { VARIANCE_VALUE })
          }
        }
      cmmsMeasurements += PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name
    }
  }

// Population metric
val REQUESTING_POPULATION_METRIC = metric {
  reportingSet = INTERNAL_POPULATION_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = POPULATION_METRIC_SPEC
  filters += INTERNAL_PENDING_POPULATION_METRIC.details.filtersList
  containingReport = CONTAINING_REPORT
}

val PENDING_POPULATION_METRIC =
  REQUESTING_POPULATION_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_POPULATION_METRIC.externalMetricId,
        )
        .toName()
    state = Metric.State.RUNNING
    metricSpec = metricSpec {
      populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
    }
    createTime = INTERNAL_PENDING_POPULATION_METRIC.createTime
  }

val SUCCEEDED_POPULATION_METRIC =
  PENDING_POPULATION_METRIC.copy {
    state = Metric.State.SUCCEEDED
    result = metricResult {
      populationCount = MetricResultKt.populationCountResult { value = TOTAL_POPULATION_VALUE }
      cmmsMeasurements += PENDING_POPULATION_MEASUREMENT.name
    }
  }

@RunWith(JUnit4::class)
class MetricsServiceTest {
  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    onBlocking { checkPermissions(any()) } doReturn CheckPermissionsResponse.getDefaultInstance()
  }

  private val internalMetricsMock: MetricsCoroutineImplBase = mockService {
    onBlocking { createMetric(any()) }.thenReturn(INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC)
    onBlocking { batchCreateMetrics(any()) }
      .thenReturn(
        internalBatchCreateMetricsResponse {
          metrics += INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC
          metrics += INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )
    onBlocking { streamMetrics(any()) }
      .thenReturn(
        flowOf(
          INTERNAL_PENDING_INCREMENTAL_REACH_METRIC,
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC,
        )
      )

    val internalPendingMetrics =
      listOf(
          INTERNAL_PENDING_INCREMENTAL_REACH_METRIC,
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC,
          INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC,
        )
        .associateBy { it.externalMetricId }
    onBlocking { batchGetMetrics(any()) } doAnswer
      { invocation ->
        val request: InternalBatchGetMetricsRequest = invocation.getArgument(0)
        internalBatchGetMetricsResponse {
          metrics +=
            request.externalMetricIdsList.map {
              internalPendingMetrics[it]
                ?: throw Status.NOT_FOUND.withDescription("Metric with external ID $it not found")
                  .asRuntimeException()
            }
        }
      }
  }

  private val internalReportingSetsMock:
    InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase =
    mockService {
      onBlocking { batchGetReportingSets(any()) }
        .thenAnswer {
          val request = it.arguments[0] as BatchGetReportingSetsRequest
          val internalReportingSetsMap =
            mapOf(
              INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId to
                INTERNAL_INCREMENTAL_REPORTING_SET,
              INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId to
                INTERNAL_UNION_ALL_REPORTING_SET,
              INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId to
                INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET,
              INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId to
                INTERNAL_SINGLE_PUBLISHER_REPORTING_SET,
              INTERNAL_POPULATION_REPORTING_SET.externalReportingSetId to
                INTERNAL_POPULATION_REPORTING_SET,
            )
          batchGetReportingSetsResponse {
            reportingSets +=
              request.externalReportingSetIdsList.map { externalReportingSetId ->
                internalReportingSetsMap.getValue(externalReportingSetId)
              }
          }
        }
    }

  private val internalMeasurementsMock: InternalMeasurementsCoroutineImplBase = mockService {
    onBlocking { batchSetCmmsMeasurementIds(any()) }.thenReturn(Empty.getDefaultInstance())
    onBlocking { batchSetMeasurementResults(any()) }.thenReturn(Empty.getDefaultInstance())
    onBlocking { batchSetMeasurementFailures(any()) }.thenReturn(Empty.getDefaultInstance())
  }

  private val measurementsMock: MeasurementsCoroutineImplBase = mockService {
    onBlocking { batchGetMeasurements(any()) }
      .thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_UNION_ALL_REACH_MEASUREMENT.name to PENDING_UNION_ALL_REACH_MEASUREMENT,
            PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to
              PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
            PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
              PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT,
            PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name to
              PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT,
            PENDING_POPULATION_MEASUREMENT.name to PENDING_POPULATION_MEASUREMENT,
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

    onBlocking { batchCreateMeasurements(any()) }
      .thenAnswer {
        val batchCreateMeasurementsRequest = it.arguments[0] as BatchCreateMeasurementsRequest
        val measurementsMap =
          mapOf(
            INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId to
              PENDING_UNION_ALL_REACH_MEASUREMENT,
            INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
              .cmmsCreateMeasurementRequestId to
              PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
            INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
              .cmmsCreateMeasurementRequestId to PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT,
            INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsCreateMeasurementRequestId to
              PENDING_POPULATION_MEASUREMENT,
          )
        batchCreateMeasurementsResponse {
          measurements +=
            batchCreateMeasurementsRequest.requestsList.map { createMeasurementRequest ->
              measurementsMap.getValue(createMeasurementRequest.requestId)
            }
        }
      }
  }

  private val measurementConsumersMock:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase =
    mockService {
      onBlocking {
          getMeasurementConsumer(
            eq(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })
          )
        }
        .thenReturn(MEASUREMENT_CONSUMERS.values.first())
    }

  private val dataProvidersMock: DataProvidersGrpcKt.DataProvidersCoroutineImplBase = mockService {
    for (dataProvider in DATA_PROVIDERS.values) {
      onBlocking { getDataProvider(eq(getDataProviderRequest { name = dataProvider.name })) }
        .thenReturn(dataProvider)
    }
    onBlocking {
        getDataProvider(eq(getDataProviderRequest { name = POPULATION_DATA_PROVIDER.name }))
      }
      .thenReturn(POPULATION_DATA_PROVIDER)
  }

  private val certificatesMock: CertificatesGrpcKt.CertificatesCoroutineImplBase = mockService {
    onBlocking { getCertificate(eq(getCertificateRequest { name = AGGREGATOR_CERTIFICATE.name })) }
      .thenReturn(AGGREGATOR_CERTIFICATE)
    for (dataProvider in DATA_PROVIDERS.values) {
      onBlocking { getCertificate(eq(getCertificateRequest { name = dataProvider.certificate })) }
        .thenReturn(
          certificate {
            name = dataProvider.certificate
            x509Der = DATA_PROVIDER_SIGNING_KEY.certificate.encoded.toByteString()
          }
        )
    }
    onBlocking {
        getCertificate(eq(getCertificateRequest { name = POPULATION_DATA_PROVIDER.certificate }))
      }
      .thenReturn(
        certificate {
          name = POPULATION_DATA_PROVIDER.certificate
          x509Der = DATA_PROVIDER_SIGNING_KEY.certificate.encoded.toByteString()
        }
      )
    for (measurementConsumer in MEASUREMENT_CONSUMERS.values) {
      onBlocking {
          getCertificate(eq(getCertificateRequest { name = measurementConsumer.certificate }))
        }
        .thenReturn(
          certificate {
            name = measurementConsumer.certificate
            x509Der = measurementConsumer.certificateDer
          }
        )
    }
  }

  private val modelLinesMock: ModelLinesCoroutineImplBase = mockService {
    onBlocking { getModelLine(any()) }.thenReturn(modelLine {})
  }

  private val randomMock: Random = mock()

  private val variancesMock =
    mock<Variances> {
      onBlocking { computeMetricVariance(any<ReachMetricVarianceParams>()) }
        .thenAnswer {
          val params = it.arguments[0] as ReachMetricVarianceParams
          val epsilon =
            params.weightedMeasurementVarianceParamsList
              .first()
              .measurementVarianceParams
              .measurementParams
              .dpParams
              .epsilon
          if (epsilon == REACH_ONLY_REACH_EPSILON || epsilon == REACH_FREQUENCY_REACH_EPSILON) {
            VARIANCE_VALUE
          } else {
            SINGLE_DATA_PROVIDER_VARIANCE_VALUE
          }
        }

      onBlocking { computeMetricVariance(any<FrequencyMetricVarianceParams>()) }
        .thenAnswer {
          val params = it.arguments[0] as FrequencyMetricVarianceParams
          val epsilon =
            params.weightedMeasurementVarianceParamsList
              .first()
              .measurementVarianceParams
              .measurementParams
              .dpParams
              .epsilon
          if (epsilon == REACH_FREQUENCY_FREQUENCY_EPSILON) {
            FREQUENCY_VARIANCES
          } else {
            SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCES
          }
        }

      onBlocking { computeMetricVariance(any<ImpressionMetricVarianceParams>()) }
        .thenReturn(VARIANCE_VALUE)
      onBlocking { computeMetricVariance(any<WatchDurationMetricVarianceParams>()) }
        .thenReturn(VARIANCE_VALUE)

      onBlocking {
          computeMeasurementVariance(any<Methodology>(), any<ReachMeasurementVarianceParams>())
        }
        .thenReturn(VARIANCE_VALUE)
      onBlocking {
          computeMeasurementVariance(any<Methodology>(), any<FrequencyMeasurementVarianceParams>())
        }
        .thenReturn(FrequencyVariances(mapOf(), mapOf(), mapOf(), mapOf()))
      onBlocking {
          computeMeasurementVariance(any<Methodology>(), any<ImpressionMeasurementVarianceParams>())
        }
        .thenReturn(VARIANCE_VALUE)
      onBlocking {
          computeMeasurementVariance(
            any<Methodology>(),
            any<WatchDurationMeasurementVarianceParams>(),
          )
        }
        .thenReturn(VARIANCE_VALUE)
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(permissionsServiceMock)
    addService(internalMetricsMock)
    addService(internalReportingSetsMock)
    addService(internalMeasurementsMock)
    addService(measurementsMock)
    addService(measurementConsumersMock)
    addService(dataProvidersMock)
    addService(certificatesMock)
    addService(modelLinesMock)
  }

  private lateinit var service: MetricsService

  @Before
  fun initService() {
    randomMock.stub {
      on { nextInt(any()) } doReturn RANDOM_OUTPUT_INT
      on { nextLong() } doReturn RANDOM_OUTPUT_LONG
    }

    service =
      MetricsService(
        METRIC_SPEC_CONFIG,
        MEASUREMENT_CONSUMER_CONFIGS,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        InternalMetricsGrpcKt.MetricsCoroutineStub(grpcTestServerRule.channel),
        variancesMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel),
        MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        CertificatesGrpcKt.CertificatesCoroutineStub(grpcTestServerRule.channel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
        ModelLinesCoroutineStub(grpcTestServerRule.channel),
        Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel)),
        ENCRYPTION_KEY_PAIR_STORE,
        randomMock,
        SECRETS_DIR,
        listOf(AGGREGATOR_ROOT_CERTIFICATE, DATA_PROVIDER_ROOT_CERTIFICATE).associateBy {
          it.subjectKeyIdentifier!!
        },
        DEFAULT_VID_MODEL_LINE,
        MEASUREMENT_CONSUMER_MODEL_LINES,
        POPULATION_DATA_PROVIDER_NAME,
      )
  }

  @Test
  fun `createMetric creates CMMS measurements for incremental reach`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          externalMetricId = METRIC_ID
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_UNION_ALL_REACH_MEASUREMENT
            requestId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
            requestId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
            nonceHashes.clear()
            nonceHashes += List(dataProvidersList.size) { Hashing.hashSha256(RANDOM_OUTPUT_LONG) }
            reportingMetadata = reportingMetadata {
              report = CONTAINING_REPORT
              metric =
                MetricKey(
                    INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId,
                    INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId,
                  )
                  .toName()
            }
          }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementId
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric creates measurements for single pub reach when single edp params set`() {
    val metricId = METRIC_ID
    val cmmsMeasurementSpec =
      BASE_MEASUREMENT_SPEC.copy {
        measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

        nonceHashes.add(Hashing.hashSha256(RANDOM_OUTPUT_LONG))

        reach =
          MeasurementSpecKt.reach {
            privacyParams = differentialPrivacyParams {
              epsilon = SINGLE_DATA_PROVIDER_REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          }
        vidSamplingInterval =
          MeasurementSpecKt.vidSamplingInterval {
            start = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_START
            width = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_WIDTH
          }
        reportingMetadata = reportingMetadata {
          report = CONTAINING_REPORT
          metric =
            MetricKey(MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId, metricId).toName()
        }
      }

    val internalMeasurement = internalMeasurement {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
      cmmsMeasurementId = "cmms_id"
      cmmsCreateMeasurementRequestId = "SINGLE_PUBLISHER_REACH_MEASUREMENT"
      timeInterval = TIME_INTERVAL
      primitiveReportingSetBases += primitiveReportingSetBasis {
        externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
        filters += METRIC_FILTER
        filters += PRIMITIVE_REPORTING_SET_FILTER
      }
      state = InternalMeasurement.State.PENDING
      details = InternalMeasurementKt.details { dataProviderCount = 1 }
    }

    val pendingSingleDataProviderReachMeasurementWithSingleDataProviderParams =
      BASE_MEASUREMENT.copy {
        dataProviders += DATA_PROVIDER_ENTRIES.getValue(DATA_PROVIDERS.keys.first())

        measurementSpec =
          signMeasurementSpec(cmmsMeasurementSpec, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
        measurementReferenceId = internalMeasurement.cmmsCreateMeasurementRequestId

        name =
          MeasurementKey(
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
              internalMeasurement.cmmsMeasurementId,
            )
            .toName()
        protocolConfig = REACH_PROTOCOL_CONFIG
        state = Measurement.State.COMPUTING
      }

    val internalPendingReachMetricWithSingleDataProviderParams = internalMetric {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
      externalMetricId = metricId
      externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
      timeInterval = TIME_INTERVAL
      metricSpec = internalMetricSpec {
        reach =
          InternalMetricSpecKt.reachParams {
            multipleDataProviderParams =
              InternalMetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  InternalMetricSpecKt.differentialPrivacyParams {
                    epsilon = REACH_ONLY_REACH_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                vidSamplingInterval =
                  InternalMetricSpecKt.vidSamplingInterval {
                    start = REACH_ONLY_VID_SAMPLING_START
                    width = REACH_ONLY_VID_SAMPLING_WIDTH
                  }
              }
            singleDataProviderParams =
              InternalMetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  InternalMetricSpecKt.differentialPrivacyParams {
                    epsilon = SINGLE_DATA_PROVIDER_REACH_ONLY_REACH_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                vidSamplingInterval =
                  InternalMetricSpecKt.vidSamplingInterval {
                    start = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_START
                    width = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_WIDTH
                  }
              }
          }
      }
      weightedMeasurements += weightedMeasurement {
        weight = 1
        binaryRepresentation = 1
        measurement = internalMeasurement.copy { clearCmmsMeasurementId() }
      }
      details =
        InternalMetricKt.details {
          filters += METRIC_FILTER
          containingReport = CONTAINING_REPORT
        }
      createTime = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.createTime
      state = InternalMetric.State.RUNNING
    }

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    wheneverBlocking { internalMetricsMock.createMetric(any()) } doReturn
      internalPendingReachMetricWithSingleDataProviderParams
    wheneverBlocking { measurementsMock.batchCreateMeasurements(any()) } doReturn
      batchCreateMeasurementsResponse {
        measurements += pendingSingleDataProviderReachMeasurementWithSingleDataProviderParams
      }

    val requestingReachMetricWithSingleDataProviderParams = metric {
      reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
      timeInterval = TIME_INTERVAL
      metricSpec = metricSpec {
        reach = reachParams {
          multipleDataProviderParams =
            MetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = REACH_ONLY_REACH_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = REACH_ONLY_VID_SAMPLING_START
                  width = REACH_ONLY_VID_SAMPLING_WIDTH
                }
            }
          singleDataProviderParams =
            MetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = SINGLE_DATA_PROVIDER_REACH_ONLY_REACH_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_START
                  width = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_WIDTH
                }
            }
        }
      }
      filters += METRIC_FILTER
      containingReport = CONTAINING_REPORT
    }

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = requestingReachMetricWithSingleDataProviderParams
      this.metricId = metricId
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val pendingReachMetricWithSingleDataProviderParams =
      requestingReachMetricWithSingleDataProviderParams.copy {
        name =
          MetricKey(MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId, request.metricId)
            .toName()
        state = Metric.State.RUNNING
        createTime = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.createTime
      }

    assertThat(result).isEqualTo(pendingReachMetricWithSingleDataProviderParams)

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric =
            internalPendingReachMetricWithSingleDataProviderParams.copy {
              clearState()
              clearCreateTime()
              clearExternalMetricId()
              weightedMeasurements.clear()
              weightedMeasurements +=
                internalPendingReachMetricWithSingleDataProviderParams.weightedMeasurementsList
                  .first()
                  .copy {
                    measurement =
                      measurement.copy {
                        clearCmmsMeasurementId()
                        clearState()
                        clearCmmsCreateMeasurementRequestId()
                      }
                  }
            }
          externalMetricId = request.metricId
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement =
              pendingSingleDataProviderReachMeasurementWithSingleDataProviderParams.copy {
                clearState()
                clearProtocolConfig()
                clearName()
              }
            requestId = internalMeasurement.cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec).isEqualTo(cmmsMeasurementSpec)

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId = internalMeasurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = internalMeasurement.cmmsMeasurementId
          }
        }
      )
  }

  @Test
  fun `createMetric creates measurements for single pub reach when single edp params not set`() {
    val cmmsMeasurementSpec =
      BASE_MEASUREMENT_SPEC.copy {
        measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

        nonceHashes.add(Hashing.hashSha256(RANDOM_OUTPUT_LONG))

        reach =
          MeasurementSpecKt.reach {
            privacyParams = differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          }
        vidSamplingInterval =
          MeasurementSpecKt.vidSamplingInterval {
            start = REACH_ONLY_VID_SAMPLING_START
            width = REACH_ONLY_VID_SAMPLING_WIDTH
          }
      }

    val internalMeasurement = internalMeasurement {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
      cmmsMeasurementId = "cmms_id"
      cmmsCreateMeasurementRequestId = "SINGLE_PUBLISHER_REACH_MEASUREMENT"
      timeInterval = TIME_INTERVAL
      primitiveReportingSetBases += primitiveReportingSetBasis {
        externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
        filters += METRIC_FILTER
        filters += PRIMITIVE_REPORTING_SET_FILTER
      }
      state = InternalMeasurement.State.PENDING
      details = InternalMeasurementKt.details { dataProviderCount = 1 }
    }

    val pendingSingleDataProviderReachMeasurement =
      BASE_MEASUREMENT.copy {
        dataProviders += DATA_PROVIDER_ENTRIES.getValue(DATA_PROVIDERS.keys.first())

        measurementSpec =
          signMeasurementSpec(cmmsMeasurementSpec, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
        measurementReferenceId = internalMeasurement.cmmsCreateMeasurementRequestId

        name =
          MeasurementKey(
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
              internalMeasurement.cmmsMeasurementId,
            )
            .toName()
        protocolConfig = REACH_PROTOCOL_CONFIG
        state = Measurement.State.COMPUTING
      }

    val internalPendingReachMetric = internalMetric {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
      externalMetricId = METRIC_ID
      externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
      timeInterval = TIME_INTERVAL
      metricSpec = internalMetricSpec {
        reach =
          InternalMetricSpecKt.reachParams {
            multipleDataProviderParams =
              InternalMetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  InternalMetricSpecKt.differentialPrivacyParams {
                    epsilon = REACH_ONLY_REACH_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                vidSamplingInterval =
                  InternalMetricSpecKt.vidSamplingInterval {
                    start = REACH_ONLY_VID_SAMPLING_START
                    width = REACH_ONLY_VID_SAMPLING_WIDTH
                  }
              }
          }
      }
      weightedMeasurements += weightedMeasurement {
        weight = 1
        binaryRepresentation = 1
        measurement = internalMeasurement.copy { clearCmmsMeasurementId() }
      }
      details =
        InternalMetricKt.details {
          filters += METRIC_FILTER
          containingReport = CONTAINING_REPORT
        }
      createTime = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.createTime
      state = InternalMetric.State.RUNNING
    }

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    wheneverBlocking { internalMetricsMock.createMetric(any()) }
      .thenReturn(internalPendingReachMetric)
    wheneverBlocking { measurementsMock.batchCreateMeasurements(any()) }
      .thenReturn(
        batchCreateMeasurementsResponse {
          measurements += pendingSingleDataProviderReachMeasurement
        }
      )

    val requestingReachMetric = metric {
      reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
      timeInterval = TIME_INTERVAL
      metricSpec = metricSpec {
        reach = reachParams {
          privacyParams =
            MetricSpecKt.differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
        }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = REACH_ONLY_VID_SAMPLING_START
            width = REACH_ONLY_VID_SAMPLING_WIDTH
          }
      }
      filters += METRIC_FILTER
      containingReport = CONTAINING_REPORT
    }

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = requestingReachMetric
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val pendingReachMetric =
      requestingReachMetric.copy {
        name =
          MetricKey(MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId, request.metricId)
            .toName()
        state = Metric.State.RUNNING
        createTime = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.createTime
      }

    assertThat(result).isEqualTo(pendingReachMetric)

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric =
            internalPendingReachMetric.copy {
              clearState()
              clearCreateTime()
              clearExternalMetricId()
              weightedMeasurements.clear()
              weightedMeasurements +=
                internalPendingReachMetric.weightedMeasurementsList.first().copy {
                  measurement =
                    measurement.copy {
                      clearCmmsMeasurementId()
                      clearState()
                      clearCmmsCreateMeasurementRequestId()
                    }
                }
            }
          externalMetricId = request.metricId
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement =
              pendingSingleDataProviderReachMeasurement.copy {
                clearState()
                clearProtocolConfig()
                clearName()
              }
            requestId = internalMeasurement.cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec).isEqualTo(cmmsMeasurementSpec)

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId = internalMeasurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = internalMeasurement.cmmsMeasurementId
          }
        }
      )
  }

  @Test
  fun `createMetric creates measurements for single pub rf when single edp params set`() {
    val cmmsMeasurementSpec =
      BASE_MEASUREMENT_SPEC.copy {
        measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

        nonceHashes.add(Hashing.hashSha256(RANDOM_OUTPUT_LONG))

        reachAndFrequency =
          MeasurementSpecKt.reachAndFrequency {
            reachPrivacyParams = differentialPrivacyParams {
              epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
            frequencyPrivacyParams = differentialPrivacyParams {
              epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_FREQUENCY_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
            maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
          }
        vidSamplingInterval =
          MeasurementSpecKt.vidSamplingInterval {
            start = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_START
            width = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_WIDTH
          }
      }

    val internalMeasurement = internalMeasurement {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
      cmmsMeasurementId = "cmms_id"
      cmmsCreateMeasurementRequestId = "SINGLE_PUBLISHER_REACH_MEASUREMENT"
      timeInterval = TIME_INTERVAL
      primitiveReportingSetBases += primitiveReportingSetBasis {
        externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
        filters += METRIC_FILTER
        filters += PRIMITIVE_REPORTING_SET_FILTER
      }
      state = InternalMeasurement.State.PENDING
      details = InternalMeasurementKt.details { dataProviderCount = 1 }
    }

    val pendingSingleDataProviderReachAndFrequencyMeasurementWithSingleDataProviderParams =
      BASE_MEASUREMENT.copy {
        dataProviders += DATA_PROVIDER_ENTRIES.getValue(DATA_PROVIDERS.keys.first())

        measurementSpec =
          signMeasurementSpec(cmmsMeasurementSpec, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
        measurementReferenceId = internalMeasurement.cmmsCreateMeasurementRequestId

        name =
          MeasurementKey(
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
              internalMeasurement.cmmsMeasurementId,
            )
            .toName()
        protocolConfig = REACH_FREQUENCY_PROTOCOL_CONFIG
        state = Measurement.State.COMPUTING
      }

    val internalPendingReachAndFrequencyMetricWithSingleDataProviderParams = internalMetric {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
      externalMetricId = METRIC_ID
      externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
      timeInterval = TIME_INTERVAL
      metricSpec = internalMetricSpec {
        reachAndFrequency =
          InternalMetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              InternalMetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  InternalMetricSpecKt.differentialPrivacyParams {
                    epsilon = REACH_FREQUENCY_REACH_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                frequencyPrivacyParams =
                  InternalMetricSpecKt.differentialPrivacyParams {
                    epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                vidSamplingInterval =
                  InternalMetricSpecKt.vidSamplingInterval {
                    start = REACH_FREQUENCY_VID_SAMPLING_START
                    width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
                  }
              }
            singleDataProviderParams =
              InternalMetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  InternalMetricSpecKt.differentialPrivacyParams {
                    epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_REACH_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                frequencyPrivacyParams =
                  InternalMetricSpecKt.differentialPrivacyParams {
                    epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_FREQUENCY_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                vidSamplingInterval =
                  InternalMetricSpecKt.vidSamplingInterval {
                    start = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_START
                    width = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_WIDTH
                  }
              }
            maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
          }
      }
      weightedMeasurements += weightedMeasurement {
        weight = 1
        binaryRepresentation = 1
        measurement = internalMeasurement.copy { clearCmmsMeasurementId() }
      }
      details =
        InternalMetricKt.details {
          filters += METRIC_FILTER
          containingReport = CONTAINING_REPORT
        }
      createTime = INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.createTime
      state = InternalMetric.State.RUNNING
    }

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    wheneverBlocking { internalMetricsMock.createMetric(any()) }
      .thenReturn(internalPendingReachAndFrequencyMetricWithSingleDataProviderParams)
    wheneverBlocking { measurementsMock.batchCreateMeasurements(any()) }
      .thenReturn(
        batchCreateMeasurementsResponse {
          measurements +=
            pendingSingleDataProviderReachAndFrequencyMeasurementWithSingleDataProviderParams
        }
      )

    val requestingReachAndFrequencyMetricWithSingleDataProviderParams = metric {
      reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
      timeInterval = TIME_INTERVAL
      metricSpec = metricSpec {
        reachAndFrequency = reachAndFrequencyParams {
          multipleDataProviderParams =
            MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
              reachPrivacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = REACH_FREQUENCY_REACH_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              frequencyPrivacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = REACH_FREQUENCY_VID_SAMPLING_START
                  width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
                }
            }
          singleDataProviderParams =
            MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
              reachPrivacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_REACH_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              frequencyPrivacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_FREQUENCY_EPSILON
                  delta = DIFFERENTIAL_PRIVACY_DELTA
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_START
                  width = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_WIDTH
                }
            }
          maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
        }
      }
      filters += METRIC_FILTER
      containingReport = CONTAINING_REPORT
    }

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = requestingReachAndFrequencyMetricWithSingleDataProviderParams
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val pendingReachAndFrequencyMetricWithSingleDataProviderParams =
      requestingReachAndFrequencyMetricWithSingleDataProviderParams.copy {
        name =
          MetricKey(MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId, request.metricId)
            .toName()
        state = Metric.State.RUNNING
        createTime = INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.createTime
      }

    assertThat(result).isEqualTo(pendingReachAndFrequencyMetricWithSingleDataProviderParams)

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric =
            internalPendingReachAndFrequencyMetricWithSingleDataProviderParams.copy {
              clearState()
              clearCreateTime()
              clearExternalMetricId()
              weightedMeasurements.clear()
              weightedMeasurements +=
                internalPendingReachAndFrequencyMetricWithSingleDataProviderParams
                  .weightedMeasurementsList
                  .first()
                  .copy {
                    measurement =
                      measurement.copy {
                        clearCmmsMeasurementId()
                        clearState()
                        clearCmmsCreateMeasurementRequestId()
                      }
                  }
            }
          externalMetricId = request.metricId
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement =
              pendingSingleDataProviderReachAndFrequencyMeasurementWithSingleDataProviderParams
                .copy {
                  clearState()
                  clearProtocolConfig()
                  clearName()
                }
            requestId = internalMeasurement.cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec).isEqualTo(cmmsMeasurementSpec)

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId = internalMeasurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = internalMeasurement.cmmsMeasurementId
          }
        }
      )
  }

  @Test
  fun `createMetric creates CMMS measurements for single pub rf when single edp params not set`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    wheneverBlocking { internalMetricsMock.createMetric(any()) }
      .thenReturn(INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC)
    wheneverBlocking { measurementsMock.batchCreateMeasurements(any()) }
      .thenReturn(
        batchCreateMeasurementsResponse {
          measurements += PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
        }
      )

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val expected = PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          externalMetricId = METRIC_ID
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
            requestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT_SPEC.copy {
            reportingMetadata = reportingMetadata {
              report = CONTAINING_REPORT
              metric =
                MetricKey(
                    INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
                      .cmmsMeasurementConsumerId,
                    INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.externalMetricId,
                  )
                  .toName()
            }
          }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric creates CMMS measurements for single pub impression metric`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    wheneverBlocking { internalMetricsMock.createMetric(any()) }
      .thenReturn(INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC)
    wheneverBlocking { measurementsMock.batchCreateMeasurements(any()) }
      .thenReturn(
        batchCreateMeasurementsResponse {
          measurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
        }
      )

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val expected = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          externalMetricId = METRIC_ID
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
            requestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC.copy {
            reportingMetadata = reportingMetadata {
              report = CONTAINING_REPORT
              metric =
                MetricKey(
                    INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC
                      .cmmsMeasurementConsumerId,
                    INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId,
                  )
                  .toName()
            }
          }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric creates CMMS measurements with custom measurement params`() = runBlocking {
    val epsilon = IMPRESSION_EPSILON * 2
    val delta = DIFFERENTIAL_PRIVACY_DELTA * 2
    val maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER + 1
    val vidSamplingIntervalStart = IMPRESSION_VID_SAMPLING_START
    val vidSamplingIntervalWidth = IMPRESSION_VID_SAMPLING_WIDTH / 2

    val internalMetricSpec = internalMetricSpec {
      impressionCount =
        InternalMetricSpecKt.impressionCountParams {
          params =
            InternalMetricSpecKt.samplingAndPrivacyParams {
              privacyParams =
                InternalMetricSpecKt.differentialPrivacyParams {
                  this.epsilon = epsilon
                  this.delta = delta
                }
              vidSamplingInterval =
                InternalMetricSpecKt.vidSamplingInterval {
                  start = vidSamplingIntervalStart
                  width = vidSamplingIntervalWidth
                }
            }
          this.maximumFrequencyPerUser = maximumFrequencyPerUser
        }
    }
    val internalRequestingSinglePublisherImpressionMetric =
      INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
        this.metricSpec = internalMetricSpec
      }

    val internalPendingInitialSinglePublisherImpressionMetric =
      INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
        this.metricSpec = internalMetricSpec
      }

    val cmmsMeasurementSpec =
      SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC.copy {
        impression =
          MeasurementSpecKt.impression {
            privacyParams = differentialPrivacyParams {
              this.epsilon = epsilon
              this.delta = delta
            }
            this.maximumFrequencyPerUser = maximumFrequencyPerUser
          }
        vidSamplingInterval =
          MeasurementSpecKt.vidSamplingInterval {
            start = vidSamplingIntervalStart
            width = vidSamplingIntervalWidth
          }
        reportingMetadata = reportingMetadata {
          report = CONTAINING_REPORT
          metric =
            MetricKey(
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
                INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId,
              )
              .toName()
        }
      }

    val requestingSinglePublisherImpressionMeasurement =
      REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
        measurementSpec =
          signMeasurementSpec(cmmsMeasurementSpec, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
      }
    val pendingSinglePublisherImpressionMeasurement =
      PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
        measurementSpec =
          signMeasurementSpec(cmmsMeasurementSpec, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
      }

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(internalPendingInitialSinglePublisherImpressionMetric)
    whenever(measurementsMock.batchCreateMeasurements(any()))
      .thenReturn(
        batchCreateMeasurementsResponse {
          measurements += pendingSinglePublisherImpressionMeasurement
        }
      )

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
          this.metricSpec = metricSpec {
            impressionCount = impressionCountParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  this.epsilon = epsilon
                  this.delta = delta
                }
              this.maximumFrequencyPerUser = maximumFrequencyPerUser
            }
            vidSamplingInterval =
              MetricSpecKt.vidSamplingInterval {
                start = vidSamplingIntervalStart
                width = vidSamplingIntervalWidth
              }
          }
        }
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val expected =
      PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
        metricSpec = metricSpec {
          impressionCount = impressionCountParams {
            params =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    this.epsilon = epsilon
                    this.delta = delta
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = vidSamplingIntervalStart
                    width = vidSamplingIntervalWidth
                  }
              }
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                this.epsilon = epsilon
                this.delta = delta
              }
            this.maximumFrequencyPerUser = maximumFrequencyPerUser
          }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = vidSamplingIntervalStart
              width = vidSamplingIntervalWidth
            }
        }
      }

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = internalRequestingSinglePublisherImpressionMetric
          externalMetricId = METRIC_ID
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = requestingSinglePublisherImpressionMeasurement
            requestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec).isEqualTo(cmmsMeasurementSpec)

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric creates CMMS measurements for incremental reach with a request ID`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
          externalMetricId = METRIC_ID
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_UNION_ALL_REACH_MEASUREMENT
            requestId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
            requestId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
            nonceHashes.clear()
            nonceHashes += List(dataProvidersList.size) { Hashing.hashSha256(RANDOM_OUTPUT_LONG) }
            reportingMetadata = reportingMetadata {
              report = CONTAINING_REPORT
              metric =
                MetricKey(
                    INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId,
                    INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId,
                  )
                  .toName()
            }
          }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric creates CMMS measurements when no event filter at all`() = runBlocking {
    val internalSinglePublisherReportingSet =
      INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.copy {
        clearFilter()
        weightedSubsetUnions.clear()
        weightedSubsetUnions += weightedSubsetUnion {
          primitiveReportingSetBases += primitiveReportingSetBasis {
            externalReportingSetId = this@copy.externalReportingSetId
          }
          weight = 1
          binaryRepresentation = 1
        }
      }

    val internalCreateMetricRequest = internalCreateMetricRequest {
      metric =
        INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
          weightedMeasurements.clear()
          weightedMeasurements += weightedMeasurement {
            weight = 1
            binaryRepresentation = 1
            measurement = internalMeasurement {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              timeInterval = TIME_INTERVAL
              primitiveReportingSetBases += primitiveReportingSetBasis {
                externalReportingSetId = internalSinglePublisherReportingSet.externalReportingSetId
              }
              details = InternalMeasurementKt.details { dataProviderCount = 1 }
            }
          }
          details = InternalMetricKt.details { containingReport = CONTAINING_REPORT }
        }
      externalMetricId = METRIC_ID
    }

    val internalPendingInitialSinglePublisherImpressionMetric =
      INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
        weightedMeasurements.clear()
        weightedMeasurements += weightedMeasurement {
          weight = 1
          binaryRepresentation = 1
          measurement =
            INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
              clearCmmsMeasurementId()
              primitiveReportingSetBases.clear()
              primitiveReportingSetBases += primitiveReportingSetBasis {
                externalReportingSetId = internalSinglePublisherReportingSet.externalReportingSetId
              }
            }
        }
        details = InternalMetricKt.details { containingReport = CONTAINING_REPORT }
      }

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(
        internalReportingSetsMock.batchGetReportingSets(
          eq(
            batchGetReportingSetsRequest {
              cmmsMeasurementConsumerId =
                internalSinglePublisherReportingSet.cmmsMeasurementConsumerId
              externalReportingSetIds += internalSinglePublisherReportingSet.externalReportingSetId
            }
          )
        )
      )
      .thenReturn(
        batchGetReportingSetsResponse { reportingSets += internalSinglePublisherReportingSet }
      )
    whenever(internalMetricsMock.createMetric(eq(internalCreateMetricRequest)))
      .thenReturn(internalPendingInitialSinglePublisherImpressionMetric)

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy { filters.clear() }
      metricId = METRIC_ID
    }

    withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val filters: List<String> =
        dataProvidersList.flatMap { dataProviderEntry ->
          val signedRequisitionSpec =
            decryptRequisitionSpec(
              dataProviderEntry.value.encryptedRequisitionSpec,
              DATA_PROVIDER_PRIVATE_KEY_HANDLE,
            )
          val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()

          requisitionSpec.events.eventGroupsList.map { eventGroupEntry ->
            eventGroupEntry.value.filter.expression
          }
        }
      for (filter in filters) {
        assertThat(filter).isEqualTo("")
      }
    }
  }

  @Test
  fun `createMetric calls batchGetReportingSets when request number is more than the limit`():
    Unit = runBlocking {
    val expectedNumberBatchGetReportingSetsRequests = 3
    // BatchGetReportingSets is called one time in other place for retrieving a single reporting set
    val numberBatchReportingSets = expectedNumberBatchGetReportingSetsRequests - 1
    val numberInternalReportingSets = BATCH_GET_REPORTING_SETS_LIMIT * numberBatchReportingSets

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(internalReportingSetsMock.batchGetReportingSets(any()))
      .thenAnswer {
        val batchGetReportingSetsRequest = it.arguments[0] as BatchGetReportingSetsRequest
        batchGetReportingSetsResponse {
          reportingSets +=
            batchGetReportingSetsRequest.externalReportingSetIdsList.map { externalReportingSetId ->
              INTERNAL_INCREMENTAL_REPORTING_SET.copy {
                this.externalReportingSetId = externalReportingSetId
                weightedSubsetUnions.clear()
                weightedSubsetUnions += weightedSubsetUnion {
                  (0 until numberInternalReportingSets).forEach {
                    primitiveReportingSetBases += primitiveReportingSetBasis {
                      this.externalReportingSetId = it.toString()
                    }
                  }
                }
              }
            }
        }
      }
      .thenReturn(
        batchGetReportingSetsResponse {
          (0 until numberInternalReportingSets).forEach {
            reportingSets +=
              INTERNAL_UNION_ALL_REPORTING_SET.copy { this.externalReportingSetId = it.toString() }
          }
        }
      )

    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(
        INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC.copy {
          weightedMeasurements.clear()
          weightedMeasurements += weightedMeasurement {
            weight = 1
            binaryRepresentation = 1
            measurement =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
                clearCmmsMeasurementId()
                primitiveReportingSetBases.clear()
                (0 until numberInternalReportingSets).forEach {
                  primitiveReportingSetBases += primitiveReportingSetBasis {
                    externalReportingSetId = it.toString()
                  }
                }
              }
          }
        }
      )

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }
    withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    // Verify proto argument of internal ReportingSetsCoroutineImplBase::batchGetReportingSets
    val batchGetReportingSetsCaptor: KArgumentCaptor<BatchGetReportingSetsRequest> =
      argumentCaptor()
    verifyBlocking(internalReportingSetsMock, times(expectedNumberBatchGetReportingSetsRequests)) {
      batchGetReportingSets(batchGetReportingSetsCaptor.capture())
    }
  }

  @Test
  fun `createMetric calls batchSetCmmsMeasurementIds when request number is more than the limit`():
    Unit = runBlocking {
    val weightedMeasurements =
      (0..BATCH_SET_CMMS_MEASUREMENT_IDS_LIMIT).map { id ->
        weightedMeasurement {
          weight = 1
          binaryRepresentation = 1
          measurement =
            INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
              cmmsCreateMeasurementRequestId = "$id"
              clearCmmsMeasurementId()
            }
        }
      }
    val measurementsMap: Map<String, Measurement> =
      weightedMeasurements.associate { weightedMeasurement ->
        weightedMeasurement.measurement.cmmsCreateMeasurementRequestId to
          PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
            name =
              MeasurementKey(
                  MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
                  externalIdToApiId(
                    100L + weightedMeasurement.measurement.cmmsCreateMeasurementRequestId.toLong()
                  ),
                )
                .toName()
          }
      }

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(
        INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC.copy {
          this.weightedMeasurements.clear()
          this.weightedMeasurements += weightedMeasurements
        }
      )
    whenever(measurementsMock.batchCreateMeasurements(any())).thenAnswer {
      val batchCreateMeasurementsRequest = it.arguments[0] as BatchCreateMeasurementsRequest
      batchCreateMeasurementsResponse {
        measurements +=
          batchCreateMeasurementsRequest.requestsList.map { createMeasurementRequest ->
            measurementsMap.getValue(createMeasurementRequest.requestId)
          }
      }
    }

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }
    withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    // Verify proto argument of cmms MeasurementsCoroutineImplBase::batchCreateMeasurements
    val batchCreateMeasurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> =
      argumentCaptor()
    verifyBlocking(
      measurementsMock,
      times(
        ceil(
            (1 + BATCH_SET_CMMS_MEASUREMENT_IDS_LIMIT).toDouble() / BATCH_KINGDOM_MEASUREMENTS_LIMIT
          )
          .toInt()
      ),
    ) {
      batchCreateMeasurements(batchCreateMeasurementsCaptor.capture())
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    val batchSetCmmsMeasurementIdsCaptor: KArgumentCaptor<BatchSetCmmsMeasurementIdsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, times(2)) {
      batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsCaptor.capture())
    }
  }

  @Test
  fun `createMetric without request ID when the measurements are created already`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(INTERNAL_PENDING_INCREMENTAL_REACH_METRIC)

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          externalMetricId = METRIC_ID
        }
      )

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(3)) { getDataProvider(dataProvidersCaptor.capture()) }

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, never()) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    val batchSetCmmsMeasurementIdsCaptor: KArgumentCaptor<BatchSetCmmsMeasurementIdsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsCaptor.capture())
    }

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric with request ID when the metric exists and in running state`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(INTERNAL_PENDING_INCREMENTAL_REACH_METRIC)

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
          externalMetricId = METRIC_ID
        }
      )

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(3)) { getDataProvider(dataProvidersCaptor.capture()) }

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, never()) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    val batchSetCmmsMeasurementIdsCaptor: KArgumentCaptor<BatchSetCmmsMeasurementIdsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsCaptor.capture())
    }

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric with request ID when the metric exists and in terminalstate`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC)

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
      metricId = METRIC_ID
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val expected = SUCCEEDED_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::batchGetReportingSets
    val batchGetReportingSetsCaptor: KArgumentCaptor<BatchGetReportingSetsRequest> =
      argumentCaptor()
    verifyBlocking(internalReportingSetsMock, times(2)) {
      batchGetReportingSets(batchGetReportingSetsCaptor.capture())
    }

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
          externalMetricId = METRIC_ID
        }
      )

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    val getMeasurementConsumerCaptor: KArgumentCaptor<GetMeasurementConsumerRequest> =
      argumentCaptor()
    verifyBlocking(measurementConsumersMock, never()) {
      getMeasurementConsumer(getMeasurementConsumerCaptor.capture())
    }

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, never()) { getDataProvider(dataProvidersCaptor.capture()) }

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, never()) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    val batchSetCmmsMeasurementIdsCaptor: KArgumentCaptor<BatchSetCmmsMeasurementIdsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsCaptor.capture())
    }

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric throws UNAUTHENTICATED when no principal is found`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createMetric(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createMetric throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "$name-wrong" }, SCOPES) {
          runBlocking { service.createMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when parent is unspecified`() {
    val request = createMetricRequest {
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Parent is either unspecified or invalid.")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when resource ID is unspecified`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when resource ID starts with number`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "1s"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when resource ID is too long`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "s".repeat(100)
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when resource ID contains invalid char`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "contain_invalid_char"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when metric is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Metric is not specified.")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when time interval in Metric is unspecified`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearTimeInterval() }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Time interval in metric is not specified.")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when TimeInterval startTime is unspecified`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          clearTimeInterval()
          timeInterval = interval { endTime = timestamp { seconds = 5 } }
        }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when TimeInterval endTime is unspecified`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          clearTimeInterval()
          timeInterval = interval { startTime = timestamp { seconds = 5 } }
        }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when TimeInterval endTime is before startTime`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          clearTimeInterval()
          timeInterval = interval {
            startTime = timestamp {
              seconds = 5
              nanos = 5
            }
            endTime = timestamp {
              seconds = 5
              nanos = 1
            }
          }
        }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when metric spec in Metric is unspecified`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearMetricSpec() }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Metric spec in metric is not specified.")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when privacy params is unspecified`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          metricSpec = metricSpec.copy { reach = reach.copy { clearPrivacyParams() } }
        }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when vid sampling interval start is negative`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          metricSpec =
            metricSpec.copy { vidSamplingInterval = vidSamplingInterval.copy { start = -1.0f } }
        }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when vid sampling interval start is 1`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          metricSpec =
            metricSpec.copy { vidSamplingInterval = vidSamplingInterval.copy { start = 1.0f } }
        }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when vid sampling interval width is 0`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          metricSpec =
            metricSpec.copy { vidSamplingInterval = vidSamplingInterval.copy { width = 0f } }
        }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when vid sampling interval end is larger than 1`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          metricSpec =
            metricSpec.copy {
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = 0.7f
                  width = 0.5f
                }
            }
        }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when reporting set is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearReportingSet() }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when provided reporting set name is invalid`() {
    val metricWithInvalidReportingSet =
      REQUESTING_INCREMENTAL_REACH_METRIC.copy { reportingSet = "INVALID_REPORTING_SET_NAME" }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = metricWithInvalidReportingSet
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().ignoringCase().contains("reporting_set")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when ReportingSet has different parent`() {
    val inaccessibleReportingSetName =
      ReportingSetKey(
          MEASUREMENT_CONSUMERS.keys.last().measurementConsumerId,
          externalIdToApiId(241L),
        )
        .toName()

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy { reportingSet = inaccessibleReportingSetName }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("reporting_set")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when Frequency Histogram metric is computed on non-union-only set expression`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
          reportingSet = INTERNAL_INCREMENTAL_REPORTING_SET.resourceName
        }
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws FAILED_PRECONDITION when EDP cert is revoked`() = runBlocking {
    val dataProvider = DATA_PROVIDERS.values.first()
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(
        certificatesMock.getCertificate(
          eq(getCertificateRequest { name = dataProvider.certificate })
        )
      )
      .thenReturn(
        certificate {
          name = dataProvider.certificate
          x509Der = DATA_PROVIDER_SIGNING_KEY.certificate.encoded.toByteString()
          revocationState = Certificate.RevocationState.REVOKED
        }
      )
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }

    assertThat(exception).hasMessageThat().ignoringCase().contains("revoked")
  }

  @Test
  fun `createMetric throws FAILED_PRECONDITION when EDP public key signature is invalid`() =
    runBlocking {
      val dataProvider = DATA_PROVIDERS.values.first()
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
      whenever(
          dataProvidersMock.getDataProvider(eq(getDataProviderRequest { name = dataProvider.name }))
        )
        .thenReturn(
          dataProvider.copy {
            publicKey = publicKey.copy { signature = "invalid sig".toByteStringUtf8() }
          }
        )
      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
        metricId = METRIC_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createMetric(request) }
          }
        }

      assertThat(exception).hasMessageThat().ignoringCase().contains("signature")
    }

  @Test
  fun `createMetric throws exception when internal createMetric throws exception`(): Unit =
    runBlocking {
      whenever(internalMetricsMock.createMetric(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
        metricId = METRIC_ID
      }

      assertFailsWith(Exception::class) {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    }

  @Test
  fun `createMetric throws exception when CMMS batchCreateMeasurements throws INVALID_ARGUMENT`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
      whenever(measurementsMock.batchCreateMeasurements(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
        metricId = METRIC_ID
      }

      val exception =
        assertFailsWith(Exception::class) {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createMetric(request) }
          }
        }
      assertThat(exception.grpcStatusCode()).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createMetric throws exception when batchSetCmmsMeasurementId throws exception`(): Unit =
    runBlocking {
      whenever(internalMeasurementsMock.batchSetCmmsMeasurementIds(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
        metricId = METRIC_ID
      }

      assertFailsWith(Exception::class) {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    }

  @Test
  fun `createMetric throws exception when getMeasurementConsumer throws NOT_FOUND`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(measurementConsumersMock.getMeasurementConsumer(any()))
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith(Exception::class) {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.grpcStatusCode()).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains(MEASUREMENT_CONSUMERS.values.first().name)
  }

  @Test
  fun `createMetric throws exception when the internal batchGetReportingSets throws exception`():
    Unit = runBlocking {
    whenever(internalReportingSetsMock.batchGetReportingSets(any()))
      .thenThrow(StatusRuntimeException(Status.UNKNOWN))

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }

    assertFails {
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
    }
  }

  @Test
  fun `createMetric throws exception when getDataProvider throws exception`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(dataProvidersMock.getDataProvider(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith(Exception::class) {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception).hasMessageThat().contains("dataProviders/")
  }

  @Test
  fun `createMetric throws INTERNAL when getCertificate throws unhandled status`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(certificatesMock.getCertificate(any()))
      .thenThrow(StatusRuntimeException(Status.UNKNOWN))

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = METRIC_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
    assertThat(exception).hasMessageThat().contains("certificates/")
  }

  @Test
  fun `createMetric throws FAILED_PRECONDITION when getCertificate throws NOT_FOUND`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
      whenever(certificatesMock.getCertificate(any()))
        .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
        metricId = METRIC_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.createMetric(request) }
          }
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception).hasMessageThat().contains("certificates/")
    }

  @Test
  fun `batchCreateMetrics creates CMMS measurements`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
        metricId = "metric-id1"
      }
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        metricId = "metric-id2"
      }
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.batchCreateMetrics(request) }
      }

    val expected = batchCreateMetricsResponse {
      metrics += PENDING_INCREMENTAL_REACH_METRIC
      metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
    }

    // Verify proto argument of the internal MetricsCoroutineImplBase::batchCreateMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalBatchCreateMetricsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          requests += internalCreateMetricRequest {
            metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
            externalMetricId = "metric-id1"
          }
          requests += internalCreateMetricRequest {
            metric = INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
            externalMetricId = "metric-id2"
          }
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_UNION_ALL_REACH_MEASUREMENT
            requestId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
            requestId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
            requestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          if (dataProvidersList.size == 1)
            SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC.copy {
              reportingMetadata = reportingMetadata {
                report = CONTAINING_REPORT
                metric =
                  MetricKey(
                      INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId,
                      INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId,
                    )
                    .toName()
              }
            }
          else
            UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
              nonceHashes.clear()
              nonceHashes += List(dataProvidersList.size) { Hashing.hashSha256(RANDOM_OUTPUT_LONG) }
              reportingMetadata = reportingMetadata {
                report = CONTAINING_REPORT
                metric =
                  MetricKey(
                      INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId,
                      INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId,
                    )
                    .toName()
              }
            }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementId
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `batchCreateMetrics creates CMMS measurements with 1 out of 2 population`() = runBlocking {
    wheneverBlocking { internalMetricsMock.batchCreateMetrics(any()) } doReturn
      internalBatchCreateMetricsResponse {
        metrics += INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC
        metrics += INTERNAL_PENDING_INITIAL_POPULATION_METRIC
      }
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
        metricId = "metric-id1"
      }
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_POPULATION_METRIC
        metricId = "metric-id2"
      }
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.batchCreateMetrics(request) }
      }

    val expected = batchCreateMetricsResponse {
      metrics += PENDING_INCREMENTAL_REACH_METRIC
      metrics += PENDING_POPULATION_METRIC
    }

    // Verify proto argument of the internal MetricsCoroutineImplBase::batchCreateMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalBatchCreateMetricsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          requests += internalCreateMetricRequest {
            metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
            externalMetricId = "metric-id1"
          }
          requests += internalCreateMetricRequest {
            metric = INTERNAL_REQUESTING_POPULATION_METRIC
            externalMetricId = "metric-id2"
          }
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_UNION_ALL_REACH_MEASUREMENT
            requestId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
            requestId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_POPULATION_MEASUREMENT
            requestId = INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          if (dataProvidersList.size == 1)
            POPULATION_MEASUREMENT_SPEC.copy {
              nonceHashes.clear()
              nonceHashes += List(dataProvidersList.size) { Hashing.hashSha256(RANDOM_OUTPUT_LONG) }
              reportingMetadata = reportingMetadata {
                report = CONTAINING_REPORT
                metric =
                  MetricKey(
                      INTERNAL_PENDING_POPULATION_METRIC.cmmsMeasurementConsumerId,
                      INTERNAL_PENDING_POPULATION_METRIC.externalMetricId,
                    )
                    .toName()
              }
            }
          else
            UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
              nonceHashes.clear()
              nonceHashes += List(dataProvidersList.size) { Hashing.hashSha256(RANDOM_OUTPUT_LONG) }
              reportingMetadata = reportingMetadata {
                report = CONTAINING_REPORT
                metric =
                  MetricKey(
                      INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId,
                      INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId,
                    )
                    .toName()
              }
            }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementId
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `batchCreateMetrics creates a single POPULATION CMMS measurement`() = runBlocking {
    wheneverBlocking { internalMetricsMock.batchCreateMetrics(any()) } doReturn
      internalBatchCreateMetricsResponse { metrics += INTERNAL_PENDING_INITIAL_POPULATION_METRIC }
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_POPULATION_METRIC
        metricId = "metric-id1"
      }
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.batchCreateMetrics(request) }
      }

    val expected = batchCreateMetricsResponse { metrics += PENDING_POPULATION_METRIC }

    // Verify proto argument of the internal MetricsCoroutineImplBase::batchCreateMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalBatchCreateMetricsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          requests += internalCreateMetricRequest {
            metric = INTERNAL_REQUESTING_POPULATION_METRIC
            externalMetricId = "metric-id1"
          }
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_POPULATION_MEASUREMENT
            requestId = INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          POPULATION_MEASUREMENT_SPEC.copy {
            nonceHashes.clear()
            nonceHashes += List(dataProvidersList.size) { Hashing.hashSha256(RANDOM_OUTPUT_LONG) }
            reportingMetadata = reportingMetadata {
              report = CONTAINING_REPORT
              metric =
                MetricKey(
                    INTERNAL_PENDING_POPULATION_METRIC.cmmsMeasurementConsumerId,
                    INTERNAL_PENDING_POPULATION_METRIC.externalMetricId,
                  )
                  .toName()
            }
          }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementId
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `batchCreateMetrics passes DEV ModelLine in CMMS Measurement requests`() {
    val devModelLine = "modelProviders/mp-1/modelSuites/ms-1/modelLines/dev"
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn
      checkPermissionsResponse {
        permissions += PermissionName.CREATE
        permissions += PermissionName.CREATE_WITH_DEV_MODEL_LINE
      }
    wheneverBlocking { modelLinesMock.getModelLine(any()) } doReturn
      modelLine { type = ModelLine.Type.DEV }
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { modelLine = devModelLine }
        metricId = "metric-id1"
      }
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        metricId = "metric-id2"
      }
    }

    withPrincipalAndScopes(PRINCIPAL, SCOPES) {
      runBlocking { service.batchCreateMetrics(request) }
    }

    val measurementsRequestCaptor = argumentCaptor<BatchCreateMeasurementsRequest>()
    verifyBlocking(measurementsMock) {
      batchCreateMeasurements(measurementsRequestCaptor.capture())
    }
    val batchCreateMeasurementsRequest = measurementsRequestCaptor.allValues.single()
    val modelLines =
      batchCreateMeasurementsRequest.requestsList.map {
        it.measurement.measurementSpec.unpack<MeasurementSpec>().modelLine
      }
    assertThat(modelLines).contains(devModelLine)
  }

  @Test
  fun `batchCreateMetrics throws PERMISSION_DENIED for DEV ModelLine`() {
    val devModelLine = "modelProviders/mp-1/modelSuites/ms-1/modelLines/dev"
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    wheneverBlocking { modelLinesMock.getModelLine(any()) } doReturn
      modelLine { type = ModelLine.Type.DEV }
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { modelLine = devModelLine }
        metricId = "metric-id1"
      }
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        metricId = "metric-id2"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.batchCreateMetrics(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception)
      .hasMessageThat()
      .contains(MetricsService.Permission.CREATE_WITH_DEV_MODEL_LINE)
  }

  @Test
  fun `batchCreateMetrics creates CMMS measurements with default model_line`() = runBlocking {
    service =
      MetricsService(
        METRIC_SPEC_CONFIG,
        MEASUREMENT_CONSUMER_CONFIGS,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        InternalMetricsGrpcKt.MetricsCoroutineStub(grpcTestServerRule.channel),
        variancesMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel),
        MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        CertificatesGrpcKt.CertificatesCoroutineStub(grpcTestServerRule.channel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
        ModelLinesCoroutineStub(grpcTestServerRule.channel),
        Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel)),
        ENCRYPTION_KEY_PAIR_STORE,
        randomMock,
        SECRETS_DIR,
        listOf(AGGREGATOR_ROOT_CERTIFICATE, DATA_PROVIDER_ROOT_CERTIFICATE).associateBy {
          it.subjectKeyIdentifier!!
        },
        DEFAULT_VID_MODEL_LINE,
        emptyMap(),
        POPULATION_DATA_PROVIDER_NAME,
      )

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }

    wheneverBlocking { internalMetricsMock.batchCreateMetrics(any()) } doReturn
      internalBatchCreateMetricsResponse {
        metrics += INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC
      }

    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        metricId = "metric-id2"
      }
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.batchCreateMetrics(request) }
      }

    val expected = batchCreateMetricsResponse {
      metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    // Verify proto argument of the internal MetricsCoroutineImplBase::batchCreateMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalBatchCreateMetricsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          requests += internalCreateMetricRequest {
            metric = INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
            externalMetricId = "metric-id2"
          }
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
            requestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()

      assertThat(measurementSpec)
        .isEqualTo(
          SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC.copy {
            modelLine = DEFAULT_VID_MODEL_LINE
            reportingMetadata = reportingMetadata {
              report = CONTAINING_REPORT
              metric =
                MetricKey(
                    INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId,
                    INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId,
                  )
                  .toName()
            }
          }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementId
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
          }
        }
      )
  }

  @Test
  fun `batchCreateMetrics creates CMMS measurements with specified model_line`() = runBlocking {
    val modelLineKey = ModelLineKey("1234", "124", "125")

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }

    wheneverBlocking { internalMetricsMock.batchCreateMetrics(any()) } doReturn
      internalBatchCreateMetricsResponse {
        metrics +=
          INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
            cmmsModelLine = modelLineKey.toName()
          }
      }

    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric =
          REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy { modelLine = modelLineKey.toName() }
        metricId = "metric-id2"
      }
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.batchCreateMetrics(request) }
      }

    val expected = batchCreateMetricsResponse {
      metrics +=
        PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy { modelLine = modelLineKey.toName() }
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    // Verify proto argument of the internal MetricsCoroutineImplBase::batchCreateMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalBatchCreateMetricsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          requests += internalCreateMetricRequest {
            metric =
              INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
                cmmsModelLine = modelLineKey.toName()
              }
            externalMetricId = "metric-id2"
          }
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
            requestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.single().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()

      assertThat(measurementSpec)
        .isEqualTo(
          SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC.copy {
            modelLine = modelLineKey.toName()
            reportingMetadata = reportingMetadata {
              report = CONTAINING_REPORT
              metric =
                MetricKey(
                    INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId,
                    INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId,
                  )
                  .toName()
            }
          }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE,
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementId
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                .cmmsCreateMeasurementRequestId
            cmmsMeasurementId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
          }
        }
      )
  }

  @Test
  fun `batchCreateMetrics with request IDs when metrics are already succeeded`() = runBlocking {
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
        metricId = "metric-id1"
      }
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        metricId = "metric-id2"
      }
    }

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(internalMetricsMock.batchCreateMetrics(any()))
      .thenReturn(
        internalBatchCreateMetricsResponse {
          metrics += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC
          metrics += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.batchCreateMetrics(request) }
      }

    val expected = batchCreateMetricsResponse {
      metrics += SUCCEEDED_INCREMENTAL_REACH_METRIC
      metrics += SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC
    }

    // Verify proto argument of the internal MetricsCoroutineImplBase::batchCreateMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalBatchCreateMetricsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          requests += internalCreateMetricRequest {
            metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
            externalMetricId = "metric-id1"
          }
          requests += internalCreateMetricRequest {
            metric = INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
            externalMetricId = "metric-id2"
          }
        }
      )

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    val getMeasurementConsumerCaptor: KArgumentCaptor<GetMeasurementConsumerRequest> =
      argumentCaptor()
    verifyBlocking(measurementConsumersMock, never()) {
      getMeasurementConsumer(getMeasurementConsumerCaptor.capture())
    }

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, never()) { getDataProvider(dataProvidersCaptor.capture()) }

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, never()) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    val batchSetCmmsMeasurementIdsCaptor: KArgumentCaptor<BatchSetCmmsMeasurementIdsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsCaptor.capture())
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when model_line invalid`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name

      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { modelLine = "invalid" }
        metricId = "metric-id"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.batchCreateMetrics(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "requests[0].metric.model_line"
        }
      )
  }

  @Test
  fun `batchCreateMetrics throws FAILED_PRECONDITION when model_line not found`() {
    wheneverBlocking { modelLinesMock.getModelLine(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val modelLineName = "modelProviders/mp-1/modelSuites/ms-1/modelLines/absent"
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name

      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { modelLine = modelLineName }
        metricId = "metric-id"
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.batchCreateMetrics(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.FAILED_PRECONDITION.code)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.MODEL_LINE_NOT_FOUND.name
          metadata[Errors.Metadata.MODEL_LINE.key] = modelLineName
        }
      )
  }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when number of requests exceeds limit`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name

      requests +=
        List(MAX_BATCH_SIZE + 1) {
          createMetricRequest {
            parent = MEASUREMENT_CONSUMERS.values.first().name
            metric = REQUESTING_INCREMENTAL_REACH_METRIC
            metricId = "metric-id$it"
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.batchCreateMetrics(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("At most $MAX_BATCH_SIZE requests can be supported in a batch.")
  }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when duplicate metric IDs`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name

      requests +=
        List(MAX_BATCH_SIZE + 1) {
          createMetricRequest {
            parent = MEASUREMENT_CONSUMERS.values.first().name
            metric = REQUESTING_INCREMENTAL_REACH_METRIC
            metricId = METRIC_ID
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.batchCreateMetrics(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMetrics returns without a next page token when there is no previous page token`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }

    val expected = listMetricsResponse {
      metrics += PENDING_INCREMENTAL_REACH_METRIC
      metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
    }

    // Verify proto argument of internal MetricsCoroutineImplBase::streamMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::streamMetrics)
      .isEqualTo(
        streamMetricsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
    val batchGetMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> = argumentCaptor()
    verifyBlocking(internalMetricsMock, never()) {
      batchGetMetrics(batchGetMetricsCaptor.capture())
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listMetrics returns with a next page token when there is no previous page token`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse { metrics += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC }
        )

      val pageSize = 1
      val request = listMetricsRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        this.pageSize = pageSize
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }

      val expected = listMetricsResponse {
        metrics += PENDING_INCREMENTAL_REACH_METRIC

        nextPageToken =
          listMetricsPageToken {
              this.pageSize = pageSize
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              lastMetric = previousPageEnd {
                cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
                externalMetricId = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
              }
            }
            .toByteString()
            .base64UrlEncode()
      }

      // Verify proto argument of internal MetricsCoroutineImplBase::streamMetrics
      verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::streamMetrics)
        .isEqualTo(
          streamMetricsRequest {
            limit = pageSize + 1
            this.filter = filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            }
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> = argumentCaptor()
      verifyBlocking(internalMetricsMock, never()) {
        batchGetMetrics(batchGetMetricsCaptor.capture())
      }

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listMetrics returns without a next page token when there is a previous page token`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(internalMetricsMock.streamMetrics(any()))
        .thenReturn(flowOf(INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC))
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          }
        )

      val pageSize = 1
      val request = listMetricsRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        this.pageSize = pageSize
        pageToken =
          listMetricsPageToken {
              this.pageSize = pageSize
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              lastMetric = previousPageEnd {
                cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
                externalMetricId = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
              }
            }
            .toByteString()
            .base64UrlEncode()
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }

      val expected = listMetricsResponse { metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC }

      // Verify proto argument of internal MetricsCoroutineImplBase::streamMetrics
      verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::streamMetrics)
        .isEqualTo(
          streamMetricsRequest {
            limit = pageSize + 1
            this.filter = filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalMetricIdAfter = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
            }
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> = argumentCaptor()
      verifyBlocking(internalMetricsMock, never()) {
        batchGetMetrics(batchGetMetricsCaptor.capture())
      }

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listMetrics with page size replaced with a valid value and no previous page token`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val invalidPageSize = MAX_PAGE_SIZE * 2

    val request = listMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = invalidPageSize
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }

    val expected = listMetricsResponse {
      metrics += PENDING_INCREMENTAL_REACH_METRIC
      metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
    }

    // Verify proto argument of internal MetricsCoroutineImplBase::streamMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::streamMetrics)
      .isEqualTo(
        streamMetricsRequest {
          limit = MAX_PAGE_SIZE + 1
          this.filter = filter {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
    val batchGetMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> = argumentCaptor()
    verifyBlocking(internalMetricsMock, never()) {
      batchGetMetrics(batchGetMetricsCaptor.capture())
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listMetrics with invalid page size replaced with the one in previous page token`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(internalMetricsMock.streamMetrics(any()))
        .thenReturn(flowOf(INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC))
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          }
        )

      val invalidPageSize = MAX_PAGE_SIZE * 2
      val previousPageSize = 1

      val request = listMetricsRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        this.pageSize = invalidPageSize
        pageToken =
          listMetricsPageToken {
              this.pageSize = previousPageSize
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              lastMetric = previousPageEnd {
                cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
                externalMetricId = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
              }
            }
            .toByteString()
            .base64UrlEncode()
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }

      val expected = listMetricsResponse { metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC }

      // Verify proto argument of internal MetricsCoroutineImplBase::streamMetrics
      verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::streamMetrics)
        .isEqualTo(
          streamMetricsRequest {
            limit = previousPageSize + 1
            this.filter = filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalMetricIdAfter = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
            }
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> = argumentCaptor()
      verifyBlocking(internalMetricsMock, never()) {
        batchGetMetrics(batchGetMetricsCaptor.capture())
      }

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listMetrics with a new page size replacing the old one in previous page token`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(internalMetricsMock.streamMetrics(any()))
        .thenReturn(flowOf(INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC))
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          }
        )

      val newPageSize = 10
      val previousPageSize = 1

      val request = listMetricsRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        this.pageSize = newPageSize
        pageToken =
          listMetricsPageToken {
              this.pageSize = previousPageSize
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              lastMetric = previousPageEnd {
                cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
                externalMetricId = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
              }
            }
            .toByteString()
            .base64UrlEncode()
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }

      val expected = listMetricsResponse { metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC }

      // Verify proto argument of internal MetricsCoroutineImplBase::streamMetrics
      verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::streamMetrics)
        .isEqualTo(
          streamMetricsRequest {
            limit = newPageSize + 1
            this.filter = filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalMetricIdAfter = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
            }
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> = argumentCaptor()
      verifyBlocking(internalMetricsMock, never()) {
        batchGetMetrics(batchGetMetricsCaptor.capture())
      }

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listMetrics returns succeeded metrics when the measurements are SUCCEEDED`() = runBlocking {
    val measurementsMap =
      mapOf(
        SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
        SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to
          SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
        PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
          PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT,
      )

    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
      val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
      batchGetMeasurementsResponse {
        measurements +=
          batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
      }
    }
    whenever(
        internalMetricsMock.batchGetMetrics(
          eq(
            internalBatchGetMetricsRequest {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalMetricIds += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
              externalMetricIds +=
                INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
            }
          )
        )
      )
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics +=
            INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.copy {
              state = InternalMetric.State.SUCCEEDED
              weightedMeasurements.clear()
              weightedMeasurements += weightedMeasurement {
                weight = 1
                binaryRepresentation = 3
                measurement = INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT
              }
              weightedMeasurements += weightedMeasurement {
                weight = -1
                binaryRepresentation = 2
                measurement = INTERNAL_SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
              }
            }
          metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )

    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }

    val expected = listMetricsResponse {
      metrics += SUCCEEDED_INCREMENTAL_REACH_METRIC
      metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
    }

    // Verify proto argument of internal MetricsCoroutineImplBase::streamMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::streamMetrics)
      .isEqualTo(
        streamMetricsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, times(1)) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }
    val capturedBatchSetMeasurementResultRequests = batchSetMeasurementResultsCaptor.allValues
    assertThat(capturedBatchSetMeasurementResultRequests)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        batchSetMeasurementResultsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          measurementResults += measurementResult {
            cmmsMeasurementId = INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
            this.results += INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.details.resultsList
          }
          measurementResults += measurementResult {
            cmmsMeasurementId =
              INTERNAL_SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
            this.results +=
              INTERNAL_SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.details.resultsList
          }
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listMetrics returns succeeded metrics when the metrics are SUCCEEDED`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    whenever(internalMetricsMock.streamMetrics(any()))
      .thenReturn(
        flowOf(
          INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC,
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC,
        )
      )
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC
          metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )

    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }

    val expected = listMetricsResponse {
      metrics += SUCCEEDED_INCREMENTAL_REACH_METRIC
      metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
    }

    // Verify proto argument of internal MetricsCoroutineImplBase::streamMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::streamMetrics)
      .isEqualTo(
        streamMetricsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
    val batchGetMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> = argumentCaptor()
    verifyBlocking(internalMetricsMock, never()) {
      batchGetMetrics(batchGetMetricsCaptor.capture())
    }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listMetrics returns failed metrics when the measurement is FAILED`() = runBlocking {
    val measurementsMap =
      mapOf(
        PENDING_UNION_ALL_REACH_MEASUREMENT.name to PENDING_UNION_ALL_REACH_MEASUREMENT,
        PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to
          PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
        PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
          PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
            state = Measurement.State.FAILED
            failure = failure {
              reason = Measurement.Failure.Reason.REQUISITION_REFUSED
              message =
                INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.details.failure.message
            }
          },
      )
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
      val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
      batchGetMeasurementsResponse {
        measurements +=
          batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
      }
    }
    whenever(
        internalMetricsMock.batchGetMetrics(
          eq(
            internalBatchGetMetricsRequest {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalMetricIds += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
              externalMetricIds +=
                INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
            }
          )
        )
      )
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC
          metrics +=
            INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
              state = InternalMetric.State.FAILED
              weightedMeasurements.clear()
              weightedMeasurements += weightedMeasurement {
                weight = 1
                binaryRepresentation = 1
                measurement = INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
              }
            }
        }
      )

    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }

    val expected = listMetricsResponse {
      metrics += PENDING_INCREMENTAL_REACH_METRIC
      metrics +=
        PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
          state = Metric.State.FAILED
          this.result = metricResult {
            cmmsMeasurements +=
              MeasurementKey(
                  INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementConsumerId,
                  INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId,
                )
                .toName()
          }
          failure = MetricKt.failure { reason = Metric.Failure.Reason.MEASUREMENT_STATE_INVALID }
        }
    }

    // Verify proto argument of internal MetricsCoroutineImplBase::streamMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::streamMetrics)
      .isEqualTo(
        streamMetricsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, times(1)) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }
    val capturedBatchSetMeasurementFailureRequests = batchSetMeasurementFailuresCaptor.allValues
    assertThat(capturedBatchSetMeasurementFailureRequests)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        batchSetMeasurementFailuresRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          measurementFailures += measurementFailure {
            cmmsMeasurementId =
              INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
            this.failure = INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.details.failure
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listMetrics throws UNAUTHENTICATED when no principal is found`() {
    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listMetrics(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listMetrics throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "$name-wrong" }, SCOPES) {
          runBlocking { service.listMetrics(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listMetrics throws INVALID_ARGUMENT when page size is less than 0`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val request = listMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = -1
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Page size cannot be less than 0.")
  }

  @Test
  fun `listMetrics throws INVALID_ARGUMENT when parent is unspecified`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.listMetrics(ListMetricsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMetrics throws INVALID_ARGUMENT when MC ID doesn't match one in page token`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    val request = listMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageToken =
        listMetricsPageToken {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.last().measurementConsumerId
            lastMetric = previousPageEnd {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.last().measurementConsumerId
              externalMetricId = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMetrics throws Exception when the internal streamMetrics throws Exception`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(internalMetricsMock.streamMetrics(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
    }

  @Test
  fun `listMetrics throws Exception when batchGetMeasurements throws Exception`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(measurementsMock.batchGetMeasurements(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))
      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
    }

  @Test
  fun `listMetrics throws Exception when internal batchSetMeasurementResults throws Exception`() {
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
            SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to
              SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }
      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenThrow(StatusRuntimeException(Status.UNKNOWN))
      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
    }
  }

  @Test
  fun `listMetrics throws Exception when internal batchSetMeasurementFailures throws Exception`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(measurementsMock.batchGetMeasurements(any()))
        .thenReturn(
          batchGetMeasurementsResponse {
            measurements +=
              PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
                state = Measurement.State.FAILED
                failure = failure {
                  reason = Measurement.Failure.Reason.REQUISITION_REFUSED
                  message =
                    INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.details.failure.message
                }
              }
          }
        )
      whenever(internalMeasurementsMock.batchSetMeasurementFailures(any()))
        .thenThrow(StatusRuntimeException(Status.UNKNOWN))

      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
    }

  @Test
  fun `listMetrics throws Exception when internal batchGetMetrics throws Exception`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
            SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to
              SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
            PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
              PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT,
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenThrow(StatusRuntimeException(Status.UNKNOWN))

      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
    }

  @Test
  fun `listMetrics throws FAILED_PRECONDITION when the measurement public key is not valid`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
            SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to
              SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
                measurementSpec =
                  signMeasurementSpec(
                    UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
                      measurementPublicKey =
                        MEASUREMENT_CONSUMER_PUBLIC_KEY.copy { clearData() }.pack()
                    },
                    MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE,
                  )
              },
            SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
              SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT,
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception)
        .hasMessageThat()
        .contains(SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name)
    }

  @Test
  fun `listMetrics throws Exception when the getCertificate throws Exception`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.LIST }
    whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
      val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
      val measurementsMap =
        mapOf(
          SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
          SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to
            SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
          SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
            SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT,
        )
      batchGetMeasurementsResponse {
        measurements +=
          batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
      }
    }
    whenever(certificatesMock.getCertificate(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.listMetrics(request) } }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
    assertThat(exception).hasMessageThat().contains(AGGREGATOR_CERTIFICATE.name)
  }

  @Test
  fun `getMetric returns the metric with SUCCEEDED when the metric has state STATE_UNSPECIFIED`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.copy { clearState() }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId
            externalMetricIds += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.externalMetricId
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result).isEqualTo(SUCCEEDED_INCREMENTAL_REACH_METRIC)
    }

  @Test
  fun `getMetric returns the metric with INVALID when metric has state INVALID`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }

    val invalidatedMetric =
      INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.copy { state = InternalMetric.State.INVALID }

    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(internalBatchGetMetricsResponse { metrics += invalidatedMetric })

    val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

    // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
    val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
      argumentCaptor()
    verifyBlocking(internalMetricsMock, times(1)) {
      batchGetMetrics(batchGetInternalMetricsCaptor.capture())
    }
    val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
    assertThat(capturedInternalGetMetricRequests)
      .containsExactly(
        internalBatchGetMetricsRequest {
          cmmsMeasurementConsumerId = invalidatedMetric.cmmsMeasurementConsumerId
          externalMetricIds += invalidatedMetric.externalMetricId
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal
    // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    assertThat(result)
      .isEqualTo(
        SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
          state = Metric.State.INVALID
          clearResult()
          this.result = metricResult {
            for (weightedMeasurement in
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.weightedMeasurementsList) {
              cmmsMeasurements +=
                MeasurementKey(
                    measurementConsumerId =
                      weightedMeasurement.measurement.cmmsMeasurementConsumerId,
                    measurementId = weightedMeasurement.measurement.cmmsMeasurementId,
                  )
                  .toName()
            }
          }
        }
      )
  }

  @Test
  fun `getMetric returns SUCCEEDED metric when metric already succeeded and single params set`() =
    runBlocking {
      val internalMeasurement = internalMeasurement {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
        cmmsMeasurementId = "cmms_id"
        cmmsCreateMeasurementRequestId = "SINGLE_PUBLISHER_REACH_MEASUREMENT"
        timeInterval = TIME_INTERVAL
        primitiveReportingSetBases += primitiveReportingSetBasis {
          externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
          filters += METRIC_FILTER
          filters += PRIMITIVE_REPORTING_SET_FILTER
        }
        state = InternalMeasurement.State.SUCCEEDED
        details =
          InternalMeasurementKt.details {
            results +=
              InternalMeasurementKt.result {
                reach =
                  InternalMeasurementKt.ResultKt.reach {
                    value = INCREMENTAL_REACH_VALUE
                    noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                    deterministicCountDistinct =
                      InternalDeterministicCountDistinct.getDefaultInstance()
                  }
              }
            dataProviderCount = 1
          }
      }

      val internalSucceededReachMetricWithSingleDataProviderParams = internalMetric {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
        externalMetricId = METRIC_ID
        externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
        timeInterval = TIME_INTERVAL
        metricSpec = internalMetricSpec {
          reach =
            InternalMetricSpecKt.reachParams {
              multipleDataProviderParams =
                InternalMetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    InternalMetricSpecKt.differentialPrivacyParams {
                      epsilon = REACH_ONLY_REACH_EPSILON
                      delta = DIFFERENTIAL_PRIVACY_DELTA
                    }
                  vidSamplingInterval =
                    InternalMetricSpecKt.vidSamplingInterval {
                      start = REACH_ONLY_VID_SAMPLING_START
                      width = REACH_ONLY_VID_SAMPLING_WIDTH
                    }
                }
              singleDataProviderParams =
                InternalMetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    InternalMetricSpecKt.differentialPrivacyParams {
                      epsilon = SINGLE_DATA_PROVIDER_REACH_ONLY_REACH_EPSILON
                      delta = DIFFERENTIAL_PRIVACY_DELTA
                    }
                  vidSamplingInterval =
                    InternalMetricSpecKt.vidSamplingInterval {
                      start = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_START
                      width = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_WIDTH
                    }
                }
            }
        }
        weightedMeasurements += weightedMeasurement {
          weight = 1
          binaryRepresentation = 1
          measurement = internalMeasurement
        }
        details =
          InternalMetricKt.details {
            filters += METRIC_FILTER
            containingReport = CONTAINING_REPORT
          }
        createTime = INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.createTime
        state = InternalMetric.State.SUCCEEDED
      }

      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += internalSucceededReachMetricWithSingleDataProviderParams
          }
        )

      val metricName =
        MetricKey(
            internalSucceededReachMetricWithSingleDataProviderParams.cmmsMeasurementConsumerId,
            internalSucceededReachMetricWithSingleDataProviderParams.externalMetricId,
          )
          .toName()

      val request = getMetricRequest { name = metricName }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              internalSucceededReachMetricWithSingleDataProviderParams.cmmsMeasurementConsumerId
            externalMetricIds +=
              internalSucceededReachMetricWithSingleDataProviderParams.externalMetricId
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      val succeededReachMetricWithSingleDataProviderParams = metric {
        name = metricName
        reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
        timeInterval = TIME_INTERVAL
        metricSpec = metricSpec {
          reach = reachParams {
            multipleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = REACH_ONLY_REACH_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = REACH_ONLY_VID_SAMPLING_START
                    width = REACH_ONLY_VID_SAMPLING_WIDTH
                  }
              }
            singleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = SINGLE_DATA_PROVIDER_REACH_ONLY_REACH_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_START
                    width = SINGLE_DATA_PROVIDER_REACH_ONLY_VID_SAMPLING_WIDTH
                  }
              }
          }
        }
        filters += METRIC_FILTER
        containingReport = CONTAINING_REPORT
        state = Metric.State.SUCCEEDED
        createTime = INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.createTime
        this.result = metricResult {
          reach =
            MetricResultKt.reachResult {
              value = INCREMENTAL_REACH_VALUE
              univariateStatistics = univariateStatistics {
                standardDeviation = sqrt(SINGLE_DATA_PROVIDER_VARIANCE_VALUE)
              }
            }
          cmmsMeasurements +=
            MeasurementKey(
                internalMeasurement.cmmsMeasurementConsumerId,
                internalMeasurement.cmmsMeasurementId,
              )
              .toName()
        }
      }

      assertThat(result).isEqualTo(succeededReachMetricWithSingleDataProviderParams)
    }

  @Test
  fun `getMetric returns SUCCEEDED metric when metric already succeeded and no single params`() =
    runBlocking {
      val internalMeasurement = internalMeasurement {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
        cmmsMeasurementId = "cmms_id"
        cmmsCreateMeasurementRequestId = "SINGLE_PUBLISHER_REACH_MEASUREMENT"
        timeInterval = TIME_INTERVAL
        primitiveReportingSetBases += primitiveReportingSetBasis {
          externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
          filters += METRIC_FILTER
          filters += PRIMITIVE_REPORTING_SET_FILTER
        }
        state = InternalMeasurement.State.SUCCEEDED
        details =
          InternalMeasurementKt.details {
            results +=
              InternalMeasurementKt.result {
                reach =
                  InternalMeasurementKt.ResultKt.reach {
                    value = INCREMENTAL_REACH_VALUE
                    noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                    deterministicCountDistinct =
                      InternalDeterministicCountDistinct.getDefaultInstance()
                  }
              }
            dataProviderCount = 1
          }
      }

      val internalSucceededReachMetric = internalMetric {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
        externalMetricId = METRIC_ID
        externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
        timeInterval = TIME_INTERVAL
        metricSpec = internalMetricSpec {
          reach =
            InternalMetricSpecKt.reachParams {
              multipleDataProviderParams =
                InternalMetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    InternalMetricSpecKt.differentialPrivacyParams {
                      epsilon = REACH_ONLY_REACH_EPSILON
                      delta = DIFFERENTIAL_PRIVACY_DELTA
                    }
                  vidSamplingInterval =
                    InternalMetricSpecKt.vidSamplingInterval {
                      start = REACH_ONLY_VID_SAMPLING_START
                      width = REACH_ONLY_VID_SAMPLING_WIDTH
                    }
                }
            }
        }
        weightedMeasurements += weightedMeasurement {
          weight = 1
          binaryRepresentation = 1
          measurement = internalMeasurement
        }
        details =
          InternalMetricKt.details {
            filters += METRIC_FILTER
            containingReport = CONTAINING_REPORT
          }
        createTime = INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.createTime
        state = InternalMetric.State.SUCCEEDED
      }

      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(internalBatchGetMetricsResponse { metrics += internalSucceededReachMetric })

      val metricName =
        MetricKey(
            internalSucceededReachMetric.cmmsMeasurementConsumerId,
            internalSucceededReachMetric.externalMetricId,
          )
          .toName()

      val request = getMetricRequest { name = metricName }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId = internalSucceededReachMetric.cmmsMeasurementConsumerId
            externalMetricIds += internalSucceededReachMetric.externalMetricId
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      val succeededReachMetric = metric {
        name = metricName
        reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
        timeInterval = TIME_INTERVAL
        metricSpec = metricSpec {
          reach = reachParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = REACH_ONLY_REACH_EPSILON
                delta = DIFFERENTIAL_PRIVACY_DELTA
              }
          }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = REACH_ONLY_VID_SAMPLING_START
              width = REACH_ONLY_VID_SAMPLING_WIDTH
            }
        }
        filters += METRIC_FILTER
        containingReport = CONTAINING_REPORT
        state = Metric.State.SUCCEEDED
        createTime = INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.createTime
        this.result = metricResult {
          reach =
            MetricResultKt.reachResult {
              value = INCREMENTAL_REACH_VALUE
              univariateStatistics = univariateStatistics {
                standardDeviation = sqrt(VARIANCE_VALUE)
              }
            }
          cmmsMeasurements +=
            MeasurementKey(
                internalMeasurement.cmmsMeasurementConsumerId,
                internalMeasurement.cmmsMeasurementId,
              )
              .toName()
        }
      }

      assertThat(result).isEqualTo(succeededReachMetric)
    }

  @Test
  fun `getMetric returns the metric with SUCCEEDED when the metric is already succeeded`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse { metrics += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC }
        )

      val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId
            externalMetricIds += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.externalMetricId
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result).isEqualTo(SUCCEEDED_INCREMENTAL_REACH_METRIC)
    }

  @Test
  fun `getMetric returns reach metric with statistics not set when measurement has no noise mechanism`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = -1
                  binaryRepresentation = 2
                  measurement = INTERNAL_SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                }
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 3
                  measurement =
                    INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            InternalMeasurementKt.result {
                              reach =
                                InternalMeasurementKt.ResultKt.reach {
                                  value = UNION_ALL_REACH_VALUE
                                  reachOnlyLiquidLegionsV2 = reachOnlyLiquidLegionsV2 {
                                    sketchParams = internalReachOnlyLiquidLegionsSketchParams {
                                      decayRate = REACH_ONLY_LLV2_DECAY_RATE
                                      maxSize = REACH_ONLY_LLV2_SKETCH_SIZE
                                    }
                                  }
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId
            externalMetricIds += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.externalMetricId
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
            this.result = metricResult {
              reach = MetricResultKt.reachResult { value = INCREMENTAL_REACH_VALUE }
              cmmsMeasurements += PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name
              cmmsMeasurements += PENDING_UNION_ALL_REACH_MEASUREMENT.name
            }
          }
        )
    }

  @Test
  fun `getMetric returns failed reach metric when reach methodology is unspecified`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = -1
                  binaryRepresentation = 2
                  measurement = INTERNAL_SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                }
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 3
                  measurement =
                    INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            InternalMeasurementKt.result {
                              reach =
                                InternalMeasurementKt.ResultKt.reach {
                                  value = UNION_ALL_REACH_VALUE
                                  noiseMechanism = NoiseMechanism.DISCRETE_GAUSSIAN
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId
            externalMetricIds += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.externalMetricId
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
            state = Metric.State.FAILED
            failure =
              MetricKt.failure {
                reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
                message = "Problem with variance calculation"
              }
            this.result = metricResult {
              reach = MetricResultKt.reachResult { value = INCREMENTAL_REACH_VALUE }
              cmmsMeasurements += PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name
              cmmsMeasurements += PENDING_UNION_ALL_REACH_MEASUREMENT.name
            }
          }
        )
    }

  @Test
  fun `getMetric returns reach metric without statistics when variance in custom methodology is unavailable`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = -1
                  binaryRepresentation = 2
                  measurement = INTERNAL_SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
                }
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 3
                  measurement =
                    INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            InternalMeasurementKt.result {
                              reach =
                                InternalMeasurementKt.ResultKt.reach {
                                  value = UNION_ALL_REACH_VALUE
                                  noiseMechanism = NoiseMechanism.DISCRETE_GAUSSIAN
                                  customDirectMethodology = internalCustomDirectMethodology {
                                    variance =
                                      InternalCustomDirectMethodologyKt.variance {
                                        unavailable =
                                          InternalCustomDirectMethodologyKt.VarianceKt.unavailable {
                                            reason =
                                              InternalCustomDirectMethodology.Variance.Unavailable
                                                .Reason
                                                .UNDERIVABLE
                                          }
                                      }
                                  }
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
            this.result = metricResult {
              reach = MetricResultKt.reachResult { value = INCREMENTAL_REACH_VALUE }
              cmmsMeasurements += PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name
              cmmsMeasurements += PENDING_UNION_ALL_REACH_MEASUREMENT.name
            }
          }
        )
    }

  @Test
  fun `getMetric throws FAILED_PRECONDITION when measurements SUCCEEDED but EDP cert is revoked`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          listOf(SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT).associateBy { it.name }
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name ->
              measurementsMap[name]
                ?: throw Status.NOT_FOUND.withDescription("Measurement $name not found")
                  .asRuntimeException()
            }
        }
      }

      val dataProvider = DATA_PROVIDERS.values.first()
      whenever(certificatesMock.getCertificate(any()))
        .thenReturn(
          certificate {
            name = dataProvider.certificate
            x509Der = DATA_PROVIDER_SIGNING_KEY.certificate.encoded.toByteString()
            revocationState = Certificate.RevocationState.REVOKED
          }
        )
      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }
        }

      assertThat(exception).hasMessageThat().ignoringCase().contains("revoked")
    }

  @Test
  fun `getMetric throw StatusRuntimeException when variance type in custom methodology is unspecified`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC
          }
        )

      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name to
              PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
                state = Measurement.State.SUCCEEDED

                results +=
                  DATA_PROVIDERS.keys.zip(WATCH_DURATION_LIST).map {
                    (dataProviderKey, watchDuration) ->
                    val dataProvider = DATA_PROVIDERS.getValue(dataProviderKey)
                    resultOutput {
                      val result =
                        MeasurementKt.result {
                          this.watchDuration =
                            MeasurementKt.ResultKt.watchDuration {
                              value = watchDuration
                              noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
                              customDirectMethodology = CustomDirectMethodology.getDefaultInstance()
                            }
                        }
                      encryptedResult =
                        encryptResult(
                          signResult(result, DATA_PROVIDER_SIGNING_KEY),
                          MEASUREMENT_CONSUMER_PUBLIC_KEY,
                        )
                      certificate = dataProvider.certificate
                    }
                  }
              }
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.UNKNOWN)
      assertThat(exception.message).contains("Variance in CustomDirectMethodology is not set")
    }

  @Test
  fun `getMetric throw StatusRuntimeException when unavailable variance has no reason specified`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC
          }
        )

      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name to
              PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
                state = Measurement.State.SUCCEEDED

                results +=
                  DATA_PROVIDERS.keys.zip(WATCH_DURATION_LIST).map {
                    (dataProviderKey, watchDuration) ->
                    val dataProvider = DATA_PROVIDERS.getValue(dataProviderKey)
                    resultOutput {
                      val result =
                        MeasurementKt.result {
                          this.watchDuration =
                            MeasurementKt.ResultKt.watchDuration {
                              value = watchDuration
                              noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
                              customDirectMethodology = customDirectMethodology {
                                variance =
                                  CustomDirectMethodologyKt.variance {
                                    unavailable =
                                      CustomDirectMethodology.Variance.Unavailable
                                        .getDefaultInstance()
                                  }
                              }
                            }
                        }
                      encryptedResult =
                        encryptResult(
                          signResult(result, DATA_PROVIDER_SIGNING_KEY),
                          MEASUREMENT_CONSUMER_PUBLIC_KEY,
                        )
                      certificate = dataProvider.certificate
                    }
                  }
              }
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.UNKNOWN)
      assertThat(exception.message).contains("no reason specified")
    }

  @Test
  fun `getMetric returns failed metric for reach when it has measurement with two results`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 3
                  measurement =
                    INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          val result =
                            InternalMeasurementKt.result {
                              reach =
                                InternalMeasurementKt.ResultKt.reach {
                                  value = UNION_ALL_REACH_VALUE
                                  noiseMechanism = NoiseMechanism.DISCRETE_GAUSSIAN
                                  reachOnlyLiquidLegionsV2 = reachOnlyLiquidLegionsV2 {
                                    sketchParams = internalReachOnlyLiquidLegionsSketchParams {
                                      decayRate = REACH_ONLY_LLV2_DECAY_RATE
                                      maxSize = REACH_ONLY_LLV2_SKETCH_SIZE
                                    }
                                  }
                                }
                            }
                          results += result
                          results += result
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(response.state).isEqualTo(Metric.State.FAILED)
      assertThat(response.failure)
        .isEqualTo(
          MetricKt.failure {
            reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
            message = "Problem with result"
          }
        )
      assertThat(response.result)
        .isEqualTo(metricResult { cmmsMeasurements += SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name })
    }

  @Test
  fun `getMetric returns failed metric when the succeeded measurement contains no result`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 3
                  measurement =
                    INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.copy {
                      details = InternalMeasurement.Details.getDefaultInstance()
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(response.state).isEqualTo(Metric.State.FAILED)
      assertThat(response.failure)
        .isEqualTo(
          MetricKt.failure {
            reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
            message = "Problem with result"
          }
        )
      assertThat(response.result)
        .isEqualTo(metricResult { cmmsMeasurements += SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name })
    }

  @Test
  fun `getMetric returns failed metric for reach metric when custom direct methodology has freq`():
    Unit = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics +=
            INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
              weightedMeasurements += weightedMeasurement {
                weight = 1
                binaryRepresentation = 3
                measurement =
                  INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.copy {
                    details =
                      InternalMeasurementKt.details {
                        results +=
                          InternalMeasurementKt.result {
                            reach =
                              InternalMeasurementKt.ResultKt.reach {
                                value = UNION_ALL_REACH_VALUE
                                noiseMechanism = NoiseMechanism.DISCRETE_GAUSSIAN
                                customDirectMethodology = internalCustomDirectMethodology {
                                  variance =
                                    InternalCustomDirectMethodologyKt.variance {
                                      frequency =
                                        InternalCustomDirectMethodology.Variance.FrequencyVariances
                                          .getDefaultInstance()
                                    }
                                }
                              }
                          }
                      }
                  }
              }
            }
        }
      )

    val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

    assertThat(response)
      .isEqualTo(
        SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
          state = Metric.State.FAILED
          failure =
            MetricKt.failure {
              reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
              message = "Problem with variance calculation"
            }
          result =
            result.copy {
              cmmsMeasurements += SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name
              reach =
                reach.copy {
                  value += UNION_ALL_REACH_VALUE
                  clearUnivariateStatistics()
                }
            }
        }
      )
  }

  @Test
  fun `getMetric calls batchSetMeasurementResults when request number is more than the limit`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      val weightedMeasurements =
        (0..BATCH_SET_MEASUREMENT_RESULTS_LIMIT).map { id ->
          weightedMeasurement {
            weight = 1
            binaryRepresentation = 1
            measurement =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
                cmmsCreateMeasurementRequestId = "UNION_ALL_REACH_MEASUREMENT$id"
                cmmsMeasurementId = externalIdToApiId(100L + id.toLong())
              }
          }
        }
      val measurementsMap: Map<String, Measurement> =
        weightedMeasurements.associate { weightedMeasurement ->
          val measurementName =
            MeasurementKey(
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
                weightedMeasurement.measurement.cmmsMeasurementId,
              )
              .toName()

          measurementName to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.copy { name = measurementName }
        }

      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.copy {
                this.weightedMeasurements.clear()
                this.weightedMeasurements += weightedMeasurements
              }
          }
        )
      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      val request = getMetricRequest { name = PENDING_INCREMENTAL_REACH_METRIC.name }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of cmms MeasurementsCoroutineImplBase::batchGetMeasurements
      val batchGetMeasurementsCaptor: KArgumentCaptor<BatchGetMeasurementsRequest> =
        argumentCaptor()
      verifyBlocking(
        measurementsMock,
        times(
          ceil(
              (1 + BATCH_SET_MEASUREMENT_RESULTS_LIMIT).toDouble() /
                BATCH_KINGDOM_MEASUREMENTS_LIMIT
            )
            .toInt()
        ),
      ) {
        batchGetMeasurements(batchGetMeasurementsCaptor.capture())
      }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, times(2)) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }
    }

  @Test
  fun `getMetric calls batchSetMeasurementFailures when request number is more than the limit`() =
    runBlocking {
      val weightedMeasurements =
        (0..BATCH_SET_MEASUREMENT_FAILURES_LIMIT).map { id ->
          weightedMeasurement {
            weight = 1
            binaryRepresentation = 1
            measurement =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
                cmmsCreateMeasurementRequestId = "UNION_ALL_REACH_MEASUREMENT$id"
                cmmsMeasurementId = externalIdToApiId(100L + id.toLong())
              }
          }
        }
      val measurementsMap: Map<String, Measurement> =
        weightedMeasurements.associate { weightedMeasurement ->
          val measurementName =
            MeasurementKey(
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
                weightedMeasurement.measurement.cmmsMeasurementId,
              )
              .toName()

          measurementName to
            PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
              name = measurementName
              state = Measurement.State.FAILED
              failure = failure {
                reason = Measurement.Failure.Reason.REQUISITION_REFUSED
                message = "failed"
              }
            }
        }

      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.copy {
                this.weightedMeasurements.clear()
                this.weightedMeasurements += weightedMeasurements
              }
          }
        )
      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      val request = getMetricRequest { name = PENDING_INCREMENTAL_REACH_METRIC.name }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of cmms MeasurementsCoroutineImplBase::batchGetMeasurements
      val batchGetMeasurementsCaptor: KArgumentCaptor<BatchGetMeasurementsRequest> =
        argumentCaptor()
      verifyBlocking(
        measurementsMock,
        times(
          ceil(
              (1 + BATCH_SET_MEASUREMENT_FAILURES_LIMIT).toDouble() /
                BATCH_KINGDOM_MEASUREMENTS_LIMIT
            )
            .toInt()
        ),
      ) {
        batchGetMeasurements(batchGetMeasurementsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, times(2)) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }
    }

  @Test
  fun `getMetric returns the metric with FAILED when the metric is already failed`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics += INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )

    val request = getMetricRequest { name = FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

    // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
    val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
      argumentCaptor()
    verifyBlocking(internalMetricsMock, times(1)) {
      batchGetMetrics(batchGetInternalMetricsCaptor.capture())
    }
    val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
    assertThat(capturedInternalGetMetricRequests)
      .containsExactly(
        internalBatchGetMetricsRequest {
          cmmsMeasurementConsumerId =
            INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId
          externalMetricIds += INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal
    // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    assertThat(result)
      .isEqualTo(
        FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
          failure = MetricKt.failure { reason = Metric.Failure.Reason.MEASUREMENT_STATE_INVALID }
          this.result = metricResult {
            cmmsMeasurements +=
              MeasurementKey(
                  INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementConsumerId,
                  INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId,
                )
                .toName()
          }
        }
      )
  }

  @Test
  fun `getMetric returns the metric with RUNNING when measurements are pending`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse { metrics += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC }
      )

    val request = getMetricRequest { name = PENDING_INCREMENTAL_REACH_METRIC.name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

    // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
    val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
      argumentCaptor()
    verifyBlocking(internalMetricsMock, times(1)) {
      batchGetMetrics(batchGetInternalMetricsCaptor.capture())
    }
    val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
    assertThat(capturedInternalGetMetricRequests)
      .containsExactly(
        internalBatchGetMetricsRequest {
          cmmsMeasurementConsumerId =
            INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId
          externalMetricIds += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal
    // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    assertThat(result).isEqualTo(PENDING_INCREMENTAL_REACH_METRIC)
  }

  @Test
  fun `getMetric returns frequency histogram metric with SUCCEEDED when measurements are updated to SUCCEEDED`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      doReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          },
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          },
        )
        .wheneverBlocking(internalMetricsMock) {
          batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.externalMetricId
              }
            )
          )
        }
      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.name to
              SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenReturn(Empty.getDefaultInstance())

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, times(1)) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }
      assertThat(batchSetMeasurementResultsCaptor.allValues)
        .containsExactly(
          batchSetMeasurementResultsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
                .cmmsMeasurementConsumerId
            measurementResults += measurementResult {
              cmmsMeasurementId =
                INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.cmmsMeasurementId
              this.results +=
                INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.details.resultsList
            }
          }
        )

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result).isEqualTo(SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC)
    }

  @Test
  fun `getMetric returns SUCCEEDED rf when metric already succeeded and single params set`() =
    runBlocking {
      val internalSucceededReachAndFrequencyMetricWithSingleDataProviderParams = internalMetric {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
        externalMetricId = METRIC_ID
        externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
        timeInterval = TIME_INTERVAL
        metricSpec = internalMetricSpec {
          reachAndFrequency =
            InternalMetricSpecKt.reachAndFrequencyParams {
              multipleDataProviderParams =
                InternalMetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                  reachPrivacyParams =
                    InternalMetricSpecKt.differentialPrivacyParams {
                      epsilon = REACH_FREQUENCY_REACH_EPSILON
                      delta = DIFFERENTIAL_PRIVACY_DELTA
                    }
                  frequencyPrivacyParams =
                    InternalMetricSpecKt.differentialPrivacyParams {
                      epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
                      delta = DIFFERENTIAL_PRIVACY_DELTA
                    }
                  vidSamplingInterval =
                    InternalMetricSpecKt.vidSamplingInterval {
                      start = REACH_FREQUENCY_VID_SAMPLING_START
                      width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
                    }
                }
              singleDataProviderParams =
                InternalMetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                  reachPrivacyParams =
                    InternalMetricSpecKt.differentialPrivacyParams {
                      epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_REACH_EPSILON
                      delta = DIFFERENTIAL_PRIVACY_DELTA
                    }
                  frequencyPrivacyParams =
                    InternalMetricSpecKt.differentialPrivacyParams {
                      epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_FREQUENCY_EPSILON
                      delta = DIFFERENTIAL_PRIVACY_DELTA
                    }
                  vidSamplingInterval =
                    InternalMetricSpecKt.vidSamplingInterval {
                      start = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_START
                      width = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_WIDTH
                    }
                }
              maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
            }
        }
        weightedMeasurements += weightedMeasurement {
          weight = 1
          binaryRepresentation = 1
          measurement = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
        }
        details =
          InternalMetricKt.details {
            filters += METRIC_FILTER
            containingReport = CONTAINING_REPORT
          }
        createTime = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.createTime
        state = InternalMetric.State.SUCCEEDED
      }

      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += internalSucceededReachAndFrequencyMetricWithSingleDataProviderParams
          }
        )

      val metricName =
        MetricKey(
            internalSucceededReachAndFrequencyMetricWithSingleDataProviderParams
              .cmmsMeasurementConsumerId,
            internalSucceededReachAndFrequencyMetricWithSingleDataProviderParams.externalMetricId,
          )
          .toName()

      val request = getMetricRequest { name = metricName }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              internalSucceededReachAndFrequencyMetricWithSingleDataProviderParams
                .cmmsMeasurementConsumerId
            externalMetricIds +=
              internalSucceededReachAndFrequencyMetricWithSingleDataProviderParams.externalMetricId
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      val succeededReachMetricWithSingleDataProviderParams = metric {
        name = metricName
        reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
        timeInterval = TIME_INTERVAL
        metricSpec = metricSpec {
          reachAndFrequency = reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = REACH_FREQUENCY_REACH_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = REACH_FREQUENCY_VID_SAMPLING_START
                    width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
                  }
              }
            singleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_REACH_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_FREQUENCY_EPSILON
                    delta = DIFFERENTIAL_PRIVACY_DELTA
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_START
                    width = SINGLE_DATA_PROVIDER_REACH_FREQUENCY_VID_SAMPLING_WIDTH
                  }
              }
            maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
          }
        }
        filters += METRIC_FILTER
        containingReport = CONTAINING_REPORT
        state = Metric.State.SUCCEEDED
        createTime = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.createTime
        this.result = metricResult {
          cmmsMeasurements += PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.name
          reachAndFrequency =
            MetricResultKt.reachAndFrequencyResult {
              reach =
                MetricResultKt.reachResult {
                  value = REACH_FREQUENCY_REACH_VALUE
                  univariateStatistics = univariateStatistics {
                    standardDeviation = sqrt(SINGLE_DATA_PROVIDER_VARIANCE_VALUE)
                  }
                }
              frequencyHistogram =
                MetricResultKt.histogramResult {
                  bins +=
                    (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).map { frequency ->
                      MetricResultKt.HistogramResultKt.bin {
                        label = frequency.toString()
                        binResult =
                          MetricResultKt.HistogramResultKt.binResult {
                            value =
                              REACH_FREQUENCY_REACH_VALUE *
                                REACH_FREQUENCY_FREQUENCY_VALUE.getOrDefault(
                                  frequency.toLong(),
                                  0.0,
                                )
                          }
                        resultUnivariateStatistics = univariateStatistics {
                          standardDeviation =
                            sqrt(
                              SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCES.countVariances.getValue(
                                frequency
                              )
                            )
                        }
                        relativeUnivariateStatistics = univariateStatistics {
                          standardDeviation =
                            sqrt(
                              SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCES.relativeVariances.getValue(
                                frequency
                              )
                            )
                        }
                        kPlusUnivariateStatistics = univariateStatistics {
                          standardDeviation =
                            sqrt(
                              SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCES.kPlusCountVariances.getValue(
                                frequency
                              )
                            )
                        }
                        relativeKPlusUnivariateStatistics = univariateStatistics {
                          standardDeviation =
                            sqrt(
                              SINGLE_DATA_PROVIDER_FREQUENCY_VARIANCES.kPlusRelativeVariances
                                .getValue(frequency)
                            )
                        }
                      }
                    }
                }
            }
        }
      }

      assertThat(result).isEqualTo(succeededReachMetricWithSingleDataProviderParams)
    }

  @Test
  fun `getMetric returns reach frequency metric with statistics not set when reach lacks info for variance`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            InternalMeasurementKt.result {
                              reach =
                                InternalMeasurementKt.ResultKt.reach {
                                  value = REACH_FREQUENCY_REACH_VALUE
                                }
                              frequency =
                                InternalMeasurementKt.ResultKt.frequency {
                                  relativeFrequencyDistribution.putAll(
                                    REACH_FREQUENCY_FREQUENCY_VALUE
                                  )
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                  liquidLegionsDistribution = internalLiquidLegionsDistribution {
                                    decayRate = LL_DISTRIBUTION_DECAY_RATE
                                    maxSize = LL_DISTRIBUTION_SKETCH_SIZE
                                  }
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest {
        name = SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
            this.result =
              this.result.copy {
                reachAndFrequency =
                  MetricResultKt.reachAndFrequencyResult {
                    reach = MetricResultKt.reachResult { value = REACH_FREQUENCY_REACH_VALUE }
                    frequencyHistogram =
                      MetricResultKt.histogramResult {
                        bins +=
                          (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).map { frequency ->
                            MetricResultKt.HistogramResultKt.bin {
                              label = frequency.toString()
                              binResult =
                                MetricResultKt.HistogramResultKt.binResult {
                                  value =
                                    REACH_FREQUENCY_REACH_VALUE *
                                      REACH_FREQUENCY_FREQUENCY_VALUE.getOrDefault(
                                        frequency.toLong(),
                                        0.0,
                                      )
                                }
                            }
                          }
                      }
                  }
              }
          }
        )
    }

  @Test
  fun `getMetric returns reach frequency metric with statistics not set when frequency noise mechanism is unspecified`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            InternalMeasurementKt.result {
                              reach =
                                InternalMeasurementKt.ResultKt.reach {
                                  value = REACH_FREQUENCY_REACH_VALUE
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                  deterministicCountDistinct =
                                    InternalDeterministicCountDistinct.getDefaultInstance()
                                }
                              frequency =
                                InternalMeasurementKt.ResultKt.frequency {
                                  relativeFrequencyDistribution.putAll(
                                    REACH_FREQUENCY_FREQUENCY_VALUE
                                  )
                                  liquidLegionsDistribution = internalLiquidLegionsDistribution {
                                    decayRate = LL_DISTRIBUTION_DECAY_RATE
                                    maxSize = LL_DISTRIBUTION_SKETCH_SIZE
                                  }
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest {
        name = SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
            this.result =
              this.result.copy {
                reachAndFrequency =
                  MetricResultKt.reachAndFrequencyResult {
                    reach =
                      MetricResultKt.reachResult {
                        value = REACH_FREQUENCY_REACH_VALUE
                        univariateStatistics = univariateStatistics {
                          standardDeviation = sqrt(VARIANCE_VALUE)
                        }
                      }
                    frequencyHistogram =
                      MetricResultKt.histogramResult {
                        bins +=
                          (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).map { frequency ->
                            MetricResultKt.HistogramResultKt.bin {
                              label = frequency.toString()
                              binResult =
                                MetricResultKt.HistogramResultKt.binResult {
                                  value =
                                    REACH_FREQUENCY_REACH_VALUE *
                                      REACH_FREQUENCY_FREQUENCY_VALUE.getOrDefault(
                                        frequency.toLong(),
                                        0.0,
                                      )
                                }
                            }
                          }
                      }
                  }
              }
          }
        )
    }

  @Test
  fun `getMetric returns failed rf metric when frequency methodology is unspecified`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            InternalMeasurementKt.result {
                              reach =
                                InternalMeasurementKt.ResultKt.reach {
                                  value = REACH_FREQUENCY_REACH_VALUE
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                  deterministicCountDistinct =
                                    InternalDeterministicCountDistinct.getDefaultInstance()
                                }
                              frequency =
                                InternalMeasurementKt.ResultKt.frequency {
                                  relativeFrequencyDistribution.putAll(
                                    REACH_FREQUENCY_FREQUENCY_VALUE
                                  )
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest {
        name = SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
            state = Metric.State.FAILED
            failure =
              MetricKt.failure {
                reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
                message = "Problem with variance calculation"
              }
            this.result =
              this.result.copy {
                reachAndFrequency =
                  MetricResultKt.reachAndFrequencyResult {
                    reach = MetricResultKt.reachResult { value = REACH_FREQUENCY_REACH_VALUE }
                    frequencyHistogram =
                      MetricResultKt.histogramResult {
                        bins +=
                          (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).map { frequency ->
                            MetricResultKt.HistogramResultKt.bin {
                              label = frequency.toString()
                              binResult =
                                MetricResultKt.HistogramResultKt.binResult {
                                  value =
                                    REACH_FREQUENCY_REACH_VALUE *
                                      REACH_FREQUENCY_FREQUENCY_VALUE.getOrDefault(
                                        frequency.toLong(),
                                        0.0,
                                      )
                                }
                            }
                          }
                      }
                  }
              }
          }
        )
    }

  @Test
  fun `getMetric returns reach frequency metric without statistics when variance in custom methodology is unavailable`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            InternalMeasurementKt.result {
                              reach =
                                InternalMeasurementKt.ResultKt.reach {
                                  value = REACH_FREQUENCY_REACH_VALUE
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                  deterministicCountDistinct =
                                    InternalDeterministicCountDistinct.getDefaultInstance()
                                }
                              frequency =
                                InternalMeasurementKt.ResultKt.frequency {
                                  relativeFrequencyDistribution.putAll(
                                    REACH_FREQUENCY_FREQUENCY_VALUE
                                  )
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                  customDirectMethodology = internalCustomDirectMethodology {
                                    variance =
                                      InternalCustomDirectMethodologyKt.variance {
                                        unavailable =
                                          InternalCustomDirectMethodologyKt.VarianceKt.unavailable {
                                            reason =
                                              InternalCustomDirectMethodology.Variance.Unavailable
                                                .Reason
                                                .UNDERIVABLE
                                          }
                                      }
                                  }
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest {
        name = SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
            this.result =
              this.result.copy {
                reachAndFrequency =
                  MetricResultKt.reachAndFrequencyResult {
                    reach =
                      MetricResultKt.reachResult {
                        value = REACH_FREQUENCY_REACH_VALUE
                        univariateStatistics = univariateStatistics {
                          standardDeviation = sqrt(VARIANCE_VALUE)
                        }
                      }
                    frequencyHistogram =
                      MetricResultKt.histogramResult {
                        bins +=
                          (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).map { frequency ->
                            MetricResultKt.HistogramResultKt.bin {
                              label = frequency.toString()
                              binResult =
                                MetricResultKt.HistogramResultKt.binResult {
                                  value =
                                    REACH_FREQUENCY_REACH_VALUE *
                                      REACH_FREQUENCY_FREQUENCY_VALUE.getOrDefault(
                                        frequency.toLong(),
                                        0.0,
                                      )
                                }
                            }
                          }
                      }
                  }
              }
          }
        )
    }

  @Test
  fun `getMetric returns failed metric when metric contains measurement with two rf results`():
    Unit = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics +=
            INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
              weightedMeasurements.clear()
              weightedMeasurements += weightedMeasurement {
                weight = 1
                binaryRepresentation = 1
                measurement =
                  INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
                    details =
                      InternalMeasurementKt.details {
                        val result =
                          InternalMeasurementKt.result {
                            reach =
                              InternalMeasurementKt.ResultKt.reach {
                                value = REACH_FREQUENCY_REACH_VALUE
                                noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                deterministicCountDistinct =
                                  InternalDeterministicCountDistinct.getDefaultInstance()
                              }
                            frequency =
                              InternalMeasurementKt.ResultKt.frequency {
                                relativeFrequencyDistribution.putAll(
                                  REACH_FREQUENCY_FREQUENCY_VALUE
                                )
                                noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                liquidLegionsDistribution = internalLiquidLegionsDistribution {
                                  decayRate = LL_DISTRIBUTION_DECAY_RATE
                                  maxSize = LL_DISTRIBUTION_SKETCH_SIZE
                                }
                              }
                          }
                        results += result
                        results += result
                      }
                  }
              }
            }
        }
      )

    val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name }

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }
    assertThat(response.state).isEqualTo(Metric.State.FAILED)
    assertThat(response.failure)
      .isEqualTo(
        MetricKt.failure {
          reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
          message = "Problem with result"
        }
      )
    assertThat(response.result)
      .isEqualTo(
        metricResult {
          cmmsMeasurements += SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.name
        }
      )
  }

  @Test
  fun `getMetric returns failed metric for rf when custom direct methodology has scalar`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 3
                  measurement =
                    INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
                      details =
                        INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.details
                          .copy {
                            results.clear()
                            results +=
                              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
                                .details
                                .resultsList
                                .first()
                                .copy {
                                  frequency =
                                    INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
                                      .details
                                      .resultsList
                                      .first()
                                      .frequency
                                      .copy {
                                        customDirectMethodology = internalCustomDirectMethodology {
                                          variance =
                                            InternalCustomDirectMethodologyKt.variance {
                                              scalar = 10.0
                                            }
                                        }
                                      }
                                }
                          }
                    }
                }
              }
          }
        )

      val request = getMetricRequest {
        name = SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name
      }

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(response)
        .isEqualTo(
          SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
            state = Metric.State.FAILED
            failure =
              MetricKt.failure {
                reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
                message = "Problem with variance calculation"
              }
            result =
              result.copy {
                reachAndFrequency =
                  reachAndFrequency.copy {
                    reach = reach.copy { clearUnivariateStatistics() }
                    frequencyHistogram =
                      frequencyHistogram.copy {
                        bins.clear()
                        SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.result.reachAndFrequency
                          .frequencyHistogram
                          .binsList
                          .forEach {
                            bins +=
                              it.copy {
                                clearKPlusUnivariateStatistics()
                                clearResultUnivariateStatistics()
                                clearRelativeUnivariateStatistics()
                                clearRelativeKPlusUnivariateStatistics()
                              }
                          }
                      }
                  }
              }
          }
        )
    }

  @Test
  fun `getMetric returns rf metric with result of 0 when measurement results are 0`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }

      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
                      state = InternalMeasurement.State.SUCCEEDED
                      details =
                        details.copy {
                          results +=
                            InternalMeasurementKt.result {
                              reach =
                                InternalMeasurementKt.ResultKt.reach {
                                  value = 0
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                  deterministicCountDistinct =
                                    InternalDeterministicCountDistinct.getDefaultInstance()
                                }
                              frequency =
                                InternalMeasurementKt.ResultKt.frequency {
                                  relativeFrequencyDistribution.putAll(
                                    REACH_FREQUENCY_FREQUENCY_VALUE
                                  )
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                  liquidLegionsDistribution = internalLiquidLegionsDistribution {
                                    decayRate = LL_DISTRIBUTION_DECAY_RATE
                                    maxSize = LL_DISTRIBUTION_SKETCH_SIZE
                                  }
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest {
        name = SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
            state = Metric.State.SUCCEEDED
            this.result = metricResult {
              cmmsMeasurements += PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.name
              reachAndFrequency =
                MetricResultKt.reachAndFrequencyResult {
                  reach =
                    MetricResultKt.reachResult {
                      value = 0L
                      univariateStatistics = univariateStatistics {
                        standardDeviation = sqrt(VARIANCE_VALUE)
                      }
                    }
                  frequencyHistogram =
                    MetricResultKt.histogramResult {
                      bins +=
                        (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).map { frequency ->
                          MetricResultKt.HistogramResultKt.bin {
                            label = frequency.toString()
                            binResult = MetricResultKt.HistogramResultKt.binResult { value = 0.0 }
                          }
                        }
                    }
                }
            }
          }
        )
    }

  @Test
  fun `getMetric returns impression metric with 0 result when measurement results are 0`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }

      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
                      state = InternalMeasurement.State.SUCCEEDED
                      details =
                        details.copy {
                          results +=
                            InternalMeasurementKt.result {
                              impression = InternalMeasurementKt.ResultKt.impression { value = 0 }
                            }
                        }
                    }
                }
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
                      state = InternalMeasurement.State.SUCCEEDED
                      details =
                        details.copy {
                          results +=
                            InternalMeasurementKt.result {
                              impression = InternalMeasurementKt.ResultKt.impression { value = 0 }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
            state = Metric.State.SUCCEEDED
            this.result = metricResult {
              cmmsMeasurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name
              cmmsMeasurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name
              impressionCount = MetricResultKt.impressionCountResult { value = 0L }
            }
          }
        )
    }

  @Test
  fun `getMetric returns reach metric with 0 result when measurement results are 0`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }

      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
                      state = InternalMeasurement.State.SUCCEEDED
                      details =
                        details.copy {
                          results +=
                            InternalMeasurementKt.result {
                              reach = InternalMeasurementKt.ResultKt.reach { value = 0 }
                            }
                        }
                    }
                }
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
                      state = InternalMeasurement.State.SUCCEEDED
                      details =
                        details.copy {
                          results +=
                            InternalMeasurementKt.result {
                              reach = InternalMeasurementKt.ResultKt.reach { value = 0 }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          PENDING_INCREMENTAL_REACH_METRIC.copy {
            state = Metric.State.SUCCEEDED
            this.result = metricResult {
              cmmsMeasurements += PENDING_UNION_ALL_REACH_MEASUREMENT.name
              cmmsMeasurements += PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name
              reach = MetricResultKt.reachResult { value = 0L }
            }
          }
        )
    }

  @Test
  fun `getMetric returns duration metric with 0 result when measurement results are 0`(): Unit =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }

      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
                      state = InternalMeasurement.State.SUCCEEDED
                      details =
                        details.copy {
                          results +=
                            InternalMeasurementKt.result {
                              watchDuration =
                                InternalMeasurementKt.ResultKt.watchDuration {
                                  value = duration {
                                    seconds = 0
                                    nanos = 0
                                  }
                                }
                            }
                        }
                    }
                }
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
                      state = InternalMeasurement.State.SUCCEEDED
                      details =
                        details.copy {
                          results +=
                            InternalMeasurementKt.result {
                              watchDuration =
                                InternalMeasurementKt.ResultKt.watchDuration {
                                  value = duration {
                                    seconds = 0
                                    nanos = 0
                                  }
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
            state = Metric.State.SUCCEEDED
            this.result = metricResult {
              cmmsMeasurements += PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name
              cmmsMeasurements += PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name
              watchDuration = MetricResultKt.watchDurationResult { value = 0.0 }
            }
          }
        )
    }

  @Test
  fun `getMetric returns duration metric with SUCCEEDED when measurements are updated to SUCCEEDED`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC
          },
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC
          },
        )

      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name to
              SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }
      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenReturn(Empty.getDefaultInstance())

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, times(1)) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }
      assertThat(batchSetMeasurementResultsCaptor.allValues)
        .containsExactly(
          batchSetMeasurementResultsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT.cmmsMeasurementConsumerId
            measurementResults += measurementResult {
              cmmsMeasurementId =
                INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT.cmmsMeasurementId
              this.results +=
                INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT.details.resultsList
            }
          }
        )

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result).isEqualTo(SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC)
    }

  @Test
  fun `getMetric returns succeeded duration metric without stats when 2 measurements`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
                weightedMeasurements += weightedMeasurements[0]
              }
          }
        )

      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name to
              SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }
      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenReturn(Empty.getDefaultInstance())

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(response)
        .isEqualTo(
          SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
            result = metricResult {
              watchDuration =
                MetricResultKt.watchDurationResult {
                  value = TOTAL_WATCH_DURATION.seconds.toDouble() * 2
                }
              cmmsMeasurements += PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name
              cmmsMeasurements += PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name
            }
          }
        )
    }

  @Test
  fun `getMetric returns impression metric with SUCCEEDED when measurements are updated to SUCCEEDED`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          },
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC
          },
        )

      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
              SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenReturn(Empty.getDefaultInstance())

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, times(1)) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }
      assertThat(batchSetMeasurementResultsCaptor.allValues)
        .containsExactly(
          batchSetMeasurementResultsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementConsumerId
            measurementResults += measurementResult {
              cmmsMeasurementId =
                INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
              this.results +=
                INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.details.resultsList
            }
          }
        )

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result).isEqualTo(SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC)
    }

  @Test
  fun `getMetric returns succeeded impression metric without stats when 2 measurements`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
                weightedMeasurements += weightedMeasurements[0]
              }
          }
        )

      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
              SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenReturn(Empty.getDefaultInstance())

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(response)
        .isEqualTo(
          SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
            result = metricResult {
              impressionCount =
                MetricResultKt.impressionCountResult { value = IMPRESSION_VALUE * 2 }
              cmmsMeasurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name
              cmmsMeasurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name
            }
          }
        )
    }

  @Test
  fun `getMetric returns impression metric with SUCCEEDED when measurements have custom frequency cap and are updated to SUCCEEDED`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          },
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC_CUSTOM_CAP
          },
        )

      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
              SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_CUSTOM_CAP
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenReturn(Empty.getDefaultInstance())

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, times(1)) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }
      assertThat(batchSetMeasurementResultsCaptor.allValues)
        .containsExactly(
          batchSetMeasurementResultsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_CUSTOM_CAP
                .cmmsMeasurementConsumerId
            measurementResults += measurementResult {
              cmmsMeasurementId =
                INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_CUSTOM_CAP
                  .cmmsMeasurementId
              this.results +=
                INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_CUSTOM_CAP.details
                  .resultsList
            }
          }
        )

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result).isEqualTo(SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC)
    }

  @Test
  fun `getMetric returns the metric with FAILED when measurements are updated to FAILED`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          },
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC
          },
        )

      val failedSinglePublisherImpressionMeasurement =
        PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
          state = Measurement.State.FAILED
          failure = failure {
            reason = Measurement.Failure.Reason.REQUISITION_REFUSED
            message =
              INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.details.failure.message
          }
        }

      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
              failedSinglePublisherImpressionMeasurement
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      whenever(internalMeasurementsMock.batchSetMeasurementFailures(any()))
        .thenReturn(Empty.getDefaultInstance())

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, times(1)) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }
      assertThat(batchSetMeasurementFailuresCaptor.allValues)
        .containsExactly(
          batchSetMeasurementFailuresRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementConsumerId
            measurementFailures += measurementFailure {
              cmmsMeasurementId =
                INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
              this.failure = INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.details.failure
            }
          }
        )

      assertThat(result)
        .isEqualTo(
          FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
            failure = MetricKt.failure { reason = Metric.Failure.Reason.MEASUREMENT_STATE_INVALID }
            this.result = metricResult {
              cmmsMeasurements +=
                MeasurementKey(
                    INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                      .cmmsMeasurementConsumerId,
                    INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId,
                  )
                  .toName()
            }
          }
        )
    }

  @Test
  fun `getMetric returns the metric with FAILED when measurements are updated to CANCELLED`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          },
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC
          },
        )

      val cancelledSinglePublisherImpressionMeasurement =
        PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy { state = Measurement.State.CANCELLED }

      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to
              cancelledSinglePublisherImpressionMeasurement
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      whenever(internalMeasurementsMock.batchSetMeasurementFailures(any()))
        .thenReturn(Empty.getDefaultInstance())

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, times(1)) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }
      assertThat(batchSetMeasurementFailuresCaptor.allValues)
        .containsExactly(
          batchSetMeasurementFailuresRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementConsumerId
            measurementFailures += measurementFailure {
              cmmsMeasurementId =
                INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
            }
          }
        )

      assertThat(result)
        .isEqualTo(
          FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
            failure = MetricKt.failure { reason = Metric.Failure.Reason.MEASUREMENT_STATE_INVALID }
            this.result = metricResult {
              cmmsMeasurements +=
                MeasurementKey(
                    INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                      .cmmsMeasurementConsumerId,
                    INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId,
                  )
                  .toName()
            }
          }
        )
    }

  @Test
  fun `getMetric returns reach frequency metric with SUCCEEDED when measurements are already SUCCEEDED`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          }
        )

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.cmmsMeasurementConsumerId
            externalMetricIds +=
              INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.externalMetricId
          }
        )

      // Verify proto argument of MeasurementsCoroutineImplBase::batchGetMeasurements
      val batchGetMeasurementsCaptor: KArgumentCaptor<BatchGetMeasurementsRequest> =
        argumentCaptor()
      verifyBlocking(measurementsMock, never()) {
        batchGetMeasurements(batchGetMeasurementsCaptor.capture())
      }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result).isEqualTo(SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC)
    }

  @Test
  fun `getMetric returns impression metric without statistics when set expression is not union-only`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = -1
                  binaryRepresentation = 1
                  measurement = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                }
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId
            externalMetricIds +=
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
          }
        )

      // Verify proto argument of MeasurementsCoroutineImplBase::batchGetMeasurements
      val batchGetMeasurementsCaptor: KArgumentCaptor<BatchGetMeasurementsRequest> =
        argumentCaptor()
      verifyBlocking(measurementsMock, never()) {
        batchGetMeasurements(batchGetMeasurementsCaptor.capture())
      }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
            this.result = metricResult {
              impressionCount = MetricResultKt.impressionCountResult { value = 0L }
              cmmsMeasurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name
              cmmsMeasurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name
            }
          }
        )
    }

  @Test
  fun `getMetric returns impression metric without statistics when noise mechanism is unspecified`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            InternalMeasurementKt.result {
                              impression =
                                InternalMeasurementKt.ResultKt.impression {
                                  value = IMPRESSION_VALUE
                                  deterministicCount =
                                    InternalDeterministicCount.getDefaultInstance()
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
            this.result =
              this.result.copy {
                impressionCount = impressionCount.copy { clearUnivariateStatistics() }
              }
          }
        )
    }

  @Test
  fun `getMetric returns failed impression metric when methodology is not set`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics +=
            INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
              weightedMeasurements.clear()
              weightedMeasurements += weightedMeasurement {
                weight = 1
                binaryRepresentation = 1
                measurement =
                  INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
                    details =
                      InternalMeasurementKt.details {
                        results +=
                          InternalMeasurementKt.result {
                            impression =
                              InternalMeasurementKt.ResultKt.impression {
                                value = IMPRESSION_VALUE
                                noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                              }
                          }
                      }
                  }
              }
            }
        }
      )

    val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

    assertThat(result)
      .isEqualTo(
        SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
          state = Metric.State.FAILED
          failure =
            MetricKt.failure {
              reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
              message = "Problem with variance calculation"
            }
          this.result =
            this.result.copy {
              impressionCount = impressionCount.copy { clearUnivariateStatistics() }
            }
        }
      )
  }

  @Test
  fun `getMetric returns impression metric without statistics when variance in custom methodology is unavailable`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            InternalMeasurementKt.result {
                              impression =
                                InternalMeasurementKt.ResultKt.impression {
                                  value = IMPRESSION_VALUE
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                  customDirectMethodology = internalCustomDirectMethodology {
                                    variance =
                                      InternalCustomDirectMethodologyKt.variance {
                                        unavailable =
                                          InternalCustomDirectMethodologyKt.VarianceKt.unavailable {
                                            reason =
                                              InternalCustomDirectMethodology.Variance.Unavailable
                                                .Reason
                                                .UNDERIVABLE
                                          }
                                      }
                                  }
                                }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
            this.result =
              this.result.copy {
                impressionCount = impressionCount.copy { clearUnivariateStatistics() }
              }
          }
        )
    }

  @Test
  fun `getMetric returns failed metric for impression when custom direct methodology has freq`():
    Unit = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics +=
            INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
              weightedMeasurements.clear()
              weightedMeasurements += weightedMeasurement {
                weight = 1
                binaryRepresentation = 1
                measurement =
                  INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
                    details =
                      InternalMeasurementKt.details {
                        results +=
                          InternalMeasurementKt.result {
                            impression =
                              InternalMeasurementKt.ResultKt.impression {
                                value = IMPRESSION_VALUE
                                noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                customDirectMethodology = internalCustomDirectMethodology {
                                  InternalCustomDirectMethodologyKt.variance {
                                    frequency =
                                      InternalCustomDirectMethodology.Variance.FrequencyVariances
                                        .getDefaultInstance()
                                  }
                                }
                              }
                          }
                      }
                  }
              }
            }
        }
      )

    val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

    assertThat(response)
      .isEqualTo(
        SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
          state = Metric.State.FAILED
          failure =
            MetricKt.failure {
              reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
              message = "Problem with variance calculation"
            }
          result =
            result.copy { impressionCount = impressionCount.copy { clearUnivariateStatistics() } }
        }
      )
  }

  @Test
  fun `getMetric returns duration metric with SUCCEEDED when measurements are already SUCCEEDED`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC
          }
        )

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.cmmsMeasurementConsumerId
            externalMetricIds +=
              INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.externalMetricId
          }
        )

      // Verify proto argument of MeasurementsCoroutineImplBase::batchGetMeasurements
      val batchGetMeasurementsCaptor: KArgumentCaptor<BatchGetMeasurementsRequest> =
        argumentCaptor()
      verifyBlocking(measurementsMock, never()) {
        batchGetMeasurements(batchGetMeasurementsCaptor.capture())
      }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result).isEqualTo(SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC)
    }

  @Test
  fun `getMetric returns duration metric without statistics when set expression is not union-only`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement = INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT
                }
                weightedMeasurements += weightedMeasurement {
                  weight = -1
                  binaryRepresentation = 1
                  measurement = INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT
                }
              }
          }
        )

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
            this.result = metricResult {
              watchDuration = MetricResultKt.watchDurationResult { value = 0.0 }
              cmmsMeasurements += PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name
              cmmsMeasurements += PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name
            }
          }
        )
    }

  @Test
  fun `getMetric returns duration metric without statistics when noise mechanism is unspecified`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            WATCH_DURATION_LIST.map { duration ->
                              InternalMeasurementKt.result {
                                watchDuration =
                                  InternalMeasurementKt.ResultKt.watchDuration {
                                    value = duration
                                    deterministicSum = InternalDeterministicSum.getDefaultInstance()
                                  }
                              }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
            this.result =
              this.result.copy {
                watchDuration = watchDuration.copy { clearUnivariateStatistics() }
              }
          }
        )
    }

  @Test
  fun `getMetric returns failed duration metric when methodology is not set`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics +=
            INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
              weightedMeasurements.clear()
              weightedMeasurements += weightedMeasurement {
                weight = 1
                binaryRepresentation = 1
                measurement =
                  INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
                    details =
                      InternalMeasurementKt.details {
                        results +=
                          WATCH_DURATION_LIST.map { duration ->
                            InternalMeasurementKt.result {
                              watchDuration =
                                InternalMeasurementKt.ResultKt.watchDuration {
                                  value = duration
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                }
                            }
                          }
                      }
                  }
              }
            }
        }
      )

    val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

    assertThat(result)
      .isEqualTo(
        SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
          state = Metric.State.FAILED
          failure =
            MetricKt.failure {
              reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
              message = "Problem with variance calculation"
            }
          this.result =
            this.result.copy { watchDuration = watchDuration.copy { clearUnivariateStatistics() } }
        }
      )
  }

  @Test
  fun `getMetric returns duration metric without statistics when variance in custom methodology is unavailable`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
                weightedMeasurements.clear()
                weightedMeasurements += weightedMeasurement {
                  weight = 1
                  binaryRepresentation = 1
                  measurement =
                    INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
                      details =
                        InternalMeasurementKt.details {
                          results +=
                            WATCH_DURATION_LIST.map { duration ->
                              InternalMeasurementKt.result {
                                watchDuration =
                                  InternalMeasurementKt.ResultKt.watchDuration {
                                    value = duration
                                    noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                    customDirectMethodology = internalCustomDirectMethodology {
                                      variance =
                                        InternalCustomDirectMethodologyKt.variance {
                                          unavailable =
                                            InternalCustomDirectMethodologyKt.VarianceKt
                                              .unavailable {
                                                reason =
                                                  InternalCustomDirectMethodology.Variance
                                                    .Unavailable
                                                    .Reason
                                                    .UNDERIVABLE
                                              }
                                        }
                                    }
                                  }
                              }
                            }
                        }
                    }
                }
              }
          }
        )

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
            this.result =
              this.result.copy {
                watchDuration = watchDuration.copy { clearUnivariateStatistics() }
              }
          }
        )
    }

  @Test
  fun `getMetric return failed metric for dur metric when custom direct methodology has freq`():
    Unit = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics +=
            INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
              weightedMeasurements.clear()
              weightedMeasurements += weightedMeasurement {
                weight = 1
                binaryRepresentation = 1
                measurement =
                  INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
                    details =
                      InternalMeasurementKt.details {
                        results +=
                          WATCH_DURATION_LIST.map { duration ->
                            InternalMeasurementKt.result {
                              watchDuration =
                                InternalMeasurementKt.ResultKt.watchDuration {
                                  value = duration
                                  noiseMechanism = NoiseMechanism.CONTINUOUS_LAPLACE
                                  customDirectMethodology = internalCustomDirectMethodology {
                                    InternalCustomDirectMethodologyKt.variance {
                                      frequency =
                                        InternalCustomDirectMethodology.Variance.FrequencyVariances
                                          .getDefaultInstance()
                                    }
                                  }
                                }
                            }
                          }
                      }
                  }
              }
            }
        }
      )

    val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

    assertThat(response)
      .isEqualTo(
        SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
          state = Metric.State.FAILED
          failure =
            MetricKt.failure {
              reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
              message = "Problem with variance calculation"
            }
          result =
            result.copy { watchDuration = watchDuration.copy { clearUnivariateStatistics() } }
        }
      )
  }

  @Test
  fun `getMetric throws INVALID_ARGUMENT when Metric name is invalid`() {
    val request = getMetricRequest { name = "invalid_metric_name" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getMetric throws PERMISSION_DENIED when MeasurementConsumer's identity does not match`() {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "$name-wrong" }, SCOPES) {
          runBlocking { service.getMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getMetric throws FAILED_PRECONDITION when the measurement public key is not valid`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
            SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to
              SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
                measurementSpec =
                  signMeasurementSpec(
                    UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
                      measurementPublicKey =
                        MEASUREMENT_CONSUMER_PUBLIC_KEY.copy { clearData() }.pack()
                    },
                    MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE,
                  )
              },
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      val request = getMetricRequest { name = PENDING_INCREMENTAL_REACH_METRIC.name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception)
        .hasMessageThat()
        .contains(SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name)
    }

  @Test
  fun `getMetric throws UNKNOWN when variance in CustomMethodology in a measurement is not set`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      doReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          },
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          },
        )
        .wheneverBlocking(internalMetricsMock) {
          batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.externalMetricId
              }
            )
          )
        }
      whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.name to
              SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
                results.clear()
                results += resultOutput {
                  val result =
                    MeasurementKt.result {
                      reach =
                        MeasurementKt.ResultKt.reach {
                          value = REACH_FREQUENCY_REACH_VALUE
                          noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
                          customDirectMethodology = CustomDirectMethodology.getDefaultInstance()
                        }
                      frequency =
                        MeasurementKt.ResultKt.frequency {
                          relativeFrequencyDistribution.putAll(REACH_FREQUENCY_FREQUENCY_VALUE)
                          noiseMechanism = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
                          liquidLegionsDistribution = liquidLegionsDistribution {
                            decayRate = LL_DISTRIBUTION_DECAY_RATE
                            maxSize = LL_DISTRIBUTION_SKETCH_SIZE
                          }
                        }
                    }
                  encryptedResult =
                    encryptResult(
                      signResult(result, AGGREGATOR_SIGNING_KEY),
                      MEASUREMENT_CONSUMER_PUBLIC_KEY,
                    )
                  certificate = AGGREGATOR_CERTIFICATE.name
                }
              }
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name -> measurementsMap.getValue(name) }
        }
      }

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.UNKNOWN)
    }

  @Test
  fun `batchGetMetrics returns metrics with SUCCEEDED when the metric is already succeeded`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          }
        )

      val request = batchGetMetricsRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        names += SUCCEEDED_INCREMENTAL_REACH_METRIC.name
        names += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name
      }

      val result =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.batchGetMetrics(request) }
        }

      // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
      val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, times(1)) {
        batchGetMetrics(batchGetInternalMetricsCaptor.capture())
      }
      val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
      assertThat(capturedInternalGetMetricRequests)
        .containsExactly(
          internalBatchGetMetricsRequest {
            cmmsMeasurementConsumerId =
              INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId
            externalMetricIds += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.externalMetricId
            externalMetricIds +=
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
          }
        )

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
      val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
      }

      // Verify proto argument of internal
      // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
      val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
        argumentCaptor()
      verifyBlocking(internalMeasurementsMock, never()) {
        batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
      }

      assertThat(result)
        .isEqualTo(
          batchGetMetricsResponse {
            metrics += SUCCEEDED_INCREMENTAL_REACH_METRIC
            metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          }
        )
    }

  @Test
  fun `batchGetMetrics returns metrics with RUNNING when measurements are pending`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    val request = batchGetMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      names += PENDING_INCREMENTAL_REACH_METRIC.name
      names += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name
    }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.batchGetMetrics(request) } }

    // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
    val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
      argumentCaptor()
    verifyBlocking(internalMetricsMock, times(1)) {
      batchGetMetrics(batchGetInternalMetricsCaptor.capture())
    }
    val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
    assertThat(capturedInternalGetMetricRequests)
      .containsExactly(
        internalBatchGetMetricsRequest {
          cmmsMeasurementConsumerId =
            INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId
          externalMetricIds += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
          externalMetricIds += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal
    // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    assertThat(result)
      .isEqualTo(
        batchGetMetricsResponse {
          metrics += PENDING_INCREMENTAL_REACH_METRIC
          metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )
  }

  @Test
  fun `batchGetMetrics returns failed metrics when variance calculation fails`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(variancesMock.computeMetricVariance(any<ReachMetricVarianceParams>()))
      .thenThrow(IllegalArgumentException("Negative"))

    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC
          metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )

    val request = batchGetMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      names += SUCCEEDED_INCREMENTAL_REACH_METRIC.name
      names += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name
    }

    val response =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.batchGetMetrics(request) } }

    // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
    val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
      argumentCaptor()
    verifyBlocking(internalMetricsMock, times(1)) {
      batchGetMetrics(batchGetInternalMetricsCaptor.capture())
    }
    val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
    assertThat(capturedInternalGetMetricRequests)
      .containsExactly(
        internalBatchGetMetricsRequest {
          cmmsMeasurementConsumerId =
            INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.cmmsMeasurementConsumerId
          externalMetricIds += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.externalMetricId
          externalMetricIds += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal
    // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchGetMetricsResponse {
          metrics +=
            SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
              state = Metric.State.FAILED
              failure =
                MetricKt.failure {
                  reason = Metric.Failure.Reason.MEASUREMENT_RESULT_INVALID
                  message = "Problem with variance calculation"
                }
              result = result.copy { reach = reach.copy { clearUnivariateStatistics() } }
            }
          metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )
  }

  @Test
  fun `batchGetMetrics throws INVALID_ARGUMENT when number of requests exceeds limit`() =
    runBlocking {
      wheneverBlocking {
        permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
      } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
      val request = batchGetMetricsRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        names += List(MAX_BATCH_SIZE + 1) { "metric_name" }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            runBlocking { service.batchGetMetrics(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.status.description)
        .isEqualTo("At most $MAX_BATCH_SIZE metrics can be supported in a batch.")
    }

  @Test
  fun `createMetric creates CMMS measurements for population`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.CREATE }
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(INTERNAL_PENDING_INITIAL_POPULATION_METRIC)
    whenever(measurementsMock.batchCreateMeasurements(any()))
      .thenReturn(
        batchCreateMeasurementsResponse { measurements += PENDING_POPULATION_MEASUREMENT }
      )

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_POPULATION_METRIC
      metricId = METRIC_ID
    }
    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.createMetric(request) } }

    val expected = PENDING_POPULATION_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_POPULATION_METRIC
          externalMetricId = METRIC_ID
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) {
      batchCreateMeasurements(measurementsCaptor.capture())
    }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(MEASUREMENT_SPEC_FIELD, ENCRYPTED_REQUISITION_SPEC_FIELD)
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_POPULATION_MEASUREMENT
            requestId = INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
        }
      )

    val capturedMeasurementRequest = capturedMeasurementRequests.single().requestsList.first()
    verifyMeasurementSpec(
      capturedMeasurementRequest.measurement.measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
    )

    val dataProvidersList =
      capturedMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

    val measurementSpec: MeasurementSpec =
      capturedMeasurementRequest.measurement.measurementSpec.unpack()

    assertThat(measurementSpec)
      .isEqualTo(
        POPULATION_MEASUREMENT_SPEC.copy {
          nonceHashes.clear()
          nonceHashes += List(dataProvidersList.size) { Hashing.hashSha256(RANDOM_OUTPUT_LONG) }
          reportingMetadata = reportingMetadata {
            report = CONTAINING_REPORT
            metric =
              MetricKey(
                  INTERNAL_PENDING_INITIAL_POPULATION_METRIC.cmmsMeasurementConsumerId,
                  INTERNAL_PENDING_INITIAL_POPULATION_METRIC.externalMetricId,
                )
                .toName()
          }
        }
      )

    dataProvidersList.map { dataProviderEntry ->
      val signedRequisitionSpec =
        decryptRequisitionSpec(
          dataProviderEntry.value.encryptedRequisitionSpec,
          DATA_PROVIDER_PRIVATE_KEY_HANDLE,
        )
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
      verifyRequisitionSpec(
        signedRequisitionSpec,
        requisitionSpec,
        measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER,
      )
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementId
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds,
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          this.measurementIds += measurementIds {
            cmmsCreateMeasurementRequestId =
              INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = INTERNAL_PENDING_POPULATION_MEASUREMENT.cmmsMeasurementId
          }
        }
      )
    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `getMetric returns population metric`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.GET }
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse { metrics += INTERNAL_SUCCEEDED_POPULATION_METRIC }
      )

    val request = getMetricRequest { name = SUCCEEDED_POPULATION_METRIC.name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { runBlocking { service.getMetric(request) } }

    // Verify proto argument of internal MetricsCoroutineImplBase::batchGetMetrics
    val batchGetInternalMetricsCaptor: KArgumentCaptor<InternalBatchGetMetricsRequest> =
      argumentCaptor()
    verifyBlocking(internalMetricsMock, times(1)) {
      batchGetMetrics(batchGetInternalMetricsCaptor.capture())
    }
    val capturedInternalGetMetricRequests = batchGetInternalMetricsCaptor.allValues
    assertThat(capturedInternalGetMetricRequests)
      .containsExactly(
        internalBatchGetMetricsRequest {
          cmmsMeasurementConsumerId = INTERNAL_SUCCEEDED_POPULATION_METRIC.cmmsMeasurementConsumerId
          externalMetricIds += INTERNAL_SUCCEEDED_POPULATION_METRIC.externalMetricId
        }
      )

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetMeasurementResults
    val batchSetMeasurementResultsCaptor: KArgumentCaptor<BatchSetMeasurementResultsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementResults(batchSetMeasurementResultsCaptor.capture())
    }

    // Verify proto argument of internal
    // MeasurementsCoroutineImplBase::batchSetMeasurementFailures
    val batchSetMeasurementFailuresCaptor: KArgumentCaptor<BatchSetMeasurementFailuresRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetMeasurementFailures(batchSetMeasurementFailuresCaptor.capture())
    }

    assertThat(result).isEqualTo(SUCCEEDED_POPULATION_METRIC)
  }

  @Test
  fun `invalidateMetric returns Metric with state INVALID`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.INVALIDATE }

    whenever(internalMetricsMock.invalidateMetric(any()))
      .thenReturn(
        INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
          state = InternalMetric.State.INVALID
        }
      )

    val request = invalidateMetricRequest { name = FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

    val result =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        runBlocking { service.invalidateMetric(request) }
      }

    assertThat(result)
      .isEqualTo(FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy { state = Metric.State.INVALID })

    // Verify proto argument of the internal MetricsCoroutineImplBase::invalidateMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::invalidateMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalInvalidateMetricRequest {
          cmmsMeasurementConsumerId =
            INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId
          externalMetricId = INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
        }
      )
  }

  @Test
  fun `invalidateMetric throws FAILED_PRECONDITION when metric FAILED`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.INVALIDATE }

    val measurementConsumerKey =
      MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMERS.values.first().name)
    val metricKey = MetricKey(measurementConsumerKey!!, "aaa")
    whenever(internalMetricsMock.invalidateMetric(any()))
      .thenThrow(
        MetricNotFoundException(
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
            externalMetricId = metricKey.metricId,
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)
      )

    val request = invalidateMetricRequest { name = metricKey.toName() }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.invalidateMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.METRIC_NOT_FOUND.name
          metadata[Errors.Metadata.METRIC.key] = request.name
        }
      )
  }

  fun `invalidateMetric throws NOT_FOUND when metric not found`() = runBlocking {
    wheneverBlocking {
      permissionsServiceMock.checkPermissions(hasPrincipal(PRINCIPAL.name))
    } doReturn checkPermissionsResponse { permissions += PermissionName.INVALIDATE }

    val measurementConsumerKey =
      MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMERS.values.first().name)
    val metricKey = MetricKey(measurementConsumerKey!!, "aaa")
    whenever(internalMetricsMock.invalidateMetric(any()))
      .thenThrow(
        InvalidMetricStateTransitionException(
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
            externalMetricId = metricKey.metricId,
            metricState = InternalMetric.State.FAILED,
            newMetricState = InternalMetric.State.INVALID,
          )
          .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      )

    val request = invalidateMetricRequest { name = metricKey.toName() }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.invalidateMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.METRIC_NOT_FOUND.name
          metadata[Errors.Metadata.METRIC.key] = request.name
          metadata[Errors.Metadata.METRIC_STATE.key] = Metric.State.FAILED.name
          metadata[Errors.Metadata.NEW_METRIC_STATE.key] = Metric.State.INVALID.name
        }
      )
  }

  @Test
  fun `invalidateMetric throws INVALID_ARGUMENT when Metric name is invalid`() {
    val request = invalidateMetricRequest { name = "invalid_metric_name" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.invalidateMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `invalidateMetric throws PERMISSION_DENIED when MC's identity does not match`() {
    val request = invalidateMetricRequest {
      name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          runBlocking { service.invalidateMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  object PermissionName {
    const val GET = "permissions/${MetricsService.Permission.GET}"
    const val LIST = "permissions/${MetricsService.Permission.LIST}"
    const val CREATE = "permissions/${MetricsService.Permission.CREATE}"
    const val CREATE_WITH_DEV_MODEL_LINE =
      "permissions/${MetricsService.Permission.CREATE_WITH_DEV_MODEL_LINE}"
    const val INVALIDATE = "permissions/${MetricsService.Permission.INVALIDATE}"
  }

  companion object {
    private val PRINCIPAL = principal { name = "principals/mc-user" }
    private val ALL_PERMISSIONS =
      setOf(
        MetricsService.Permission.GET,
        MetricsService.Permission.LIST,
        MetricsService.Permission.CREATE,
        MetricsService.Permission.CREATE_WITH_DEV_MODEL_LINE,
        MetricsService.Permission.INVALIDATE,
      )
    private val SCOPES = ALL_PERMISSIONS

    private val MEASUREMENT_SPEC_FIELD =
      Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER)
    private val ENCRYPTED_REQUISITION_SPEC_FIELD =
      Measurement.DataProviderEntry.Value.getDescriptor()
        .findFieldByNumber(
          Measurement.DataProviderEntry.Value.ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER
        )
  }
}

private fun CmmsEventGroupKey.toInternal(): InternalReportingSet.Primitive.EventGroupKey {
  val source = this
  return InternalReportingSetKt.PrimitiveKt.eventGroupKey {
    cmmsDataProviderId = source.dataProviderId
    cmmsEventGroupId = source.eventGroupId
  }
}

private val InternalReportingSet.resourceKey: ReportingSetKey
  get() = ReportingSetKey(cmmsMeasurementConsumerId, externalReportingSetId)
private val InternalReportingSet.resourceName: String
  get() = resourceKey.toName()
