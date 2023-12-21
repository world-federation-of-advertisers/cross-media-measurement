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
import com.google.protobuf.duration
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Paths
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.Instant
import kotlin.math.pow
import kotlin.math.sqrt
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
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
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
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultOutput
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.batchCreateMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.customDirectMethodology
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.liquidLegionsDistribution
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.reachOnlyLiquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
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
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
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
import org.wfanet.measurement.internal.reporting.v2.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricKt.weightedMeasurement
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
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementFailuresResponse
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsResponse
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementResultsResponse
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest as internalCreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.customDirectMethodology as internalCustomDirectMethodology
import org.wfanet.measurement.internal.reporting.v2.liquidLegionsDistribution as internalLiquidLegionsDistribution
import org.wfanet.measurement.internal.reporting.v2.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2.metric as internalMetric
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.reachOnlyLiquidLegionsSketchParams as internalReachOnlyLiquidLegionsSketchParams
import org.wfanet.measurement.internal.reporting.v2.reachOnlyLiquidLegionsV2
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.api.v2alpha.batchCreateMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsRequest
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
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.ListMetricsPageTokenKt.previousPageEnd
import org.wfanet.measurement.reporting.v2alpha.ListMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
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

private const val REACH_FREQUENCY_VID_SAMPLING_WIDTH = 5.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_VID_SAMPLING_START = 48.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_REACH_EPSILON = 0.0033
private const val REACH_FREQUENCY_FREQUENCY_EPSILON = 0.115
private const val REACH_FREQUENCY_MAXIMUM_FREQUENCY = 5

private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_VID_SAMPLING_START = 143.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_EPSILON = 0.0011
private const val IMPRESSION_MAXIMUM_FREQUENCY_PER_USER = 60

private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_VID_SAMPLING_START = 205.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_EPSILON = 0.001
private val MAXIMUM_WATCH_DURATION_PER_USER = Durations.fromSeconds(4000)

private const val DIFFERENTIAL_PRIVACY_DELTA = 1e-12

private const val SECURE_RANDOM_OUTPUT_INT = 0
private const val SECURE_RANDOM_OUTPUT_LONG = 0L

private val METRIC_SPEC_CONFIG = metricSpecConfig {
  reachParams =
    MetricSpecConfigKt.reachParams {
      privacyParams =
        MetricSpecConfigKt.differentialPrivacyParams {
          epsilon = REACH_ONLY_REACH_EPSILON
          delta = DIFFERENTIAL_PRIVACY_DELTA
        }
    }
  reachVidSamplingInterval =
    MetricSpecConfigKt.vidSamplingInterval {
      start = REACH_ONLY_VID_SAMPLING_START
      width = REACH_ONLY_VID_SAMPLING_WIDTH
    }

  reachAndFrequencyParams =
    MetricSpecConfigKt.reachAndFrequencyParams {
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
      maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
    }
  reachAndFrequencyVidSamplingInterval =
    MetricSpecConfigKt.vidSamplingInterval {
      start = REACH_FREQUENCY_VID_SAMPLING_START
      width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
    }

  impressionCountParams =
    MetricSpecConfigKt.impressionCountParams {
      privacyParams =
        MetricSpecConfigKt.differentialPrivacyParams {
          epsilon = IMPRESSION_EPSILON
          delta = DIFFERENTIAL_PRIVACY_DELTA
        }
      maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
    }
  impressionCountVidSamplingInterval =
    MetricSpecConfigKt.vidSamplingInterval {
      start = IMPRESSION_VID_SAMPLING_START
      width = IMPRESSION_VID_SAMPLING_WIDTH
    }

  watchDurationParams =
    MetricSpecConfigKt.watchDurationParams {
      privacyParams =
        MetricSpecConfigKt.differentialPrivacyParams {
          epsilon = WATCH_DURATION_EPSILON
          delta = DIFFERENTIAL_PRIVACY_DELTA
        }
      maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
    }
  watchDurationVidSamplingInterval =
    MetricSpecConfigKt.vidSamplingInterval {
      start = WATCH_DURATION_VID_SAMPLING_START
      width = WATCH_DURATION_VID_SAMPLING_WIDTH
    }
}

private val SECRETS_DIR =
  getRuntimePath(
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "k8s",
        "testing",
        "secretfiles",
      )
    )!!
    .toFile()

// Authentication key
private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"

// Aggregator certificate

private val AGGREGATOR_SIGNING_KEY: SigningKeyHandle by lazy {
  loadSigningKey(
    SECRETS_DIR.resolve("aggregator_cs_cert.der"),
    SECRETS_DIR.resolve("aggregator_cs_private.der")
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
        ExternalId(it + 120L).apiId.value
      )
    measurementConsumerKey to
      measurementConsumer {
        name = measurementConsumerKey.toName()
        certificate = certificateKey.toName()
        certificateDer = MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE.certificate.encoded.toByteString()
        publicKey =
          signEncryptionPublicKey(
            MEASUREMENT_CONSUMER_PUBLIC_KEY,
            MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
          )
      }
  }

private val CONFIG = measurementConsumerConfig {
  apiKey = API_AUTHENTICATION_KEY
  signingCertificateName = MEASUREMENT_CONSUMERS.values.first().certificate
  signingPrivateKeyPath = "mc_cs_private.der"
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
      }
    )
  )

private val DATA_PROVIDER_PUBLIC_KEY = encryptionPublicKey {
  format = EncryptionPublicKey.Format.TINK_KEYSET
  data = SECRETS_DIR.resolve("edp1_enc_public.tink").readByteString()
}
private val DATA_PROVIDER_PRIVATE_KEY_HANDLE =
  loadPrivateKey(SECRETS_DIR.resolve("edp1_enc_private.tink"))
private val DATA_PROVIDER_SIGNING_KEY =
  loadSigningKey(
    SECRETS_DIR.resolve("edp1_cs_cert.der"),
    SECRETS_DIR.resolve("edp1_cs_private.der")
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
private val DATA_PROVIDERS_LIST = DATA_PROVIDERS.values.toList()

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
              ExternalId(index + 670L).apiId.value
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
      }
    )
    .mapValues {
      requisitionSpec {
        events = RequisitionSpecKt.events { eventGroups += it.value }
        measurementPublicKey = MEASUREMENT_CONSUMERS.values.first().publicKey.message
        nonce = SECURE_RANDOM_OUTPUT_LONG
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
              dataProvider.publicKey.unpack()
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
}

private val INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT =
  INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    details =
      InternalMeasurementKt.details {
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
      InternalMeasurementKt.details {
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
}

private val INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT =
  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    details =
      InternalMeasurementKt.details {
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
}

private val INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    state = InternalMeasurement.State.FAILED
    details =
      InternalMeasurementKt.details {
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
      InternalMeasurementKt.details {
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

// Internal cross-publisher watch duration measurements
private val INTERNAL_REQUESTING_UNION_ALL_WATCH_DURATION_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  timeInterval = TIME_INTERVAL
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
    filters += listOf(METRIC_FILTER, PRIMITIVE_REPORTING_SET_FILTER)
  }
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
      InternalMeasurementKt.details {
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

// CMMS measurements

// CMMS incremental reach measurements
private val UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

  nonceHashes +=
    listOf(
      Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG),
      Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG),
      Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG)
    )

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
          nonceHashes += Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG)
        },
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
      )
    measurementReferenceId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
  }
private val REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDERS.keys.take(2).map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(
        UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC,
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
      )
    measurementReferenceId = INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_UNION_ALL_REACH_MEASUREMENT =
  REQUESTING_UNION_ALL_REACH_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
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
          INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
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
private val SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

  nonceHashes.add(Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG))

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
              ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
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
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
      )
    measurementReferenceId = INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT =
  REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.cmmsMeasurementId
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
private val SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

  nonceHashes.add(Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG))

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
              ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
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
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
      )
    measurementReferenceId = INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
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

// CMMS cross publisher watch duration measurements
private val UNION_ALL_WATCH_DURATION_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.pack()

  nonceHashes +=
    listOf(
      Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG),
      Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG),
      Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG)
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
              ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
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
          nonceHashes += Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG)
        },
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
      )
  }

private val PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT =
  REQUESTING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.cmmsMeasurementId
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
              MEASUREMENT_CONSUMER_PUBLIC_KEY
            )
          certificate = dataProvider.certificate
        }
      }
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
        privacyParams =
          InternalMetricSpecKt.differentialPrivacyParams {
            epsilon = REACH_ONLY_REACH_EPSILON
            delta = DIFFERENTIAL_PRIVACY_DELTA
          }
      }
    vidSamplingInterval =
      InternalMetricSpecKt.vidSamplingInterval {
        start = REACH_ONLY_VID_SAMPLING_START
        width = REACH_ONLY_VID_SAMPLING_WIDTH
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
  details = InternalMetricKt.details { filters += listOf(METRIC_FILTER) }
}

private val INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC =
  INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC.copy {
    externalMetricId = "331L"
    createTime = Instant.now().toProtoTime()
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
        maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
      }
    vidSamplingInterval =
      InternalMetricSpecKt.vidSamplingInterval {
        start = REACH_FREQUENCY_VID_SAMPLING_START
        width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
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
  details = InternalMetricKt.details { filters += listOf(METRIC_FILTER) }
}

private val INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC =
  INTERNAL_REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
    externalMetricId = "332L"
    createTime = Instant.now().toProtoTime()
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
        privacyParams =
          InternalMetricSpecKt.differentialPrivacyParams {
            epsilon = IMPRESSION_EPSILON
            delta = DIFFERENTIAL_PRIVACY_DELTA
          }
        maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
      }
    vidSamplingInterval =
      InternalMetricSpecKt.vidSamplingInterval {
        start = IMPRESSION_VID_SAMPLING_START
        width = IMPRESSION_VID_SAMPLING_WIDTH
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
  details = InternalMetricKt.details { filters += listOf(METRIC_FILTER) }
}

private val INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    externalMetricId = "333L"
    createTime = Instant.now().toProtoTime()
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
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
    }
  }

private val INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
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
        privacyParams =
          InternalMetricSpecKt.differentialPrivacyParams {
            epsilon = WATCH_DURATION_EPSILON
            delta = DIFFERENTIAL_PRIVACY_DELTA
          }
        maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
      }
    vidSamplingInterval =
      InternalMetricSpecKt.vidSamplingInterval {
        start = WATCH_DURATION_VID_SAMPLING_START
        width = WATCH_DURATION_VID_SAMPLING_WIDTH
      }
  }
  weightedMeasurements += weightedMeasurement {
    weight = 1
    binaryRepresentation = 1
    measurement = INTERNAL_REQUESTING_UNION_ALL_WATCH_DURATION_MEASUREMENT
  }
  details = InternalMetricKt.details { filters += listOf(METRIC_FILTER) }
}

private val INTERNAL_PENDING_INITIAL_CROSS_PUBLISHER_WATCH_DURATION_METRIC =
  INTERNAL_REQUESTING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
    externalMetricId = "334L"
    createTime = Instant.now().toProtoTime()
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
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      binaryRepresentation = 1
      measurement = INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT
    }
  }

// Public Metrics

// Incremental reach metrics
private val REQUESTING_INCREMENTAL_REACH_METRIC = metric {
  reportingSet = INTERNAL_INCREMENTAL_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = REACH_METRIC_SPEC
  filters += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.details.filtersList
}

private val PENDING_INCREMENTAL_REACH_METRIC =
  REQUESTING_INCREMENTAL_REACH_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId
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

private val FREQUENCY_VARIANCE: Map<Int, Double> =
  (1..REACH_FREQUENCY_MAXIMUM_FREQUENCY).associateWith { it.toDouble().pow(2.0) }
private val FREQUENCY_VARIANCES =
  FrequencyVariances(FREQUENCY_VARIANCE, FREQUENCY_VARIANCE, FREQUENCY_VARIANCE, FREQUENCY_VARIANCE)

private val SUCCEEDED_INCREMENTAL_REACH_METRIC =
  PENDING_INCREMENTAL_REACH_METRIC.copy {
    state = Metric.State.SUCCEEDED

    result = metricResult {
      reach =
        MetricResultKt.reachResult {
          value = INCREMENTAL_REACH_VALUE
          univariateStatistics = univariateStatistics { standardDeviation = sqrt(VARIANCE_VALUE) }
        }
    }
  }

// Single publisher reach-frequency metrics
private val REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC = metric {
  reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = REACH_FREQUENCY_METRIC_SPEC
  filters += INTERNAL_REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.details.filtersList
}

private val PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC =
  REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.externalMetricId
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
}

private val PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
        )
        .toName()
    metricSpec = metricSpec {
      impressionCount = impressionCountParams {
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
  PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy { state = Metric.State.FAILED }

private val SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    state = Metric.State.SUCCEEDED
    result = metricResult {
      impressionCount =
        MetricResultKt.impressionCountResult {
          value = IMPRESSION_VALUE
          univariateStatistics = univariateStatistics { standardDeviation = sqrt(VARIANCE_VALUE) }
        }
    }
  }

// Cross publisher watch duration metrics
private val REQUESTING_CROSS_PUBLISHER_WATCH_DURATION_METRIC = metric {
  reportingSet = INTERNAL_UNION_ALL_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = WATCH_DURATION_METRIC_SPEC
  filters += INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.details.filtersList
}

private val PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC =
  REQUESTING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.externalMetricId
        )
        .toName()
    metricSpec = metricSpec {
      watchDuration = watchDurationParams {
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
    }
  }

@RunWith(JUnit4::class)
class MetricsServiceTest {

  private val internalMetricsMock: MetricsCoroutineImplBase = mockService {
    onBlocking { createMetric(any()) }
      .thenReturn(
        INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC,
      )
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
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        )
      )
    onBlocking { batchGetMetrics(any()) }
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC
          metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )
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
                INTERNAL_SINGLE_PUBLISHER_REPORTING_SET
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
    onBlocking { batchSetCmmsMeasurementIds(any()) }
      .thenReturn(
        batchSetCmmsMeasurementIdsResponse {
          measurements += INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT
          measurements += INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
        }
      )
    onBlocking { batchSetMeasurementResults(any()) }
      .thenReturn(
        batchSetCmmsMeasurementResultsResponse {
          measurements += INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT
        }
      )
    onBlocking { batchSetMeasurementFailures(any()) }
      .thenReturn(
        batchSetCmmsMeasurementFailuresResponse {
          measurements += INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
        }
      )
  }

  private val measurementsMock: MeasurementsCoroutineImplBase = mockService {
    onBlocking { batchGetMeasurements(any()) }
      .thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            PENDING_UNION_ALL_REACH_MEASUREMENT.name to PENDING_UNION_ALL_REACH_MEASUREMENT,
            PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
            PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT,
            PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name to PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name ->
              measurementsMap.getValue(name)
            }
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
            INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsCreateMeasurementRequestId to
              PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
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

  private val secureRandomMock: SecureRandom = mock()

  private object VariancesMock : Variances {
    override fun computeMetricVariance(params: ReachMetricVarianceParams): Double = VARIANCE_VALUE

    override fun computeMeasurementVariance(
      methodology: Methodology,
      measurementVarianceParams: ReachMeasurementVarianceParams
    ): Double = VARIANCE_VALUE

    override fun computeMetricVariance(params: FrequencyMetricVarianceParams): FrequencyVariances =
      FREQUENCY_VARIANCES

    override fun computeMeasurementVariance(
      methodology: Methodology,
      measurementVarianceParams: FrequencyMeasurementVarianceParams
    ): FrequencyVariances = FrequencyVariances(mapOf(), mapOf(), mapOf(), mapOf())

    override fun computeMetricVariance(params: ImpressionMetricVarianceParams): Double =
      VARIANCE_VALUE

    override fun computeMeasurementVariance(
      methodology: Methodology,
      measurementVarianceParams: ImpressionMeasurementVarianceParams
    ): Double = VARIANCE_VALUE

    override fun computeMetricVariance(params: WatchDurationMetricVarianceParams): Double =
      VARIANCE_VALUE

    override fun computeMeasurementVariance(
      methodology: Methodology,
      measurementVarianceParams: WatchDurationMeasurementVarianceParams
    ): Double = VARIANCE_VALUE
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalMetricsMock)
    addService(internalReportingSetsMock)
    addService(internalMeasurementsMock)
    addService(measurementsMock)
    addService(measurementConsumersMock)
    addService(dataProvidersMock)
    addService(certificatesMock)
  }

  private lateinit var service: MetricsService

  @Before
  fun initService() {
    secureRandomMock.stub {
      on { nextInt(any()) } doReturn SECURE_RANDOM_OUTPUT_INT
      on { nextLong() } doReturn SECURE_RANDOM_OUTPUT_LONG
    }

    service =
      MetricsService(
        METRIC_SPEC_CONFIG,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        InternalMetricsGrpcKt.MetricsCoroutineStub(grpcTestServerRule.channel),
        VariancesMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel),
        MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        CertificatesGrpcKt.CertificatesCoroutineStub(grpcTestServerRule.channel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_KEY_PAIR_STORE,
        secureRandomMock,
        SECRETS_DIR,
        listOf(AGGREGATOR_ROOT_CERTIFICATE, DATA_PROVIDER_ROOT_CERTIFICATE).associateBy {
          it.subjectKeyIdentifier!!
        },
      )
  }

  @Test
  fun `createMetric creates CMMS measurements for incremental reach`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "metric-id"
    }

    val result =
      withMeasurementConsumerPrincipal(request.parent, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          externalMetricId = "metric-id"
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) { batchCreateMeasurements(measurementsCaptor.capture()) }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        MEASUREMENT_SPEC_FIELD,
        ENCRYPTED_REQUISITION_SPEC_FIELD,
      )
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

    capturedMeasurementRequests.first().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
            nonceHashes.clear()
            nonceHashes +=
              List(dataProvidersList.size) { Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG) }
          }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementId
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
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
  fun `createMetric creates CMMS measurements for single pub reach frequency metric`() =
    runBlocking {
      whenever(internalMetricsMock.createMetric(any()))
        .thenReturn(INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC)
      whenever(measurementsMock.batchCreateMeasurements(any()))
        .thenReturn(
          batchCreateMeasurementsResponse {
            measurements += PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
          }
        )

      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
        metricId = "metric-id"
      }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }

      val expected = PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC

      // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
      verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          internalCreateMetricRequest {
            metric = INTERNAL_REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
            externalMetricId = "metric-id"
          }
        )

      // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
      val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
      verifyBlocking(measurementsMock, times(1)) { batchCreateMeasurements(measurementsCaptor.capture()) }
      val capturedMeasurementRequests = measurementsCaptor.allValues
      assertThat(capturedMeasurementRequests)
        .ignoringRepeatedFieldOrder()
        .ignoringFieldDescriptors(
          MEASUREMENT_SPEC_FIELD,
          ENCRYPTED_REQUISITION_SPEC_FIELD,
        )
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

      capturedMeasurementRequests.first().requestsList.forEach { createMeasurementRequest ->
        verifyMeasurementSpec(
          createMeasurementRequest.measurement.measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER
        )

        val dataProvidersList =
          createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

        val measurementSpec: MeasurementSpec =
          createMeasurementRequest.measurement.measurementSpec.unpack()
        assertThat(measurementSpec).isEqualTo(SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT_SPEC)

        dataProvidersList.map { dataProviderEntry ->
          val signedRequisitionSpec =
            decryptRequisitionSpec(
              dataProviderEntry.value.encryptedRequisitionSpec,
              DATA_PROVIDER_PRIVATE_KEY_HANDLE
            )
          val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
          verifyRequisitionSpec(
            signedRequisitionSpec,
            requisitionSpec,
            measurementSpec,
            MEASUREMENT_CONSUMER_CERTIFICATE,
            TRUSTED_MEASUREMENT_CONSUMER_ISSUER
          )
        }
      }

      // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
      verifyProtoArgument(
          internalMeasurementsMock,
          InternalMeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
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
  fun `createMetric creates CMMS measurements for single pub impression metric`() = runBlocking {
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC)
    whenever(measurementsMock.batchCreateMeasurements(any()))
      .thenReturn(batchCreateMeasurementsResponse {
        measurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
      })

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
      metricId = "metric-id"
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
          externalMetricId = "metric-id"
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) { batchCreateMeasurements(measurementsCaptor.capture()) }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        MEASUREMENT_SPEC_FIELD,
        ENCRYPTED_REQUISITION_SPEC_FIELD,
      )
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
            requestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.first().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec).isEqualTo(SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC)

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
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
          privacyParams =
            InternalMetricSpecKt.differentialPrivacyParams {
              this.epsilon = epsilon
              this.delta = delta
            }
          this.maximumFrequencyPerUser = maximumFrequencyPerUser
        }
      vidSamplingInterval =
        InternalMetricSpecKt.vidSamplingInterval {
          start = vidSamplingIntervalStart
          width = vidSamplingIntervalWidth
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

    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(internalPendingInitialSinglePublisherImpressionMetric)
    whenever(measurementsMock.batchCreateMeasurements(any()))
      .thenReturn(batchCreateMeasurementsResponse {
        measurements += pendingSinglePublisherImpressionMeasurement
      })

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
      metricId = "metric-id"
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected =
      PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
        metricSpec = metricSpec {
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

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = internalRequestingSinglePublisherImpressionMetric
          externalMetricId = "metric-id"
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) { batchCreateMeasurements(measurementsCaptor.capture()) }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        MEASUREMENT_SPEC_FIELD,
        ENCRYPTED_REQUISITION_SPEC_FIELD,
      )
      .containsExactly(
        batchCreateMeasurementsRequest {
          parent = request.parent
          requests += createMeasurementRequest {
            parent = request.parent
            measurement = requestingSinglePublisherImpressionMeasurement
            requestId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.first().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
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
            DATA_PROVIDER_PRIVATE_KEY_HANDLE
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
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
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
      metricId = "metric-id"
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
          externalMetricId = "metric-id"
        }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) { batchCreateMeasurements(measurementsCaptor.capture()) }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        MEASUREMENT_SPEC_FIELD,
        ENCRYPTED_REQUISITION_SPEC_FIELD,
      )
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

    capturedMeasurementRequests.first().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
            nonceHashes.clear()
            nonceHashes +=
              List(dataProvidersList.size) { Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG) }
          }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
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
            }
          }
          details = InternalMetricKt.details {}
        }
      externalMetricId = "metric-id"
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
        details = InternalMetricKt.details {}
      }

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
      metricId = "metric-id"
    }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
      runBlocking { service.createMetric(request) }
    }

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(1)) { batchCreateMeasurements(measurementsCaptor.capture()) }
    val capturedMeasurementRequests = measurementsCaptor.allValues

    capturedMeasurementRequests.first().requestsList.forEach { createMeasurementRequest ->
      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val filters: List<String> =
        dataProvidersList.flatMap { dataProviderEntry ->
          val signedRequisitionSpec =
            decryptRequisitionSpec(
              dataProviderEntry.value.encryptedRequisitionSpec,
              DATA_PROVIDER_PRIVATE_KEY_HANDLE
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

    whenever(internalReportingSetsMock.batchGetReportingSets(any())).thenAnswer {
      val batchGetReportingSetsRequest = it.arguments[0] as BatchGetReportingSetsRequest
      batchGetReportingSetsResponse {
        reportingSets +=
          batchGetReportingSetsRequest.externalReportingSetIdsList.map { externalReportingSetId ->
            INTERNAL_INCREMENTAL_REPORTING_SET.copy {
              this.externalReportingSetId = externalReportingSetId
            }
          }
      }
    }

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "metric-id"
    }
    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
      runBlocking { service.createMetric(request) }
    }

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
                  )
                )
                .toName()
          }
      }

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
      metricId = "metric-id"
    }
    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
      runBlocking { service.createMetric(request) }
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
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(INTERNAL_PENDING_INCREMENTAL_REACH_METRIC)

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "metric-id"
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          externalMetricId = "metric-id"
        }
      )

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(3)) { getDataProvider(dataProvidersCaptor.capture()) }

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, never()) { batchCreateMeasurements(measurementsCaptor.capture()) }

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
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(INTERNAL_PENDING_INCREMENTAL_REACH_METRIC)

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
      metricId = "metric-id"
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
          externalMetricId = "metric-id"
        }
      )

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(3)) { getDataProvider(dataProvidersCaptor.capture()) }

    // Verify proto argument of MeasurementsCoroutineImplBase::batchCreateMeasurements
    val measurementsCaptor: KArgumentCaptor<BatchCreateMeasurementsRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, never()) { batchCreateMeasurements(measurementsCaptor.capture()) }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
    val batchSetCmmsMeasurementIdsCaptor: KArgumentCaptor<BatchSetCmmsMeasurementIdsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsCaptor.capture())
    }

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric with request ID when the metric exists and in terminate state`() = runBlocking {
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC)

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
      metricId = "metric-id"
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = SUCCEEDED_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::batchGetReportingSets
    val batchGetReportingSetsCaptor: KArgumentCaptor<BatchGetReportingSetsRequest> =
      argumentCaptor()
    verifyBlocking(internalReportingSetsMock, times(1)) {
      batchGetReportingSets(batchGetReportingSetsCaptor.capture())
    }

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateMetricRequest {
          metric = INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC
          requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
          externalMetricId = "metric-id"
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
    verifyBlocking(measurementsMock, never()) { batchCreateMeasurements(measurementsCaptor.capture()) }

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
      metricId = "metric-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createMetric(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createMetric throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "metric-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.last().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Cannot create a Metric for another MeasurementConsumer.")
  }

  @Test
  fun `createMetric throws PERMISSION_DENIED when metric doesn't belong to caller`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.last().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "metric-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Cannot create a Metric for another MeasurementConsumer.")
  }

  @Test
  fun `createMetric throws UNAUTHENTICATED when the caller is not MeasurementConsumer`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "metric-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_LIST[0].name) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("No ReportingPrincipal found")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when parent is unspecified`() {
    val request = createMetricRequest {
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Parent is either unspecified or invalid.")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when resource ID is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when resource ID starts with number`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "1s"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when resource ID is too long`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "s".repeat(100)
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when resource ID contains invalid char`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "contain_invalid_char"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when metric is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Metric is not specified.")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when time interval in Metric is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearTimeInterval() }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Time interval in metric is not specified.")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when TimeInterval startTime is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          clearTimeInterval()
          timeInterval = interval { endTime = timestamp { seconds = 5 } }
        }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when TimeInterval endTime is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          clearTimeInterval()
          timeInterval = interval { startTime = timestamp { seconds = 5 } }
        }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when TimeInterval endTime is before startTime`() {
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
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when metric spec in Metric is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearMetricSpec() }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Metric spec in metric is not specified.")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when privacy params is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          metricSpec = metricSpec.copy { reach = reach.copy { clearPrivacyParams() } }
        }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when vid sampling interval start is negative`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          metricSpec =
            metricSpec.copy { vidSamplingInterval = vidSamplingInterval.copy { start = -1.0f } }
        }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when vid sampling interval start is 1`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          metricSpec =
            metricSpec.copy { vidSamplingInterval = vidSamplingInterval.copy { start = 1.0f } }
        }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when vid sampling interval width is 0`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy {
          metricSpec =
            metricSpec.copy { vidSamplingInterval = vidSamplingInterval.copy { width = 0f } }
        }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when vid sampling interval end is larger than 1`() {
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
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when reporting set is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearReportingSet() }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
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
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("Invalid reporting set name ${metricWithInvalidReportingSet.reportingSet}.")
  }

  @Test
  fun `createMetric throws PERMISSION_DENIED when reporting set is not accessible to caller`() {
    val inaccessibleReportingSetName =
      ReportingSetKey(
          MEASUREMENT_CONSUMERS.keys.last().measurementConsumerId,
          externalIdToApiId(241L)
        )
        .toName()

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy { reportingSet = inaccessibleReportingSetName }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("No access to the reporting set [$inaccessibleReportingSetName].")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when Frequency Histogram metric is computed on non-union-only set expression`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
          reportingSet = INTERNAL_INCREMENTAL_REPORTING_SET.resourceName
        }
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws FAILED_PRECONDITION when EDP cert is revoked`() = runBlocking {
    val dataProvider = DATA_PROVIDERS.values.first()
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
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }

    assertThat(exception).hasMessageThat().ignoringCase().contains("revoked")
  }

  @Test
  fun `createMetric throws FAILED_PRECONDITION when EDP public key signature is invalid`() =
    runBlocking {
      val dataProvider = DATA_PROVIDERS.values.first()
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
        metricId = "metric-id"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
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
        metricId = "metric-id"
      }

      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    }

  @Test
  fun `createMetric throws exception when CMMS batchCreateMeasurements throws INVALID_ARGUMENT`() =
    runBlocking {
      whenever(measurementsMock.batchCreateMeasurements(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
        metricId = "metric-id"
      }

      val exception =
        assertFailsWith(Exception::class) {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
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
        metricId = "metric-id"
      }

      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    }

  @Test
  fun `createMetric throws exception when getMeasurementConsumer throws NOT_FOUND`() = runBlocking {
    whenever(measurementConsumersMock.getMeasurementConsumer(any()))
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
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
      metricId = "metric-id"
    }

    assertFails {
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }
    }
  }

  @Test
  fun `createMetric throws exception when getDataProvider throws exception`() = runBlocking {
    whenever(dataProvidersMock.getDataProvider(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      metricId = "metric-id"
    }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception).hasMessageThat().contains("dataProviders/")
  }

  @Test
  fun `batchCreateMetrics creates CMMS measurements`() = runBlocking {
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
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
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
    verifyBlocking(measurementsMock, times(1)) { batchCreateMeasurements(measurementsCaptor.capture()) }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        MEASUREMENT_SPEC_FIELD,
        ENCRYPTED_REQUISITION_SPEC_FIELD,
      )
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
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsCreateMeasurementRequestId
          }
        }
      )

    capturedMeasurementRequests.first().requestsList.forEach { createMeasurementRequest ->
      verifyMeasurementSpec(
        createMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )

      val dataProvidersList =
        createMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec: MeasurementSpec =
        createMeasurementRequest.measurement.measurementSpec.unpack()
      assertThat(measurementSpec)
        .isEqualTo(
          if (dataProvidersList.size == 1) SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC
          else
            UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
              nonceHashes.clear()
              nonceHashes +=
                List(dataProvidersList.size) { Hashing.hashSha256(SECURE_RANDOM_OUTPUT_LONG) }
            }
        )

      dataProvidersList.map { dataProviderEntry ->
        val signedRequisitionSpec =
          decryptRequisitionSpec(
            dataProviderEntry.value.encryptedRequisitionSpec,
            DATA_PROVIDER_PRIVATE_KEY_HANDLE
          )
        val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
        verifyRequisitionSpec(
          signedRequisitionSpec,
          requisitionSpec,
          measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER
        )
      }
    }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::batchSetCmmsMeasurementId
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsGrpcKt.MeasurementsCoroutineImplBase::batchSetCmmsMeasurementIds
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
  fun `batchCreateMetrics throws INVALID_ARGUMENT when number of requests exceeds limit`() =
    runBlocking {
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
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.batchCreateMetrics(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.status.description)
        .isEqualTo("At most $MAX_BATCH_SIZE requests can be supported in a batch.")
    }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when duplicate metric IDs`() = runBlocking {
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name

      requests +=
        List(MAX_BATCH_SIZE + 1) {
          createMetricRequest {
            parent = MEASUREMENT_CONSUMERS.values.first().name
            metric = REQUESTING_INCREMENTAL_REACH_METRIC
            metricId = "metric-id"
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.batchCreateMetrics(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMetrics returns without a next page token when there is no previous page token`() {
    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listMetrics(request) }
      }

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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }

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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }

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
    val invalidPageSize = MAX_PAGE_SIZE * 2

    val request = listMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = invalidPageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listMetrics(request) }
      }

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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }

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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }

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

    whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
      val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
      batchGetMeasurementsResponse {
        measurements +=
          batchGetMeasurementsRequest.namesList.map { name ->
            measurementsMap.getValue(name)
          }
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
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listMetrics(request) }
      }

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
    whenever(internalMetricsMock.streamMetrics(any()))
      .thenReturn(
        flowOf(
          INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC,
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC
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
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listMetrics(request) }
      }

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
          }
      )
    whenever(measurementsMock.batchGetMeasurements(any())).thenAnswer {
      val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
      batchGetMeasurementsResponse {
        measurements +=
          batchGetMeasurementsRequest.namesList.map { name ->
            measurementsMap.getValue(name)
          }
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
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listMetrics(request) }
      }

    val expected = listMetricsResponse {
      metrics += PENDING_INCREMENTAL_REACH_METRIC
      metrics += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy { state = Metric.State.FAILED }
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
    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.last().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Cannot list Metrics belonging to other MeasurementConsumers.")
  }

  @Test
  fun `listMetrics throws UNAUTHENTICATED when the caller is not MeasurementConsumer`() {
    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS.values.first().name) {
          runBlocking { service.listMetrics(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("No ReportingPrincipal found")
  }

  @Test
  fun `listMetrics throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = -1
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Page size cannot be less than 0.")
  }

  @Test
  fun `listMetrics throws INVALID_ARGUMENT when parent is unspecified`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(ListMetricsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMetrics throws INVALID_ARGUMENT when MC ID doesn't match one in page token`() {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listMetrics throws Exception when the internal streamMetrics throws Exception`(): Unit =
    runBlocking {
      whenever(internalMetricsMock.streamMetrics(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }
      }
    }

  @Test
  fun `listMetrics throws Exception when batchGetMeasurements throws Exception`(): Unit =
    runBlocking {
      whenever(measurementsMock.batchGetMeasurements(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }
      }
    }

  @Test
  fun `listMetrics throws Exception when internal batchSetMeasurementResults throws Exception`() {
    runBlocking {
      whenever(measurementsMock.batchGetMeasurements(any()))
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
              SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }
      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenThrow(StatusRuntimeException(Status.UNKNOWN))

      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }
      }
    }
  }

  @Test
  fun `listMetrics throws Exception when internal batchSetMeasurementFailures throws Exception`() {
    runBlocking {
      whenever(measurementsMock.batchGetMeasurements(any()))
        .thenReturn(
          batchGetMeasurementsResponse {
            measurements += PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
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

      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }
      }
    }
  }

  @Test
  fun `listMetrics throws Exception when internal batchGetMetrics throws Exception`(): Unit =
    runBlocking {
      whenever(measurementsMock.batchGetMeasurements(any()))
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
              SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
              PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenThrow(StatusRuntimeException(Status.UNKNOWN))

      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }
      }
    }

  @Test
  fun `listMetrics throws FAILED_PRECONDITION when the measurement public key is not valid`() =
    runBlocking {
      whenever(measurementsMock.batchGetMeasurements(any()))
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
              SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
                measurementSpec =
                  signMeasurementSpec(
                    UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
                      measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.copy { clearData() }.pack()
                    },
                    MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
                  )
              },
              SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }

      val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.listMetrics(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception)
        .hasMessageThat()
        .contains(SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name)
    }

  @Test
  fun `listMetrics throws Exception when the getCertificate throws Exception`() = runBlocking {
    whenever(measurementsMock.batchGetMeasurements(any()))
      .thenAnswer {
        val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
        val measurementsMap =
          mapOf(
            SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
            SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
            SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
          )
        batchGetMeasurementsResponse {
          measurements +=
            batchGetMeasurementsRequest.namesList.map { name ->
              measurementsMap.getValue(name)
            }
        }
      }
    whenever(certificatesMock.getCertificate(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = listMetricsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listMetrics(request) }
        }
      }

    assertThat(exception).hasMessageThat().contains(AGGREGATOR_CERTIFICATE.name)
  }

  @Test
  fun `getMetric returns the metric with SUCCEEDED when the metric is already succeeded`() =
    runBlocking {
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse { metrics += INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC }
        )

      val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
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
            }
          }
        )
    }

  @Test
  fun `getMetric returns reach metric without statistics when reach methodology is unspecified`() =
    runBlocking {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
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
            }
          }
        )
    }

  @Test
  fun `getMetric returns reach metric without statistics when variance in custom methodology is unavailable`() =
    runBlocking {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_INCREMENTAL_REACH_METRIC.copy {
            this.result = metricResult {
              reach = MetricResultKt.reachResult { value = INCREMENTAL_REACH_VALUE }
            }
          }
        )
    }

  @Test
  fun `getMetric throw StatusRuntimeException when variance type in custom methodology is unspecified`() =
    runBlocking {
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

      whenever(
          measurementsMock.batchGetMeasurements(any())
        )
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name to PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
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
                              customDirectMethodology = CustomDirectMethodology.getDefaultInstance()
                            }
                        }
                      encryptedResult =
                        encryptResult(
                          signResult(result, DATA_PROVIDER_SIGNING_KEY),
                          MEASUREMENT_CONSUMER_PUBLIC_KEY
                        )
                      certificate = dataProvider.certificate
                    }
                  }
              }
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.getMetric(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.UNKNOWN)
      assertThat(exception.message).contains("Variance in CustomDirectMethodology is not set")
    }

  @Test
  fun `getMetric throw StatusRuntimeException when unavailable variance has no reason specified`() =
    runBlocking {
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

      whenever(
        measurementsMock.batchGetMeasurements(any())
      )
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name to PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.copy {
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
                              customDirectMethodology = customDirectMethodology {
                                variance =
                                  CustomDirectMethodologyKt.variance {
                                    unavailable =
                                      CustomDirectMethodology.Variance.Unavailable.getDefaultInstance()
                                  }
                              }
                            }
                        }
                      encryptedResult =
                        encryptResult(
                          signResult(result, DATA_PROVIDER_SIGNING_KEY),
                          MEASUREMENT_CONSUMER_PUBLIC_KEY
                        )
                      certificate = dataProvider.certificate
                    }
                  }
              }
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.getMetric(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.UNKNOWN)
      assertThat(exception.message).contains("no reason specified")
    }

  @Test
  fun `getMetric throws StatusRuntimeException for reach when the succeeded metric contains measurement with two results`():
    Unit = runBlocking {
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

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `getMetric throws StatusRuntimeException when the succeeded metric contains no measurement`():
    Unit = runBlocking {
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
                    details = InternalMeasurement.Details.getDefaultInstance()
                  }
              }
            }
        }
      )

    val request = getMetricRequest { name = SUCCEEDED_INCREMENTAL_REACH_METRIC.name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `getMetric throws StatusRuntimeException for reach metric when custom direct methodology has frequency`():
    Unit = runBlocking {
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

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `getMetric calls batchSetMeasurementResults when request number is more than the limit`() =
    runBlocking {
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
                weightedMeasurement.measurement.cmmsMeasurementId
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
            batchGetMeasurementsRequest.namesList.map { name ->
              measurementsMap.getValue(name)
            }
        }
      }

      val request = getMetricRequest { name = PENDING_INCREMENTAL_REACH_METRIC.name }

      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.getMetric(request) }
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
                weightedMeasurement.measurement.cmmsMeasurementId
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
            batchGetMeasurementsRequest.namesList.map { name ->
              measurementsMap.getValue(name)
            }
        }
      }

      val request = getMetricRequest { name = PENDING_INCREMENTAL_REACH_METRIC.name }

      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.getMetric(request) }
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
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics += INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC
        }
      )

    val request = getMetricRequest { name = FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.getMetric(request) }
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

    assertThat(result).isEqualTo(FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC)
  }

  @Test
  fun `getMetric returns the metric with RUNNING when measurements are pending`() = runBlocking {
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse { metrics += INTERNAL_PENDING_INCREMENTAL_REACH_METRIC },
      )

    val request = getMetricRequest { name = PENDING_INCREMENTAL_REACH_METRIC.name }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.getMetric(request) }
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
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          },
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          },
        )

      whenever(
        measurementsMock.batchGetMeasurements(any())
      )
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.name to SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }

      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenReturn(
          batchSetCmmsMeasurementResultsResponse {
            measurements += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT
          }
        )

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
  fun `getMetric returns reach frequency metric with statistics not set when reach lacks info for variance`() =
    runBlocking {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
                                        0.0
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
                                        0.0
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
  fun `getMetric returns reach frequency metric without statistics when frequency methodology is unspecified`() =
    runBlocking {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
                                        0.0
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
                                        0.0
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
  fun `getMetric throws StatusRuntimeException when the succeeded metric contains measurement with two reach frequency results`():
    Unit = runBlocking {
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics +=
            INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
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

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `getMetric throws StatusRuntimeException for reach frueqency metric when custom direct methodology has scalar`():
    Unit = runBlocking {
    whenever(internalMetricsMock.batchGetMetrics(any()))
      .thenReturn(
        internalBatchGetMetricsResponse {
          metrics +=
            INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.copy {
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
                                    InternalCustomDirectMethodologyKt.variance { scalar = 10.0 }
                                }
                              }
                          }
                      }
                  }
              }
            }
        }
      )

    val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `getMetric returns duration metric with SUCCEEDED when measurements are updated to SUCCEEDED`() =
    runBlocking {
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

      whenever(
          measurementsMock.batchGetMeasurements(any())
        )
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              PENDING_UNION_ALL_WATCH_DURATION_MEASUREMENT.name to SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }
      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenReturn(
          batchSetCmmsMeasurementResultsResponse {
            measurements += INTERNAL_SUCCEEDED_UNION_ALL_WATCH_DURATION_MEASUREMENT
          }
        )

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
  fun `getMetric returns impression metric with SUCCEEDED when measurements are updated to SUCCEEDED`() =
    runBlocking {
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

      whenever(
          measurementsMock.batchGetMeasurements(any())
        )
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }

      whenever(internalMeasurementsMock.batchSetMeasurementResults(any()))
        .thenReturn(
          batchSetCmmsMeasurementResultsResponse {
            measurements += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
          }
        )

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
  fun `getMetric returns the metric with FAILED when measurements are updated to FAILED`() =
    runBlocking {
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

      whenever(
          measurementsMock.batchGetMeasurements(any())
        )
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.name to failedSinglePublisherImpressionMeasurement
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }

      whenever(internalMeasurementsMock.batchSetMeasurementFailures(any()))
        .thenReturn(
          batchSetCmmsMeasurementFailuresResponse {
            measurements += INTERNAL_FAILED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
          }
        )

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
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

      assertThat(result).isEqualTo(FAILED_SINGLE_PUBLISHER_IMPRESSION_METRIC)
    }

  @Test
  fun `getMetric returns reach frequency metric with SUCCEEDED when measurements are already SUCCEEDED`() =
    runBlocking {
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          },
        )

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
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
              INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.cmmsMeasurementConsumerId
            externalMetricIds +=
              INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.externalMetricId
          },
        )

      // Verify proto argument of MeasurementsCoroutineImplBase::batchGetMeasurements
      val batchGetMeasurementsCaptor: KArgumentCaptor<BatchGetMeasurementsRequest> = argumentCaptor()
      verifyBlocking(measurementsMock, never()) { batchGetMeasurements(batchGetMeasurementsCaptor.capture()) }

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
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
                weightedMeasurements += weightedMeasurement {
                  weight = -1
                  binaryRepresentation = 1
                  measurement = INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
                }
              }
          },
        )

      val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
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
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.cmmsMeasurementConsumerId
            externalMetricIds +=
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId
          },
        )

      // Verify proto argument of MeasurementsCoroutineImplBase::batchGetMeasurements
      val batchGetMeasurementsCaptor: KArgumentCaptor<BatchGetMeasurementsRequest> = argumentCaptor()
      verifyBlocking(measurementsMock, never()) { batchGetMeasurements(batchGetMeasurementsCaptor.capture()) }

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
            }
          }
        )
    }

  @Test
  fun `getMetric returns impression metric without statistics when noise mechanism is unspecified`() =
    runBlocking {
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
          },
        )

      val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
  fun `getMetric returns impression metric without statistics when methodology is not set`() =
    runBlocking {
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
          },
        )

      val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
  fun `getMetric returns impression metric without statistics when variance in custom methodology is unavailable`() =
    runBlocking {
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
          },
        )

      val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
  fun `getMetric throws StatusRuntimeException for impression metric when custom direct methodology has frequency`():
    Unit = runBlocking {
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
        },
      )

    val request = getMetricRequest { name = SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_METRIC.name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `getMetric returns duration metric with SUCCEEDED when measurements are already SUCCEEDED`() =
    runBlocking {
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC
          },
        )

      val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
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
              INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.cmmsMeasurementConsumerId
            externalMetricIds +=
              INTERNAL_PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.externalMetricId
          },
        )

      // Verify proto argument of MeasurementsCoroutineImplBase::batchGetMeasurements
      val batchGetMeasurementsCaptor: KArgumentCaptor<BatchGetMeasurementsRequest> = argumentCaptor()
      verifyBlocking(measurementsMock, never()) { batchGetMeasurements(batchGetMeasurementsCaptor.capture()) }

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
      whenever(internalMetricsMock.batchGetMetrics(any()))
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics +=
              INTERNAL_SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

      assertThat(result)
        .isEqualTo(
          SUCCEEDED_CROSS_PUBLISHER_WATCH_DURATION_METRIC.copy {
            this.result = metricResult {
              watchDuration = MetricResultKt.watchDurationResult { value = 0.0 }
            }
          }
        )
    }

  @Test
  fun `getMetric returns duration metric without statistics when noise mechanism is unspecified`() =
    runBlocking {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
  fun `getMetric returns duration metric without statistics when methodology is not set`() =
    runBlocking {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
  fun `getMetric returns duration metric without statistics when variance in custom methodology is unavailable`() =
    runBlocking {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }

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
  fun `getMetric throws throws StatusRuntimeException for watch duration metric when custom direct methodology has frequency`():
    Unit = runBlocking {
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

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `getMetric throws INVALID_ARGUMENT when Report name is invalid`() {
    val request = getMetricRequest { name = "invalid_metric_name" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getMetric throws PERMISSION_DENIED when MeasurementConsumer's identity does not match`() {
    val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.last().name, CONFIG) {
          runBlocking { service.getMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getMetric throws UNAUTHENTICATED when the caller is not a MeasurementConsumer`() {
    val request = getMetricRequest { name = PENDING_CROSS_PUBLISHER_WATCH_DURATION_METRIC.name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS.values.first().name) {
          runBlocking { service.getMetric(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getMetric throws FAILED_PRECONDITION when the measurement public key is not valid`() =
    runBlocking {
      whenever(measurementsMock.batchGetMeasurements(any()))
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
              SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name to SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
                measurementSpec =
                  signMeasurementSpec(
                    UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
                      measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.copy { clearData() }.pack()
                    },
                    MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
                  )
              }
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }

      val request = getMetricRequest { name = PENDING_INCREMENTAL_REACH_METRIC.name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.getMetric(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception)
        .hasMessageThat()
        .contains(SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.name)
    }

  @Test
  fun `getMetric throws UNKNOWN when variance in CustomMethodology in a measurement is not set`() =
    runBlocking {
      whenever(
          internalMetricsMock.batchGetMetrics(
            eq(
              internalBatchGetMetricsRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.cmmsMeasurementConsumerId
                externalMetricIds +=
                  INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.externalMetricId
              }
            )
          )
        )
        .thenReturn(
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          },
          internalBatchGetMetricsResponse {
            metrics += INTERNAL_SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC
          },
        )

      whenever(
          measurementsMock.batchGetMeasurements(any())
        )
        .thenAnswer {
          val batchGetMeasurementsRequest = it.arguments[0] as BatchGetMeasurementsRequest
          val measurementsMap =
            mapOf(
              PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.name to SUCCEEDED_SINGLE_PUBLISHER_REACH_FREQUENCY_MEASUREMENT.copy {
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
                      MEASUREMENT_CONSUMER_PUBLIC_KEY
                    )
                  certificate = AGGREGATOR_CERTIFICATE.name
                }
              }
            )
          batchGetMeasurementsResponse {
            measurements +=
              batchGetMeasurementsRequest.namesList.map { name ->
                measurementsMap.getValue(name)
              }
          }
        }

      val request = getMetricRequest { name = PENDING_SINGLE_PUBLISHER_REACH_FREQUENCY_METRIC.name }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.getMetric(request) }
          }
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.UNKNOWN)
    }

  @Test
  fun `batchGetMetrics returns metrics with SUCCEEDED when the metric is already succeeded`() =
    runBlocking {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
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
    val request = batchGetMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      names += PENDING_INCREMENTAL_REACH_METRIC.name
      names += PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.name
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
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
  fun `batchGetMetrics throws INVALID_ARGUMENT when number of requests exceeds limit`() =
    runBlocking {
      val request = batchGetMetricsRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        names += List(MAX_BATCH_SIZE + 1) { "metric_name" }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.batchGetMetrics(request) }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.status.description)
        .isEqualTo("At most $MAX_BATCH_SIZE metrics can be supported in a batch.")
    }

  companion object {
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
