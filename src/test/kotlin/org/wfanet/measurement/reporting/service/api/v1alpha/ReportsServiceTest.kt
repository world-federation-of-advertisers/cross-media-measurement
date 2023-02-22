// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.duration
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.timestamp
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Paths
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.Instant
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
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2.alpha.ListReportsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.listReportsPageToken
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry.Value.ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultPair
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval as measurementTimeInterval
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.internal.reporting.CreateReportRequestKt as InternalCreateReportRequestKt
import org.wfanet.measurement.internal.reporting.GetReportRequest as GetInternalReportRequest
import org.wfanet.measurement.internal.reporting.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt as InternalMeasurementResultKt
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineImplBase as InternalMeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.MetricKt.MeasurementCalculationKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.MetricKt.SetOperationKt.reportingSetKey
import org.wfanet.measurement.internal.reporting.MetricKt.measurementCalculation
import org.wfanet.measurement.internal.reporting.Report as InternalReport
import org.wfanet.measurement.internal.reporting.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.ReportKt.DetailsKt as InternalReportDetailsKt
import org.wfanet.measurement.internal.reporting.ReportKt.DetailsKt.ResultKt as InternalReportResultKt
import org.wfanet.measurement.internal.reporting.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetKt as InternalReportingSetKt
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase as InternalReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.StreamReportsRequestKt.filter
import org.wfanet.measurement.internal.reporting.batchGetReportingSetRequest
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.getReportByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.getReportRequest as getInternalReportRequest
import org.wfanet.measurement.internal.reporting.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.metric as internalMetric
import org.wfanet.measurement.internal.reporting.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.report as internalReport
import org.wfanet.measurement.internal.reporting.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.setMeasurementFailureRequest
import org.wfanet.measurement.internal.reporting.setMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.streamReportsRequest
import org.wfanet.measurement.internal.reporting.timeInterval as internalTimeInterval
import org.wfanet.measurement.internal.reporting.timeIntervals as internalTimeIntervals
import org.wfanet.measurement.reporting.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt
import org.wfanet.measurement.reporting.v1alpha.MetricKt.frequencyHistogramParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.impressionCountParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.namedSetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.reachParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.setOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.watchDurationParams
import org.wfanet.measurement.reporting.v1alpha.Report
import org.wfanet.measurement.reporting.v1alpha.ReportKt
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.HistogramTableKt.row
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.column
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.histogramTable
import org.wfanet.measurement.reporting.v1alpha.ReportKt.ResultKt.scalarTable
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.copy
import org.wfanet.measurement.reporting.v1alpha.createReportRequest
import org.wfanet.measurement.reporting.v1alpha.getReportRequest
import org.wfanet.measurement.reporting.v1alpha.listReportsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.v1alpha.metric
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report
import org.wfanet.measurement.reporting.v1alpha.timeInterval
import org.wfanet.measurement.reporting.v1alpha.timeIntervals

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000
private const val PAGE_SIZE = 3

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

private const val SECURE_RANDOM_OUTPUT_INT = 0
private const val SECURE_RANDOM_OUTPUT_LONG = 0L

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

private val INVALID_MEASUREMENT_PUBLIC_KEY_DATA = "Invalid public key".toByteStringUtf8()

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
          EncryptionPublicKey.parseFrom(it.publicKey.data).data to
            MEASUREMENT_CONSUMER_PRIVATE_KEY_HANDLE
        )
      }
    )
  )

// Report IDs and names
private val REPORT_EXTERNAL_IDS = listOf(331L, 332L, 333L, 334L)
private val REPORT_NAMES =
  REPORT_EXTERNAL_IDS.map {
    ReportKey(MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId, externalIdToApiId(it))
      .toName()
  }

// Typo causes invalid name
private const val INVALID_REPORT_NAME = "measurementConsumer/AAAAAAAAAG8/report/AAAAAAAAAU0"

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

private val COVERED_EVENT_GROUP_KEYS =
  DATA_PROVIDERS.keys.mapIndexed { index, dataProviderKey ->
    val measurementConsumerKey = MEASUREMENT_CONSUMERS.keys.first()
    EventGroupKey(
      measurementConsumerKey.measurementConsumerId,
      dataProviderKey.dataProviderId,
      ExternalId(index + 660L).apiId.value
    )
  }
private val UNCOVERED_EVENT_GROUP_KEY =
  EventGroupKey(
    MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
    DATA_PROVIDERS.keys.last().dataProviderId,
    ExternalId(664L).apiId.value
  )
private val UNCOVERED_EVENT_GROUP_NAME = UNCOVERED_EVENT_GROUP_KEY.toName()
private val UNCOVERED_INTERNAL_EVENT_GROUP_KEY = UNCOVERED_EVENT_GROUP_KEY.toInternal()

// Reporting sets
private const val REPORTING_SET_FILTER = "AGE>18"

private val INTERNAL_REPORTING_SETS =
  COVERED_EVENT_GROUP_KEYS.mapIndexed { index, eventGroupKey ->
    internalReportingSet {
      measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
      externalReportingSetId = index + 220L
      eventGroupKeys += eventGroupKey.toInternal()
      filter = REPORTING_SET_FILTER
      displayName = "$measurementConsumerReferenceId-$externalReportingSetId-$filter"
    }
  }
private val UNCOVERED_INTERNAL_REPORTING_SET = internalReportingSet {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_REPORTING_SETS.last().externalReportingSetId + 1
  eventGroupKeys += UNCOVERED_INTERNAL_EVENT_GROUP_KEY
  filter = REPORTING_SET_FILTER
  displayName = "$measurementConsumerReferenceId-$externalReportingSetId-$filter"
}

// Reporting set IDs and names

private const val REPORTING_SET_EXTERNAL_ID_FOR_MC_2 = 241L

private const val INVALID_REPORTING_SET_NAME = "INVALID_REPORTING_SET_NAME"
private val REPORTING_SET_NAME_FOR_MC_2 =
  ReportingSetKey(
      MEASUREMENT_CONSUMERS.keys.last().measurementConsumerId,
      externalIdToApiId(REPORTING_SET_EXTERNAL_ID_FOR_MC_2)
    )
    .toName()

// Time intervals
private val START_INSTANT = Instant.now()
private val END_INSTANT = START_INSTANT.plus(Duration.ofDays(1))

private val START_TIME = START_INSTANT.toProtoTime()
private val TIME_INTERVAL_INCREMENT = Duration.ofDays(1).toProtoDuration()
private const val INTERVAL_COUNT = 1
private val END_TIME = END_INSTANT.toProtoTime()
private val MEASUREMENT_TIME_INTERVAL = measurementTimeInterval {
  startTime = START_TIME
  endTime = END_TIME
}
private val INTERNAL_TIME_INTERVAL = internalTimeInterval {
  startTime = START_TIME
  endTime = END_TIME
}
private val INTERNAL_PERIODIC_TIME_INTERVAL = internalPeriodicTimeInterval {
  startTime = START_TIME
  increment = TIME_INTERVAL_INCREMENT
  intervalCount = INTERVAL_COUNT
}
private val PERIODIC_TIME_INTERVAL = periodicTimeInterval {
  startTime = START_TIME
  increment = TIME_INTERVAL_INCREMENT
  intervalCount = INTERVAL_COUNT
}

// Report idempotency keys
private const val REACH_REPORT_IDEMPOTENCY_KEY = "TEST_REACH_REPORT"
private const val IMPRESSION_REPORT_IDEMPOTENCY_KEY = "TEST_IMPRESSION_REPORT"
private const val WATCH_DURATION_REPORT_IDEMPOTENCY_KEY = "TEST_WATCH_DURATION_REPORT"
private const val FREQUENCY_HISTOGRAM_REPORT_IDEMPOTENCY_KEY = "TEST_FREQUENCY_HISTOGRAM_REPORT"

// Set operation unique names
private const val REACH_SET_OPERATION_UNIQUE_NAME = "Reach Set Operation"
private const val FREQUENCY_HISTOGRAM_SET_OPERATION_UNIQUE_NAME =
  "Frequency Histogram Set Operation"
private const val IMPRESSION_SET_OPERATION_UNIQUE_NAME = "Impression Set Operation"
private const val WATCH_DURATION_SET_OPERATION_UNIQUE_NAME = "Watch Duration Set Operation"

// Measurement IDs and names
private val REACH_MEASUREMENT_REFERENCE_ID =
  "$REACH_REPORT_IDEMPOTENCY_KEY-Reach-$REACH_SET_OPERATION_UNIQUE_NAME-$START_INSTANT-" +
    "$END_INSTANT-measurement-0"
private val REACH_MEASUREMENT_REFERENCE_ID_2 =
  "$REACH_REPORT_IDEMPOTENCY_KEY-Reach-$REACH_SET_OPERATION_UNIQUE_NAME-$START_INSTANT-" +
    "$END_INSTANT-measurement-1"
private val FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID =
  "$FREQUENCY_HISTOGRAM_REPORT_IDEMPOTENCY_KEY-FrequencyHistogram-" +
    "$FREQUENCY_HISTOGRAM_SET_OPERATION_UNIQUE_NAME-$START_INSTANT-$END_INSTANT-measurement-0"
private val IMPRESSION_MEASUREMENT_REFERENCE_ID =
  "$IMPRESSION_REPORT_IDEMPOTENCY_KEY-ImpressionCount-$IMPRESSION_SET_OPERATION_UNIQUE_NAME" +
    "-$START_INSTANT-$END_INSTANT-measurement-0"
private val WATCH_DURATION_MEASUREMENT_REFERENCE_ID =
  "$WATCH_DURATION_REPORT_IDEMPOTENCY_KEY-WatchDuration-$WATCH_DURATION_SET_OPERATION_UNIQUE_NAME" +
    "-$START_INSTANT-$END_INSTANT-measurement-0"

private val REACH_MEASUREMENT_NAME =
  MeasurementKey(
      MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
      REACH_MEASUREMENT_REFERENCE_ID
    )
    .toName()
private val REACH_MEASUREMENT_NAME_2 =
  MeasurementKey(
      MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
      REACH_MEASUREMENT_REFERENCE_ID_2
    )
    .toName()
private val FREQUENCY_HISTOGRAM_MEASUREMENT_NAME =
  MeasurementKey(
      MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
      FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID
    )
    .toName()
private val IMPRESSION_MEASUREMENT_NAME =
  MeasurementKey(
      MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
      IMPRESSION_MEASUREMENT_REFERENCE_ID
    )
    .toName()
private val WATCH_DURATION_MEASUREMENT_NAME =
  MeasurementKey(
      MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
      WATCH_DURATION_MEASUREMENT_REFERENCE_ID
    )
    .toName()

// Set operations
private val INTERNAL_SET_OPERATION =
  InternalMetricKt.setOperation {
    type = InternalMetric.SetOperation.Type.UNION
    lhs =
      InternalMetricKt.SetOperationKt.operand {
        reportingSetId = reportingSetKey {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetId = INTERNAL_REPORTING_SETS[0].externalReportingSetId
        }
      }
    rhs =
      InternalMetricKt.SetOperationKt.operand {
        reportingSetId = reportingSetKey {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetId = INTERNAL_REPORTING_SETS[1].externalReportingSetId
        }
      }
  }

private val SET_OPERATION = setOperation {
  type = SetOperation.Type.UNION
  lhs = SetOperationKt.operand { reportingSet = INTERNAL_REPORTING_SETS[0].resourceName }
  rhs = SetOperationKt.operand { reportingSet = INTERNAL_REPORTING_SETS[1].resourceName }
}
private val DATA_PROVIDER_KEYS_IN_SET_OPERATION = DATA_PROVIDERS.keys.take(2)

private val SET_OPERATION_WITH_INVALID_REPORTING_SET = setOperation {
  type = SetOperation.Type.UNION
  lhs = SetOperationKt.operand { reportingSet = INVALID_REPORTING_SET_NAME }
  rhs = SetOperationKt.operand { reportingSet = INTERNAL_REPORTING_SETS[1].resourceName }
}

private val SET_OPERATION_WITH_INACCESSIBLE_REPORTING_SET = setOperation {
  type = SetOperation.Type.UNION
  lhs = SetOperationKt.operand { reportingSet = REPORTING_SET_NAME_FOR_MC_2 }
  rhs = SetOperationKt.operand { reportingSet = INTERNAL_REPORTING_SETS[1].resourceName }
}

// Event group filters
private const val EVENT_GROUP_FILTER = "AGE>20"
private val EVENT_GROUP_FILTERS_MAP =
  COVERED_EVENT_GROUP_KEYS.associateBy(EventGroupKey::toName) { EVENT_GROUP_FILTER }

// Event group entries
private val EVENT_GROUP_ENTRIES =
  COVERED_EVENT_GROUP_KEYS.groupBy(
    { DataProviderKey(it.dataProviderReferenceId) },
    {
      eventGroupEntry {
        key = it.toName()
        value =
          EventGroupEntryKt.value {
            collectionInterval = MEASUREMENT_TIME_INTERVAL
            filter = eventFilter {
              expression = "($REPORTING_SET_FILTER) AND ($EVENT_GROUP_FILTER)"
            }
          }
      }
    }
  )

// Requisition specs
private val REQUISITION_SPECS: Map<DataProviderKey, RequisitionSpec> =
  EVENT_GROUP_ENTRIES.mapValues {
    requisitionSpec {
      eventGroups += it.value
      measurementPublicKey = MEASUREMENT_CONSUMERS.values.first().publicKey.data
      nonce = SECURE_RANDOM_OUTPUT_LONG
    }
  }

// Data provider entries
private val DATA_PROVIDER_ENTRIES =
  REQUISITION_SPECS.mapValues { (dataProviderKey, requisitionSpec) ->
    val dataProvider = DATA_PROVIDERS.getValue(dataProviderKey)
    dataProviderEntry {
      key = dataProvider.name
      value =
        DataProviderEntryKt.value {
          dataProviderCertificate = dataProvider.certificate
          dataProviderPublicKey = dataProvider.publicKey
          encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signRequisitionSpec(requisitionSpec, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE),
              EncryptionPublicKey.parseFrom(dataProvider.publicKey.data)
            )
          nonceHash = hashSha256(requisitionSpec.nonce)
        }
    }
  }

// Measurements
private val BASE_MEASUREMENT = measurement {
  measurementConsumerCertificate = MEASUREMENT_CONSUMERS.values.first().certificate
}

// Measurement values
private const val REACH_VALUE = 100_000L
private val FREQUENCY_DISTRIBUTION = mapOf(1L to 1.0 / 6, 2L to 2.0 / 6, 3L to 3.0 / 6)
private val IMPRESSION_VALUES = listOf(100L, 150L)
private val TOTAL_IMPRESSION_VALUE = IMPRESSION_VALUES.sum()
private val WATCH_DURATION_SECOND_LIST = listOf(100L, 200L)
private val WATCH_DURATION_LIST = WATCH_DURATION_SECOND_LIST.map { duration { seconds = it } }
private val TOTAL_WATCH_DURATION = duration { seconds = WATCH_DURATION_SECOND_LIST.sum() }

// Reach measurement
private val BASE_REACH_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    name = REACH_MEASUREMENT_NAME
    measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
  }
private val BASE_REACH_MEASUREMENT_2 =
  BASE_MEASUREMENT.copy {
    name = REACH_MEASUREMENT_NAME_2
    measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID_2
  }

private val PENDING_REACH_MEASUREMENT =
  BASE_REACH_MEASUREMENT.copy { state = Measurement.State.COMPUTING }

private val REACH_ONLY_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.toByteString()

  nonceHashes.addAll(
    listOf(hashSha256(SECURE_RANDOM_OUTPUT_LONG), hashSha256(SECURE_RANDOM_OUTPUT_LONG))
  )

  reachAndFrequency =
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
  vidSamplingInterval = vidSamplingInterval {
    start = REACH_ONLY_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
    width = REACH_ONLY_VID_SAMPLING_WIDTH
  }
}

private val SUCCEEDED_REACH_MEASUREMENT =
  BASE_REACH_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(REACH_ONLY_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)

    state = Measurement.State.SUCCEEDED

    results += resultPair {
      val result =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = REACH_VALUE }
          frequency =
            MeasurementKt.ResultKt.frequency {
              relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
            }
        }
      encryptedResult =
        encryptResult(signResult(result, AGGREGATOR_SIGNING_KEY), MEASUREMENT_CONSUMER_PUBLIC_KEY)
      certificate = AGGREGATOR_CERTIFICATE.name
    }
  }

private val INTERNAL_PENDING_REACH_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.PENDING
}
private val INTERNAL_SUCCEEDED_REACH_MEASUREMENT =
  INTERNAL_PENDING_REACH_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    result =
      InternalMeasurementKt.result {
        reach = InternalMeasurementResultKt.reach { value = REACH_VALUE }
        frequency =
          InternalMeasurementResultKt.frequency {
            relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
          }
      }
  }

// Frequency histogram measurement
private val BASE_REACH_FREQUENCY_HISTOGRAM_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    name = FREQUENCY_HISTOGRAM_MEASUREMENT_NAME
    measurementReferenceId = FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID
  }

private val REACH_FREQUENCY_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.toByteString()

  nonceHashes.addAll(
    listOf(hashSha256(SECURE_RANDOM_OUTPUT_LONG), hashSha256(SECURE_RANDOM_OUTPUT_LONG))
  )

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
      maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
    }
  vidSamplingInterval = vidSamplingInterval {
    start = REACH_FREQUENCY_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
    width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
  }
}

private val SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT =
  BASE_REACH_FREQUENCY_HISTOGRAM_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(REACH_FREQUENCY_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)

    state = Measurement.State.SUCCEEDED
    results += resultPair {
      val result =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = REACH_VALUE }
          frequency =
            MeasurementKt.ResultKt.frequency {
              relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
            }
        }
      encryptedResult =
        encryptResult(signResult(result, AGGREGATOR_SIGNING_KEY), MEASUREMENT_CONSUMER_PUBLIC_KEY)
      certificate = AGGREGATOR_CERTIFICATE.name
    }
  }

private val INTERNAL_PENDING_FREQUENCY_HISTOGRAM_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  measurementReferenceId = FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.PENDING
}

private val INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT =
  INTERNAL_PENDING_FREQUENCY_HISTOGRAM_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    result =
      InternalMeasurementKt.result {
        reach = InternalMeasurementResultKt.reach { value = REACH_VALUE }
        frequency =
          InternalMeasurementResultKt.frequency {
            relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
          }
      }
  }

// Impression measurement
private val BASE_IMPRESSION_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    name = IMPRESSION_MEASUREMENT_NAME
    measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
  }

private val IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.toByteString()

  nonceHashes.addAll(
    listOf(hashSha256(SECURE_RANDOM_OUTPUT_LONG), hashSha256(SECURE_RANDOM_OUTPUT_LONG))
  )

  impression =
    MeasurementSpecKt.impression {
      privacyParams = differentialPrivacyParams {
        epsilon = IMPRESSION_EPSILON
        delta = DIFFERENTIAL_PRIVACY_DELTA
      }
      maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
    }
  vidSamplingInterval = vidSamplingInterval {
    start = IMPRESSION_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
    width = IMPRESSION_VID_SAMPLING_WIDTH
  }
}

private val SUCCEEDED_IMPRESSION_MEASUREMENT =
  BASE_IMPRESSION_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(IMPRESSION_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)

    state = Measurement.State.SUCCEEDED

    results +=
      DATA_PROVIDER_KEYS_IN_SET_OPERATION.zip(IMPRESSION_VALUES).map {
        (dataProviderKey, numImpressions) ->
        val dataProvider = DATA_PROVIDERS.getValue(dataProviderKey)
        resultPair {
          val result =
            MeasurementKt.result {
              impression = MeasurementKt.ResultKt.impression { value = numImpressions }
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

private val INTERNAL_PENDING_IMPRESSION_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.PENDING
}

private val INTERNAL_SUCCEEDED_IMPRESSION_MEASUREMENT =
  INTERNAL_PENDING_IMPRESSION_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    result =
      InternalMeasurementKt.result {
        impression = InternalMeasurementResultKt.impression { value = TOTAL_IMPRESSION_VALUE }
      }
  }

// Watch Duration measurement
private val BASE_WATCH_DURATION_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    name = WATCH_DURATION_MEASUREMENT_NAME
    measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
  }

private val PENDING_WATCH_DURATION_MEASUREMENT =
  BASE_WATCH_DURATION_MEASUREMENT.copy { state = Measurement.State.COMPUTING }

private val WATCH_DURATION_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.toByteString()

  nonceHashes.addAll(
    listOf(hashSha256(SECURE_RANDOM_OUTPUT_LONG), hashSha256(SECURE_RANDOM_OUTPUT_LONG))
  )

  duration =
    MeasurementSpecKt.duration {
      privacyParams = differentialPrivacyParams {
        epsilon = WATCH_DURATION_EPSILON
        delta = DIFFERENTIAL_PRIVACY_DELTA
      }
      maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
      maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
    }
  vidSamplingInterval = vidSamplingInterval {
    start = WATCH_DURATION_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
    width = WATCH_DURATION_VID_SAMPLING_WIDTH
  }
}

private val SUCCEEDED_WATCH_DURATION_MEASUREMENT =
  BASE_WATCH_DURATION_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(WATCH_DURATION_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)

    state = Measurement.State.SUCCEEDED

    results +=
      DATA_PROVIDER_KEYS_IN_SET_OPERATION.zip(WATCH_DURATION_LIST).map {
        (dataProviderKey, watchDuration) ->
        val dataProvider = DATA_PROVIDERS.getValue(dataProviderKey)
        resultPair {
          val result =
            MeasurementKt.result {
              this.watchDuration = MeasurementKt.ResultKt.watchDuration { value = watchDuration }
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

private val INTERNAL_PENDING_WATCH_DURATION_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.PENDING
}
private val INTERNAL_SUCCEEDED_WATCH_DURATION_MEASUREMENT =
  INTERNAL_PENDING_WATCH_DURATION_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    result =
      InternalMeasurementKt.result {
        watchDuration = InternalMeasurementResultKt.watchDuration { value = TOTAL_WATCH_DURATION }
      }
  }

// Weighted measurements
private val WEIGHTED_REACH_MEASUREMENT = weightedMeasurement {
  measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
  coefficient = 1
}

private val WEIGHTED_FREQUENCY_HISTOGRAM_MEASUREMENT = weightedMeasurement {
  measurementReferenceId = FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID
  coefficient = 1
}

private val WEIGHTED_IMPRESSION_MEASUREMENT = weightedMeasurement {
  measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
  coefficient = 1
}

private val WEIGHTED_WATCH_DURATION_MEASUREMENT = weightedMeasurement {
  measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
  coefficient = 1
}

// Measurement Calculations
private val REACH_MEASUREMENT_CALCULATION = measurementCalculation {
  timeInterval = INTERNAL_TIME_INTERVAL
  weightedMeasurements.add(WEIGHTED_REACH_MEASUREMENT)
}

private val FREQUENCY_HISTOGRAM_MEASUREMENT_CALCULATION = measurementCalculation {
  timeInterval = INTERNAL_TIME_INTERVAL
  weightedMeasurements.add(WEIGHTED_FREQUENCY_HISTOGRAM_MEASUREMENT)
}

private val IMPRESSION_MEASUREMENT_CALCULATION = measurementCalculation {
  timeInterval = INTERNAL_TIME_INTERVAL
  weightedMeasurements.add(WEIGHTED_IMPRESSION_MEASUREMENT)
}

private val WATCH_DURATION_MEASUREMENT_CALCULATION = measurementCalculation {
  timeInterval = INTERNAL_TIME_INTERVAL
  weightedMeasurements.add(WEIGHTED_WATCH_DURATION_MEASUREMENT)
}

// Named set operations
// Reach set operation
private val INTERNAL_NAMED_REACH_SET_OPERATION =
  InternalMetricKt.namedSetOperation {
    displayName = REACH_SET_OPERATION_UNIQUE_NAME
    setOperation = INTERNAL_SET_OPERATION
    measurementCalculations += REACH_MEASUREMENT_CALCULATION
  }
private val NAMED_REACH_SET_OPERATION = namedSetOperation {
  uniqueName = REACH_SET_OPERATION_UNIQUE_NAME
  setOperation = SET_OPERATION
}

// Frequency histogram set operation
private val INTERNAL_NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION =
  InternalMetricKt.namedSetOperation {
    displayName = FREQUENCY_HISTOGRAM_SET_OPERATION_UNIQUE_NAME
    setOperation = INTERNAL_SET_OPERATION
    measurementCalculations += FREQUENCY_HISTOGRAM_MEASUREMENT_CALCULATION
  }
private val NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION = namedSetOperation {
  uniqueName = FREQUENCY_HISTOGRAM_SET_OPERATION_UNIQUE_NAME
  setOperation = SET_OPERATION
}

// Impression set operation
private val INTERNAL_NAMED_IMPRESSION_SET_OPERATION =
  InternalMetricKt.namedSetOperation {
    displayName = IMPRESSION_SET_OPERATION_UNIQUE_NAME
    setOperation = INTERNAL_SET_OPERATION
    measurementCalculations += IMPRESSION_MEASUREMENT_CALCULATION
  }
private val NAMED_IMPRESSION_SET_OPERATION = namedSetOperation {
  uniqueName = IMPRESSION_SET_OPERATION_UNIQUE_NAME
  setOperation = SET_OPERATION
}

// Watch duration set operation
private val INTERNAL_NAMED_WATCH_DURATION_SET_OPERATION =
  InternalMetricKt.namedSetOperation {
    displayName = WATCH_DURATION_SET_OPERATION_UNIQUE_NAME
    setOperation = INTERNAL_SET_OPERATION
    measurementCalculations += WATCH_DURATION_MEASUREMENT_CALCULATION
  }
private val NAMED_WATCH_DURATION_SET_OPERATION = namedSetOperation {
  uniqueName = WATCH_DURATION_SET_OPERATION_UNIQUE_NAME
  setOperation = SET_OPERATION
}

// Internal metrics
private const val MAXIMUM_FREQUENCY_PER_USER = 10
private const val MAXIMUM_WATCH_DURATION_PER_USER = 300

// Reach metric
private val REACH_METRIC = metric {
  reach = reachParams {}
  cumulative = false
  setOperations.add(NAMED_REACH_SET_OPERATION)
}
private val INTERNAL_REACH_METRIC = internalMetric {
  details =
    InternalMetricKt.details {
      reach = InternalMetricKt.reachParams {}
      cumulative = false
    }
  namedSetOperations.add(INTERNAL_NAMED_REACH_SET_OPERATION)
}

// Frequency histogram metric
private val FREQUENCY_HISTOGRAM_METRIC = metric {
  frequencyHistogram = frequencyHistogramParams {
    maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
  }
  cumulative = false
  setOperations.add(NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION)
}
private val INTERNAL_FREQUENCY_HISTOGRAM_METRIC = internalMetric {
  details =
    InternalMetricKt.details {
      frequencyHistogram =
        InternalMetricKt.frequencyHistogramParams {
          maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
        }
      cumulative = false
    }
  namedSetOperations.add(INTERNAL_NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION)
}

// Impression metric
private val IMPRESSION_METRIC = metric {
  impressionCount = impressionCountParams { maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER }
  cumulative = false
  setOperations.add(NAMED_IMPRESSION_SET_OPERATION)
}
private val INTERNAL_IMPRESSION_METRIC = internalMetric {
  details =
    InternalMetricKt.details {
      impressionCount =
        InternalMetricKt.impressionCountParams {
          maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
        }
      cumulative = false
    }
  namedSetOperations.add(INTERNAL_NAMED_IMPRESSION_SET_OPERATION)
}

// Watch duration metric
private val WATCH_DURATION_METRIC = metric {
  watchDuration = watchDurationParams {
    maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
    maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
  }
  cumulative = false
  setOperations.add(NAMED_WATCH_DURATION_SET_OPERATION)
}
private val INTERNAL_WATCH_DURATION_METRIC = internalMetric {
  details =
    InternalMetricKt.details {
      watchDuration =
        InternalMetricKt.watchDurationParams {
          maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
          maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
        }
      cumulative = false
    }
  namedSetOperations.add(INTERNAL_NAMED_WATCH_DURATION_SET_OPERATION)
}

// Internal reports with running states
// Internal reports of reach
private val INTERNAL_PENDING_REACH_REPORT = internalReport {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportId = REPORT_EXTERNAL_IDS[0]
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_REACH_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(REACH_MEASUREMENT_REFERENCE_ID, INTERNAL_PENDING_REACH_MEASUREMENT)
  details = InternalReportKt.details { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
  createTime = timestamp { seconds = 1000 }
  reportIdempotencyKey = REACH_REPORT_IDEMPOTENCY_KEY
}
private val INTERNAL_SUCCEEDED_REACH_REPORT =
  INTERNAL_PENDING_REACH_REPORT.copy {
    state = InternalReport.State.SUCCEEDED
    measurements.put(REACH_MEASUREMENT_REFERENCE_ID, INTERNAL_SUCCEEDED_REACH_MEASUREMENT)
  }

// Internal reports of impression
private val INTERNAL_PENDING_IMPRESSION_REPORT = internalReport {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportId = REPORT_EXTERNAL_IDS[1]
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_IMPRESSION_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(IMPRESSION_MEASUREMENT_REFERENCE_ID, INTERNAL_PENDING_IMPRESSION_MEASUREMENT)
  details = InternalReportKt.details { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
  createTime = timestamp { seconds = 2000 }
  reportIdempotencyKey = IMPRESSION_REPORT_IDEMPOTENCY_KEY
}
private val INTERNAL_SUCCEEDED_IMPRESSION_REPORT =
  INTERNAL_PENDING_IMPRESSION_REPORT.copy {
    state = InternalReport.State.SUCCEEDED
    measurements.put(IMPRESSION_MEASUREMENT_REFERENCE_ID, INTERNAL_SUCCEEDED_IMPRESSION_MEASUREMENT)
  }

// Internal reports of watch duration
private val INTERNAL_PENDING_WATCH_DURATION_REPORT = internalReport {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportId = REPORT_EXTERNAL_IDS[2]
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_WATCH_DURATION_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(
    WATCH_DURATION_MEASUREMENT_REFERENCE_ID,
    INTERNAL_PENDING_WATCH_DURATION_MEASUREMENT
  )
  details = InternalReportKt.details { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
  createTime = timestamp { seconds = 3000 }
  reportIdempotencyKey = WATCH_DURATION_REPORT_IDEMPOTENCY_KEY
}
private val INTERNAL_SUCCEEDED_WATCH_DURATION_REPORT =
  INTERNAL_PENDING_WATCH_DURATION_REPORT.copy {
    state = InternalReport.State.SUCCEEDED
    measurements.put(
      WATCH_DURATION_MEASUREMENT_REFERENCE_ID,
      INTERNAL_SUCCEEDED_WATCH_DURATION_MEASUREMENT
    )
  }

// Internal reports of frequency histogram
private val INTERNAL_PENDING_FREQUENCY_HISTOGRAM_REPORT = internalReport {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportId = REPORT_EXTERNAL_IDS[3]
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_FREQUENCY_HISTOGRAM_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(
    FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID,
    INTERNAL_PENDING_FREQUENCY_HISTOGRAM_MEASUREMENT
  )
  details = InternalReportKt.details { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
  createTime = timestamp { seconds = 4000 }
  reportIdempotencyKey = FREQUENCY_HISTOGRAM_REPORT_IDEMPOTENCY_KEY
}
private val INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_REPORT =
  INTERNAL_PENDING_FREQUENCY_HISTOGRAM_REPORT.copy {
    state = InternalReport.State.SUCCEEDED
    measurements.put(
      FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID,
      INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT
    )
  }

private val EVENT_GROUP_UNIVERSE = eventGroupUniverse {
  eventGroupEntries +=
    COVERED_EVENT_GROUP_KEYS.map {
      EventGroupUniverseKt.eventGroupEntry {
        key = it.toName()
        value = EVENT_GROUP_FILTER
      }
    }
}

// Public reports with running states
// Reports of reach
private val PENDING_REACH_REPORT = report {
  name = REPORT_NAMES[0]
  reportIdempotencyKey = REACH_REPORT_IDEMPOTENCY_KEY
  measurementConsumer = MEASUREMENT_CONSUMERS.values.first().name
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(REACH_METRIC)
  state = Report.State.RUNNING
}
private val SUCCEEDED_REACH_REPORT = PENDING_REACH_REPORT.copy { state = Report.State.SUCCEEDED }

// Reports of impression
private val PENDING_IMPRESSION_REPORT = report {
  name = REPORT_NAMES[1]
  reportIdempotencyKey = IMPRESSION_REPORT_IDEMPOTENCY_KEY
  measurementConsumer = MEASUREMENT_CONSUMERS.values.first().name
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(IMPRESSION_METRIC)
  state = Report.State.RUNNING
}
private val SUCCEEDED_IMPRESSION_REPORT =
  PENDING_IMPRESSION_REPORT.copy { state = Report.State.SUCCEEDED }

// Reports of watch duration
private val PENDING_WATCH_DURATION_REPORT = report {
  name = REPORT_NAMES[2]
  reportIdempotencyKey = WATCH_DURATION_REPORT_IDEMPOTENCY_KEY
  measurementConsumer = MEASUREMENT_CONSUMERS.values.first().name
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(WATCH_DURATION_METRIC)
  state = Report.State.RUNNING
}
private val SUCCEEDED_WATCH_DURATION_REPORT =
  PENDING_WATCH_DURATION_REPORT.copy { state = Report.State.SUCCEEDED }

// Reports of frequency histogram
private val PENDING_FREQUENCY_HISTOGRAM_REPORT = report {
  name = REPORT_NAMES[3]
  reportIdempotencyKey = FREQUENCY_HISTOGRAM_REPORT_IDEMPOTENCY_KEY
  measurementConsumer = MEASUREMENT_CONSUMERS.values.first().name
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(FREQUENCY_HISTOGRAM_METRIC)
  state = Report.State.RUNNING
}
private val SUCCEEDED_FREQUENCY_HISTOGRAM_REPORT =
  PENDING_FREQUENCY_HISTOGRAM_REPORT.copy { state = Report.State.SUCCEEDED }

@RunWith(JUnit4::class)
class ReportsServiceTest {

  private val internalReportsMock: ReportsCoroutineImplBase = mockService {
    onBlocking { createReport(any()) }
      .thenReturn(
        INTERNAL_PENDING_REACH_REPORT,
        INTERNAL_PENDING_IMPRESSION_REPORT,
        INTERNAL_PENDING_WATCH_DURATION_REPORT,
        INTERNAL_PENDING_FREQUENCY_HISTOGRAM_REPORT,
      )
    onBlocking { streamReports(any()) }
      .thenReturn(
        flowOf(
          INTERNAL_PENDING_REACH_REPORT,
          INTERNAL_PENDING_IMPRESSION_REPORT,
          INTERNAL_PENDING_WATCH_DURATION_REPORT,
          INTERNAL_PENDING_FREQUENCY_HISTOGRAM_REPORT,
        )
      )
    onBlocking { getReport(any()) }
      .thenReturn(
        INTERNAL_SUCCEEDED_REACH_REPORT,
        INTERNAL_SUCCEEDED_IMPRESSION_REPORT,
        INTERNAL_SUCCEEDED_WATCH_DURATION_REPORT,
        INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_REPORT,
      )
    onBlocking { getReportByIdempotencyKey(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))
  }

  private val internalReportingSetsMock: InternalReportingSetsCoroutineImplBase = mockService {
    onBlocking { batchGetReportingSet(any()) }
      .thenReturn(
        flowOf(
          INTERNAL_REPORTING_SETS[0],
          INTERNAL_REPORTING_SETS[1],
          INTERNAL_REPORTING_SETS[0],
          INTERNAL_REPORTING_SETS[1]
        )
      )
  }

  private val internalMeasurementsMock: InternalMeasurementsCoroutineImplBase = mockService {
    onBlocking { getMeasurement(any()) }.thenThrow(StatusRuntimeException(Status.NOT_FOUND))
  }

  private val measurementsMock: MeasurementsCoroutineImplBase = mockService {
    onBlocking { getMeasurement(any()) }
      .thenReturn(
        SUCCEEDED_REACH_MEASUREMENT,
        SUCCEEDED_IMPRESSION_MEASUREMENT,
        SUCCEEDED_WATCH_DURATION_MEASUREMENT,
        SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT,
      )

    onBlocking { createMeasurement(any()) }.thenReturn(BASE_REACH_MEASUREMENT)
  }

  private val measurementConsumersMock: MeasurementConsumersCoroutineImplBase = mockService {
    onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMERS.values.first())
  }

  private val dataProvidersMock: DataProvidersCoroutineImplBase = mockService {
    var stubbing = onBlocking { getDataProvider(any()) }
    for (dataProvider in DATA_PROVIDERS.values) {
      stubbing = stubbing.thenReturn(dataProvider)
    }
  }

  private val certificateMock: CertificatesCoroutineImplBase = mockService {
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

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalReportsMock)
    addService(internalReportingSetsMock)
    addService(internalMeasurementsMock)
    addService(measurementsMock)
    addService(measurementConsumersMock)
    addService(dataProvidersMock)
    addService(certificateMock)
  }

  private lateinit var service: ReportsService

  @Before
  fun initService() {
    secureRandomMock.stub {
      on { nextInt(any()) } doReturn SECURE_RANDOM_OUTPUT_INT
      on { nextLong() } doReturn SECURE_RANDOM_OUTPUT_LONG
    }

    service =
      ReportsService(
        InternalReportsCoroutineStub(grpcTestServerRule.channel),
        InternalReportingSetsCoroutineStub(grpcTestServerRule.channel),
        InternalMeasurementsCoroutineStub(grpcTestServerRule.channel),
        DataProvidersCoroutineStub(grpcTestServerRule.channel),
        MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        CertificatesCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_KEY_PAIR_STORE,
        secureRandomMock,
        SECRETS_DIR,
        listOf(AGGREGATOR_ROOT_CERTIFICATE, DATA_PROVIDER_ROOT_CERTIFICATE).associateBy {
          it.subjectKeyIdentifier!!
        }
      )
  }

  @Test
  fun `createReport returns a report of reach with RUNNING state`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createReport(request) }
      }

    val expected = PENDING_REACH_REPORT

    // Verify proto argument of ReportsCoroutineImplBase::getReportByIdempotencyKey
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReportByIdempotencyKey)
      .isEqualTo(
        getReportByIdempotencyKeyRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          reportIdempotencyKey = REACH_REPORT_IDEMPOTENCY_KEY
        }
      )

    // Verify proto argument of InternalReportingSetsCoroutineImplBase::batchGetReportingSet
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsCoroutineImplBase::batchGetReportingSet
      )
      .isEqualTo(
        batchGetReportingSetRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetIds += INTERNAL_REPORTING_SETS[0].externalReportingSetId
          externalReportingSetIds += INTERNAL_REPORTING_SETS[1].externalReportingSetId
        }
      )

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    verifyProtoArgument(
        measurementConsumersMock,
        MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      )
      .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(2)) { getDataProvider(dataProvidersCaptor.capture()) }
    val capturedDataProviderRequests = dataProvidersCaptor.allValues
    assertThat(capturedDataProviderRequests)
      .containsExactly(
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val capturedMeasurementRequest =
      captureFirst<CreateMeasurementRequest> {
        runBlocking { verify(measurementsMock).createMeasurement(capture()) }
      }
    val capturedMeasurement = capturedMeasurementRequest.measurement
    val expectedMeasurement =
      BASE_REACH_MEASUREMENT.copy {
        dataProviders +=
          DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }
        measurementSpec =
          signMeasurementSpec(REACH_ONLY_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
      }

    assertThat(capturedMeasurement)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER),
        Measurement.DataProviderEntry.Value.getDescriptor()
          .findFieldByNumber(ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER),
      )
      .isEqualTo(expectedMeasurement)

    verifyMeasurementSpec(
      capturedMeasurement.measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER
    )
    val measurementSpec = MeasurementSpec.parseFrom(capturedMeasurement.measurementSpec.data)
    assertThat(measurementSpec).isEqualTo(REACH_ONLY_MEASUREMENT_SPEC)

    val dataProvidersList = capturedMeasurement.dataProvidersList.sortedBy { it.key }

    dataProvidersList.map { dataProviderEntry ->
      val signedRequisitionSpec =
        decryptRequisitionSpec(
          dataProviderEntry.value.encryptedRequisitionSpec,
          DATA_PROVIDER_PRIVATE_KEY_HANDLE
        )
      val requisitionSpec = RequisitionSpec.parseFrom(signedRequisitionSpec.data)
      verifyRequisitionSpec(
        signedRequisitionSpec,
        requisitionSpec,
        measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )
    }

    // Verify proto argument of InternalMeasurementsCoroutineImplBase::createMeasurement
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::createMeasurement
      )
      .isEqualTo(
        internalMeasurement {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
          state = InternalMeasurement.State.PENDING
        }
      )

    // Verify proto argument of InternalReportsCoroutineImplBase::createReport
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::createReport)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateReportRequest {
          report =
            INTERNAL_PENDING_REACH_REPORT.copy {
              clearState()
              clearExternalReportId()
              measurements.clear()
              clearCreateTime()
            }
          measurements +=
            InternalCreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
            }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createReport returns a report of reach with RUNNING state when timeIntervals set`() {
    val internalReport =
      INTERNAL_PENDING_REACH_REPORT.copy {
        clearTime()
        timeIntervals = internalTimeIntervals {
          timeIntervals += internalTimeInterval {
            startTime = START_TIME
            endTime = Timestamps.add(START_TIME, TIME_INTERVAL_INCREMENT)
          }
        }
      }
    runBlocking { whenever(internalReportsMock.createReport(any())).thenReturn(internalReport) }

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
          timeIntervals = timeIntervals {
            timeIntervals += timeInterval {
              startTime = START_TIME
              endTime = Timestamps.add(START_TIME, TIME_INTERVAL_INCREMENT)
            }
          }
        }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createReport(request) }
      }

    val expected =
      PENDING_REACH_REPORT.copy {
        clearTime()
        timeIntervals = timeIntervals {
          timeIntervals += timeInterval {
            startTime = START_TIME
            endTime = Timestamps.add(START_TIME, TIME_INTERVAL_INCREMENT)
          }
        }
      }

    // Verify proto argument of ReportsCoroutineImplBase::getReportByIdempotencyKey
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReportByIdempotencyKey)
      .isEqualTo(
        getReportByIdempotencyKeyRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          reportIdempotencyKey = REACH_REPORT_IDEMPOTENCY_KEY
        }
      )

    // Verify proto argument of InternalReportingSetsCoroutineImplBase::batchGetReportingSet
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsCoroutineImplBase::batchGetReportingSet
      )
      .isEqualTo(
        batchGetReportingSetRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetIds += INTERNAL_REPORTING_SETS[0].externalReportingSetId
          externalReportingSetIds += INTERNAL_REPORTING_SETS[1].externalReportingSetId
        }
      )

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    verifyProtoArgument(
        measurementConsumersMock,
        MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      )
      .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(2)) { getDataProvider(dataProvidersCaptor.capture()) }
    val capturedDataProviderRequests = dataProvidersCaptor.allValues
    assertThat(capturedDataProviderRequests)
      .containsExactly(
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val capturedMeasurementRequest =
      captureFirst<CreateMeasurementRequest> {
        runBlocking { verify(measurementsMock).createMeasurement(capture()) }
      }
    val capturedMeasurement = capturedMeasurementRequest.measurement
    val expectedMeasurement =
      BASE_REACH_MEASUREMENT.copy {
        dataProviders +=
          DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }
        measurementSpec =
          signMeasurementSpec(REACH_ONLY_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
      }

    assertThat(capturedMeasurement)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER),
        Measurement.DataProviderEntry.Value.getDescriptor()
          .findFieldByNumber(ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER),
      )
      .isEqualTo(expectedMeasurement)

    verifyMeasurementSpec(
      capturedMeasurement.measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER
    )
    val measurementSpec = MeasurementSpec.parseFrom(capturedMeasurement.measurementSpec.data)
    assertThat(measurementSpec).isEqualTo(REACH_ONLY_MEASUREMENT_SPEC)

    val dataProvidersList = capturedMeasurement.dataProvidersList.sortedBy { it.key }

    dataProvidersList.map { dataProviderEntry ->
      val signedRequisitionSpec =
        decryptRequisitionSpec(
          dataProviderEntry.value.encryptedRequisitionSpec,
          DATA_PROVIDER_PRIVATE_KEY_HANDLE
        )
      val requisitionSpec = RequisitionSpec.parseFrom(signedRequisitionSpec.data)
      verifyRequisitionSpec(
        signedRequisitionSpec,
        requisitionSpec,
        measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )
    }

    // Verify proto argument of InternalMeasurementsCoroutineImplBase::createMeasurement
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::createMeasurement
      )
      .isEqualTo(
        internalMeasurement {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
          state = InternalMeasurement.State.PENDING
        }
      )

    // Verify proto argument of InternalReportsCoroutineImplBase::createReport
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::createReport)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateReportRequest {
          report =
            internalReport.copy {
              clearState()
              clearExternalReportId()
              measurements.clear()
              clearCreateTime()
            }
          measurements +=
            InternalCreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
            }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createReport returns a report with a cumulative metric`() {
    val internalCumulativeReport =
      INTERNAL_PENDING_REACH_REPORT.copy {
        metrics.clear()
        metrics +=
          INTERNAL_PENDING_REACH_REPORT.metricsList[0].copy {
            details =
              INTERNAL_PENDING_REACH_REPORT.metricsList[0].details.copy { cumulative = true }
          }
      }
    runBlocking {
      whenever(internalReportsMock.createReport(any())).thenReturn(internalCumulativeReport)
    }

    val cumulativeReport =
      PENDING_REACH_REPORT.copy {
        metrics.clear()
        metrics += PENDING_REACH_REPORT.metricsList[0].copy { cumulative = true }
      }

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = cumulativeReport.copy { clearState() }
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createReport(request) }
      }

    // Verify proto argument of ReportsCoroutineImplBase::getReportByIdempotencyKey
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReportByIdempotencyKey)
      .isEqualTo(
        getReportByIdempotencyKeyRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          reportIdempotencyKey = REACH_REPORT_IDEMPOTENCY_KEY
        }
      )

    // Verify proto argument of InternalReportingSetsCoroutineImplBase::batchGetReportingSet
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsCoroutineImplBase::batchGetReportingSet
      )
      .isEqualTo(
        batchGetReportingSetRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetIds += INTERNAL_REPORTING_SETS[0].externalReportingSetId
          externalReportingSetIds += INTERNAL_REPORTING_SETS[1].externalReportingSetId
        }
      )

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    verifyProtoArgument(
        measurementConsumersMock,
        MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      )
      .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(2)) { getDataProvider(dataProvidersCaptor.capture()) }
    val capturedDataProviderRequests = dataProvidersCaptor.allValues
    assertThat(capturedDataProviderRequests)
      .containsExactly(
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val capturedMeasurementRequest =
      captureFirst<CreateMeasurementRequest> {
        runBlocking { verify(measurementsMock).createMeasurement(capture()) }
      }
    val capturedMeasurement = capturedMeasurementRequest.measurement
    val expectedMeasurement =
      BASE_REACH_MEASUREMENT.copy {
        dataProviders +=
          DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }
        measurementSpec =
          signMeasurementSpec(REACH_ONLY_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
      }

    assertThat(capturedMeasurement)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER),
        Measurement.DataProviderEntry.Value.getDescriptor()
          .findFieldByNumber(ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER),
      )
      .isEqualTo(expectedMeasurement)

    verifyMeasurementSpec(
      capturedMeasurement.measurementSpec,
      MEASUREMENT_CONSUMER_CERTIFICATE,
      TRUSTED_MEASUREMENT_CONSUMER_ISSUER
    )
    val measurementSpec = MeasurementSpec.parseFrom(capturedMeasurement.measurementSpec.data)
    assertThat(measurementSpec).isEqualTo(REACH_ONLY_MEASUREMENT_SPEC)

    val dataProvidersList = capturedMeasurement.dataProvidersList.sortedBy { it.key }

    dataProvidersList.map { dataProviderEntry ->
      val signedRequisitionSpec =
        decryptRequisitionSpec(
          dataProviderEntry.value.encryptedRequisitionSpec,
          DATA_PROVIDER_PRIVATE_KEY_HANDLE
        )
      val requisitionSpec = RequisitionSpec.parseFrom(signedRequisitionSpec.data)
      verifyRequisitionSpec(
        signedRequisitionSpec,
        requisitionSpec,
        measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )
    }

    // Verify proto argument of InternalMeasurementsCoroutineImplBase::createMeasurement
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::createMeasurement
      )
      .isEqualTo(
        internalMeasurement {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
          state = InternalMeasurement.State.PENDING
        }
      )

    // Verify proto argument of InternalReportsCoroutineImplBase::createReport
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::createReport)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateReportRequest {
          report =
            internalCumulativeReport.copy {
              clearState()
              clearExternalReportId()
              measurements.clear()
              clearCreateTime()
            }
          measurements +=
            InternalCreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
            }
        }
      )

    assertThat(result).isEqualTo(cumulativeReport)
  }

  @Test
  fun `createReport returns a report with set operation type DIFFERENCE`() {
    val internalPendingReachReportWithSetDifference =
      INTERNAL_PENDING_REACH_REPORT.copy {
        val source = this
        measurements.clear()
        clearCreateTime()
        val metric = internalMetric {
          details = InternalMetricKt.details { reach = InternalMetricKt.reachParams {} }
          namedSetOperations +=
            source.metrics[0].namedSetOperationsList[0].copy {
              setOperation =
                setOperation.copy { type = InternalMetric.SetOperation.Type.DIFFERENCE }
              measurementCalculations.clear()
              measurementCalculations +=
                source.metrics[0].namedSetOperationsList[0].measurementCalculationsList[0].copy {
                  weightedMeasurements.clear()
                  weightedMeasurements += weightedMeasurement {
                    measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
                    coefficient = -1
                  }
                  weightedMeasurements += weightedMeasurement {
                    measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID_2
                    coefficient = 1
                  }
                }
            }
        }
        metrics.clear()
        metrics += metric
      }

    runBlocking {
      whenever(internalReportsMock.createReport(any()))
        .thenReturn(internalPendingReachReportWithSetDifference)
      whenever(measurementsMock.createMeasurement(any()))
        .thenReturn(BASE_REACH_MEASUREMENT, BASE_REACH_MEASUREMENT_2)
    }

    val pendingReachReportWithSetDifference =
      PENDING_REACH_REPORT.copy {
        metrics.clear()
        metrics += metric {
          reach = reachParams {}
          cumulative = false
          setOperations += namedSetOperation {
            uniqueName = REACH_SET_OPERATION_UNIQUE_NAME
            setOperation = setOperation {
              type = SetOperation.Type.DIFFERENCE
              lhs =
                SetOperationKt.operand { reportingSet = INTERNAL_REPORTING_SETS[0].resourceName }
              rhs =
                SetOperationKt.operand { reportingSet = INTERNAL_REPORTING_SETS[1].resourceName }
            }
          }
        }
      }

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = pendingReachReportWithSetDifference
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createReport(request) }
      }

    // Verify proto argument of ReportsCoroutineImplBase::getReportByIdempotencyKey
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReportByIdempotencyKey)
      .isEqualTo(
        getReportByIdempotencyKeyRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          reportIdempotencyKey = REACH_REPORT_IDEMPOTENCY_KEY
        }
      )

    // Verify proto argument of InternalReportingSetsCoroutineImplBase::batchGetReportingSet
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsCoroutineImplBase::batchGetReportingSet
      )
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        batchGetReportingSetRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetIds += INTERNAL_REPORTING_SETS[0].externalReportingSetId
          externalReportingSetIds += INTERNAL_REPORTING_SETS[1].externalReportingSetId
        }
      )

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    verifyProtoArgument(
        measurementConsumersMock,
        MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      )
      .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(3)) { getDataProvider(dataProvidersCaptor.capture()) }
    val capturedDataProviderRequests = dataProvidersCaptor.allValues
    assertThat(capturedDataProviderRequests)
      .containsExactly(
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name }
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val measurementCaptor: KArgumentCaptor<CreateMeasurementRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(2)) { createMeasurement(measurementCaptor.capture()) }
    assertThat(measurementCaptor.allValues.map { it.measurement }).containsNoDuplicates()

    // Verify proto argument of InternalMeasurementsCoroutineImplBase::createMeasurement
    val internalMeasurementCaptor: KArgumentCaptor<InternalMeasurement> = argumentCaptor()
    verifyBlocking(internalMeasurementsMock, times(2)) {
      createMeasurement(internalMeasurementCaptor.capture())
    }

    // Verify proto argument of InternalReportsCoroutineImplBase::createReport
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::createReport)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalCreateReportRequest {
          report =
            internalPendingReachReportWithSetDifference.copy {
              clearState()
              clearExternalReportId()
            }
          measurements +=
            InternalCreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
            }
          measurements +=
            InternalCreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID_2
            }
        }
      )

    assertThat(result).isEqualTo(pendingReachReportWithSetDifference)
  }

  @Test
  fun `createReport succeeds when the internal createMeasurement throws ALREADY_EXISTS`() =
    runBlocking {
      whenever(internalMeasurementsMock.createMeasurement(any()))
        .thenThrow(StatusRuntimeException(Status.ALREADY_EXISTS))

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        report = PENDING_REACH_REPORT.copy { clearState() }
      }

      val report =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      assertThat(report.state).isEqualTo(Report.State.RUNNING)
    }

  @Test
  fun `createReport throws UNAUTHENTICATED when no principal is found`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createReport(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createReport throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.last().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Cannot create a Report for another MeasurementConsumer.")
  }

  @Test
  fun `createReport throws PERMISSION_DENIED when report doesn't belong to caller`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.last().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Cannot create a Report for another MeasurementConsumer.")
  }

  @Test
  fun `createReport throws UNAUTHENTICATED when the caller is not MeasurementConsumer`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS_LIST[0].name) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("No ReportingPrincipal found")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when parent is unspecified`() {
    val request = createReportRequest { report = PENDING_REACH_REPORT.copy { clearState() } }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Parent is either unspecified or invalid.")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when report is unspecified`() {
    val request = createReportRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Report is not specified.")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when reportIdempotencyKey is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearReportIdempotencyKey()
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("ReportIdempotencyKey is not specified.")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when eventGroupUniverse in Report is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearEventGroupUniverse()
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("EventGroupUniverse is not specified.")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when eventGroupUniverse entries list is empty`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          eventGroupUniverse = eventGroupUniverse { eventGroupEntries.clear() }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when eventGroupUniverse entry is missing key`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          eventGroupUniverse = eventGroupUniverse {
            eventGroupEntries += EventGroupUniverseKt.eventGroupEntry {}
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when setOperationName duplicate for same metricType`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          metrics.add(REACH_METRIC)
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("The names of the set operations within the same metric type should be unique.")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when time in Report is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("The time in Report is not specified.")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeIntervals is set and cumulative is true`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
          timeIntervals = timeIntervals {
            timeIntervals += timeInterval {
              startTime = timestamp { seconds = 1 }
              endTime = timestamp { seconds = 5 }
            }
          }
          metrics.clear()
          metrics += PENDING_REACH_REPORT.metricsList[0].copy { cumulative = true }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeIntervals timeIntervalsList is empty`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
          timeIntervals = timeIntervals {}
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeInterval startTime is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
          timeIntervals = timeIntervals {
            timeIntervals += timeInterval { endTime = timestamp { seconds = 5 } }
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeInterval endTime is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
          timeIntervals = timeIntervals {
            timeIntervals += timeInterval { startTime = timestamp { seconds = 5 } }
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeInterval endTime is before startTime`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
          timeIntervals = timeIntervals {
            timeIntervals += timeInterval {
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
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when PeriodicTimeInterval startTime is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
          periodicTimeInterval = periodicTimeInterval {
            increment = duration { seconds = 5 }
            intervalCount = 3
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when PeriodicTimeInterval increment is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
          periodicTimeInterval = periodicTimeInterval {
            startTime = timestamp {
              seconds = 5
              nanos = 5
            }
            intervalCount = 3
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when PeriodicTimeInterval intervalCount is 0`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          clearTime()
          periodicTimeInterval = periodicTimeInterval {
            startTime = timestamp {
              seconds = 5
              nanos = 5
            }
            increment = duration { seconds = 5 }
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when any metric type in Report is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          metrics.clear()
          metrics.add(REACH_METRIC.copy { clearReach() })
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("The metric type in Report is not specified.")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when metrics list is empty`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          metrics.clear()
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when namedSetOperation uniqueName is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          metrics.clear()
          metrics += metric {
            reach = reachParams {}
            setOperations += namedSetOperation {
              setOperation = setOperation {
                type = SetOperation.Type.UNION
                lhs =
                  SetOperationKt.operand { reportingSet = INTERNAL_REPORTING_SETS[0].resourceName }
              }
            }
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when setOperation type is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          metrics.clear()
          metrics += metric {
            reach = reachParams {}
            setOperations += namedSetOperation {
              uniqueName = "name"
              setOperation = setOperation {
                lhs =
                  SetOperationKt.operand { reportingSet = INTERNAL_REPORTING_SETS[0].resourceName }
              }
            }
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when setOperation lhs is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          metrics.clear()
          metrics += metric {
            reach = reachParams {}
            setOperations += namedSetOperation {
              uniqueName = "name"
              setOperation = setOperation { type = SetOperation.Type.UNION }
            }
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when provided reporting set name is invalid`() {
    val invalidMetric = metric {
      reach = reachParams {}
      cumulative = false
      setOperations.add(
        NAMED_REACH_SET_OPERATION.copy { setOperation = SET_OPERATION_WITH_INVALID_REPORTING_SET }
      )
    }

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          metrics.clear()
          metrics.add(invalidMetric)
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("Invalid reporting set name $INVALID_REPORTING_SET_NAME.")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when any reporting set is not accessible to caller`() {
    val invalidMetric = metric {
      reach = reachParams {}
      cumulative = false
      setOperations.add(
        NAMED_REACH_SET_OPERATION.copy {
          setOperation = SET_OPERATION_WITH_INACCESSIBLE_REPORTING_SET
        }
      )
    }

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report =
        PENDING_REACH_REPORT.copy {
          clearState()
          metrics.clear()
          metrics.add(invalidMetric)
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("No access to the reporting set [$REPORTING_SET_NAME_FOR_MC_2].")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when eventGroup isn't covered by eventGroupUniverse`() =
    runBlocking {
      whenever(internalReportingSetsMock.batchGetReportingSet(any()))
        .thenReturn(flowOf(INTERNAL_REPORTING_SETS[0], UNCOVERED_INTERNAL_REPORTING_SET))
      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        report = PENDING_REACH_REPORT.copy { clearState() }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.createReport(request) }
          }
        }
      val expectedExceptionDescription =
        "The event group [$UNCOVERED_EVENT_GROUP_NAME] in the reporting set" +
          " [${UNCOVERED_INTERNAL_REPORTING_SET.displayName}] is not included in the event group " +
          "universe."
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.status.description).isEqualTo(expectedExceptionDescription)
    }

  @Test
  fun `createReport throws NOT_FOUND when reporting set is not found`() = runBlocking {
    whenever(internalReportingSetsMock.batchGetReportingSet(any()))
      .thenReturn(flowOf(INTERNAL_REPORTING_SETS[0]))
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createReport throws FAILED_PRECONDITION when EDP cert is revoked`() = runBlocking {
    val dataProvider = DATA_PROVIDERS.values.first()
    whenever(
        certificateMock.getCertificate(
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
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }

    assertThat(exception).hasMessageThat().ignoringCase().contains("revoked")
  }

  @Test
  fun `createReport throws FAILED_PRECONDITION when EDP public key signature is invalid`() =
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
      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        report = PENDING_REACH_REPORT.copy { clearState() }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.createReport(request) }
          }
        }

      assertThat(exception).hasMessageThat().ignoringCase().contains("signature")
    }

  @Test
  fun `createReport throws exception from getReportByIdempotencyKey when status isn't NOT_FOUND`() =
    runBlocking {
      whenever(internalReportsMock.getReportByIdempotencyKey(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        report = PENDING_REACH_REPORT.copy { clearState() }
      }

      val exception =
        assertFailsWith(Exception::class) {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.createReport(request) }
          }
        }
      val expectedExceptionDescription =
        "Unable to retrieve a report from the reporting database using the provided " +
          "reportIdempotencyKey [${PENDING_REACH_REPORT.reportIdempotencyKey}]."
      assertThat(exception.message).isEqualTo(expectedExceptionDescription)
    }

  @Test
  fun `createReport throws exception when internal createReport throws exception`() = runBlocking {
    whenever(internalReportsMock.createReport(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    val expectedExceptionDescription = "Unable to create a report in the reporting database."
    assertThat(exception.message).isEqualTo(expectedExceptionDescription)
  }

  @Test
  fun `createReport throws exception when the CMM createMeasurement throws exception`() =
    runBlocking {
      whenever(measurementsMock.createMeasurement(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        report = PENDING_REACH_REPORT.copy { clearState() }
      }

      val exception =
        assertFailsWith(Exception::class) {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.createReport(request) }
          }
        }
      val expectedExceptionDescription =
        "Unable to create the measurement [$REACH_MEASUREMENT_NAME]."
      assertThat(exception.message).isEqualTo(expectedExceptionDescription)
    }

  @Test
  fun `createReport throws exception when the internal createMeasurement throws exception`() =
    runBlocking {
      whenever(internalMeasurementsMock.createMeasurement(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        report = PENDING_REACH_REPORT.copy { clearState() }
      }

      val exception =
        assertFailsWith(Exception::class) {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.createReport(request) }
          }
        }
      val expectedExceptionDescription =
        "Unable to create the measurement [$REACH_MEASUREMENT_NAME] in the reporting database."
      assertThat(exception.message).isEqualTo(expectedExceptionDescription)
    }

  @Test
  fun `createReport throws exception when getMeasurementConsumer throws exception`() = runBlocking {
    whenever(measurementConsumersMock.getMeasurementConsumer(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    val expectedExceptionDescription =
      "Unable to retrieve the measurement consumer [${MEASUREMENT_CONSUMERS.values.first().name}]."
    assertThat(exception.message).isEqualTo(expectedExceptionDescription)
  }

  @Test
  fun `createReport throws exception when the internal batchGetReportingSet throws exception`():
    Unit = runBlocking {
    whenever(internalReportingSetsMock.batchGetReportingSet(any()))
      .thenThrow(StatusRuntimeException(Status.UNKNOWN))

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }

    assertFails {
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createReport(request) }
      }
    }
  }

  @Test
  fun `createReport throws exception when getDataProvider throws exception`() = runBlocking {
    whenever(dataProvidersMock.getDataProvider(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      report = PENDING_REACH_REPORT.copy { clearState() }
    }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception).hasMessageThat().contains("dataProviders/")
  }

  @Test
  fun `listReports returns without a next page token when there is no previous page token`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)
      reports.add(SUCCEEDED_FREQUENCY_HISTOGRAM_REPORT)
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns with a next page token when there is no previous page token`() {
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = PAGE_SIZE
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)

      nextPageToken =
        listReportsPageToken {
            pageSize = PAGE_SIZE
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            lastReport = previousPageEnd {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalReportId = REPORT_EXTERNAL_IDS[2]
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns with a next page token when there is a previous page token`() {
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = PAGE_SIZE
      pageToken =
        listReportsPageToken {
            pageSize = PAGE_SIZE
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            lastReport = previousPageEnd {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalReportId = REPORT_EXTERNAL_IDS[0]
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)

      nextPageToken =
        listReportsPageToken {
            pageSize = PAGE_SIZE
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            lastReport = previousPageEnd {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalReportId = REPORT_EXTERNAL_IDS[2]
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportIdAfter = REPORT_EXTERNAL_IDS[0]
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports with page size replaced with a valid value and no previous page token`() {
    val invalidPageSize = MAX_PAGE_SIZE * 2
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = invalidPageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)
      reports.add(SUCCEEDED_FREQUENCY_HISTOGRAM_REPORT)
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = MAX_PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports with invalid page size replaced with the one in previous page token`() {
    val invalidPageSize = MAX_PAGE_SIZE * 2
    val previousPageSize = PAGE_SIZE
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = invalidPageSize
      pageToken =
        listReportsPageToken {
            pageSize = previousPageSize
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            lastReport = previousPageEnd {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalReportId = REPORT_EXTERNAL_IDS[0]
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)

      nextPageToken =
        listReportsPageToken {
            pageSize = previousPageSize
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            lastReport = previousPageEnd {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalReportId = REPORT_EXTERNAL_IDS[2]
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = previousPageSize + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportIdAfter = REPORT_EXTERNAL_IDS[0]
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports with page size replacing the one in previous page token`() {
    val newPageSize = PAGE_SIZE
    val previousPageSize = 1
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = newPageSize
      pageToken =
        listReportsPageToken {
            pageSize = previousPageSize
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            lastReport = previousPageEnd {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalReportId = REPORT_EXTERNAL_IDS[0]
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)

      nextPageToken =
        listReportsPageToken {
            pageSize = newPageSize
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            lastReport = previousPageEnd {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
              externalReportId = REPORT_EXTERNAL_IDS[2]
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = newPageSize + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportIdAfter = REPORT_EXTERNAL_IDS[0]
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports throws UNAUTHENTICATED when no principal is found`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listReports(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReports throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.last().name, CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Cannot list Reports belonging to other MeasurementConsumers.")
  }

  @Test
  fun `listReports throws UNAUTHENTICATED when the caller is not MeasurementConsumer`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS.values.first().name) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
    assertThat(exception.status.description).isEqualTo("No ReportingPrincipal found")
  }

  @Test
  fun `listReports throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageSize = -1
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Page size cannot be less than 0")
  }

  @Test
  fun `listReports throws INVALID_ARGUMENT when parent is unspecified`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listReports(ListReportsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listReports throws INVALID_ARGUMENT when mc id doesn't match one in page token`() {
    val measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.last().measurementConsumerId
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      pageToken =
        listReportsPageToken {
            this.measurementConsumerReferenceId = measurementConsumerReferenceId
            lastReport = previousPageEnd {
              this.measurementConsumerReferenceId = measurementConsumerReferenceId
              externalReportId = REPORT_EXTERNAL_IDS[0]
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listReports throws Exception when the internal streamReports throws Exception`() =
    runBlocking {
      whenever(internalReportsMock.streamReports(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith(Exception::class) {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.listReports(request) }
          }
        }
      val expectedExceptionDescription = "Unable to list reports from the reporting database."
      assertThat(exception.message).isEqualTo(expectedExceptionDescription)
    }

  @Test
  fun `listReports throws Exception when the internal getReport throws Exception`() = runBlocking {
    whenever(internalReportsMock.getReport(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }
    val expectedExceptionDescription =
      "Unable to get the report [${REPORT_NAMES[0]}] from the reporting database."
    assertThat(exception.message).isEqualTo(expectedExceptionDescription)
  }

  @Test
  fun `listReports throws Exception when the CMM getMeasurement throws Exception`() = runBlocking {
    whenever(measurementsMock.getMeasurement(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }
    val expectedExceptionDescription =
      "Unable to retrieve the measurement [$REACH_MEASUREMENT_NAME]."
    assertThat(exception.message).isEqualTo(expectedExceptionDescription)
  }

  @Test
  fun `listReports throws Exception when the internal setMeasurementResult throws Exception`() =
    runBlocking {
      whenever(internalMeasurementsMock.setMeasurementResult(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith(Exception::class) {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.listReports(request) }
          }
        }
      val expectedExceptionDescription =
        "Unable to update the measurement [$REACH_MEASUREMENT_NAME] in the reporting database."
      assertThat(exception.message).isEqualTo(expectedExceptionDescription)
    }

  @Test
  fun `listReports throws Exception when the internal setMeasurementFailure throws Exception`() =
    runBlocking {
      whenever(internalMeasurementsMock.setMeasurementFailure(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      whenever(internalReportsMock.streamReports(any()))
        .thenReturn(flowOf(INTERNAL_PENDING_REACH_REPORT))
      whenever(measurementsMock.getMeasurement(any()))
        .thenReturn(
          PENDING_REACH_MEASUREMENT.copy {
            state = Measurement.State.FAILED
            failure = failure {
              reason = Measurement.Failure.Reason.REQUISITION_REFUSED
              message = "Privacy budget exceeded."
            }
          }
        )
      whenever(internalReportsMock.getReport(any()))
        .thenReturn(
          INTERNAL_PENDING_REACH_REPORT.copy { state = InternalReport.State.FAILED },
        )

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val exception =
        assertFailsWith(Exception::class) {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.listReports(request) }
          }
        }
      val expectedExceptionDescription =
        "Unable to update the measurement [$REACH_MEASUREMENT_NAME] in the reporting database."
      assertThat(exception.message).isEqualTo(expectedExceptionDescription)
    }

  @Test
  fun `listReports throws Exception when the getCertificate throws Exception`() = runBlocking {
    whenever(certificateMock.getCertificate(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }

    assertThat(exception).hasMessageThat().contains(AGGREGATOR_CERTIFICATE.name)
  }

  @Test
  fun `listReports returns reports with SUCCEEDED states when reports are already succeeded`() {
    whenever(internalReportsMock.streamReports(any()))
      .thenReturn(
        flowOf(
          INTERNAL_SUCCEEDED_REACH_REPORT,
          INTERNAL_SUCCEEDED_IMPRESSION_REPORT,
          INTERNAL_SUCCEEDED_WATCH_DURATION_REPORT,
          INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_REPORT,
        )
      )

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)
      reports.add(SUCCEEDED_FREQUENCY_HISTOGRAM_REPORT)
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns reports with FAILED states when reports are already failed`() {
    whenever(internalReportsMock.streamReports(any()))
      .thenReturn(
        flowOf(
          INTERNAL_PENDING_REACH_REPORT.copy { state = InternalReport.State.FAILED },
          INTERNAL_PENDING_IMPRESSION_REPORT.copy { state = InternalReport.State.FAILED },
          INTERNAL_PENDING_WATCH_DURATION_REPORT.copy { state = InternalReport.State.FAILED },
          INTERNAL_PENDING_FREQUENCY_HISTOGRAM_REPORT.copy { state = InternalReport.State.FAILED },
        )
      )

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(PENDING_REACH_REPORT.copy { state = Report.State.FAILED })
      reports.add(PENDING_IMPRESSION_REPORT.copy { state = Report.State.FAILED })
      reports.add(PENDING_WATCH_DURATION_REPORT.copy { state = Report.State.FAILED })
      reports.add(PENDING_FREQUENCY_HISTOGRAM_REPORT.copy { state = Report.State.FAILED })
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns reports with RUNNING states when measurements are PENDING`() =
    runBlocking {
      whenever(internalReportsMock.streamReports(any()))
        .thenReturn(flowOf(INTERNAL_PENDING_REACH_REPORT))
      whenever(measurementsMock.getMeasurement(any()))
        .thenReturn(
          PENDING_REACH_MEASUREMENT.copy {
            state = Measurement.State.COMPUTING
            results.clear()
          }
        )
      whenever(internalReportsMock.getReport(any())).thenReturn(INTERNAL_PENDING_REACH_REPORT)

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listReports(request) }
        }

      val expected = listReportsResponse { reports.add(PENDING_REACH_REPORT) }

      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
        .isEqualTo(
          streamReportsRequest {
            limit = DEFAULT_PAGE_SIZE + 1
            this.filter = filter {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            }
          }
        )
      verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
        .isEqualTo(getMeasurementRequest { name = REACH_MEASUREMENT_NAME })
      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[0]
          }
        )

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listReports returns reports with FAILED states when measurements are FAILED`() =
    runBlocking {
      whenever(internalReportsMock.streamReports(any()))
        .thenReturn(flowOf(INTERNAL_PENDING_REACH_REPORT))
      whenever(measurementsMock.getMeasurement(any()))
        .thenReturn(
          PENDING_REACH_MEASUREMENT.copy {
            state = Measurement.State.FAILED
            failure = failure {
              reason = Measurement.Failure.Reason.REQUISITION_REFUSED
              message = "Privacy budget exceeded."
            }
          }
        )
      whenever(internalReportsMock.getReport(any()))
        .thenReturn(
          INTERNAL_PENDING_REACH_REPORT.copy { state = InternalReport.State.FAILED },
        )

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listReports(request) }
        }

      val expected = listReportsResponse {
        reports.add(PENDING_REACH_REPORT.copy { state = Report.State.FAILED })
      }

      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
        .isEqualTo(
          streamReportsRequest {
            limit = DEFAULT_PAGE_SIZE + 1
            this.filter = filter {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            }
          }
        )
      verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
        .isEqualTo(getMeasurementRequest { name = REACH_MEASUREMENT_NAME })
      verifyProtoArgument(
          internalMeasurementsMock,
          InternalMeasurementsCoroutineImplBase::setMeasurementFailure
        )
        .isEqualTo(
          setMeasurementFailureRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
            failure =
              InternalMeasurementKt.failure {
                reason = InternalMeasurement.Failure.Reason.REQUISITION_REFUSED
                message = "Privacy budget exceeded."
              }
          }
        )
      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[0]
          }
        )

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listReports returns reports with SUCCEEDED states when measurements are SUCCEEDED`() =
    runBlocking {
      whenever(internalReportsMock.streamReports(any()))
        .thenReturn(flowOf(INTERNAL_PENDING_REACH_REPORT))
      whenever(measurementsMock.getMeasurement(any())).thenReturn(SUCCEEDED_REACH_MEASUREMENT)
      whenever(internalReportsMock.getReport(any())).thenReturn(INTERNAL_SUCCEEDED_REACH_REPORT)

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.listReports(request) }
        }

      val expected = listReportsResponse { reports.add(SUCCEEDED_REACH_REPORT) }

      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
        .isEqualTo(
          streamReportsRequest {
            limit = DEFAULT_PAGE_SIZE + 1
            this.filter = filter {
              measurementConsumerReferenceId =
                MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            }
          }
        )
      verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
        .isEqualTo(getMeasurementRequest { name = REACH_MEASUREMENT_NAME })
      verifyProtoArgument(
          internalMeasurementsMock,
          InternalMeasurementsCoroutineImplBase::setMeasurementResult
        )
        .usingDoubleTolerance(1e-12)
        .isEqualTo(
          setMeasurementResultRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
            this.result =
              InternalMeasurementKt.result {
                reach = InternalMeasurementResultKt.reach { value = REACH_VALUE }
                frequency =
                  InternalMeasurementResultKt.frequency {
                    relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
                  }
              }
          }
        )
      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[0]
          }
        )

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listReports returns an impression report with aggregated results`() = runBlocking {
    whenever(internalReportsMock.streamReports(any()))
      .thenReturn(flowOf(INTERNAL_PENDING_IMPRESSION_REPORT))
    whenever(measurementsMock.getMeasurement(any())).thenReturn(SUCCEEDED_IMPRESSION_MEASUREMENT)
    whenever(internalReportsMock.getReport(any())).thenReturn(INTERNAL_SUCCEEDED_IMPRESSION_REPORT)

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse { reports.add(SUCCEEDED_IMPRESSION_REPORT) }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )
    verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
      .isEqualTo(getMeasurementRequest { name = IMPRESSION_MEASUREMENT_NAME })
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::setMeasurementResult
      )
      .isEqualTo(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
          this.result =
            InternalMeasurementKt.result {
              impression = InternalMeasurementResultKt.impression { value = TOTAL_IMPRESSION_VALUE }
            }
        }
      )
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
      .isEqualTo(
        getInternalReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportId = REPORT_EXTERNAL_IDS[1]
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns a watch duration report with aggregated results`() = runBlocking {
    whenever(internalReportsMock.streamReports(any()))
      .thenReturn(flowOf(INTERNAL_PENDING_WATCH_DURATION_REPORT))
    whenever(measurementsMock.getMeasurement(any()))
      .thenReturn(SUCCEEDED_WATCH_DURATION_MEASUREMENT)
    whenever(internalReportsMock.getReport(any()))
      .thenReturn(INTERNAL_SUCCEEDED_WATCH_DURATION_REPORT)

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse { reports.add(SUCCEEDED_WATCH_DURATION_REPORT) }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          }
        }
      )
    verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
      .isEqualTo(getMeasurementRequest { name = WATCH_DURATION_MEASUREMENT_NAME })
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::setMeasurementResult
      )
      .isEqualTo(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
          this.result =
            InternalMeasurementKt.result {
              watchDuration =
                InternalMeasurementResultKt.watchDuration { value = TOTAL_WATCH_DURATION }
            }
        }
      )
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
      .isEqualTo(
        getInternalReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportId = REPORT_EXTERNAL_IDS[2]
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `getReport returns the report with SUCCEEDED when the report is already succeeded`() =
    runBlocking {
      whenever(internalReportsMock.getReport(any()))
        .thenReturn(INTERNAL_SUCCEEDED_WATCH_DURATION_REPORT)

      val request = getReportRequest { name = REPORT_NAMES[2] }

      val report =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getReport(request) }
        }

      assertThat(report).isEqualTo(SUCCEEDED_WATCH_DURATION_REPORT)

      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[2]
          }
        )
    }

  @Test
  fun `getReport returns the report with FAILED when the report is already failed`() = runBlocking {
    whenever(internalReportsMock.getReport(any()))
      .thenReturn(
        INTERNAL_PENDING_WATCH_DURATION_REPORT.copy { state = InternalReport.State.FAILED }
      )

    val request = getReportRequest { name = REPORT_NAMES[2] }

    val report =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.getReport(request) }
      }

    assertThat(report).isEqualTo(PENDING_WATCH_DURATION_REPORT.copy { state = Report.State.FAILED })

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
      .isEqualTo(
        getInternalReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportId = REPORT_EXTERNAL_IDS[2]
        }
      )
  }

  @Test
  fun `getReport returns the report with RUNNING when measurements are pending`(): Unit =
    runBlocking {
      whenever(internalReportsMock.getReport(any()))
        .thenReturn(INTERNAL_PENDING_WATCH_DURATION_REPORT)
      whenever(measurementsMock.getMeasurement(any()))
        .thenReturn(PENDING_WATCH_DURATION_MEASUREMENT)

      val request = getReportRequest { name = REPORT_NAMES[2] }

      val report =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getReport(request) }
        }

      assertThat(report).isEqualTo(PENDING_WATCH_DURATION_REPORT)

      verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
        .comparingExpectedFieldsOnly()
        .isEqualTo(getMeasurementRequest { name = WATCH_DURATION_MEASUREMENT_NAME })

      val internalReportCaptor: KArgumentCaptor<GetInternalReportRequest> = argumentCaptor()
      verifyBlocking(internalReportsMock, times(2)) { getReport(internalReportCaptor.capture()) }
      assertThat(internalReportCaptor.allValues)
        .containsExactly(
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[2]
          },
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[2]
          }
        )
    }

  @Test
  fun `getReport syncs and returns an SUCCEEDED report with aggregated results`(): Unit =
    runBlocking {
      whenever(measurementsMock.getMeasurement(any())).thenReturn(SUCCEEDED_IMPRESSION_MEASUREMENT)
      whenever(internalReportsMock.getReport(any()))
        .thenReturn(INTERNAL_PENDING_IMPRESSION_REPORT, INTERNAL_SUCCEEDED_IMPRESSION_REPORT)

      val request = getReportRequest { name = REPORT_NAMES[1] }

      val report =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getReport(request) }
        }

      assertThat(report).isEqualTo(SUCCEEDED_IMPRESSION_REPORT)

      verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
        .isEqualTo(getMeasurementRequest { name = IMPRESSION_MEASUREMENT_NAME })
      verifyProtoArgument(
          internalMeasurementsMock,
          InternalMeasurementsCoroutineImplBase::setMeasurementResult
        )
        .isEqualTo(
          setMeasurementResultRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
            this.result =
              InternalMeasurementKt.result {
                impression =
                  InternalMeasurementResultKt.impression { value = TOTAL_IMPRESSION_VALUE }
              }
          }
        )

      val internalReportCaptor: KArgumentCaptor<GetInternalReportRequest> = argumentCaptor()
      verifyBlocking(internalReportsMock, times(2)) { getReport(internalReportCaptor.capture()) }
      assertThat(internalReportCaptor.allValues)
        .containsExactly(
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[1]
          },
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[1]
          }
        )
    }

  @Test
  fun `getReport syncs and returns an FAILED report when measurements failed`(): Unit =
    runBlocking {
      whenever(measurementsMock.getMeasurement(any()))
        .thenReturn(
          BASE_IMPRESSION_MEASUREMENT.copy {
            state = Measurement.State.FAILED
            failure = failure {
              reason = Measurement.Failure.Reason.REQUISITION_REFUSED
              message = "Privacy budget exceeded."
            }
          }
        )
      whenever(internalReportsMock.getReport(any()))
        .thenReturn(
          INTERNAL_PENDING_IMPRESSION_REPORT,
          INTERNAL_PENDING_IMPRESSION_REPORT.copy { state = InternalReport.State.FAILED }
        )

      val request = getReportRequest { name = REPORT_NAMES[1] }

      val report =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getReport(request) }
        }

      assertThat(report).isEqualTo(PENDING_IMPRESSION_REPORT.copy { state = Report.State.FAILED })

      verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
        .isEqualTo(getMeasurementRequest { name = IMPRESSION_MEASUREMENT_NAME })
      verifyProtoArgument(
          internalMeasurementsMock,
          InternalMeasurementsCoroutineImplBase::setMeasurementFailure
        )
        .isEqualTo(
          setMeasurementFailureRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
            failure =
              InternalMeasurementKt.failure {
                reason = InternalMeasurement.Failure.Reason.REQUISITION_REFUSED
                message = "Privacy budget exceeded."
              }
          }
        )

      val internalReportCaptor: KArgumentCaptor<GetInternalReportRequest> = argumentCaptor()
      verifyBlocking(internalReportsMock, times(2)) { getReport(internalReportCaptor.capture()) }
      assertThat(internalReportCaptor.allValues)
        .containsExactly(
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[1]
          },
          getInternalReportRequest {
            measurementConsumerReferenceId =
              MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportId = REPORT_EXTERNAL_IDS[1]
          }
        )
    }

  @Test
  fun `getReport throws INVALID_ARGUMENT when Report name is invalid`() {
    val request = getReportRequest { name = INVALID_REPORT_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getReport(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getReport throws PERMISSION_DENIED when MeasurementConsumer's identity does not match`() {
    val request = getReportRequest { name = REPORT_NAMES[0] }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.last().name, CONFIG) {
          runBlocking { service.getReport(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getReport throws UNAUTHENTICATED when the caller is not a MeasurementConsumer`() {
    val request = getReportRequest { name = REPORT_NAMES[0] }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDERS.values.first().name) {
          runBlocking { service.getReport(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getReport throws PERMISSION_DENIED when encryption private key not found`() = runBlocking {
    whenever(internalReportsMock.getReport(any()))
      .thenReturn(INTERNAL_PENDING_WATCH_DURATION_REPORT)

    whenever(measurementsMock.getMeasurement(any()))
      .thenReturn(
        SUCCEEDED_WATCH_DURATION_MEASUREMENT.copy {
          val measurementSpec = measurementSpec {
            measurementPublicKey =
              MEASUREMENT_CONSUMER_PUBLIC_KEY.copy { data = INVALID_MEASUREMENT_PUBLIC_KEY_DATA }
                .toByteString()
          }
          this.measurementSpec =
            signMeasurementSpec(measurementSpec, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
        }
      )

    val request = getReportRequest { name = REPORT_NAMES[2] }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getReport(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description).contains("private key")
  }

  @Test
  fun `getReport throws Exception when the internal GetReport throws Exception`() = runBlocking {
    whenever(internalReportsMock.getReport(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = getReportRequest { name = REPORT_NAMES[2] }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.getReport(request) }
        }
      }
    val expectedExceptionDescription = "Unable to get the report from the reporting database."
    assertThat(exception.message).isEqualTo(expectedExceptionDescription)
  }

  @Test
  fun `toResult converts internal result to external result with the same content`() = runBlocking {
    val internalResult =
      InternalReportDetailsKt.result {
        scalarTable =
          InternalReportResultKt.scalarTable {
            rowHeaders += listOf("row1", "row2", "row3")
            columns +=
              InternalReportResultKt.column {
                columnHeader = "column1"
                setOperations += listOf(1.0, 2.0, 3.0)
              }
          }
        histogramTables +=
          InternalReportResultKt.histogramTable {
            rows +=
              InternalReportResultKt.HistogramTableKt.row {
                rowHeader = "row4"
                frequency = 100
              }
            rows +=
              InternalReportResultKt.HistogramTableKt.row {
                rowHeader = "row5"
                frequency = 101
              }
            columns +=
              InternalReportResultKt.column {
                columnHeader = "column1"
                setOperations += listOf(10.0, 11.0, 12.0)
              }
            columns +=
              InternalReportResultKt.column {
                columnHeader = "column2"
                setOperations += listOf(20.0, 21.0, 22.0)
              }
          }
      }

    whenever(internalReportsMock.getReport(any()))
      .thenReturn(
        INTERNAL_SUCCEEDED_WATCH_DURATION_REPORT.copy {
          details = InternalReportKt.details { result = internalResult }
        }
      )

    val request = getReportRequest { name = REPORT_NAMES[2] }

    val report =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.getReport(request) }
      }

    assertThat(report.result)
      .isEqualTo(
        ReportKt.result {
          scalarTable = scalarTable {
            rowHeaders += listOf("row1", "row2", "row3")
            columns += column {
              columnHeader = "column1"
              setOperations += listOf(1.0, 2.0, 3.0)
            }
          }
          histogramTables += histogramTable {
            rows += row {
              rowHeader = "row4"
              frequency = 100
            }
            rows += row {
              rowHeader = "row5"
              frequency = 101
            }
            columns += column {
              columnHeader = "column1"
              setOperations += listOf(10.0, 11.0, 12.0)
            }
            columns += column {
              columnHeader = "column2"
              setOperations += listOf(20.0, 21.0, 22.0)
            }
          }
        }
      )
  }
}

private fun EventGroupKey.toInternal(): InternalReportingSet.EventGroupKey {
  val source = this
  return InternalReportingSetKt.eventGroupKey {
    measurementConsumerReferenceId = source.measurementConsumerReferenceId
    dataProviderReferenceId = source.dataProviderReferenceId
    eventGroupReferenceId = source.eventGroupReferenceId
  }
}

private val InternalReportingSet.resourceKey: ReportingSetKey
  get() =
    ReportingSetKey(measurementConsumerReferenceId, ExternalId(externalReportingSetId).apiId.value)
private val InternalReportingSet.resourceName: String
  get() = resourceKey.toName()
