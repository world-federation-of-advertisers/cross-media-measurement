// Copyright 2023 The Cross-Media Measurement Authors
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

import org.wfanet.measurement.api.v2alpha.timeInterval as measurementTimeInterval
import com.google.common.truth.Truth
import com.google.common.truth.extensions.proto.ProtoTruth
import java.time.Duration
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.timestamp
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Paths
import java.security.SecureRandom
import java.security.cert.X509Certificate
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
import org.wfanet.measurement.api.v2.alpha.ListReportsPageTokenKt
import org.wfanet.measurement.api.v2.alpha.listReportsPageToken
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
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
import org.wfanet.measurement.internal.reporting.v2alpha.getMetricByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.v2alpha.GetMetricRequest
import org.wfanet.measurement.internal.reporting.v2alpha.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricResultKt as InternalMetricResultKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.internal.reporting.v2alpha.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2alpha.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricsGrpcKt
import org.wfanet.measurement.internal.reporting.v2alpha.StreamMetricsRequestKt
import org.wfanet.measurement.internal.reporting.v2alpha.batchGetReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2alpha.copy
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSet.SetExpression as InternalSetExpression
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt as InternalReportingSetKt
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.SetExpressionKt.operand as internalOperand
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.composite as internalComposite
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.primitive as internalPrimitive
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.setExpression as internalSetExpression
import org.wfanet.measurement.internal.reporting.v2alpha.getMetricByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.v2alpha.getMetricRequest
import org.wfanet.measurement.internal.reporting.v2alpha.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.metric as internalMetric
import org.wfanet.measurement.internal.reporting.v2alpha.metricResult as internalMetricResult
import org.wfanet.measurement.internal.reporting.v2alpha.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2alpha.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.v2alpha.setMeasurementFailureRequest
import org.wfanet.measurement.internal.reporting.v2alpha.setMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.v2alpha.streamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2alpha.timeInterval as internalTimeInterval
import org.wfanet.measurement.internal.reporting.v2alpha.timeIntervals as internalTimeIntervals
import com.google.protobuf.Timestamp
import com.google.protobuf.duration
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.TimeInterval
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.internal.reporting.v2alpha.MetricKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.primitiveReportingSetBasis
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricsService
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.ListMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.integerResult
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.frequencyHistogramParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.impressionCountParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.reachParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.watchDurationParams
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.getMetricRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.listMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.timeInterval
import org.wfanet.measurement.reporting.v2alpha.timeIntervals
import sun.jvm.hotspot.oops.CellTypeState.value


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

private val EVENT_GROUP_KEYS =
  DATA_PROVIDERS.keys.mapIndexed { index, dataProviderKey ->
    val measurementConsumerKey = MEASUREMENT_CONSUMERS.keys.first()
    EventGroupKey(
      measurementConsumerKey.measurementConsumerId,
      dataProviderKey.dataProviderId,
      ExternalId(index + 660L).apiId.value
    )
  }

// Event filters
private const val INCREMENTAL_REPORTING_SET_FILTER = "AGE>18"
private const val METRIC_FILTER = "media_type==video"
private const val PRIMITIVE_REPORTING_SET_FILTER = "gender==male"

// Internal reporting sets

private val INTERNAL_SINGLE_PUBLISHER_REPORTING_SETS =
  EVENT_GROUP_KEYS.mapIndexed { index, eventGroupKey ->
    internalReportingSet {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
      externalReportingSetId = index + 220L
      this.primitive = internalPrimitive {
        eventGroupKeys += eventGroupKey.toInternal()
      }
      displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
    }
  }
private val INTERNAL_UNION_ALL_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SETS.last().externalReportingSetId + 1
  this.primitive = internalPrimitive {
    eventGroupKeys += EVENT_GROUP_KEYS.map {it.toInternal()}
  }
  filter = PRIMITIVE_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
}
private val INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SETS.last().externalReportingSetId + 1
  this.primitive = internalPrimitive {
    (0 until EVENT_GROUP_KEYS.size - 1).map { i ->
      eventGroupKeys += EVENT_GROUP_KEYS[i].toInternal()
    }
  }
  filter = PRIMITIVE_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
}

private val INTERNAL_INCREMENTAL_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SETS.last().externalReportingSetId + 1
  this.composite = internalComposite {
    expression = internalSetExpression {
      operation = InternalSetExpression.Operation.DIFFERENCE
      lhs = internalOperand {
        externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
      }
      rhs = internalOperand {
        externalReportingSetId = INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
      }
    }
  }
  filter = INCREMENTAL_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
}

// Reporting set IDs and names

private const val REPORTING_SET_EXTERNAL_ID_FOR_MC_2 = 241L
private val REPORTING_SET_NAME_FOR_MC_2 =
  ReportingSetKey(
    MEASUREMENT_CONSUMERS.keys.last().measurementConsumerId,
    externalIdToApiId(REPORTING_SET_EXTERNAL_ID_FOR_MC_2)
  )
    .toName()
private const val INVALID_REPORTING_SET_NAME = "INVALID_REPORTING_SET_NAME"

// Time intervals

private val START_INSTANT = Instant.now()
private val END_INSTANT = START_INSTANT.plus(Duration.ofDays(1))

private val START_TIME: Timestamp = START_INSTANT.toProtoTime()
private val END_TIME = END_INSTANT.toProtoTime()
private val MEASUREMENT_TIME_INTERVAL = measurementTimeInterval {
  startTime = START_TIME
  endTime = END_TIME
}
private val INTERNAL_TIME_INTERVAL = internalTimeInterval {
  startTime = START_TIME
  endTime = END_TIME
}
private val TIME_INTERVAL = timeInterval {
  startTime = START_TIME
  endTime = END_TIME
}

// Event group entries for CMMs measurements
private val EVENT_GROUP_ENTRIES =
  EVENT_GROUP_KEYS.groupBy(
    { DataProviderKey(it.cmmsDataProviderId) },
    {
      RequisitionSpecKt.eventGroupEntry {
        key = it.toName()
        value =
          RequisitionSpecKt.EventGroupEntryKt.value {
            collectionInterval = MEASUREMENT_TIME_INTERVAL
            filter = RequisitionSpecKt.eventFilter {
              expression = "($INCREMENTAL_REPORTING_SET_FILTER) AND ($METRIC_FILTER) AND ($PRIMITIVE_REPORTING_SET_FILTER)"
            }
          }
      }
    }
  )
//
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
    MeasurementKt.dataProviderEntry {
      key = dataProvider.name
      value =
        MeasurementKt.DataProviderEntryKt.value {
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
private const val UNION_ALL_REACH_VALUE = 100_000L
private const val UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE = 70_000L
private const val INCREMENTAL_REACH_VALUE = UNION_ALL_REACH_VALUE - UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE
private val FREQUENCY_DISTRIBUTION = mapOf(1L to 1.0 / 6, 2L to 2.0 / 6, 3L to 3.0 / 6)
private val IMPRESSION_VALUES = listOf(100L, 150L)
private val TOTAL_IMPRESSION_VALUE = IMPRESSION_VALUES.sum()
private val WATCH_DURATION_SECOND_LIST = listOf(100L, 200L)
private val WATCH_DURATION_LIST = WATCH_DURATION_SECOND_LIST.map { duration { seconds = it } }
private val TOTAL_WATCH_DURATION = duration { seconds = WATCH_DURATION_SECOND_LIST.sum() }

// Reach measurements
// Internal reach measurements
private val INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  cmmsMeasurementId = externalIdToApiId(401L)
  externalMeasurementId = 411L
  cmmsCreateMeasurementRequestId = "UNION_ALL_REACH_MEASUREMENT"
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
    filters += INCREMENTAL_REPORTING_SET_FILTER
  }
  state = InternalMeasurement.State.PENDING
}
private val INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  cmmsMeasurementId = externalIdToApiId(402L)
  externalMeasurementId = 412L
  cmmsCreateMeasurementRequestId = "UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT"
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
    filters += INCREMENTAL_REPORTING_SET_FILTER
  }
  state = InternalMeasurement.State.PENDING
}

private val INTERNAL_SUCCEEDED_UNION_ALL_REACH_MEASUREMENT =
  INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    result =
      InternalMeasurementKt.result {
        reach = InternalMeasurementKt.ResultKt.reach { value = UNION_ALL_REACH_VALUE }
        frequency =
          InternalMeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
          }
      }
  }
private val INTERNAL_SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
    state = InternalMeasurement.State.SUCCEEDED
    result =
      InternalMeasurementKt.result {
        reach = InternalMeasurementKt.ResultKt.reach { value = UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE }
        frequency =
          InternalMeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
          }
      }
  }

// CMMs reach measurements
private val BASE_UNION_ALL_REACH_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    name = MeasurementKey(
      MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
      INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
    )
      .toName()
    measurementReferenceId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
  }
private val BASE_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    name = MeasurementKey(
      MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
      INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
    )
      .toName()
    measurementReferenceId = INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_UNION_ALL_REACH_MEASUREMENT =
  BASE_UNION_ALL_REACH_MEASUREMENT.copy { state = Measurement.State.COMPUTING }
private val PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  BASE_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy { state = Measurement.State.COMPUTING }

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
  vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval {
    start = REACH_ONLY_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
    width = REACH_ONLY_VID_SAMPLING_WIDTH
  }
}

private val SUCCEEDED_UNION_ALL_REACH_MEASUREMENT =
  BASE_UNION_ALL_REACH_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDERS.keys.map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(REACH_ONLY_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)

    state = Measurement.State.SUCCEEDED

    results += MeasurementKt.resultPair {
      val result =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = UNION_ALL_REACH_VALUE }
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
private val SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  BASE_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDERS.keys.take(2).map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(REACH_ONLY_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)

    state = Measurement.State.SUCCEEDED

    results += MeasurementKt.resultPair {
      val result =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE }
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


// // Frequency histogram measurement
// private val BASE_REACH_FREQUENCY_HISTOGRAM_MEASUREMENT =
//   BASE_MEASUREMENT.copy {
//     name = FREQUENCY_HISTOGRAM_MEASUREMENT_NAME
//     measurementReferenceId = FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID
//   }
//
// private val REACH_FREQUENCY_MEASUREMENT_SPEC = measurementSpec {
//   measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.toByteString()
//
//   nonceHashes.addAll(
//     listOf(hashSha256(SECURE_RANDOM_OUTPUT_LONG), hashSha256(SECURE_RANDOM_OUTPUT_LONG))
//   )
//
//   reachAndFrequency =
//     MeasurementSpecKt.reachAndFrequency {
//       reachPrivacyParams = differentialPrivacyParams {
//         epsilon = REACH_FREQUENCY_REACH_EPSILON
//         delta = DIFFERENTIAL_PRIVACY_DELTA
//       }
//       frequencyPrivacyParams = differentialPrivacyParams {
//         epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
//         delta = DIFFERENTIAL_PRIVACY_DELTA
//       }
//       maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
//     }
//   vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval {
//     start = REACH_FREQUENCY_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
//     width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
//   }
// }
//
// private val SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT =
//   BASE_REACH_FREQUENCY_HISTOGRAM_MEASUREMENT.copy {
//     dataProviders += DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }
//
//     measurementSpec =
//       signMeasurementSpec(REACH_FREQUENCY_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
//
//     state = org.wfanet.measurement.api.v2alpha.Measurement.State.SUCCEEDED
//     results += MeasurementKt.resultPair {
//       val result =
//         MeasurementKt.result {
//           reach = MeasurementKt.ResultKt.reach { value = REACH_VALUE }
//           frequency =
//             MeasurementKt.ResultKt.frequency {
//               relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
//             }
//         }
//       encryptedResult =
//         encryptResult(signResult(result, AGGREGATOR_SIGNING_KEY), MEASUREMENT_CONSUMER_PUBLIC_KEY)
//       certificate = AGGREGATOR_CERTIFICATE.name
//     }
//   }
//
// private val INTERNAL_PENDING_FREQUENCY_HISTOGRAM_MEASUREMENT = internalMeasurement {
//   cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//   cmmsMeasurementId = FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID
//   state = Measurement.State.PENDING
// }
//
// private val INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT =
//   INTERNAL_PENDING_FREQUENCY_HISTOGRAM_MEASUREMENT.copy {
//     state = Measurement.State.SUCCEEDED
//     result =
//       InternalMeasurementKt.result {
//         reach = InternalMeasurementKt.ResultKt.reach { value = REACH_VALUE }
//         frequency =
//           InternalMeasurementKt.ResultKt.frequency {
//             relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
//           }
//       }
//   }
//
// // Impression measurement
// private val BASE_IMPRESSION_MEASUREMENT =
//   BASE_MEASUREMENT.copy {
//     name = IMPRESSION_MEASUREMENT_NAME
//     measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
//   }
//
// private val IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
//   measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.toByteString()
//
//   nonceHashes.addAll(
//     listOf(hashSha256(SECURE_RANDOM_OUTPUT_LONG), hashSha256(SECURE_RANDOM_OUTPUT_LONG))
//   )
//
//   impression =
//     MeasurementSpecKt.impression {
//       privacyParams = differentialPrivacyParams {
//         epsilon = IMPRESSION_EPSILON
//         delta = DIFFERENTIAL_PRIVACY_DELTA
//       }
//       maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
//     }
//   vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval {
//     start = IMPRESSION_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
//     width = IMPRESSION_VID_SAMPLING_WIDTH
//   }
// }
//
// private val SUCCEEDED_IMPRESSION_MEASUREMENT =
//   BASE_IMPRESSION_MEASUREMENT.copy {
//     dataProviders += DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }
//
//     measurementSpec =
//       signMeasurementSpec(IMPRESSION_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
//
//     state = org.wfanet.measurement.api.v2alpha.Measurement.State.SUCCEEDED
//
//     results +=
//       DATA_PROVIDER_KEYS_IN_SET_OPERATION.zip(IMPRESSION_VALUES).map {
//           (dataProviderKey, numImpressions) ->
//         val dataProvider = DATA_PROVIDERS.getValue(dataProviderKey)
//         MeasurementKt.resultPair {
//           val result =
//             MeasurementKt.result {
//               impression = MeasurementKt.ResultKt.impression { value = numImpressions }
//             }
//           encryptedResult =
//             encryptResult(
//               signResult(result, DATA_PROVIDER_SIGNING_KEY),
//               MEASUREMENT_CONSUMER_PUBLIC_KEY
//             )
//           certificate = dataProvider.certificate
//         }
//       }
//   }
//
// private val INTERNAL_PENDING_IMPRESSION_MEASUREMENT = internalMeasurement {
//   cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//   cmmsMeasurementId = IMPRESSION_MEASUREMENT_REFERENCE_ID
//   state = Measurement.State.PENDING
// }
//
// private val INTERNAL_SUCCEEDED_IMPRESSION_MEASUREMENT =
//   INTERNAL_PENDING_IMPRESSION_MEASUREMENT.copy {
//     state = Measurement.State.SUCCEEDED
//     result =
//       InternalMeasurementKt.result {
//         impression = InternalMeasurementKt.ResultKt.impression { value = TOTAL_IMPRESSION_VALUE }
//       }
//   }
//
// // Watch Duration measurement
// private val BASE_WATCH_DURATION_MEASUREMENT =
//   BASE_MEASUREMENT.copy {
//     name = WATCH_DURATION_MEASUREMENT_NAME
//     measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
//   }
//
// private val PENDING_WATCH_DURATION_MEASUREMENT =
//   BASE_WATCH_DURATION_MEASUREMENT.copy { state = Measurement.State.COMPUTING }
//
// private val WATCH_DURATION_MEASUREMENT_SPEC = measurementSpec {
//   measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.toByteString()
//
//   nonceHashes.addAll(
//     listOf(hashSha256(SECURE_RANDOM_OUTPUT_LONG), hashSha256(SECURE_RANDOM_OUTPUT_LONG))
//   )
//
//   duration =
//     MeasurementSpecKt.duration {
//       privacyParams = differentialPrivacyParams {
//         epsilon = WATCH_DURATION_EPSILON
//         delta = DIFFERENTIAL_PRIVACY_DELTA
//       }
//       maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
//       maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
//     }
//   vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval {
//     start = WATCH_DURATION_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
//     width = WATCH_DURATION_VID_SAMPLING_WIDTH
//   }
// }
//
// private val SUCCEEDED_WATCH_DURATION_MEASUREMENT =
//   BASE_WATCH_DURATION_MEASUREMENT.copy {
//     dataProviders += DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }
//
//     measurementSpec =
//       signMeasurementSpec(WATCH_DURATION_MEASUREMENT_SPEC, MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE)
//
//     state = org.wfanet.measurement.api.v2alpha.Measurement.State.SUCCEEDED
//
//     results +=
//       DATA_PROVIDER_KEYS_IN_SET_OPERATION.zip(WATCH_DURATION_LIST).map {
//           (dataProviderKey, watchDuration) ->
//         val dataProvider = DATA_PROVIDERS.getValue(dataProviderKey)
//         MeasurementKt.resultPair {
//           val result =
//             MeasurementKt.result {
//               this.watchDuration = MeasurementKt.ResultKt.watchDuration { value = watchDuration }
//             }
//           encryptedResult =
//             encryptResult(
//               signResult(result, DATA_PROVIDER_SIGNING_KEY),
//               MEASUREMENT_CONSUMER_PUBLIC_KEY
//             )
//           certificate = dataProvider.certificate
//         }
//       }
//   }
//
// private val INTERNAL_PENDING_WATCH_DURATION_MEASUREMENT = internalMeasurement {
//   cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//   cmmsMeasurementId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
//   state = Measurement.State.PENDING
// }
// private val INTERNAL_SUCCEEDED_WATCH_DURATION_MEASUREMENT =
//   INTERNAL_PENDING_WATCH_DURATION_MEASUREMENT.copy {
//     state = Measurement.State.SUCCEEDED
//     result =
//       InternalMeasurementKt.result {
//         watchDuration = InternalMeasurementKt.ResultKt.watchDuration { value = TOTAL_WATCH_DURATION }
//       }
//   }
//


// Metric Specs

private const val MAXIMUM_FREQUENCY_PER_USER = 10
private const val MAXIMUM_WATCH_DURATION_PER_USER = 300

private val REACH_METRIC_SPEC: MetricSpec = metricSpec {
  reach = reachParams{}
}
private val FREQUENCY_HISTOGRAM_METRIC_SPEC: MetricSpec = metricSpec {
  frequencyHistogram = frequencyHistogramParams{
    maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
  }
}
private val IMPRESSION_COUNT_METRIC_SPEC: MetricSpec = metricSpec {
  impressionCount = impressionCountParams{
    maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
  }
}
private val WATCH_DURATION_METRIC_SPEC: MetricSpec = metricSpec {
  watchDuration = watchDurationParams{
    maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
  }
}

// Metrics

// Metric idempotency keys
private const val INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY = "TEST_INCREMENTAL_REACH_METRIC"
// private const val IMPRESSION_METRIC_IDEMPOTENCY_KEY = "TEST_IMPRESSION_METRIC"
// private const val WATCH_DURATION_METRIC_IDEMPOTENCY_KEY = "TEST_WATCH_DURATION_METRIC"
// private const val FREQUENCY_HISTOGRAM_METRIC_IDEMPOTENCY_KEY = "TEST_FREQUENCY_HISTOGRAM_METRIC"

// Typo causes invalid name
private const val INVALID_METRIC_NAME = "measurementConsumer/AAAAAAAAAG8/metric/AAAAAAAAAU0"

// Internal Metrics
private val PENDING_INTERNAL_INCREMENTAL_REACH_METRIC = internalMetric {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalMetricId = 331L
  metricIdempotencyKey = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
  createTime = Instant.now().toProtoTime()
  externalReportingSetId = INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId
  timeInterval = INTERNAL_TIME_INTERVAL
  metricSpec = internalMetricSpec {
    reach = InternalMetricSpecKt.reachParams{}
  }
  weightedMeasurements += weightedMeasurement {
    weight = 1
    measurement = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT
  }
  weightedMeasurements += weightedMeasurement {
    weight = -1
    measurement = INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
  }
  state = InternalMetric.State.RUNNING
  details = InternalMetricKt.details {
    filters += listOf(METRIC_FILTER)
  }
}

private val SUCCEEDED_INTERNAL_INCREMENTAL_REACH_METRIC = PENDING_INTERNAL_INCREMENTAL_REACH_METRIC.copy {
  state = InternalMetric.State.SUCCEEDED
  details = InternalMetricKt.details {
    filters += this@copy.details.filtersList
    result = internalMetricResult {
      reach = InternalMetricResultKt.integerResult { value = INCREMENTAL_REACH_VALUE}
    }
  }
}

// Public Metrics
private val PENDING_INCREMENTAL_REACH_METRIC = metric {
  name = MetricKey(MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId, externalIdToApiId(PENDING_INTERNAL_INCREMENTAL_REACH_METRIC.externalMetricId))
    .toName()
  reportingSet = INTERNAL_INCREMENTAL_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = REACH_METRIC_SPEC
  filters += PENDING_INTERNAL_INCREMENTAL_REACH_METRIC.details.filtersList
  state = Metric.State.RUNNING
  createTime = PENDING_INTERNAL_INCREMENTAL_REACH_METRIC.createTime
}

private val METRIC_WITH_INVALID_REPORTING_SET = metric {
  name = MetricKey(MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId, externalIdToApiId(332L))
    .toName()
  reportingSet = INVALID_REPORTING_SET_NAME
  timeInterval = TIME_INTERVAL
  metricSpec = REACH_METRIC_SPEC
  filters += listOf(METRIC_FILTER)
  createTime = Instant.now().toProtoTime()
}

private val SUCCEEDED_INCREMENTAL_REACH_METRIC = PENDING_INCREMENTAL_REACH_METRIC.copy {
  state = Metric.State.SUCCEEDED
  result = metricResult {
    reach = integerResult { value = SUCCEEDED_INTERNAL_INCREMENTAL_REACH_METRIC.details.result.reach.value }
  }
}

// @RunWith(JUnit4::class)
// class MetricsServiceTest {
//
//   private val internalMetricsMock: MetricsGrpcKt.MetricsCoroutineImplBase = mockService {
//     onBlocking { createMetric(any()) }
//       .thenReturn(
//         INTERNAL_PENDING_REACH_METRIC,
//         INTERNAL_PENDING_IMPRESSION_METRIC,
//         INTERNAL_PENDING_WATCH_DURATION_METRIC,
//         INTERNAL_PENDING_FREQUENCY_HISTOGRAM_METRIC,
//       )
//     onBlocking { streamMetrics(any()) }
//       .thenReturn(
//         flowOf(
//           INTERNAL_PENDING_REACH_METRIC,
//           INTERNAL_PENDING_IMPRESSION_METRIC,
//           INTERNAL_PENDING_WATCH_DURATION_METRIC,
//           INTERNAL_PENDING_FREQUENCY_HISTOGRAM_METRIC,
//         )
//       )
//     onBlocking { getMetric(any()) }
//       .thenReturn(
//         INTERNAL_SUCCEEDED_REACH_METRIC,
//         INTERNAL_SUCCEEDED_IMPRESSION_METRIC,
//         INTERNAL_SUCCEEDED_WATCH_DURATION_METRIC,
//         INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_METRIC,
//       )
//     onBlocking { getMetricByIdempotencyKey(any()) }
//       .thenThrow(StatusRuntimeException(Status.NOT_FOUND))
//   }
//
//   private val internalReportingSetsMock: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase =
//     mockService {
//       onBlocking { batchGetReportingSet(any()) }
//         .thenReturn(
//           flowOf(
//             INTERNAL_REPORTING_SETS[0],
//             INTERNAL_REPORTING_SETS[1],
//             INTERNAL_REPORTING_SETS[0],
//             INTERNAL_REPORTING_SETS[1]
//           )
//         )
//     }
//
//   private val measurementsMock: MeasurementsCoroutineImplBase =
//     mockService {
//       onBlocking { getMeasurement(any()) }
//         .thenReturn(
//           SUCCEEDED_REACH_MEASUREMENT,
//           SUCCEEDED_IMPRESSION_MEASUREMENT,
//           SUCCEEDED_WATCH_DURATION_MEASUREMENT,
//           SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT,
//         )
//
//       onBlocking { createMeasurement(any()) }.thenReturn(BASE_REACH_MEASUREMENT)
//     }
//
//   private val measurementConsumersMock: MeasurementConsumersCoroutineImplBase =
//     mockService {
//       onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMERS.values.first())
//     }
//
//   private val dataProvidersMock: DataProvidersCoroutineImplBase = mockService {
//     var stubbing = onBlocking { getDataProvider(any()) }
//     for (dataProvider in DATA_PROVIDERS.values) {
//       stubbing = stubbing.thenReturn(dataProvider)
//     }
//   }
//
//   private val certificateMock: CertificatesCoroutineImplBase = mockService {
//     onBlocking { getCertificate(eq(getCertificateRequest { name = AGGREGATOR_CERTIFICATE.name })) }
//       .thenReturn(AGGREGATOR_CERTIFICATE)
//     for (dataProvider in DATA_PROVIDERS.values) {
//       onBlocking { getCertificate(eq(getCertificateRequest { name = dataProvider.certificate })) }
//         .thenReturn(
//           certificate {
//             name = dataProvider.certificate
//             x509Der = DATA_PROVIDER_SIGNING_KEY.certificate.encoded.toByteString()
//           }
//         )
//     }
//     for (measurementConsumer in MEASUREMENT_CONSUMERS.values) {
//       onBlocking {
//         getCertificate(eq(getCertificateRequest { name = measurementConsumer.certificate }))
//       }
//         .thenReturn(
//           certificate {
//             name = measurementConsumer.certificate
//             x509Der = measurementConsumer.certificateDer
//           }
//         )
//     }
//   }
//
//   private val secureRandomMock: SecureRandom = mock()
//
//   @get:Rule
//   val grpcTestServerRule = GrpcTestServerRule {
//     addService(internalMetricsMock)
//     addService(internalReportingSetsMock)
//     addService(internalMeasurementsMock)
//     addService(measurementsMock)
//     addService(measurementConsumersMock)
//     addService(dataProvidersMock)
//     addService(certificateMock)
//   }
//
//   private lateinit var service: MetricsService
//
//   @Before
//   fun initService() {
//     secureRandomMock.stub {
//       on { nextInt(any()) } doReturn SECURE_RANDOM_OUTPUT_INT
//       on { nextLong() } doReturn SECURE_RANDOM_OUTPUT_LONG
//     }
//
//     service =
//       MetricsService(
//         MetricsCoroutineStub(grpcTestServerRule.channel),
//         ReportingSetsCoroutineStub(grpcTestServerRule.channel),
//         MeasurementsCoroutineStub(grpcTestServerRule.channel),
//         DataProvidersCoroutineStub(grpcTestServerRule.channel),
//         MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
//         MeasurementsCoroutineStub(
//           grpcTestServerRule.channel
//         ),
//         CertificatesCoroutineStub(grpcTestServerRule.channel),
//         ENCRYPTION_KEY_PAIR_STORE,
//         secureRandomMock,
//         org.wfanet.measurement.reporting.service.api.v1alpha.SECRETS_DIR,
//         listOf(AGGREGATOR_ROOT_CERTIFICATE, DATA_PROVIDER_ROOT_CERTIFICATE).associateBy {
//           it.subjectKeyIdentifier!!
//         }
//       )
//   }
//
//   @Test
//   fun `createMetric returns a metric of reach over a primitive reporting set with RUNNING state`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//
//     val result =
//       org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//         MEASUREMENT_CONSUMERS.values.first().name,
//         CONFIG
//       ) {
//         runBlocking { service.createMetric(request) }
//       }
//
//     val expected = PENDING_REACH_REPORT
//
//     // Verify proto argument of ReportsCoroutineImplBase::getReportByIdempotencyKey
//     verifyProtoArgument(
//       internalReportsMock,
//       ReportsCoroutineImplBase::getReportByIdempotencyKey
//     )
//       .isEqualTo(
//         getReportByIdempotencyKeyRequest {
//           measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//           reportIdempotencyKey = REACH_REPORT_IDEMPOTENCY_KEY
//         }
//       )
//
//     // Verify proto argument of InternalReportingSetsCoroutineImplBase::batchGetReportingSet
//     verifyProtoArgument(
//       internalReportingSetsMock,
//       ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::batchGetReportingSet
//     )
//       .isEqualTo(
//         batchGetReportingSetRequest {
//           measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//           externalReportingSetIds += INTERNAL_REPORTING_SETS[0].externalReportingSetId
//           externalReportingSetIds += INTERNAL_REPORTING_SETS[1].externalReportingSetId
//         }
//       )
//
//     // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
//     verifyProtoArgument(
//       measurementConsumersMock,
//       MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
//     )
//       .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })
//
//     // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
//     val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
//     verifyBlocking(dataProvidersMock, times(2)) { getDataProvider(dataProvidersCaptor.capture()) }
//     val capturedDataProviderRequests = dataProvidersCaptor.allValues
//     ProtoTruth.assertThat(capturedDataProviderRequests)
//       .containsExactly(
//         getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
//         getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name }
//       )
//
//     // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
//     val capturedMeasurementRequest =
//       captureFirst<CreateMeasurementRequest> {
//         runBlocking { verify(measurementsMock).createMeasurement(capture()) }
//       }
//     val capturedMeasurement = capturedMeasurementRequest.measurement
//     val expectedMeasurement =
//       BASE_REACH_MEASUREMENT.copy {
//         dataProviders +=
//           DATA_PROVIDER_KEYS_IN_SET_OPERATION.map { DATA_PROVIDER_ENTRIES.getValue(it) }
//         measurementSpec =
//           signMeasurementSpec(
//             org.wfanet.measurement.reporting.service.api.v1alpha.REACH_ONLY_MEASUREMENT_SPEC,
//             MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
//           )
//       }
//
//     ProtoTruth.assertThat(capturedMeasurement)
//       .ignoringRepeatedFieldOrder()
//       .ignoringFieldDescriptors(
//         org.wfanet.measurement.api.v2alpha.Measurement.getDescriptor()
//           .findFieldByNumber(org.wfanet.measurement.api.v2alpha.Measurement.MEASUREMENT_SPEC_FIELD_NUMBER),
//         org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry.Value.getDescriptor()
//           .findFieldByNumber(org.wfanet.measurement.api.v2alpha.Measurement.DataProviderEntry.Value.ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER),
//       )
//       .isEqualTo(expectedMeasurement)
//
//     verifyMeasurementSpec(
//       capturedMeasurement.measurementSpec,
//       MEASUREMENT_CONSUMER_CERTIFICATE,
//       TRUSTED_MEASUREMENT_CONSUMER_ISSUER
//     )
//     val measurementSpec = MeasurementSpec.parseFrom(capturedMeasurement.measurementSpec.data)
//     ProtoTruth.assertThat(measurementSpec)
//       .isEqualTo(org.wfanet.measurement.reporting.service.api.v1alpha.REACH_ONLY_MEASUREMENT_SPEC)
//
//     val dataProvidersList = capturedMeasurement.dataProvidersList.sortedBy { it.key }
//
//     dataProvidersList.map { dataProviderEntry ->
//       val signedRequisitionSpec =
//         decryptRequisitionSpec(
//           dataProviderEntry.value.encryptedRequisitionSpec,
//           DATA_PROVIDER_PRIVATE_KEY_HANDLE
//         )
//       val requisitionSpec = RequisitionSpec.parseFrom(signedRequisitionSpec.data)
//       verifyRequisitionSpec(
//         signedRequisitionSpec,
//         requisitionSpec,
//         measurementSpec,
//         MEASUREMENT_CONSUMER_CERTIFICATE,
//         TRUSTED_MEASUREMENT_CONSUMER_ISSUER
//       )
//     }
//
//     // Verify proto argument of InternalMeasurementsCoroutineImplBase::createMeasurement
//     verifyProtoArgument(
//       internalMeasurementsMock,
//       MeasurementsGrpcKt.MeasurementsCoroutineImplBase::createMeasurement
//     )
//       .isEqualTo(
//         measurement {
//           measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//           measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
//           state = Measurement.State.PENDING
//         }
//       )
//
//     // Verify proto argument of InternalReportsCoroutineImplBase::createMetric
//     verifyProtoArgument(internalReportsMock, ReportsGrpcKt.ReportsCoroutineImplBase::createMetric)
//       .ignoringRepeatedFieldOrder()
//       .isEqualTo(
//         createReportRequest {
//           report =
//             INTERNAL_PENDING_REACH_REPORT.copy {
//               clearState()
//               clearExternalReportId()
//               measurements.clear()
//               clearCreateTime()
//             }
//           measurements +=
//             CreateReportRequestKt.measurementKey {
//               measurementConsumerReferenceId =
//                 MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//               measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
//             }
//         }
//       )
//
//     ProtoTruth.assertThat(result).isEqualTo(expected)
//   }
//
//   @Test
//   fun `createMetric returns a metric over a reporting set that has a DIFFERENCE operation`() {
//     val internalPendingReachReportWithSetDifference =
//       INTERNAL_PENDING_REACH_REPORT.copy {
//         val source = this
//         measurements.clear()
//         clearCreateTime()
//         val metric = metric {
//           details = MetricKt.details { reach = MetricKt.reachParams {} }
//           namedSetOperations +=
//             source.metrics[0].namedSetOperationsList[0].copy {
//               setOperation =
//                 setOperation.copy { type = Metric.SetOperation.Type.DIFFERENCE }
//               measurementCalculations.clear()
//               measurementCalculations +=
//                 source.metrics[0].namedSetOperationsList[0].measurementCalculationsList[0].copy {
//                   weightedMeasurements.clear()
//                   weightedMeasurements += MetricKt.MeasurementCalculationKt.weightedMeasurement {
//                     measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
//                     coefficient = -1
//                   }
//                   weightedMeasurements += MetricKt.MeasurementCalculationKt.weightedMeasurement {
//                     measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID_2
//                     coefficient = 1
//                   }
//                 }
//             }
//         }
//         metrics.clear()
//         metrics += metric
//       }
//
//     runBlocking {
//       whenever(internalReportsMock.createMetric(any()))
//         .thenReturn(internalPendingReachReportWithSetDifference)
//       whenever(measurementsMock.createMeasurement(any()))
//         .thenReturn(BASE_REACH_MEASUREMENT, BASE_REACH_MEASUREMENT_2)
//     }
//
//     val pendingReachReportWithSetDifference =
//       PENDING_REACH_REPORT.copy {
//         metrics.clear()
//         metrics += metric {
//           reach = org.wfanet.measurement.reporting.v1alpha.MetricKt.reachParams {}
//           cumulative = false
//           setOperations += org.wfanet.measurement.reporting.v1alpha.MetricKt.namedSetOperation {
//             uniqueName = REACH_SET_OPERATION_UNIQUE_NAME
//             setOperation = org.wfanet.measurement.reporting.v1alpha.MetricKt.setOperation {
//               type = org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation.Type.DIFFERENCE
//               lhs =
//                 org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt.operand {
//                   reportingSet = INTERNAL_REPORTING_SETS[0].resourceName
//                 }
//               rhs =
//                 org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt.operand {
//                   reportingSet = INTERNAL_REPORTING_SETS[1].resourceName
//                 }
//             }
//           }
//         }
//       }
//
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = pendingReachReportWithSetDifference
//     }
//
//     val result =
//       org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//         MEASUREMENT_CONSUMERS.values.first().name,
//         CONFIG
//       ) {
//         runBlocking { service.createMetric(request) }
//       }
//
//     // Verify proto argument of ReportsCoroutineImplBase::getReportByIdempotencyKey
//     verifyProtoArgument(
//       internalReportsMock,
//       ReportsGrpcKt.ReportsCoroutineImplBase::getReportByIdempotencyKey
//     )
//       .isEqualTo(
//         getReportByIdempotencyKeyRequest {
//           measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//           reportIdempotencyKey = REACH_REPORT_IDEMPOTENCY_KEY
//         }
//       )
//
//     // Verify proto argument of InternalReportingSetsCoroutineImplBase::batchGetReportingSet
//     verifyProtoArgument(
//       internalReportingSetsMock,
//       ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::batchGetReportingSet
//     )
//       .ignoringRepeatedFieldOrder()
//       .isEqualTo(
//         batchGetReportingSetRequest {
//           measurementConsumerReferenceId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//           externalReportingSetIds += INTERNAL_REPORTING_SETS[0].externalReportingSetId
//           externalReportingSetIds += INTERNAL_REPORTING_SETS[1].externalReportingSetId
//         }
//       )
//
//     // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
//     verifyProtoArgument(
//       measurementConsumersMock,
//       MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
//     )
//       .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })
//
//     // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
//     val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
//     verifyBlocking(dataProvidersMock, times(3)) { getDataProvider(dataProvidersCaptor.capture()) }
//     val capturedDataProviderRequests = dataProvidersCaptor.allValues
//     ProtoTruth.assertThat(capturedDataProviderRequests)
//       .containsExactly(
//         getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name },
//         getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
//         getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name }
//       )
//
//     // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
//     val measurementCaptor: KArgumentCaptor<CreateMeasurementRequest> = argumentCaptor()
//     verifyBlocking(measurementsMock, times(2)) { createMeasurement(measurementCaptor.capture()) }
//     ProtoTruth.assertThat(measurementCaptor.allValues.map { it.measurement }).containsNoDuplicates()
//
//     // Verify proto argument of InternalMeasurementsCoroutineImplBase::createMeasurement
//     val internalMeasurementCaptor: KArgumentCaptor<Measurement> = argumentCaptor()
//     verifyBlocking(internalMeasurementsMock, times(2)) {
//       createMeasurement(internalMeasurementCaptor.capture())
//     }
//
//     // Verify proto argument of InternalReportsCoroutineImplBase::createMetric
//     verifyProtoArgument(internalReportsMock, ReportsGrpcKt.ReportsCoroutineImplBase::createMetric)
//       .ignoringRepeatedFieldOrder()
//       .isEqualTo(
//         createReportRequest {
//           report =
//             internalPendingReachReportWithSetDifference.copy {
//               clearState()
//               clearExternalReportId()
//             }
//           measurements +=
//             CreateReportRequestKt.measurementKey {
//               measurementConsumerReferenceId =
//                 MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//               measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
//             }
//           measurements +=
//             CreateReportRequestKt.measurementKey {
//               measurementConsumerReferenceId =
//                 MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
//               measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID_2
//             }
//         }
//       )
//
//     ProtoTruth.assertThat(result).isEqualTo(pendingReachReportWithSetDifference)
//   }
//
//   @Test
//   fun `createMetric succeeds when the internal createMeasurement throws ALREADY_EXISTS`() =
//     runBlocking {
//       whenever(internalMeasurementsMock.createMeasurement(any()))
//         .thenThrow(StatusRuntimeException(Status.ALREADY_EXISTS))
//
//       val request = createReportRequest {
//         parent = MEASUREMENT_CONSUMERS.values.first().name
//         report = PENDING_REACH_REPORT.copy { clearState() }
//       }
//
//       val report =
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       Truth.assertThat(report.state).isEqualTo(Report.State.RUNNING)
//     }
//
//   @Test
//   fun `createMetric throws UNAUTHENTICATED when no principal is found`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//     val exception =
//       assertFailsWith<StatusRuntimeException> { runBlocking { service.createMetric(request) } }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
//   }
//
//   @Test
//   fun `createMetric throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.last().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
//     Truth.assertThat(exception.status.description)
//       .isEqualTo("Cannot create a Report for another MeasurementConsumer.")
//   }
//
//   @Test
//   fun `createMetric throws PERMISSION_DENIED when metric doesn't belong to caller`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.last().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
//     Truth.assertThat(exception.status.description)
//       .isEqualTo("Cannot create a Report for another MeasurementConsumer.")
//   }
//
//   @Test
//   fun `createMetric throws UNAUTHENTICATED when the caller is not MeasurementConsumer`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         withDataProviderPrincipal(DATA_PROVIDERS_LIST[0].name) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
//     Truth.assertThat(exception.status.description).isEqualTo("No ReportingPrincipal found")
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when parent is unspecified`() {
//     val request = createReportRequest { report = PENDING_REACH_REPORT.copy { clearState() } }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//     Truth.assertThat(exception.status.description).isEqualTo("Parent is either unspecified or invalid.")
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when report is unspecified`() {
//     val request = createReportRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//     Truth.assertThat(exception.status.description).isEqualTo("Report is not specified.")
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when time interval in Metric is unspecified`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report =
//         PENDING_REACH_REPORT.copy {
//           clearState()
//           clearTime()
//         }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//     Truth.assertThat(exception.status.description).isEqualTo("The time in Report is not specified.")
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when TimeInterval startTime is unspecified`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report =
//         PENDING_REACH_REPORT.copy {
//           clearState()
//           clearTime()
//           timeIntervals = timeIntervals {
//             timeIntervals += timeInterval { endTime = timestamp { seconds = 5 } }
//           }
//         }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when TimeInterval endTime is unspecified`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report =
//         PENDING_REACH_REPORT.copy {
//           clearState()
//           clearTime()
//           timeIntervals = timeIntervals {
//             timeIntervals += timeInterval { startTime = timestamp { seconds = 5 } }
//           }
//         }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when TimeInterval endTime is before startTime`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report =
//         PENDING_REACH_REPORT.copy {
//           clearState()
//           clearTime()
//           timeIntervals = timeIntervals {
//             timeIntervals += timeInterval {
//               startTime = timestamp {
//                 seconds = 5
//                 nanos = 5
//               }
//               endTime = timestamp {
//                 seconds = 5
//                 nanos = 1
//               }
//             }
//           }
//         }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when metric type in Metric is unspecified`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report =
//         PENDING_REACH_REPORT.copy {
//           clearState()
//           metrics.clear()
//           metrics.add(REACH_METRIC.copy { clearReach() })
//         }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//     Truth.assertThat(exception.status.description)
//       .isEqualTo("The metric type in Report is not specified.")
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when reporting set is unspecified`() {
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report =
//         PENDING_REACH_REPORT.copy {
//           clearState()
//           metrics.clear()
//         }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when provided reporting set name is invalid`() {
//     val invalidMetric = metric {
//       reach = org.wfanet.measurement.reporting.v1alpha.MetricKt.reachParams {}
//       cumulative = false
//       setOperations.add(
//         NAMED_REACH_SET_OPERATION.copy { setOperation = SET_OPERATION_WITH_INVALID_REPORTING_SET }
//       )
//     }
//
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report =
//         PENDING_REACH_REPORT.copy {
//           clearState()
//           metrics.clear()
//           metrics.add(invalidMetric)
//         }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//     Truth.assertThat(exception.status.description)
//       .isEqualTo("Invalid reporting set name $INVALID_REPORTING_SET_NAME.")
//   }
//
//   @Test
//   fun `createMetric throws INVALID_ARGUMENT when any reporting set is not accessible to caller`() {
//     val invalidMetric = metric {
//       reach = org.wfanet.measurement.reporting.v1alpha.MetricKt.reachParams {}
//       cumulative = false
//       setOperations.add(
//         NAMED_REACH_SET_OPERATION.copy {
//           setOperation = SET_OPERATION_WITH_INACCESSIBLE_REPORTING_SET
//         }
//       )
//     }
//
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report =
//         PENDING_REACH_REPORT.copy {
//           clearState()
//           metrics.clear()
//           metrics.add(invalidMetric)
//         }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
//     Truth.assertThat(exception.status.description)
//       .isEqualTo("No access to the reporting set [$REPORTING_SET_NAME_FOR_MC_2].")
//   }
//
//   @Test
//   fun `createMetric throws NOT_FOUND when reporting set is not found`() = runBlocking {
//     whenever(internalReportingSetsMock.batchGetReportingSet(any()))
//       .thenReturn(flowOf(INTERNAL_REPORTING_SETS[0]))
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
//   }
//
//   @Test
//   fun `createMetric throws FAILED_PRECONDITION when EDP cert is revoked`() = runBlocking {
//     val dataProvider = DATA_PROVIDERS.values.first()
//     whenever(
//       certificateMock.getCertificate(
//         eq(getCertificateRequest { name = dataProvider.certificate })
//       )
//     )
//       .thenReturn(
//         certificate {
//           name = dataProvider.certificate
//           x509Der = DATA_PROVIDER_SIGNING_KEY.certificate.encoded.toByteString()
//           revocationState = Certificate.RevocationState.REVOKED
//         }
//       )
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//
//     val exception =
//       assertFailsWith<StatusRuntimeException> {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//
//     Truth.assertThat(exception).hasMessageThat().ignoringCase().contains("revoked")
//   }
//
//   @Test
//   fun `createMetric throws FAILED_PRECONDITION when EDP public key signature is invalid`() =
//     runBlocking {
//       val dataProvider = DATA_PROVIDERS.values.first()
//       whenever(
//         dataProvidersMock.getDataProvider(eq(getDataProviderRequest { name = dataProvider.name }))
//       )
//         .thenReturn(
//           dataProvider.copy {
//             publicKey = publicKey.copy { signature = "invalid sig".toByteStringUtf8() }
//           }
//         )
//       val request = createReportRequest {
//         parent = MEASUREMENT_CONSUMERS.values.first().name
//         report = PENDING_REACH_REPORT.copy { clearState() }
//       }
//
//       val exception =
//         assertFailsWith<StatusRuntimeException> {
//           org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//             MEASUREMENT_CONSUMERS.values.first().name,
//             CONFIG
//           ) {
//             runBlocking { service.createMetric(request) }
//           }
//         }
//
//       Truth.assertThat(exception).hasMessageThat().ignoringCase().contains("signature")
//     }
//
//   @Test
//   fun `createMetric throws exception from getMetricByIdempotencyKey when status isn't NOT_FOUND`() =
//     runBlocking {
//       whenever(internalReportsMock.getReportByIdempotencyKey(any()))
//         .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))
//
//       val request = createReportRequest {
//         parent = MEASUREMENT_CONSUMERS.values.first().name
//         report = PENDING_REACH_REPORT.copy { clearState() }
//       }
//
//       val exception =
//         assertFailsWith(Exception::class) {
//           org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//             MEASUREMENT_CONSUMERS.values.first().name,
//             CONFIG
//           ) {
//             runBlocking { service.createMetric(request) }
//           }
//         }
//       val expectedExceptionDescription =
//         "Unable to retrieve a report from the reporting database using the provided " +
//           "reportIdempotencyKey [${PENDING_REACH_REPORT.reportIdempotencyKey}]."
//       Truth.assertThat(exception.message).isEqualTo(expectedExceptionDescription)
//     }
//
//   @Test
//   fun `createMetric throws exception when internal createMetric throws exception`() = runBlocking {
//     whenever(internalReportsMock.createMetric(any()))
//       .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))
//
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//
//     val exception =
//       assertFailsWith(Exception::class) {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     val expectedExceptionDescription = "Unable to create a report in the reporting database."
//     Truth.assertThat(exception.message).isEqualTo(expectedExceptionDescription)
//   }
//
//   @Test
//   fun `createMetric throws exception when the CMM createMeasurement throws exception`() =
//     runBlocking {
//       whenever(measurementsMock.createMeasurement(any()))
//         .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))
//
//       val request = createReportRequest {
//         parent = MEASUREMENT_CONSUMERS.values.first().name
//         report = PENDING_REACH_REPORT.copy { clearState() }
//       }
//
//       val exception =
//         assertFailsWith(Exception::class) {
//           org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//             MEASUREMENT_CONSUMERS.values.first().name,
//             CONFIG
//           ) {
//             runBlocking { service.createMetric(request) }
//           }
//         }
//       val expectedExceptionDescription =
//         "Unable to create the measurement [$REACH_MEASUREMENT_NAME]."
//       Truth.assertThat(exception.message).isEqualTo(expectedExceptionDescription)
//     }
//
//   @Test
//   fun `createMetric throws exception when the internal createMeasurement throws exception`() =
//     runBlocking {
//       whenever(internalMeasurementsMock.createMeasurement(any()))
//         .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))
//
//       val request = createReportRequest {
//         parent = MEASUREMENT_CONSUMERS.values.first().name
//         report = PENDING_REACH_REPORT.copy { clearState() }
//       }
//
//       val exception =
//         assertFailsWith(Exception::class) {
//           org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//             MEASUREMENT_CONSUMERS.values.first().name,
//             CONFIG
//           ) {
//             runBlocking { service.createMetric(request) }
//           }
//         }
//       val expectedExceptionDescription =
//         "Unable to create the measurement [$REACH_MEASUREMENT_NAME] in the reporting database."
//       Truth.assertThat(exception.message).isEqualTo(expectedExceptionDescription)
//     }
//
//   @Test
//   fun `createMetric throws exception when getMeasurementConsumer throws exception`() = runBlocking {
//     whenever(measurementConsumersMock.getMeasurementConsumer(any()))
//       .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))
//
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//
//     val exception =
//       assertFailsWith(Exception::class) {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     val expectedExceptionDescription =
//       "Unable to retrieve the measurement consumer [${MEASUREMENT_CONSUMERS.values.first().name}]."
//     Truth.assertThat(exception.message).isEqualTo(expectedExceptionDescription)
//   }
//
//   @Test
//   fun `createMetric throws exception when the internal batchGetReportingSet throws exception`():
//     Unit = runBlocking {
//     whenever(internalReportingSetsMock.batchGetReportingSet(any()))
//       .thenThrow(StatusRuntimeException(Status.UNKNOWN))
//
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//
//     assertFails {
//       org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//         MEASUREMENT_CONSUMERS.values.first().name,
//         CONFIG
//       ) {
//         runBlocking { service.createMetric(request) }
//       }
//     }
//   }
//
//   @Test
//   fun `createMetric throws exception when getDataProvider throws exception`() = runBlocking {
//     whenever(dataProvidersMock.getDataProvider(any()))
//       .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))
//
//     val request = createReportRequest {
//       parent = MEASUREMENT_CONSUMERS.values.first().name
//       report = PENDING_REACH_REPORT.copy { clearState() }
//     }
//
//     val exception =
//       assertFailsWith(Exception::class) {
//         org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerPrincipal(
//           MEASUREMENT_CONSUMERS.values.first().name,
//           CONFIG
//         ) {
//           runBlocking { service.createMetric(request) }
//         }
//       }
//     Truth.assertThat(exception).hasMessageThat().contains("dataProviders/")
//   }
// }

private fun EventGroupKey.toInternal(): InternalReportingSet.Primitive.EventGroupKey {
  val source = this
  return InternalReportingSetKt.PrimitiveKt.eventGroupKey {
    cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
    cmmsDataProviderId = source.cmmsDataProviderId
    cmmsEventGroupId = source.cmmsEventGroupId
  }
}

private val InternalReportingSet.resourceKey: ReportingSetKey
  get() =
    ReportingSetKey(cmmsMeasurementConsumerId, ExternalId(externalReportingSetId).apiId.value)
private val InternalReportingSet.resourceName: String
  get() = resourceKey.toName()
