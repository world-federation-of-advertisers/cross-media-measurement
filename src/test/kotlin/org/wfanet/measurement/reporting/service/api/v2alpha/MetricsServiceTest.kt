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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import com.google.protobuf.duration
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.timestamp
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
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.v2alpha.GetMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval as measurementTimeInterval
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
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
import org.wfanet.measurement.common.testing.verifyProtoArgument
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
import org.wfanet.measurement.internal.reporting.v2alpha.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2alpha.BatchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2alpha.BatchSetCmmsMeasurementIdsRequestKt.measurementIds
import org.wfanet.measurement.internal.reporting.v2alpha.GetMetricByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.v2alpha.GetReportingSetRequest as InternalGetReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2alpha.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.MeasurementKt as InternalMeasurementKt
import org.wfanet.measurement.internal.reporting.v2alpha.MeasurementsGrpcKt as InternalMeasurementsGrpcKt
import org.wfanet.measurement.internal.reporting.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase as InternalMeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2alpha.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.v2alpha.MetricKt as InternalMetricKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.MetricResultKt as InternalMetricResultKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricsGrpcKt as InternalMetricsGrpcKt
import org.wfanet.measurement.internal.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSet.SetExpression as InternalSetExpression
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt as InternalReportingSetKt
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.SetExpressionKt.operand as internalOperand
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.composite as internalComposite
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.primitive as internalPrimitive
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.primitiveReportingSetBasis
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.setExpression as internalSetExpression
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetKt.weightedSubsetUnion
import org.wfanet.measurement.internal.reporting.v2alpha.ReportingSetsGrpcKt as InternalReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.v2alpha.batchCreateMetricsRequest as internalBatchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2alpha.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2alpha.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2alpha.copy
import org.wfanet.measurement.internal.reporting.v2alpha.getReportingSetRequest as internalGetReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2alpha.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.v2alpha.metric as internalMetric
import org.wfanet.measurement.internal.reporting.v2alpha.metricResult as internalMetricResult
import org.wfanet.measurement.internal.reporting.v2alpha.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2alpha.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.v2alpha.timeInterval as internalTimeInterval
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.integerResult
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.frequencyHistogramParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.impressionCountParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.reachParams
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt.watchDurationParams
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.timeInterval

private const val MAX_BATCH_SIZE = 1000
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
      ExternalId(index * 10 + 660L).apiId.value
    )
  }
private val EVENT_GROUP_KEYS_OF_FIRST_PUBLISHER =
  (0L until 3L).map { index ->
    val measurementConsumerKey = MEASUREMENT_CONSUMERS.keys.first()
    EventGroupKey(
      measurementConsumerKey.measurementConsumerId,
      DATA_PROVIDERS.keys.first().dataProviderId,
      ExternalId(index + 660L).apiId.value
    )
  }

// Event filters
private const val INCREMENTAL_REPORTING_SET_FILTER = "AGE>18"
private const val METRIC_FILTER = "media_type==video"
private const val PRIMITIVE_REPORTING_SET_FILTER = "gender==male"
private val ALL_FILTERS =
  listOf(INCREMENTAL_REPORTING_SET_FILTER, METRIC_FILTER, PRIMITIVE_REPORTING_SET_FILTER)

// Internal reporting sets

private val INTERNAL_SINGLE_PUBLISHER_REPORTING_SETS =
  EVENT_GROUP_KEYS.mapIndexed { index, eventGroupKey ->
    internalReportingSet {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
      externalReportingSetId = index + 220L
      this.primitive = internalPrimitive { eventGroupKeys += eventGroupKey.toInternal() }
      displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
      weightedSubsetUnions += weightedSubsetUnion {
        primitiveReportingSetBases += primitiveReportingSetBasis {
          externalReportingSetId = this@internalReportingSet.externalReportingSetId
        }
        weight = 1
      }
    }
  }
private val INTERNAL_UNION_ALL_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId =
    INTERNAL_SINGLE_PUBLISHER_REPORTING_SETS.last().externalReportingSetId + 1
  this.primitive = internalPrimitive { eventGroupKeys += EVENT_GROUP_KEYS.map { it.toInternal() } }
  filter = PRIMITIVE_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
  weightedSubsetUnions += weightedSubsetUnion {
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId = this@internalReportingSet.externalReportingSetId
      filters += this@internalReportingSet.filter
    }
    weight = 1
  }
}
private val INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId + 1
  this.primitive = internalPrimitive {
    (0 until EVENT_GROUP_KEYS.size - 1).map { i ->
      eventGroupKeys += EVENT_GROUP_KEYS[i].toInternal()
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
  }
}
private val INTERNAL_SINGLE_PUBLISHER_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId =
    INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId + 1
  this.primitive = internalPrimitive {
    eventGroupKeys += EVENT_GROUP_KEYS_OF_FIRST_PUBLISHER.map { it.toInternal() }
  }
  filter = PRIMITIVE_REPORTING_SET_FILTER
  displayName = "$cmmsMeasurementConsumerId-$externalReportingSetId-$filter"
  weightedSubsetUnions += weightedSubsetUnion {
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId = this@internalReportingSet.externalReportingSetId
      filters += this@internalReportingSet.filter
    }
    weight = 1
  }
}

private val INTERNAL_INCREMENTAL_REPORTING_SET = internalReportingSet {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  externalReportingSetId =
    INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId + 1
  this.composite = internalComposite {
    expression = internalSetExpression {
      operation = InternalSetExpression.Operation.DIFFERENCE
      lhs = internalOperand {
        externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
      }
      rhs = internalOperand {
        externalReportingSetId =
          INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
      }
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
    weight = 1
  }
  weightedSubsetUnions += weightedSubsetUnion {
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId =
        INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
      filters += INCREMENTAL_REPORTING_SET_FILTER
      filters += INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.filter
    }
    weight = -1
  }
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
            filter =
              RequisitionSpecKt.eventFilter {
                expression =
                  "($INCREMENTAL_REPORTING_SET_FILTER) AND ($METRIC_FILTER) AND ($PRIMITIVE_REPORTING_SET_FILTER)"
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
private const val INCREMENTAL_REACH_VALUE =
  UNION_ALL_REACH_VALUE - UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE
private val FREQUENCY_DISTRIBUTION = mapOf(1L to 1.0 / 6, 2L to 2.0 / 6, 3L to 3.0 / 6)
private val FIRST_PUBLISHER_IMPRESSION_VALUE = 100L
private val IMPRESSION_VALUES = listOf(100L, 150L)
private val TOTAL_IMPRESSION_VALUE = IMPRESSION_VALUES.sum()
private val WATCH_DURATION_SECOND_LIST = listOf(100L, 200L)
private val WATCH_DURATION_LIST = WATCH_DURATION_SECOND_LIST.map { duration { seconds = it } }
private val TOTAL_WATCH_DURATION = duration { seconds = WATCH_DURATION_SECOND_LIST.sum() }

// Internal incremental reach measurements
private val INTERNAL_REQUESTING_UNION_ALL_REACH_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  timeInterval = INTERNAL_TIME_INTERVAL
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
    filters += ALL_FILTERS
  }
}
private val INTERNAL_REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  internalMeasurement {
    cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
    timeInterval = INTERNAL_TIME_INTERVAL
    primitiveReportingSetBases += primitiveReportingSetBasis {
      externalReportingSetId =
        INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
      filters += ALL_FILTERS
    }
  }

private val INTERNAL_PENDING_NOT_CREATED_UNION_ALL_REACH_MEASUREMENT =
  INTERNAL_REQUESTING_UNION_ALL_REACH_MEASUREMENT.copy {
    externalMeasurementId = 411L
    cmmsCreateMeasurementRequestId = "UNION_ALL_REACH_MEASUREMENT"
    state = InternalMeasurement.State.PENDING
  }
private val INTERNAL_PENDING_NOT_CREATED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  INTERNAL_REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
    externalMeasurementId = 412L
    cmmsCreateMeasurementRequestId = "UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT"
    state = InternalMeasurement.State.PENDING
  }

private val INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT =
  INTERNAL_PENDING_NOT_CREATED_UNION_ALL_REACH_MEASUREMENT.copy {
    cmmsMeasurementId = externalIdToApiId(401L)
  }
private val INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT =
  INTERNAL_PENDING_NOT_CREATED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
    cmmsMeasurementId = externalIdToApiId(402L)
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
        reach =
          InternalMeasurementKt.ResultKt.reach { value = UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE }
        frequency =
          InternalMeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
          }
      }
  }

// Internal single publisher impression measurements
private val INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT = internalMeasurement {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  timeInterval = INTERNAL_TIME_INTERVAL
  primitiveReportingSetBases += primitiveReportingSetBasis {
    externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
    filters += METRIC_FILTER
    filters += PRIMITIVE_REPORTING_SET_FILTER
  }
}

private val INTERNAL_PENDING_NOT_CREATED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    externalMeasurementId = 413L
    cmmsCreateMeasurementRequestId = "SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT"
    state = InternalMeasurement.State.PENDING
  }

private val INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  INTERNAL_PENDING_NOT_CREATED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    cmmsMeasurementId = externalIdToApiId(403L)
  }

// CMMs incremental reach measurements
private val UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.toByteString()

  nonceHashes.addAll(
    listOf(
      hashSha256(SECURE_RANDOM_OUTPUT_LONG),
      hashSha256(SECURE_RANDOM_OUTPUT_LONG),
      hashSha256(SECURE_RANDOM_OUTPUT_LONG)
    )
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
  vidSamplingInterval =
    MeasurementSpecKt.vidSamplingInterval {
      start = REACH_ONLY_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
      width = REACH_ONLY_VID_SAMPLING_WIDTH
    }
}

private val REQUESTING_UNION_ALL_REACH_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDERS.keys.map { DATA_PROVIDER_ENTRIES.getValue(it) }

    measurementSpec =
      signMeasurementSpec(
        UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
          nonceHashes += hashSha256(SECURE_RANDOM_OUTPUT_LONG)
        },
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
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
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
      )

    measurementReferenceId =
      INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_UNION_ALL_REACH_MEASUREMENT =
  REQUESTING_UNION_ALL_REACH_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
        )
        .toName()
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
    state = Measurement.State.COMPUTING
  }

private val SUCCEEDED_UNION_ALL_REACH_MEASUREMENT =
  PENDING_UNION_ALL_REACH_MEASUREMENT.copy {
    state = Measurement.State.SUCCEEDED

    results +=
      MeasurementKt.resultPair {
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
  PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.copy {
    state = Measurement.State.SUCCEEDED

    results +=
      MeasurementKt.resultPair {
        val result =
          MeasurementKt.result {
            reach =
              MeasurementKt.ResultKt.reach { value = UNION_ALL_BUT_LAST_PUBLISHER_REACH_VALUE }
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

// CMMs single publisher impression measurements
private val SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
  measurementPublicKey = MEASUREMENT_CONSUMER_PUBLIC_KEY.toByteString()

  nonceHashes.add(hashSha256(SECURE_RANDOM_OUTPUT_LONG))

  impression =
    MeasurementSpecKt.impression {
      privacyParams = differentialPrivacyParams {
        epsilon = IMPRESSION_EPSILON
        delta = DIFFERENTIAL_PRIVACY_DELTA
      }
      maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
    }
  vidSamplingInterval =
    MeasurementSpecKt.vidSamplingInterval {
      start = IMPRESSION_VID_SAMPLING_START_LIST[SECURE_RANDOM_OUTPUT_INT]
      width = IMPRESSION_VID_SAMPLING_WIDTH
    }
}

private val REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  BASE_MEASUREMENT.copy {
    dataProviders += DATA_PROVIDER_ENTRIES.getValue(DATA_PROVIDERS.keys.first())

    measurementSpec =
      signMeasurementSpec(
        SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC.copy {
          nonceHashes += hashSha256(SECURE_RANDOM_OUTPUT_LONG)
        },
        MEASUREMENT_CONSUMER_SIGNING_KEY_HANDLE
      )

    measurementReferenceId =
      INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsCreateMeasurementRequestId
  }

private val PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    name =
      MeasurementKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
        )
        .toName()
    state = Measurement.State.COMPUTING
  }

private val SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT =
  PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.copy {
    state = Measurement.State.SUCCEEDED

    results +=
      MeasurementKt.resultPair {
        val result =
          MeasurementKt.result {
            impression =
              MeasurementKt.ResultKt.impression { value = FIRST_PUBLISHER_IMPRESSION_VALUE }
          }
        encryptedResult =
          encryptResult(
            signResult(result, DATA_PROVIDER_SIGNING_KEY),
            MEASUREMENT_CONSUMER_PUBLIC_KEY
          )
        certificate = DATA_PROVIDERS_LIST.first().certificate
      }
  }

private val PENDING_CMMS_MEASUREMENTS =
  mapOf(
    PENDING_UNION_ALL_REACH_MEASUREMENT.measurementReferenceId to
      PENDING_UNION_ALL_REACH_MEASUREMENT,
    PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.measurementReferenceId to
      PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
    PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.measurementReferenceId to
      PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT,
  )
private val SUCCEEDED_CMMS_MEASUREMENTS =
  mapOf(
    SUCCEEDED_UNION_ALL_REACH_MEASUREMENT.measurementReferenceId to
      SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
    SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.measurementReferenceId to
      SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
    SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.measurementReferenceId to
      SUCCEEDED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT,
  )

// Metric Specs

private const val MAXIMUM_FREQUENCY_PER_USER = 10
private const val MAXIMUM_WATCH_DURATION_PER_USER = 300

private val REACH_METRIC_SPEC: MetricSpec = metricSpec { reach = reachParams {} }
private val FREQUENCY_HISTOGRAM_METRIC_SPEC: MetricSpec = metricSpec {
  frequencyHistogram = frequencyHistogramParams {
    maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
  }
}
private val IMPRESSION_COUNT_METRIC_SPEC: MetricSpec = metricSpec {
  impressionCount = impressionCountParams { maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER }
}
private val WATCH_DURATION_METRIC_SPEC: MetricSpec = metricSpec {
  watchDuration = watchDurationParams {
    maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
  }
}

// Metrics

// Metric idempotency keys
private const val INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY = "TEST_INCREMENTAL_REACH_METRIC"
private const val SINGLE_PUBLISHER_IMPRESSION_METRIC_IDEMPOTENCY_KEY =
  "TEST_SINGLE_PUBLISHER_IMPRESSION_METRIC"
// private const val IMPRESSION_METRIC_IDEMPOTENCY_KEY = "TEST_IMPRESSION_METRIC"
// private const val WATCH_DURATION_METRIC_IDEMPOTENCY_KEY = "TEST_WATCH_DURATION_METRIC"
// private const val FREQUENCY_HISTOGRAM_METRIC_IDEMPOTENCY_KEY = "TEST_FREQUENCY_HISTOGRAM_METRIC"

// Typo causes invalid name
private const val INVALID_METRIC_NAME = "measurementConsumer/AAAAAAAAAG8/metric/AAAAAAAAAU0"

// Internal Incremental Metrics
private val INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC = internalMetric {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  metricIdempotencyKey = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
  externalReportingSetId = INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId
  timeInterval = INTERNAL_TIME_INTERVAL
  metricSpec = internalMetricSpec { reach = InternalMetricSpecKt.reachParams {} }
  weightedMeasurements += weightedMeasurement {
    weight = 1
    measurement = INTERNAL_REQUESTING_UNION_ALL_REACH_MEASUREMENT
  }
  weightedMeasurements += weightedMeasurement {
    weight = -1
    measurement = INTERNAL_REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
  }
  details = InternalMetricKt.details { filters += listOf(METRIC_FILTER) }
}

private val INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC =
  INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC.copy {
    externalMetricId = 331L
    createTime = Instant.now().toProtoTime()
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      measurement = INTERNAL_PENDING_NOT_CREATED_UNION_ALL_REACH_MEASUREMENT
    }
    weightedMeasurements += weightedMeasurement {
      weight = -1
      measurement = INTERNAL_PENDING_NOT_CREATED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
    }
    state = InternalMetric.State.RUNNING
  }

private val INTERNAL_PENDING_INCREMENTAL_REACH_METRIC =
  INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC.copy {
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      measurement = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT
    }
    weightedMeasurements += weightedMeasurement {
      weight = -1
      measurement = INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
    }
  }

private val INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC =
  INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.copy {
    state = InternalMetric.State.SUCCEEDED
    details =
      InternalMetricKt.details {
        filters += this@copy.details.filtersList
        result = internalMetricResult {
          reach = InternalMetricResultKt.integerResult { value = INCREMENTAL_REACH_VALUE }
        }
      }
  }

// Internal Single publisher Metrics
private val INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC = internalMetric {
  cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
  metricIdempotencyKey = SINGLE_PUBLISHER_IMPRESSION_METRIC_IDEMPOTENCY_KEY
  externalReportingSetId = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
  timeInterval = INTERNAL_TIME_INTERVAL
  metricSpec = internalMetricSpec {
    impressionCount =
      InternalMetricSpecKt.impressionCountParams {
        maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
      }
  }
  weightedMeasurements += weightedMeasurement {
    weight = 1
    measurement = INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
  }
  details = InternalMetricKt.details { filters += listOf(METRIC_FILTER) }
}

private val INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    externalMetricId = 333L
    createTime = Instant.now().toProtoTime()
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      measurement = INTERNAL_PENDING_NOT_CREATED_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
    }
    state = InternalMetric.State.RUNNING
  }

private val INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    weightedMeasurements.clear()
    weightedMeasurements += weightedMeasurement {
      weight = 1
      measurement = INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
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

private val METRIC_WITH_INVALID_REPORTING_SET =
  REQUESTING_INCREMENTAL_REACH_METRIC.copy { reportingSet = INVALID_REPORTING_SET_NAME }

private val PENDING_INCREMENTAL_REACH_METRIC =
  REQUESTING_INCREMENTAL_REACH_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          externalIdToApiId(INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.externalMetricId)
        )
        .toName()
    state = Metric.State.RUNNING
    createTime = INTERNAL_PENDING_INCREMENTAL_REACH_METRIC.createTime
  }

private val SUCCEEDED_INCREMENTAL_REACH_METRIC =
  PENDING_INCREMENTAL_REACH_METRIC.copy {
    state = Metric.State.SUCCEEDED
    result = metricResult {
      reach = integerResult {
        value = INTERNAL_SUCCEEDED_INCREMENTAL_REACH_METRIC.details.result.reach.value
      }
    }
  }

// Single publisher impression metrics
private val REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC = metric {
  reportingSet = INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.resourceName
  timeInterval = TIME_INTERVAL
  metricSpec = IMPRESSION_COUNT_METRIC_SPEC
  filters += INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.details.filtersList
}

private val PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC =
  REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
    name =
      MetricKey(
          MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId,
          externalIdToApiId(INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalMetricId)
        )
        .toName()
    state = Metric.State.RUNNING
    createTime = INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC.createTime
  }

@RunWith(JUnit4::class)
class MetricsServiceTest {

  private val internalMetricsMock: MetricsCoroutineImplBase = mockService {
    onBlocking { createMetric(any()) }
      .thenReturn(
        INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC,
      )
    // onBlocking { streamMetrics(any()) }
    //   .thenReturn(
    //     flowOf(
    //       INTERNAL_PENDING_REACH_METRIC,
    //       INTERNAL_PENDING_IMPRESSION_METRIC,
    //       INTERNAL_PENDING_WATCH_DURATION_METRIC,
    //       INTERNAL_PENDING_FREQUENCY_HISTOGRAM_METRIC,
    //     )
    //   )
    // onBlocking { getMetric(any()) }
    //   .thenReturn(
    //     INTERNAL_SUCCEEDED_REACH_METRIC,
    //     INTERNAL_SUCCEEDED_IMPRESSION_METRIC,
    //     INTERNAL_SUCCEEDED_WATCH_DURATION_METRIC,
    //     INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_METRIC,
    //   )
    onBlocking { getMetricByIdempotencyKey(any()) }
      .thenThrow(StatusRuntimeException(Status.NOT_FOUND))
  }

  private val internalReportingSetsMock:
    InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase =
    mockService {
      onBlocking { getReportingSet(any()) }.thenReturn(INTERNAL_INCREMENTAL_REPORTING_SET)
      onBlocking { batchGetReportingSets(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_UNION_ALL_REPORTING_SET,
            INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET
          )
        )
    }

  private val internalMeasurementsMock: InternalMeasurementsCoroutineImplBase = mockService {
    onBlocking { batchSetCmmsMeasurementIds(any()) }
      .thenReturn(
        flowOf(
          INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT,
          INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
        )
      )
  }

  private val measurementsMock: MeasurementsCoroutineImplBase = mockService {
    onBlocking { getMeasurement(any()) }
      .thenReturn(
        SUCCEEDED_UNION_ALL_REACH_MEASUREMENT,
        SUCCEEDED_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT,
      )

    onBlocking { createMeasurement(any()) }
      .thenAnswer {
        val request = it.arguments[0] as CreateMeasurementRequest
        PENDING_CMMS_MEASUREMENTS.getValue(request.measurement.measurementReferenceId)
      }

    // onBlocking { createMeasurement(any()) }
    //   .thenReturn(
    //     PENDING_UNION_ALL_REACH_MEASUREMENT,
    //     PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
    //   )
  }

  private val measurementConsumersMock:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase =
    mockService {
      onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMERS.values.first())
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
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        InternalMeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        InternalMetricsGrpcKt.MetricsCoroutineStub(grpcTestServerRule.channel),
        DataProvidersGrpcKt.DataProvidersCoroutineStub(grpcTestServerRule.channel),
        MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        CertificatesGrpcKt.CertificatesCoroutineStub(grpcTestServerRule.channel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
        ENCRYPTION_KEY_PAIR_STORE,
        secureRandomMock,
        SECRETS_DIR,
        listOf(AGGREGATOR_ROOT_CERTIFICATE, DATA_PROVIDER_ROOT_CERTIFICATE).associateBy {
          it.subjectKeyIdentifier!!
        }
      )
  }

  @Test
  fun `createMetric without request ID returns incremental reach metric with RUNNING state`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::getMetricByIdempotencyKey
    val getMetricByIdempotencyKeyCaptor: KArgumentCaptor<GetMetricByIdempotencyKeyRequest> =
      argumentCaptor()
    verifyBlocking(internalMetricsMock, never()) {
      getMetricByIdempotencyKey(getMetricByIdempotencyKeyCaptor.capture())
    }

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::getReportingSet
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::getReportingSet
      )
      .isEqualTo(
        internalGetReportingSetRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetId += INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearMetricIdempotencyKey() })

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    verifyProtoArgument(
        measurementConsumersMock,
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      )
      .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::batchGetReportingSets
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::batchGetReportingSets
      )
      .isEqualTo(
        batchGetReportingSetsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetIds += INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
          externalReportingSetIds +=
            INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(5)) { getDataProvider(dataProvidersCaptor.capture()) }

    val capturedDataProviderRequests = dataProvidersCaptor.allValues
    assertThat(capturedDataProviderRequests)
      .containsExactly(
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[2].name },
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val measurementsCaptor: KArgumentCaptor<CreateMeasurementRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(2)) { createMeasurement(measurementsCaptor.capture()) }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER),
        Measurement.DataProviderEntry.Value.getDescriptor()
          .findFieldByNumber(
            Measurement.DataProviderEntry.Value.ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER
          ),
      )
      .containsExactly(
        createMeasurementRequest { measurement = REQUESTING_UNION_ALL_REACH_MEASUREMENT },
        createMeasurementRequest {
          measurement = REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
        },
      )

    capturedMeasurementRequests.forEach { capturedMeasurementRequest ->
      verifyMeasurementSpec(
        capturedMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )

      val dataProvidersList =
        capturedMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec =
        MeasurementSpec.parseFrom(capturedMeasurementRequest.measurement.measurementSpec.data)
      assertThat(measurementSpec)
        .isEqualTo(
          UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
            nonceHashes.clear()
            nonceHashes.addAll(
              List(dataProvidersList.size) { hashSha256(SECURE_RANDOM_OUTPUT_LONG) }
            )
          }
        )

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
            externalMeasurementId =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.externalMeasurementId
            cmmsMeasurementId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            externalMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.externalMeasurementId
            cmmsMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric without request ID returns single pub impression metric with RUNNING state`() =
    runBlocking {
      whenever(internalReportingSetsMock.getReportingSet(any()))
        .thenReturn(INTERNAL_SINGLE_PUBLISHER_REPORTING_SET)
      whenever(internalMetricsMock.createMetric(any()))
        .thenReturn(INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC)
      whenever(internalReportingSetsMock.batchGetReportingSets(any()))
        .thenReturn(flowOf(INTERNAL_SINGLE_PUBLISHER_REPORTING_SET))
      whenever(measurementsMock.createMeasurement(any()))
        .thenReturn(PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT)

      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
      }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }

      val expected = PENDING_SINGLE_PUBLISHER_IMPRESSION_METRIC

      // Verify proto argument of the internal MetricsCoroutineImplBase::getMetricByIdempotencyKey
      val getMetricByIdempotencyKeyCaptor: KArgumentCaptor<GetMetricByIdempotencyKeyRequest> =
        argumentCaptor()
      verifyBlocking(internalMetricsMock, never()) {
        getMetricByIdempotencyKey(getMetricByIdempotencyKeyCaptor.capture())
      }

      // Verify proto argument of the internal ReportingSetsCoroutineImplBase::getReportingSet
      verifyProtoArgument(
          internalReportingSetsMock,
          InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::getReportingSet
        )
        .isEqualTo(
          internalGetReportingSetRequest {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportingSetId += INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
          }
        )

      // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
      verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
            clearMetricIdempotencyKey()
          }
        )

      // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      verifyProtoArgument(
          measurementConsumersMock,
          MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
        )
        .isEqualTo(
          getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name }
        )

      // Verify proto argument of the internal ReportingSetsCoroutineImplBase::batchGetReportingSets
      verifyProtoArgument(
          internalReportingSetsMock,
          InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::batchGetReportingSets
        )
        .isEqualTo(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
            externalReportingSetIds +=
              INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalReportingSetId
          }
        )

      // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
      val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
      verifyBlocking(dataProvidersMock, times(1)) { getDataProvider(dataProvidersCaptor.capture()) }

      val capturedDataProviderRequests = dataProvidersCaptor.allValues
      assertThat(capturedDataProviderRequests)
        .containsExactly(
          getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        )

      // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
      val measurementsCaptor: KArgumentCaptor<CreateMeasurementRequest> = argumentCaptor()
      verifyBlocking(measurementsMock, times(1)) { createMeasurement(measurementsCaptor.capture()) }
      val capturedMeasurementRequests = measurementsCaptor.allValues
      assertThat(capturedMeasurementRequests)
        .ignoringRepeatedFieldOrder()
        .ignoringFieldDescriptors(
          Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER),
          Measurement.DataProviderEntry.Value.getDescriptor()
            .findFieldByNumber(
              Measurement.DataProviderEntry.Value.ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER
            ),
        )
        .containsExactly(
          createMeasurementRequest {
            measurement = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
          },
        )

      capturedMeasurementRequests.forEach { capturedMeasurementRequest ->
        verifyMeasurementSpec(
          capturedMeasurementRequest.measurement.measurementSpec,
          MEASUREMENT_CONSUMER_CERTIFICATE,
          TRUSTED_MEASUREMENT_CONSUMER_ISSUER
        )

        val dataProvidersList =
          capturedMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

        val measurementSpec =
          MeasurementSpec.parseFrom(capturedMeasurementRequest.measurement.measurementSpec.data)
        assertThat(measurementSpec).isEqualTo(SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC)

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
              externalMeasurementId =
                INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.externalMeasurementId
              cmmsMeasurementId =
                INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
            }
          }
        )

      assertThat(result).isEqualTo(expected)
    }

  @Test
  fun `createMetric with request ID returns incremental reach metric with RUNNING state`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
      requestId = INCREMENTAL_REACH_METRIC_IDEMPOTENCY_KEY
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::getReportingSet
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::getReportingSet
      )
      .isEqualTo(
        internalGetReportingSetRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetId += INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC)

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    verifyProtoArgument(
        measurementConsumersMock,
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      )
      .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::batchGetReportingSets
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::batchGetReportingSets
      )
      .isEqualTo(
        batchGetReportingSetsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetIds += INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
          externalReportingSetIds +=
            INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(5)) { getDataProvider(dataProvidersCaptor.capture()) }

    val capturedDataProviderRequests = dataProvidersCaptor.allValues
    assertThat(capturedDataProviderRequests)
      .containsExactly(
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[2].name },
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val measurementsCaptor: KArgumentCaptor<CreateMeasurementRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(2)) { createMeasurement(measurementsCaptor.capture()) }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER),
        Measurement.DataProviderEntry.Value.getDescriptor()
          .findFieldByNumber(
            Measurement.DataProviderEntry.Value.ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER
          ),
      )
      .containsExactly(
        createMeasurementRequest { measurement = REQUESTING_UNION_ALL_REACH_MEASUREMENT },
        createMeasurementRequest {
          measurement = REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
        },
      )

    capturedMeasurementRequests.forEach { capturedMeasurementRequest ->
      verifyMeasurementSpec(
        capturedMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )

      val dataProvidersList =
        capturedMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec =
        MeasurementSpec.parseFrom(capturedMeasurementRequest.measurement.measurementSpec.data)
      assertThat(measurementSpec)
        .isEqualTo(
          UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
            nonceHashes.clear()
            nonceHashes.addAll(
              List(dataProvidersList.size) { hashSha256(SECURE_RANDOM_OUTPUT_LONG) }
            )
          }
        )

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
            externalMeasurementId =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.externalMeasurementId
            cmmsMeasurementId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            externalMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.externalMeasurementId
            cmmsMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric without request ID when the measurements are created already`() = runBlocking {
    whenever(internalMetricsMock.createMetric(any()))
      .thenReturn(INTERNAL_PENDING_INCREMENTAL_REACH_METRIC)

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal MetricsCoroutineImplBase::getMetricByIdempotencyKey
    val getMetricByIdempotencyKeyCaptor: KArgumentCaptor<GetMetricByIdempotencyKeyRequest> =
      argumentCaptor()
    verifyBlocking(internalMetricsMock, never()) {
      getMetricByIdempotencyKey(getMetricByIdempotencyKeyCaptor.capture())
    }

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::getReportingSet
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::getReportingSet
      )
      .isEqualTo(
        internalGetReportingSetRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetId += INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearMetricIdempotencyKey() })

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    verifyProtoArgument(
        measurementConsumersMock,
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      )
      .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::batchGetReportingSets
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::batchGetReportingSets
      )
      .isEqualTo(
        batchGetReportingSetsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetIds += INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
          externalReportingSetIds +=
            INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, never()) { getDataProvider(dataProvidersCaptor.capture()) }

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val measurementsCaptor: KArgumentCaptor<CreateMeasurementRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, never()) { createMeasurement(measurementsCaptor.capture()) }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::createMeasurement
    val internalMeasurementsCaptor: KArgumentCaptor<BatchSetCmmsMeasurementIdsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetCmmsMeasurementIds(internalMeasurementsCaptor.capture())
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
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = PENDING_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::getReportingSet
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::getReportingSet
      )
      .isEqualTo(
        internalGetReportingSetRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetId += INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC)

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    verifyProtoArgument(
        measurementConsumersMock,
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      )
      .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::batchGetReportingSets
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::batchGetReportingSets
      )
      .isEqualTo(
        batchGetReportingSetsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetIds += INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
          externalReportingSetIds +=
            INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, never()) { getDataProvider(dataProvidersCaptor.capture()) }

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val measurementsCaptor: KArgumentCaptor<CreateMeasurementRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, never()) { createMeasurement(measurementsCaptor.capture()) }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::createMeasurement
    val internalMeasurementsCaptor: KArgumentCaptor<BatchSetCmmsMeasurementIdsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetCmmsMeasurementIds(internalMeasurementsCaptor.capture())
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
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
        runBlocking { service.createMetric(request) }
      }

    val expected = SUCCEEDED_INCREMENTAL_REACH_METRIC

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::getReportingSet
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::getReportingSet
      )
      .isEqualTo(
        internalGetReportingSetRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetId += INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of the internal MetricsCoroutineImplBase::createMetric
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::createMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC)

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    val getMeasurementConsumerCaptor: KArgumentCaptor<GetMeasurementConsumerRequest> =
      argumentCaptor()
    verifyBlocking(measurementConsumersMock, never()) {
      getMeasurementConsumer(getMeasurementConsumerCaptor.capture())
    }

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::batchGetReportingSets
    val batchGetReportingSetsCaptor: KArgumentCaptor<BatchGetReportingSetsRequest> =
      argumentCaptor()
    verifyBlocking(internalReportingSetsMock, never()) {
      batchGetReportingSets(batchGetReportingSetsCaptor.capture())
    }

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, never()) { getDataProvider(dataProvidersCaptor.capture()) }

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val measurementsCaptor: KArgumentCaptor<CreateMeasurementRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, never()) { createMeasurement(measurementsCaptor.capture()) }

    // Verify proto argument of internal MeasurementsCoroutineImplBase::createMeasurement
    val internalMeasurementsCaptor: KArgumentCaptor<BatchSetCmmsMeasurementIdsRequest> =
      argumentCaptor()
    verifyBlocking(internalMeasurementsMock, never()) {
      batchSetCmmsMeasurementIds(internalMeasurementsCaptor.capture())
    }

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createMetric throws UNAUTHENTICATED when no principal is found`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
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
    val request = createMetricRequest { metric = REQUESTING_INCREMENTAL_REACH_METRIC }

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
  fun `createMetric throws INVALID_ARGUMENT when metric is unspecified`() {
    val request = createMetricRequest { parent = MEASUREMENT_CONSUMERS.values.first().name }

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
          timeInterval = timeInterval { endTime = timestamp { seconds = 5 } }
        }
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
          timeInterval = timeInterval { startTime = timestamp { seconds = 5 } }
        }
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
          timeInterval = timeInterval {
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
  fun `createMetric throws INVALID_ARGUMENT when reporting set is unspecified`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearReportingSet() }
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
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = METRIC_WITH_INVALID_REPORTING_SET
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("Invalid reporting set name $INVALID_REPORTING_SET_NAME.")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when reporting set is not accessible to caller`() {
    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric =
        REQUESTING_INCREMENTAL_REACH_METRIC.copy { reportingSet = REPORTING_SET_NAME_FOR_MC_2 }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description)
      .isEqualTo("No access to the reporting set [$REPORTING_SET_NAME_FOR_MC_2].")
  }

  @Test
  fun `createMetric throws NOT_FOUND when reporting set is not found`() = runBlocking {
    whenever(internalReportingSetsMock.batchGetReportingSets(any()))
      .thenReturn(flowOf(INTERNAL_UNION_ALL_REPORTING_SET))
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
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
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
  fun `createMetric throws exception when internal createMetric throws exception`() = runBlocking {
    whenever(internalMetricsMock.createMetric(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
    }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    val expectedExceptionDescription = "Unable to create the metric in the reporting database."
    assertThat(exception.message).isEqualTo(expectedExceptionDescription)
  }

  @Test
  fun `createMetric throws exception when the CMMs createMeasurement throws exception`() =
    runBlocking {
      whenever(measurementsMock.createMeasurement(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
      }

      val exception =
        assertFailsWith(Exception::class) {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.createMetric(request) }
          }
        }
      val expectedExceptionDescription = "Unable to create a CMMs measurement."
      assertThat(exception.message).isEqualTo(expectedExceptionDescription)
    }

  @Test
  fun `createMetric throws exception when batchSetCmmsMeasurementId throws exception`() =
    runBlocking {
      whenever(internalMeasurementsMock.batchSetCmmsMeasurementIds(any()))
        .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

      val request = createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
      }

      val exception =
        assertFailsWith(Exception::class) {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
            runBlocking { service.createMetric(request) }
          }
        }
      val expectedExceptionDescription =
        "Unable to set the CMMs measurement IDs for the measurements in the reporting database."
      assertThat(exception.message).isEqualTo(expectedExceptionDescription)
    }

  @Test
  fun `createMetric throws exception when getMeasurementConsumer throws exception`() = runBlocking {
    whenever(measurementConsumersMock.getMeasurementConsumer(any()))
      .thenThrow(StatusRuntimeException(Status.INVALID_ARGUMENT))

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
    }

    val exception =
      assertFailsWith(Exception::class) {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMERS.values.first().name, CONFIG) {
          runBlocking { service.createMetric(request) }
        }
      }
    val expectedExceptionDescription =
      "Unable to retrieve the measurement consumer [${MEASUREMENT_CONSUMERS.values.first().name}]."
    assertThat(exception.message).isEqualTo(expectedExceptionDescription)
  }

  @Test
  fun `createMetric throws exception when the internal batchGetReportingSets throws exception`():
    Unit = runBlocking {
    whenever(internalReportingSetsMock.batchGetReportingSets(any()))
      .thenThrow(StatusRuntimeException(Status.UNKNOWN))

    val request = createMetricRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      metric = REQUESTING_INCREMENTAL_REACH_METRIC
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
  fun `batchCreateMetrics returns metrics with RUNNING state`() = runBlocking {
    whenever(internalReportingSetsMock.getReportingSet(any()))
      .thenReturn(INTERNAL_INCREMENTAL_REPORTING_SET, INTERNAL_SINGLE_PUBLISHER_REPORTING_SET)
    whenever(internalMetricsMock.batchCreateMetrics(any()))
      .thenReturn(
        flowOf(
          INTERNAL_PENDING_INITIAL_INCREMENTAL_REACH_METRIC,
          INTERNAL_PENDING_INITIAL_SINGLE_PUBLISHER_IMPRESSION_METRIC
        )
      )
    whenever(internalReportingSetsMock.batchGetReportingSets(any()))
      .thenReturn(
        flowOf(
          INTERNAL_UNION_ALL_REPORTING_SET,
          INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET,
          INTERNAL_SINGLE_PUBLISHER_REPORTING_SET
        )
      )

    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_INCREMENTAL_REACH_METRIC
      }
      requests += createMetricRequest {
        parent = MEASUREMENT_CONSUMERS.values.first().name
        metric = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC
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

    // Verify proto argument of the internal MetricsCoroutineImplBase::getMetricByIdempotencyKey
    val getMetricByIdempotencyKeyCaptor: KArgumentCaptor<GetMetricByIdempotencyKeyRequest> =
      argumentCaptor()
    verifyBlocking(internalMetricsMock, never()) {
      getMetricByIdempotencyKey(getMetricByIdempotencyKeyCaptor.capture())
    }

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::getReportingSet
    val getReportingSetCaptor: KArgumentCaptor<InternalGetReportingSetRequest> = argumentCaptor()
    verifyBlocking(internalReportingSetsMock, times(2)) {
      getReportingSet(getReportingSetCaptor.capture())
    }
    assertThat(getReportingSetCaptor.allValues)
      .containsExactly(
        internalGetReportingSetRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetId += INTERNAL_INCREMENTAL_REPORTING_SET.externalReportingSetId
        },
        internalGetReportingSetRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetId += INTERNAL_SINGLE_PUBLISHER_REPORTING_SET.externalReportingSetId
        }
      )

    // Verify proto argument of the internal MetricsCoroutineImplBase::batchCreateMetrics
    verifyProtoArgument(internalMetricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalBatchCreateMetricsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          metrics +=
            INTERNAL_REQUESTING_INCREMENTAL_REACH_METRIC.copy { clearMetricIdempotencyKey() }
          metrics +=
            INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.copy {
              clearMetricIdempotencyKey()
            }
        }
      )

    // Verify proto argument of MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
    verifyProtoArgument(
        measurementConsumersMock,
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase::getMeasurementConsumer
      )
      .isEqualTo(getMeasurementConsumerRequest { name = MEASUREMENT_CONSUMERS.values.first().name })

    // Verify proto argument of the internal ReportingSetsCoroutineImplBase::batchGetReportingSets
    verifyProtoArgument(
        internalReportingSetsMock,
        InternalReportingSetsGrpcKt.ReportingSetsCoroutineImplBase::batchGetReportingSets
      )
      .isEqualTo(
        batchGetReportingSetsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMERS.keys.first().measurementConsumerId
          externalReportingSetIds += INTERNAL_UNION_ALL_REPORTING_SET.externalReportingSetId
          externalReportingSetIds +=
            INTERNAL_UNION_ALL_BUT_LAST_PUBLISHER_REPORTING_SET.externalReportingSetId
          externalReportingSetIds +=
            INTERNAL_REQUESTING_SINGLE_PUBLISHER_IMPRESSION_METRIC.externalReportingSetId
        }
      )

    // Verify proto argument of DataProvidersCoroutineImplBase::getDataProvider
    val dataProvidersCaptor: KArgumentCaptor<GetDataProviderRequest> = argumentCaptor()
    verifyBlocking(dataProvidersMock, times(6)) { getDataProvider(dataProvidersCaptor.capture()) }

    val capturedDataProviderRequests = dataProvidersCaptor.allValues
    assertThat(capturedDataProviderRequests)
      .containsExactly(
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[1].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[2].name },
        getDataProviderRequest { name = DATA_PROVIDERS_LIST[0].name },
      )

    // Verify proto argument of MeasurementsCoroutineImplBase::createMeasurement
    val measurementsCaptor: KArgumentCaptor<CreateMeasurementRequest> = argumentCaptor()
    verifyBlocking(measurementsMock, times(3)) { createMeasurement(measurementsCaptor.capture()) }
    val capturedMeasurementRequests = measurementsCaptor.allValues
    assertThat(capturedMeasurementRequests)
      .ignoringRepeatedFieldOrder()
      .ignoringFieldDescriptors(
        Measurement.getDescriptor().findFieldByNumber(Measurement.MEASUREMENT_SPEC_FIELD_NUMBER),
        Measurement.DataProviderEntry.Value.getDescriptor()
          .findFieldByNumber(
            Measurement.DataProviderEntry.Value.ENCRYPTED_REQUISITION_SPEC_FIELD_NUMBER
          ),
      )
      .containsExactly(
        createMeasurementRequest { measurement = REQUESTING_UNION_ALL_REACH_MEASUREMENT },
        createMeasurementRequest {
          measurement = REQUESTING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT
        },
        createMeasurementRequest {
          measurement = REQUESTING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT
        },
      )

    capturedMeasurementRequests.forEach { capturedMeasurementRequest ->
      verifyMeasurementSpec(
        capturedMeasurementRequest.measurement.measurementSpec,
        MEASUREMENT_CONSUMER_CERTIFICATE,
        TRUSTED_MEASUREMENT_CONSUMER_ISSUER
      )

      val dataProvidersList =
        capturedMeasurementRequest.measurement.dataProvidersList.sortedBy { it.key }

      val measurementSpec =
        MeasurementSpec.parseFrom(capturedMeasurementRequest.measurement.measurementSpec.data)
      assertThat(measurementSpec)
        .isEqualTo(
          if (dataProvidersList.size == 1) SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT_SPEC
          else
            UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT_SPEC.copy {
              nonceHashes.clear()
              nonceHashes.addAll(
                List(dataProvidersList.size) { hashSha256(SECURE_RANDOM_OUTPUT_LONG) }
              )
            }
        )

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
            externalMeasurementId =
              INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.externalMeasurementId
            cmmsMeasurementId = INTERNAL_PENDING_UNION_ALL_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            externalMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.externalMeasurementId
            cmmsMeasurementId =
              INTERNAL_PENDING_UNION_ALL_BUT_LAST_PUBLISHER_REACH_MEASUREMENT.cmmsMeasurementId
          }
          this.measurementIds += measurementIds {
            externalMeasurementId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.externalMeasurementId
            cmmsMeasurementId =
              INTERNAL_PENDING_SINGLE_PUBLISHER_IMPRESSION_MEASUREMENT.cmmsMeasurementId
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `batchCreateMetric throws exception when number of requests exceeds limit`() = runBlocking {
    val request = batchCreateMetricsRequest {
      parent = MEASUREMENT_CONSUMERS.values.first().name

      requests +=
        List(MAX_BATCH_SIZE + 1) {
          createMetricRequest {
            parent = MEASUREMENT_CONSUMERS.values.first().name
            metric = REQUESTING_INCREMENTAL_REACH_METRIC
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
}

private fun EventGroupKey.toInternal(): InternalReportingSet.Primitive.EventGroupKey {
  val source = this
  return InternalReportingSetKt.PrimitiveKt.eventGroupKey {
    cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
    cmmsDataProviderId = source.cmmsDataProviderId
    cmmsEventGroupId = source.cmmsEventGroupId
  }
}

private val InternalReportingSet.resourceKey: ReportingSetKey
  get() = ReportingSetKey(cmmsMeasurementConsumerId, ExternalId(externalReportingSetId).apiId.value)
private val InternalReportingSet.resourceName: String
  get() = resourceKey.toName()
