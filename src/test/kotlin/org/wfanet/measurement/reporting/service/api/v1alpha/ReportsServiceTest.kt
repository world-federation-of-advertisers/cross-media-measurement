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
import com.google.protobuf.ByteString
import com.google.protobuf.duration
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2.alpha.ListReportsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.listReportsPageToken
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.failure
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.MeasurementKt.resultPair
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.makeDataProviderCertificateName
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.testing.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.consent.client.duchy.encryptResult
import org.wfanet.measurement.consent.client.duchy.signResult
import org.wfanet.measurement.internal.reporting.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.frequency as internalFrequency
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.impression as internalImpression
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.reach as internalReach
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.watchDuration as internalWatchDuration
import org.wfanet.measurement.internal.reporting.MeasurementKt.failure as internalFailure
import org.wfanet.measurement.internal.reporting.MeasurementKt.result as internalMeasurementResult
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineImplBase as InternalMeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.Metric as InternalMetric
import org.wfanet.measurement.internal.reporting.MetricKt.MeasurementCalculationKt.weightedMeasurement
import org.wfanet.measurement.internal.reporting.MetricKt.SetOperationKt.operand as internalSetOperationOperand
import org.wfanet.measurement.internal.reporting.MetricKt.SetOperationKt.reportingSetKey
import org.wfanet.measurement.internal.reporting.MetricKt.details as internalMetricDetails
import org.wfanet.measurement.internal.reporting.MetricKt.frequencyHistogramParams as internalFrequencyHistogramParams
import org.wfanet.measurement.internal.reporting.MetricKt.impressionCountParams as internalImpressionCountParams
import org.wfanet.measurement.internal.reporting.MetricKt.measurementCalculation
import org.wfanet.measurement.internal.reporting.MetricKt.namedSetOperation as internalNamedSetOperation
import org.wfanet.measurement.internal.reporting.MetricKt.reachParams as internalReachParams
import org.wfanet.measurement.internal.reporting.MetricKt.setOperation as internalSetOperation
import org.wfanet.measurement.internal.reporting.MetricKt.watchDurationParams as internalWatchDurationParams
import org.wfanet.measurement.internal.reporting.Report as InternalReport
import org.wfanet.measurement.internal.reporting.ReportKt.details as internalReportDetails
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.StreamReportsRequestKt.filter
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.getReportRequest as getInternalReportRequest
import org.wfanet.measurement.internal.reporting.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.metric as internalMetric
import org.wfanet.measurement.internal.reporting.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.report as internalReport
import org.wfanet.measurement.internal.reporting.setMeasurementFailureRequest
import org.wfanet.measurement.internal.reporting.setMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.streamReportsRequest
import org.wfanet.measurement.internal.reporting.timeInterval as internalTimeInterval
import org.wfanet.measurement.reporting.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.v1alpha.Metric
import org.wfanet.measurement.reporting.v1alpha.MetricKt.SetOperationKt.operand as setOperationOperand
import org.wfanet.measurement.reporting.v1alpha.MetricKt.frequencyHistogramParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.impressionCountParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.namedSetOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.reachParams
import org.wfanet.measurement.reporting.v1alpha.MetricKt.setOperation
import org.wfanet.measurement.reporting.v1alpha.MetricKt.watchDurationParams
import org.wfanet.measurement.reporting.v1alpha.Report
import org.wfanet.measurement.reporting.v1alpha.ReportKt.EventGroupUniverseKt.eventGroupEntry
import org.wfanet.measurement.reporting.v1alpha.ReportKt.eventGroupUniverse
import org.wfanet.measurement.reporting.v1alpha.copy
import org.wfanet.measurement.reporting.v1alpha.listReportsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.v1alpha.metric
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000
private const val PAGE_SIZE = 3

private val SECRETS_DIR: Path =
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

// Authentication key
private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"

// Certificate
private val AGGREGATOR_CERTIFICATE_DER =
  SECRETS_DIR.resolve("aggregator_cs_cert.der").toFile().readByteString()
private val AGGREGATOR_PRIVATE_KEY_DER =
  SECRETS_DIR.resolve("aggregator_cs_private.der").toFile().readByteString()
private val AGGREGATOR_SIGNING_KEY: SigningKeyHandle by lazy {
  val consentSignal509Cert = readCertificate(AGGREGATOR_CERTIFICATE_DER)
  SigningKeyHandle(
    consentSignal509Cert,
    readPrivateKey(AGGREGATOR_PRIVATE_KEY_DER, consentSignal509Cert.publicKey.algorithm)
  )
}
private val AGGREGATOR_CERTIFICATE = certificate { x509Der = AGGREGATOR_CERTIFICATE_DER }

// Public and private keys
private val MEASUREMENT_PUBLIC_KEY_DATA =
  SECRETS_DIR.resolve("mc_enc_public.tink").toFile().readByteString()
private val MEASUREMENT_PUBLIC_KEY = encryptionPublicKey {
  format = EncryptionPublicKey.Format.TINK_KEYSET
  data = MEASUREMENT_PUBLIC_KEY_DATA
}

private val MEASUREMENT_PRIVATE_KEY_DATA = SECRETS_DIR.resolve("mc_enc_private.tink").toFile()
private val MEASUREMENT_PRIVATE_KEY: PrivateKeyHandle = loadPrivateKey(MEASUREMENT_PRIVATE_KEY_DATA)

// Measurement consumer IDs and names
private const val MEASUREMENT_CONSUMER_EXTERNAL_ID = 111L
private const val MEASUREMENT_CONSUMER_EXTERNAL_ID_2 = 112L
private val MEASUREMENT_CONSUMER_REFERENCE_ID = externalIdToApiId(MEASUREMENT_CONSUMER_EXTERNAL_ID)
private val MEASUREMENT_CONSUMER_REFERENCE_ID_2 =
  externalIdToApiId(MEASUREMENT_CONSUMER_EXTERNAL_ID_2)
private val MEASUREMENT_CONSUMER_NAME =
  MeasurementConsumerKey(MEASUREMENT_CONSUMER_REFERENCE_ID).toName()
private val MEASUREMENT_CONSUMER_NAME_2 =
  MeasurementConsumerKey(MEASUREMENT_CONSUMER_REFERENCE_ID_2).toName()

// Reporting set IDs and names
private const val REPORTING_SET_EXTERNAL_ID = 221L
private const val REPORTING_SET_EXTERNAL_ID_2 = 222L
private const val REPORTING_SET_EXTERNAL_ID_3 = 223L

private val REPORTING_SET_NAME =
  ReportingSetKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORTING_SET_EXTERNAL_ID))
    .toName()
private val REPORTING_SET_NAME_2 =
  ReportingSetKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORTING_SET_EXTERNAL_ID_2))
    .toName()
private val REPORTING_SET_NAME_3 =
  ReportingSetKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORTING_SET_EXTERNAL_ID_3))
    .toName()

// Report IDs and names
private const val REPORT_EXTERNAL_ID = 331L
private const val REPORT_EXTERNAL_ID_2 = 332L
private const val REPORT_EXTERNAL_ID_3 = 333L
private const val REPORT_EXTERNAL_ID_4 = 334L

private val REPORT_NAME =
  ReportKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORT_EXTERNAL_ID)).toName()
private val REPORT_NAME_2 =
  ReportKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORT_EXTERNAL_ID_2)).toName()
private val REPORT_NAME_3 =
  ReportKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORT_EXTERNAL_ID_3)).toName()
private val REPORT_NAME_4 =
  ReportKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORT_EXTERNAL_ID_4)).toName()

// Measurement IDs and names
private const val REACH_MEASUREMENT_EXTERNAL_ID = 441L
private const val FREQUENCY_HISTOGRAM_MEASUREMENT_EXTERNAL_ID = 442L
private const val IMPRESSION_MEASUREMENT_EXTERNAL_ID = 443L
private const val WATCH_DURATION_MEASUREMENT_EXTERNAL_ID = 444L

private val REACH_MEASUREMENT_REFERENCE_ID = externalIdToApiId(REACH_MEASUREMENT_EXTERNAL_ID)
private val FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID =
  externalIdToApiId(FREQUENCY_HISTOGRAM_MEASUREMENT_EXTERNAL_ID)
private val IMPRESSION_MEASUREMENT_REFERENCE_ID =
  externalIdToApiId(IMPRESSION_MEASUREMENT_EXTERNAL_ID)
private val WATCH_DURATION_MEASUREMENT_REFERENCE_ID =
  externalIdToApiId(WATCH_DURATION_MEASUREMENT_EXTERNAL_ID)

private val REACH_MEASUREMENT_NAME =
  MeasurementKey(MEASUREMENT_CONSUMER_REFERENCE_ID, REACH_MEASUREMENT_REFERENCE_ID).toName()
private val FREQUENCY_HISTOGRAM_MEASUREMENT_NAME =
  MeasurementKey(MEASUREMENT_CONSUMER_REFERENCE_ID, FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID)
    .toName()
private val IMPRESSION_MEASUREMENT_NAME =
  MeasurementKey(MEASUREMENT_CONSUMER_REFERENCE_ID, IMPRESSION_MEASUREMENT_REFERENCE_ID).toName()
private val WATCH_DURATION_MEASUREMENT_NAME =
  MeasurementKey(MEASUREMENT_CONSUMER_REFERENCE_ID, WATCH_DURATION_MEASUREMENT_REFERENCE_ID)
    .toName()

// Data provider IDs and names
private const val DATA_PROVIDER_EXTERNAL_ID = 551L
private const val DATA_PROVIDER_EXTERNAL_ID_2 = 552L
private const val DATA_PROVIDER_EXTERNAL_ID_3 = 553L
private val DATA_PROVIDER_REFERENCE_ID = externalIdToApiId(DATA_PROVIDER_EXTERNAL_ID)
private val DATA_PROVIDER_REFERENCE_ID_2 = externalIdToApiId(DATA_PROVIDER_EXTERNAL_ID_2)
private val DATA_PROVIDER_REFERENCE_ID_3 = externalIdToApiId(DATA_PROVIDER_EXTERNAL_ID_3)

private val DATA_PROVIDER_NAME = DataProviderKey(DATA_PROVIDER_REFERENCE_ID).toName()
private val DATA_PROVIDER_NAME_2 = DataProviderKey(DATA_PROVIDER_REFERENCE_ID_2).toName()
private val DATA_PROVIDER_NAME_3 = DataProviderKey(DATA_PROVIDER_REFERENCE_ID_3).toName()

private const val DATA_PROVIDER_CERTIFICATE_EXTERNAL_ID = 561L
private const val DATA_PROVIDER_CERTIFICATE_EXTERNAL_ID_2 = 562L
private const val DATA_PROVIDER_CERTIFICATE_EXTERNAL_ID_3 = 563L
private val DATA_PROVIDER_CERTIFICATE_REFERENCE_ID =
  externalIdToApiId(DATA_PROVIDER_CERTIFICATE_EXTERNAL_ID)
private val DATA_PROVIDER_CERTIFICATE_REFERENCE_ID_2 =
  externalIdToApiId(DATA_PROVIDER_CERTIFICATE_EXTERNAL_ID_2)
private val DATA_PROVIDER_CERTIFICATE_REFERENCE_ID_3 =
  externalIdToApiId(DATA_PROVIDER_CERTIFICATE_EXTERNAL_ID_3)

private val DATA_PROVIDER_CERTIFICATE_NAME =
  makeDataProviderCertificateName(
    DATA_PROVIDER_REFERENCE_ID,
    DATA_PROVIDER_CERTIFICATE_REFERENCE_ID
  )
private val DATA_PROVIDER_CERTIFICATE_NAME_2 =
  makeDataProviderCertificateName(
    DATA_PROVIDER_REFERENCE_ID_2,
    DATA_PROVIDER_CERTIFICATE_REFERENCE_ID_2
  )
private val DATA_PROVIDER_CERTIFICATE_NAME_3 =
  makeDataProviderCertificateName(
    DATA_PROVIDER_REFERENCE_ID_3,
    DATA_PROVIDER_CERTIFICATE_REFERENCE_ID_3
  )

// Event group IDs and names
private const val EVENT_GROUP_EXTERNAL_ID = 661L
private const val EVENT_GROUP_EXTERNAL_ID_2 = 662L
private const val EVENT_GROUP_EXTERNAL_ID_3 = 663L
private val EVENT_GROUP_REFERENCE_ID = externalIdToApiId(EVENT_GROUP_EXTERNAL_ID)
private val EVENT_GROUP_REFERENCE_ID_2 = externalIdToApiId(EVENT_GROUP_EXTERNAL_ID_2)
private val EVENT_GROUP_REFERENCE_ID_3 = externalIdToApiId(EVENT_GROUP_EXTERNAL_ID_3)

private val EVENT_GROUP_NAME =
  EventGroupKey(
      MEASUREMENT_CONSUMER_REFERENCE_ID,
      DATA_PROVIDER_REFERENCE_ID,
      EVENT_GROUP_REFERENCE_ID
    )
    .toName()
private val EVENT_GROUP_NAME_2 =
  EventGroupKey(
      MEASUREMENT_CONSUMER_REFERENCE_ID,
      DATA_PROVIDER_REFERENCE_ID_2,
      EVENT_GROUP_REFERENCE_ID_2
    )
    .toName()
private val EVENT_GROUP_NAME_3 =
  EventGroupKey(
      MEASUREMENT_CONSUMER_REFERENCE_ID,
      DATA_PROVIDER_REFERENCE_ID_3,
      EVENT_GROUP_REFERENCE_ID_3
    )
    .toName()

// Time intervals
private val START_TIME = timestamp {
  seconds = 1000
  nanos = 1000
}
private val TIME_INTERVAL_INCREMENT = duration { seconds = 1000 }
private const val INTERVAL_COUNT = 1
private val END_TIME = timestamp {
  seconds = 2000
  nanos = 1000
}
private val TIME_INTERVAL = internalTimeInterval {
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

// Set operations
private val INTERNAL_SET_OPERATION = internalSetOperation {
  type = InternalMetric.SetOperation.Type.UNION
  lhs = internalSetOperationOperand {
    reportingSetId = reportingSetKey {
      measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
      externalReportingSetId = REPORTING_SET_EXTERNAL_ID
    }
  }
  rhs = internalSetOperationOperand {
    reportingSetId = reportingSetKey {
      measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
      externalReportingSetId = REPORTING_SET_EXTERNAL_ID_2
    }
  }
}

private val SET_OPERATION = setOperation {
  type = Metric.SetOperation.Type.UNION
  lhs = setOperationOperand { reportingSet = REPORTING_SET_NAME }
  rhs = setOperationOperand { reportingSet = REPORTING_SET_NAME_2 }
}

// Measurements
private const val REACH_VALUE = 100_000L
private val FREQUENCY_DISTRIBUTION = mapOf(1L to 1.0 / 6, 2L to 2.0 / 6, 3L to 3.0 / 6)
private const val IMPRESSION_VALUE = 100L
private const val IMPRESSION_VALUE_2 = 150L
private const val IMPRESSION_VALUE_3 = 200L
private val WATCH_DURATION = duration { seconds = 100 }
private val WATCH_DURATION_2 = duration { seconds = 200 }
private val WATCH_DURATION_3 = duration { seconds = 300 }

// Reach
private val SUCCEEDED_REACH_MEASUREMENT = measurement {
  name = REACH_MEASUREMENT_NAME
  state = Measurement.State.SUCCEEDED
  measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID

  results += resultPair {
    val result = result {
      reach = MeasurementKt.ResultKt.reach { value = REACH_VALUE }
      frequency =
        MeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
        }
    }
    encryptedResult = getEncryptedResult(result)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME
  }
}
private val PENDING_REACH_MEASUREMENT =
  SUCCEEDED_REACH_MEASUREMENT.copy {
    state = Measurement.State.COMPUTING
    results.clear()
  }
private val INTERNAL_SUCCEEDED_REACH_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.SUCCEEDED
  result = internalMeasurementResult {
    reach = internalReach { value = REACH_VALUE }
    frequency = internalFrequency { relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION) }
  }
}
private val INTERNAL_PENDING_REACH_MEASUREMENT =
  INTERNAL_SUCCEEDED_REACH_MEASUREMENT.copy {
    state = InternalMeasurement.State.PENDING
    clearResult()
  }
// Frequency histogram
private val SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT = measurement {
  name = FREQUENCY_HISTOGRAM_MEASUREMENT_NAME
  state = Measurement.State.SUCCEEDED
  measurementReferenceId = FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID

  results += resultPair {
    val result = result {
      reach = MeasurementKt.ResultKt.reach { value = REACH_VALUE }
      frequency =
        MeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
        }
    }
    encryptedResult = getEncryptedResult(result)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME
  }
}
private val PENDING_FREQUENCY_HISTOGRAM_MEASUREMENT =
  SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT.copy {
    state = Measurement.State.COMPUTING
    results.clear()
  }
private val INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.SUCCEEDED
  result = internalMeasurementResult {
    reach = internalReach { value = REACH_VALUE }
    frequency = internalFrequency { relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION) }
  }
}
private val INTERNAL_PENDING_FREQUENCY_HISTOGRAM_MEASUREMENT =
  INTERNAL_SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT.copy {
    state = InternalMeasurement.State.PENDING
    clearResult()
  }
// Impression
private val SUCCEEDED_IMPRESSION_MEASUREMENT = measurement {
  name = IMPRESSION_MEASUREMENT_NAME
  state = Measurement.State.SUCCEEDED
  measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID

  results += resultPair {
    val result = result {
      impression = MeasurementKt.ResultKt.impression { value = IMPRESSION_VALUE }
    }
    encryptedResult = getEncryptedResult(result)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME
  }
  results += resultPair {
    val result = result {
      impression = MeasurementKt.ResultKt.impression { value = IMPRESSION_VALUE_2 }
    }
    encryptedResult = getEncryptedResult(result)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME_2
  }
  results += resultPair {
    val result = result {
      impression = MeasurementKt.ResultKt.impression { value = IMPRESSION_VALUE_3 }
    }
    encryptedResult = getEncryptedResult(result)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME_3
  }
}
private val PENDING_IMPRESSION_MEASUREMENT =
  SUCCEEDED_IMPRESSION_MEASUREMENT.copy {
    state = Measurement.State.COMPUTING
    results.clear()
  }
private val INTERNAL_SUCCEEDED_IMPRESSION_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.SUCCEEDED
  result = internalMeasurementResult {
    impression = internalImpression {
      value = IMPRESSION_VALUE + IMPRESSION_VALUE_2 + IMPRESSION_VALUE_3
    }
  }
}
private val INTERNAL_PENDING_IMPRESSION_MEASUREMENT =
  INTERNAL_SUCCEEDED_IMPRESSION_MEASUREMENT.copy {
    state = InternalMeasurement.State.PENDING
    clearResult()
  }
// Watch Duration
private val SUCCEEDED_WATCH_DURATION_MEASUREMENT = measurement {
  name = WATCH_DURATION_MEASUREMENT_NAME
  state = Measurement.State.SUCCEEDED
  measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID

  results += resultPair {
    val result = result {
      watchDuration = MeasurementKt.ResultKt.watchDuration { value = WATCH_DURATION }
    }
    encryptedResult = getEncryptedResult(result)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME
  }
  results += resultPair {
    val result = result {
      watchDuration = MeasurementKt.ResultKt.watchDuration { value = WATCH_DURATION_2 }
    }
    encryptedResult = getEncryptedResult(result)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME_2
  }
  results += resultPair {
    val result = result {
      watchDuration = MeasurementKt.ResultKt.watchDuration { value = WATCH_DURATION_3 }
    }
    encryptedResult = getEncryptedResult(result)
    certificate = DATA_PROVIDER_CERTIFICATE_NAME_3
  }
}
private val PENDING_WATCH_DURATION_MEASUREMENT =
  SUCCEEDED_WATCH_DURATION_MEASUREMENT.copy {
    state = Measurement.State.COMPUTING
    results.clear()
  }
private val INTERNAL_SUCCEEDED_WATCH_DURATION_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.SUCCEEDED
  result = internalMeasurementResult {
    watchDuration = internalWatchDuration {
      value = Durations.add(Durations.add(WATCH_DURATION, WATCH_DURATION_2), WATCH_DURATION_3)
    }
  }
}
private val INTERNAL_PENDING_WATCH_DURATION_MEASUREMENT =
  INTERNAL_SUCCEEDED_WATCH_DURATION_MEASUREMENT.copy {
    state = InternalMeasurement.State.PENDING
    clearResult()
  }

private fun getEncryptedResult(
  result: Measurement.Result,
): ByteString {
  val signedResult = signResult(result, AGGREGATOR_SIGNING_KEY)
  return encryptResult(signedResult, MEASUREMENT_PUBLIC_KEY)
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
  timeInterval = TIME_INTERVAL
  weightedMeasurements.add(WEIGHTED_REACH_MEASUREMENT)
}

private val FREQUENCY_HISTOGRAM_MEASUREMENT_CALCULATION = measurementCalculation {
  timeInterval = TIME_INTERVAL
  weightedMeasurements.add(WEIGHTED_FREQUENCY_HISTOGRAM_MEASUREMENT)
}

private val IMPRESSION_MEASUREMENT_CALCULATION = measurementCalculation {
  timeInterval = TIME_INTERVAL
  weightedMeasurements.add(WEIGHTED_IMPRESSION_MEASUREMENT)
}

private val WATCH_DURATION_MEASUREMENT_CALCULATION = measurementCalculation {
  timeInterval = TIME_INTERVAL
  weightedMeasurements.add(WEIGHTED_WATCH_DURATION_MEASUREMENT)
}

// Named set operations
private const val REACH_SET_OPERATION_DISPLAY_NAME = "Reach Set Operation"
private const val FREQUENCY_HISTOGRAM_SET_OPERATION_DISPLAY_NAME =
  "Frequency Histogram Set Operation"
private const val IMPRESSION_SET_OPERATION_DISPLAY_NAME = "Impression Set Operation"
private const val WATCH_DURATION_SET_OPERATION_DISPLAY_NAME = "Watch Duration Set Operation"
// Reach set operation
private val INTERNAL_NAMED_REACH_SET_OPERATION = internalNamedSetOperation {
  displayName = REACH_SET_OPERATION_DISPLAY_NAME
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(REACH_MEASUREMENT_CALCULATION)
}
private val NAMED_REACH_SET_OPERATION = namedSetOperation {
  displayName = REACH_SET_OPERATION_DISPLAY_NAME
  setOperation = SET_OPERATION
}
// Frequency histogram set operation
private val INTERNAL_NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION = internalNamedSetOperation {
  displayName = FREQUENCY_HISTOGRAM_SET_OPERATION_DISPLAY_NAME
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(FREQUENCY_HISTOGRAM_MEASUREMENT_CALCULATION)
}
private val NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION = namedSetOperation {
  displayName = FREQUENCY_HISTOGRAM_SET_OPERATION_DISPLAY_NAME
  setOperation = SET_OPERATION
}
// Impression set operation
private val INTERNAL_NAMED_IMPRESSION_SET_OPERATION = internalNamedSetOperation {
  displayName = IMPRESSION_SET_OPERATION_DISPLAY_NAME
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(IMPRESSION_MEASUREMENT_CALCULATION)
}
private val NAMED_IMPRESSION_SET_OPERATION = namedSetOperation {
  displayName = IMPRESSION_SET_OPERATION_DISPLAY_NAME
  setOperation = SET_OPERATION
}
// Watch duration set operation
private val INTERNAL_NAMED_WATCH_DURATION_SET_OPERATION = internalNamedSetOperation {
  displayName = WATCH_DURATION_SET_OPERATION_DISPLAY_NAME
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(WATCH_DURATION_MEASUREMENT_CALCULATION)
}
private val NAMED_WATCH_DURATION_SET_OPERATION = namedSetOperation {
  displayName = WATCH_DURATION_SET_OPERATION_DISPLAY_NAME
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
  details = internalMetricDetails {
    reach = internalReachParams {}
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
  details = internalMetricDetails {
    frequencyHistogram = internalFrequencyHistogramParams {
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
  details = internalMetricDetails {
    impressionCount = internalImpressionCountParams {
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
  details = internalMetricDetails {
    watchDuration = internalWatchDurationParams {
      maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
      maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
    }
    cumulative = false
  }
  namedSetOperations.add(INTERNAL_NAMED_WATCH_DURATION_SET_OPERATION)
}

// Event group filters
private const val EVENT_GROUP_FILTER = "AGE>20"
private val EVENT_GROUP_FILTERS_MAP =
  mapOf(
    EVENT_GROUP_NAME to EVENT_GROUP_FILTER,
    EVENT_GROUP_NAME_2 to EVENT_GROUP_FILTER,
    EVENT_GROUP_NAME_3 to EVENT_GROUP_FILTER,
  )

// Internal reports with running states
private const val REACH_REPORT_IDEMPOTENCY_KEY = "TEST_REACH_REPORT"
private const val IMPRESSION_REPORT_IDEMPOTENCY_KEY = "TEST_IMPRESSION_REPORT"
private const val WATCH_DURATION_REPORT_IDEMPOTENCY_KEY = "TEST_WATCH_DURATION_REPORT"
private const val FREQUENCY_HISTOGRAM_REPORT_IDEMPOTENCY_KEY = "TEST_FREQUENCY_HISTOGRAM_REPORT"
// Internal reports of reach
private val INTERNAL_PENDING_REACH_REPORT = internalReport {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportId = REPORT_EXTERNAL_ID
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_REACH_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(REACH_MEASUREMENT_REFERENCE_ID, INTERNAL_PENDING_REACH_MEASUREMENT)
  details = internalReportDetails { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
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
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportId = REPORT_EXTERNAL_ID_2
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_IMPRESSION_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(IMPRESSION_MEASUREMENT_REFERENCE_ID, INTERNAL_PENDING_IMPRESSION_MEASUREMENT)
  details = internalReportDetails { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
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
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportId = REPORT_EXTERNAL_ID_3
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_WATCH_DURATION_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(
    WATCH_DURATION_MEASUREMENT_REFERENCE_ID,
    INTERNAL_PENDING_WATCH_DURATION_MEASUREMENT
  )
  details = internalReportDetails { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
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
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportId = REPORT_EXTERNAL_ID_4
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_FREQUENCY_HISTOGRAM_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(
    FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID,
    INTERNAL_PENDING_FREQUENCY_HISTOGRAM_MEASUREMENT
  )
  details = internalReportDetails { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
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

// Event Group Universe
private val EVENT_GROUP_ENTRY = eventGroupEntry {
  key = EVENT_GROUP_NAME
  value = EVENT_GROUP_FILTER
}
private val EVENT_GROUP_ENTRY_2 = eventGroupEntry {
  key = EVENT_GROUP_NAME_2
  value = EVENT_GROUP_FILTER
}
private val EVENT_GROUP_ENTRY_3 = eventGroupEntry {
  key = EVENT_GROUP_NAME_3
  value = EVENT_GROUP_FILTER
}

private val EVENT_GROUP_UNIVERSE = eventGroupUniverse {
  eventGroupEntries.addAll(listOf(EVENT_GROUP_ENTRY, EVENT_GROUP_ENTRY_2, EVENT_GROUP_ENTRY_3))
}

// Public reports with running states
// Reports of reach
private val PENDING_REACH_REPORT = report {
  name = REPORT_NAME
  reportIdempotencyKey = REACH_REPORT_IDEMPOTENCY_KEY
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(REACH_METRIC)
  state = Report.State.RUNNING
}
private val SUCCEEDED_REACH_REPORT = PENDING_REACH_REPORT.copy { state = Report.State.SUCCEEDED }
// Reports of impression
private val PENDING_IMPRESSION_REPORT = report {
  name = REPORT_NAME_2
  reportIdempotencyKey = IMPRESSION_REPORT_IDEMPOTENCY_KEY
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(IMPRESSION_METRIC)
  state = Report.State.RUNNING
}
private val SUCCEEDED_IMPRESSION_REPORT =
  PENDING_IMPRESSION_REPORT.copy { state = Report.State.SUCCEEDED }
// Reports of watch duration
private val PENDING_WATCH_DURATION_REPORT = report {
  name = REPORT_NAME_3
  reportIdempotencyKey = WATCH_DURATION_REPORT_IDEMPOTENCY_KEY
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(WATCH_DURATION_METRIC)
  state = Report.State.RUNNING
}
private val SUCCEEDED_WATCH_DURATION_REPORT =
  PENDING_WATCH_DURATION_REPORT.copy { state = Report.State.SUCCEEDED }
// Reports of frequency histogram
private val PENDING_FREQUENCY_HISTOGRAM_REPORT = report {
  name = REPORT_NAME_4
  reportIdempotencyKey = FREQUENCY_HISTOGRAM_REPORT_IDEMPOTENCY_KEY
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(FREQUENCY_HISTOGRAM_METRIC)
  state = Report.State.RUNNING
}
private val SUCCEEDED_FREQUENCY_HISTOGRAM_REPORT =
  PENDING_FREQUENCY_HISTOGRAM_REPORT.copy { state = Report.State.SUCCEEDED }

@RunWith(JUnit4::class)
class ReportsServiceTest {

  private val internalReportsMock: ReportsCoroutineImplBase =
    mockService() {
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
    }

  private val internalMeasurementsMock: InternalMeasurementsCoroutineImplBase =
    mockService() {
      onBlocking { setMeasurementResult(any()) }.thenReturn(null)
      onBlocking { setMeasurementFailure(any()) }.thenReturn(null)
    }

  private val measurementsMock: MeasurementsCoroutineImplBase =
    mockService() {
      onBlocking { getMeasurement(any()) }
        .thenReturn(
          SUCCEEDED_REACH_MEASUREMENT,
          SUCCEEDED_IMPRESSION_MEASUREMENT,
          SUCCEEDED_WATCH_DURATION_MEASUREMENT,
          SUCCEEDED_FREQUENCY_HISTOGRAM_MEASUREMENT,
        )
    }

  private val certificateMock: CertificatesCoroutineImplBase =
    mockService() { onBlocking { getCertificate(any()) }.thenReturn(AGGREGATOR_CERTIFICATE) }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalReportsMock)
    addService(internalMeasurementsMock)
    addService(measurementsMock)
    addService(certificateMock)
  }

  private lateinit var service: ReportsService

  @Before
  fun initService() {
    service =
      ReportsService(
        ReportsCoroutineStub(grpcTestServerRule.channel),
        InternalMeasurementsCoroutineStub(grpcTestServerRule.channel),
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        CertificatesCoroutineStub(grpcTestServerRule.channel),
        MEASUREMENT_PRIVATE_KEY,
        API_AUTHENTICATION_KEY,
      )
  }

  @Test
  fun `listReports returns without a next page token when there is no previous page token`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
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
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns with a next page token when there is no previous page token`() {
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = PAGE_SIZE
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)

      nextPageToken =
        listReportsPageToken {
            pageSize = PAGE_SIZE
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            lastReport = previousPageEnd {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              externalReportId = REPORT_EXTERNAL_ID_3
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
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns with a next page token when there is a previous page token`() {
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = PAGE_SIZE
      pageToken =
        listReportsPageToken {
            pageSize = PAGE_SIZE
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            lastReport = previousPageEnd {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              externalReportId = REPORT_EXTERNAL_ID
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)

      nextPageToken =
        listReportsPageToken {
            pageSize = PAGE_SIZE
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            lastReport = previousPageEnd {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              externalReportId = REPORT_EXTERNAL_ID_3
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
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            externalReportIdAfter = REPORT_EXTERNAL_ID
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports with page size replaced with a valid value and no previous page token`() {
    val invalidPageSize = MAX_PAGE_SIZE * 2
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = invalidPageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
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
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
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
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = invalidPageSize
      pageToken =
        listReportsPageToken {
            pageSize = previousPageSize
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            lastReport = previousPageEnd {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              externalReportId = REPORT_EXTERNAL_ID
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)

      nextPageToken =
        listReportsPageToken {
            pageSize = previousPageSize
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            lastReport = previousPageEnd {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              externalReportId = REPORT_EXTERNAL_ID_3
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
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            externalReportIdAfter = REPORT_EXTERNAL_ID
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
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = newPageSize
      pageToken =
        listReportsPageToken {
            pageSize = previousPageSize
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            lastReport = previousPageEnd {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              externalReportId = REPORT_EXTERNAL_ID
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(SUCCEEDED_REACH_REPORT)
      reports.add(SUCCEEDED_IMPRESSION_REPORT)
      reports.add(SUCCEEDED_WATCH_DURATION_REPORT)

      nextPageToken =
        listReportsPageToken {
            pageSize = newPageSize
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            lastReport = previousPageEnd {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              externalReportId = REPORT_EXTERNAL_ID_3
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
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            externalReportIdAfter = REPORT_EXTERNAL_ID
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports throws UNAUTHENTICATED when no principal is found`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listReports(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReports throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Cannot list ReportingSets belonging to other MeasurementConsumers.")
  }

  @Test
  fun `listReports throws PERMISSION_DENIED when the caller is not MeasurementConsumer`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Caller does not have permission to list ReportingSets.")
  }

  @Test
  fun `listReports throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageSize = -1
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
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
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listReports(ListReportsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listReports throws INVALID_ARGUMENT when mc id doesn't match one in page token`() {
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      pageToken =
        listReportsPageToken {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID_2
            lastReport = previousPageEnd {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID_2
              externalReportId = REPORT_EXTERNAL_ID
            }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
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

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
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
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
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

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
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
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
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

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listReports(request) }
        }

      val expected = listReportsResponse { reports.add(PENDING_REACH_REPORT) }

      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
        .isEqualTo(
          streamReportsRequest {
            limit = DEFAULT_PAGE_SIZE + 1
            this.filter = filter {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            }
          }
        )
      verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
        .isEqualTo(getMeasurementRequest { name = REACH_MEASUREMENT_REFERENCE_ID })
      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getInternalReportRequest {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            externalReportId = REPORT_EXTERNAL_ID
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

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
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
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            }
          }
        )
      verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
        .isEqualTo(getMeasurementRequest { name = REACH_MEASUREMENT_REFERENCE_ID })
      verifyProtoArgument(
          internalMeasurementsMock,
          InternalMeasurementsCoroutineImplBase::setMeasurementFailure
        )
        .isEqualTo(
          setMeasurementFailureRequest {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
            failure = internalFailure {
              reason = InternalMeasurement.Failure.Reason.REQUISITION_REFUSED
              message = "Privacy budget exceeded."
            }
          }
        )
      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getInternalReportRequest {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            externalReportId = REPORT_EXTERNAL_ID
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

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listReports(request) }
        }

      val expected = listReportsResponse { reports.add(SUCCEEDED_REACH_REPORT) }

      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
        .isEqualTo(
          streamReportsRequest {
            limit = DEFAULT_PAGE_SIZE + 1
            this.filter = filter {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            }
          }
        )
      verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
        .isEqualTo(getMeasurementRequest { name = REACH_MEASUREMENT_REFERENCE_ID })
      verifyProtoArgument(
          internalMeasurementsMock,
          InternalMeasurementsCoroutineImplBase::setMeasurementResult
        )
        .isEqualTo(
          setMeasurementResultRequest {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
            this.result = internalMeasurementResult {
              reach = internalReach { value = REACH_VALUE }
              frequency = internalFrequency {
                relativeFrequencyDistribution.putAll(FREQUENCY_DISTRIBUTION)
              }
            }
          }
        )
      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getInternalReportRequest {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            externalReportId = REPORT_EXTERNAL_ID
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

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse { reports.add(SUCCEEDED_IMPRESSION_REPORT) }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          }
        }
      )
    verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
      .isEqualTo(getMeasurementRequest { name = IMPRESSION_MEASUREMENT_REFERENCE_ID })
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::setMeasurementResult
      )
      .isEqualTo(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
          this.result = internalMeasurementResult {
            impression = internalImpression {
              value = IMPRESSION_VALUE + IMPRESSION_VALUE_2 + IMPRESSION_VALUE_3
            }
          }
        }
      )
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
      .isEqualTo(
        getInternalReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          externalReportId = REPORT_EXTERNAL_ID_2
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

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_NAME }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse { reports.add(SUCCEEDED_WATCH_DURATION_REPORT) }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter = filter {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          }
        }
      )
    verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::getMeasurement)
      .isEqualTo(getMeasurementRequest { name = WATCH_DURATION_MEASUREMENT_REFERENCE_ID })
    verifyProtoArgument(
        internalMeasurementsMock,
        InternalMeasurementsCoroutineImplBase::setMeasurementResult
      )
      .isEqualTo(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
          this.result = internalMeasurementResult {
            watchDuration = internalWatchDuration {
              value =
                Durations.add(Durations.add(WATCH_DURATION, WATCH_DURATION_2), WATCH_DURATION_3)
            }
          }
        }
      )
    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::getReport)
      .isEqualTo(
        getInternalReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          externalReportId = REPORT_EXTERNAL_ID_3
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }
}
