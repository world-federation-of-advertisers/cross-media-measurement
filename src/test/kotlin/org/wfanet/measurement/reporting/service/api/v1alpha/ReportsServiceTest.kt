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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.duration
import com.google.protobuf.timestamp
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.crypto.testing.FIXED_ENCRYPTION_PRIVATE_KEYSET
import org.wfanet.measurement.common.crypto.tink.testing.loadPrivateKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.reporting.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.reporting.MeasurementKt.ResultKt.reach as internalMeasurementReach
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
import org.wfanet.measurement.internal.reporting.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.metric as internalMetric
import org.wfanet.measurement.internal.reporting.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.report as internalReport
import org.wfanet.measurement.internal.reporting.streamReportsRequest
import org.wfanet.measurement.internal.reporting.timeInterval as internalTimeInterval
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
import org.wfanet.measurement.reporting.v1alpha.listReportsRequest
import org.wfanet.measurement.reporting.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.v1alpha.metric
import org.wfanet.measurement.reporting.v1alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v1alpha.report

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000
private const val PAGE_SIZE = 2

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

private val REPORT_NAME =
  ReportKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORT_EXTERNAL_ID)).toName()
private val REPORT_NAME_2 =
  ReportKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORT_EXTERNAL_ID_2)).toName()
private val REPORT_NAME_3 =
  ReportKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORT_EXTERNAL_ID_3)).toName()

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
}

// Internal measurements
private val INTERNAL_REACH_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.SUCCEEDED
  this.result = internalMeasurementResult { reach = internalMeasurementReach { value = 100_000L } }
}
private val INTERNAL_FREQUENCY_HISTOGRAM_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.PENDING
}
private val INTERNAL_IMPRESSION_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.FAILED
}
private val INTERNAL_WATCH_DURATION_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.PENDING
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

private val INTERNAL_NAMED_REACH_SET_OPERATION = internalNamedSetOperation {
  displayName = REACH_SET_OPERATION_DISPLAY_NAME
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(REACH_MEASUREMENT_CALCULATION)
}

private val NAMED_REACH_SET_OPERATION = namedSetOperation {
  displayName = REACH_SET_OPERATION_DISPLAY_NAME
  setOperation = SET_OPERATION
}

private val INTERNAL_NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION = internalNamedSetOperation {
  displayName = FREQUENCY_HISTOGRAM_SET_OPERATION_DISPLAY_NAME
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(FREQUENCY_HISTOGRAM_MEASUREMENT_CALCULATION)
}

private val NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION = namedSetOperation {
  displayName = FREQUENCY_HISTOGRAM_SET_OPERATION_DISPLAY_NAME
  setOperation = SET_OPERATION
}

private val INTERNAL_NAMED_IMPRESSION_SET_OPERATION = internalNamedSetOperation {
  displayName = IMPRESSION_SET_OPERATION_DISPLAY_NAME
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(IMPRESSION_MEASUREMENT_CALCULATION)
}

private val NAMED_IMPRESSION_SET_OPERATION = namedSetOperation {
  displayName = IMPRESSION_SET_OPERATION_DISPLAY_NAME
  setOperation = SET_OPERATION
}

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

private val INTERNAL_REACH_METRIC = internalMetric {
  details = internalMetricDetails {
    reach = internalReachParams {}
    cumulative = false
  }
  namedSetOperations.add(INTERNAL_NAMED_REACH_SET_OPERATION)
}

private val REACH_METRIC = metric {
  reach = reachParams {}
  cumulative = false
  setOperations.add(NAMED_REACH_SET_OPERATION)
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

private val FREQUENCY_HISTOGRAM_METRIC = metric {
  frequencyHistogram = frequencyHistogramParams {
    maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
  }
  cumulative = false
  setOperations.add(NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION)
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

private val IMPRESSION_METRIC = metric {
  impressionCount = impressionCountParams { maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER }
  cumulative = false
  setOperations.add(NAMED_IMPRESSION_SET_OPERATION)
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

private val WATCH_DURATION_METRIC = metric {
  watchDuration = watchDurationParams {
    maximumFrequencyPerUser = MAXIMUM_FREQUENCY_PER_USER
    maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
  }
  cumulative = false
  setOperations.add(NAMED_WATCH_DURATION_SET_OPERATION)
}

// Event group filters
private const val EVENT_GROUP_FILTER = "AGE>20"
private val EVENT_GROUP_FILTERS_MAP =
  mapOf(
    EVENT_GROUP_NAME to EVENT_GROUP_FILTER,
    EVENT_GROUP_NAME_2 to EVENT_GROUP_FILTER,
    EVENT_GROUP_NAME_3 to EVENT_GROUP_FILTER,
  )

// Internal reports
private const val REPORT_IDEMPOTENCY_KEY = "TEST_REPORT"
private const val REPORT_IDEMPOTENCY_KEY_2 = "TEST_REPORT_2"
private const val REPORT_IDEMPOTENCY_KEY_3 = "TEST_REPORT_3"

private val INTERNAL_REACH_REPORT: InternalReport = internalReport {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportId = REPORT_EXTERNAL_ID
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_REACH_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(REACH_MEASUREMENT_REFERENCE_ID, INTERNAL_REACH_MEASUREMENT)
  details = internalReportDetails { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
  createTime = timestamp { seconds = 2000 }
  reportIdempotencyKey = REPORT_IDEMPOTENCY_KEY
}

private val INTERNAL_IMPRESSION_WATCH_DURATION_REPORT: InternalReport = internalReport {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportId = REPORT_EXTERNAL_ID_2
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.addAll(listOf(INTERNAL_IMPRESSION_METRIC, INTERNAL_WATCH_DURATION_METRIC))
  state = InternalReport.State.RUNNING
  measurements.putAll(
    mapOf(
      IMPRESSION_MEASUREMENT_REFERENCE_ID to INTERNAL_IMPRESSION_MEASUREMENT,
      WATCH_DURATION_MEASUREMENT_REFERENCE_ID to INTERNAL_WATCH_DURATION_MEASUREMENT,
    )
  )
  details = internalReportDetails { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
  createTime = timestamp { seconds = 3000 }
  reportIdempotencyKey = REPORT_IDEMPOTENCY_KEY_2
}

private val INTERNAL_FREQUENCY_HISTOGRAM_REPORT: InternalReport = internalReport {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportId = REPORT_EXTERNAL_ID_3
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.add(INTERNAL_FREQUENCY_HISTOGRAM_METRIC)
  state = InternalReport.State.RUNNING
  measurements.put(
    FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID,
    INTERNAL_FREQUENCY_HISTOGRAM_MEASUREMENT
  )
  details = internalReportDetails { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
  createTime = timestamp { seconds = 4000 }
  reportIdempotencyKey = REPORT_IDEMPOTENCY_KEY_3
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

// Public reports
private val REACH_REPORT = report {
  name = REPORT_NAME
  reportIdempotencyKey = REPORT_IDEMPOTENCY_KEY
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(REACH_METRIC)
  state = Report.State.RUNNING
}

private val IMPRESSION_WATCH_DURATION_REPORT = report {
  name = REPORT_NAME_2
  reportIdempotencyKey = REPORT_IDEMPOTENCY_KEY_2
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.addAll(listOf(IMPRESSION_METRIC, WATCH_DURATION_METRIC))
  state = Report.State.RUNNING
}

private val FREQUENCY_HISTOGRAM_REPORT = report {
  name = REPORT_NAME_3
  reportIdempotencyKey = REPORT_IDEMPOTENCY_KEY_3
  measurementConsumer = MEASUREMENT_CONSUMER_NAME
  eventGroupUniverse = EVENT_GROUP_UNIVERSE
  periodicTimeInterval = PERIODIC_TIME_INTERVAL
  metrics.add(FREQUENCY_HISTOGRAM_METRIC)
  state = Report.State.RUNNING
}

// API authentication key
private const val API_AUTHENTICATION_KEY = "API_AUTHENTICATION_KEY"

@RunWith(JUnit4::class)
class ReportsServiceTest {

  private val internalReportsMock: ReportsCoroutineImplBase =
    mockService() {
      onBlocking { streamReports(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_REACH_REPORT,
            INTERNAL_IMPRESSION_WATCH_DURATION_REPORT,
            INTERNAL_FREQUENCY_HISTOGRAM_REPORT,
          )
        )
    }

  private val internalMeasurementsMock: InternalMeasurementsCoroutineImplBase =
    mockService() {
      // onBlocking {setMeasurementResult(any())}.thenReturn()
      // onBlocking {setMeasurementFailure(any())}.thenReturn()
      // onBlocking {getMeasurement(any())}.thenReturn()
    }

  private val measurementsMock: MeasurementsCoroutineImplBase =
    mockService() {
      // onBlocking {getMeasurement(any())}.thenReturn()
    }

  private val certificateMock: CertificatesCoroutineImplBase =
    mockService() {
      // onBlocking {getCertificate(any())}.thenReturn()
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalReportsMock)
    addService(internalMeasurementsMock)
    addService(measurementsMock)
    addService(certificateMock)
  }

  // @get:Rule val grpcTestInternalServerRule = GrpcTestServerRule {
  //   addService(internalReportsMock)
  //   addService(internalMeasurementsMock)
  // }
  // @get:Rule val grpcTestKingdomServerRule = GrpcTestServerRule {
  //   addService(measurementsMock)
  //   addService(certificateMock)
  // }

  private lateinit var service: ReportsService

  @Before
  fun initService() {
    service =
      ReportsService(
        ReportsCoroutineStub(grpcTestServerRule.channel),
        InternalMeasurementsCoroutineStub(grpcTestServerRule.channel),
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        CertificatesCoroutineStub(grpcTestServerRule.channel),
        loadPrivateKey(FIXED_ENCRYPTION_PRIVATE_KEYSET),
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
      reports.addAll(
        listOf(REACH_REPORT, IMPRESSION_WATCH_DURATION_REPORT, FREQUENCY_HISTOGRAM_REPORT)
      )
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
}
