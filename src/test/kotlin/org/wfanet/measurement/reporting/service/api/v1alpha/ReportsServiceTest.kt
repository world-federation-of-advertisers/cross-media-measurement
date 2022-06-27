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

import com.google.protobuf.duration
import com.google.protobuf.timestamp
import kotlinx.coroutines.flow.flowOf
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.reporting.Measurement as InternalMeasurement
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
import org.wfanet.measurement.internal.reporting.measurement as internalMeasurement
import org.wfanet.measurement.internal.reporting.metric as internalMetric
import org.wfanet.measurement.internal.reporting.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.report as internalReport
import org.wfanet.measurement.internal.reporting.timeInterval as internalTimeInterval

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

// Internal measurements
private val INTERNAL_REACH_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = REACH_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.PENDING
  updateTime = timestamp { seconds = 2000 }
}
private val INTERNAL_FREQUENCY_HISTOGRAM_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.SUCCEEDED
  // result =
  updateTime = timestamp { seconds = 3000 }
}
private val INTERNAL_IMPRESSION_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = IMPRESSION_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.SUCCEEDED
  // result =
  updateTime = timestamp { seconds = 4000 }
}
private val INTERNAL_WATCH_DURATION_MEASUREMENT = internalMeasurement {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  measurementReferenceId = WATCH_DURATION_MEASUREMENT_REFERENCE_ID
  state = InternalMeasurement.State.SUCCEEDED
  // result =
  updateTime = timestamp { seconds = 5000 }
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
private val INTERNAL_NAMED_REACH_SET_OPERATION = internalNamedSetOperation {
  displayName = "Set Operation"
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(REACH_MEASUREMENT_CALCULATION)
}

private val INTERNAL_NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION = internalNamedSetOperation {
  displayName = "Set Operation"
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(FREQUENCY_HISTOGRAM_MEASUREMENT_CALCULATION)
}

private val INTERNAL_NAMED_IMPRESSION_SET_OPERATION = internalNamedSetOperation {
  displayName = "Set Operation"
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(IMPRESSION_MEASUREMENT_CALCULATION)
}

private val INTERNAL_NAMED_WATCH_DURATION_SET_OPERATION = internalNamedSetOperation {
  displayName = "Set Operation"
  setOperation = INTERNAL_SET_OPERATION
  measurementCalculation.add(WATCH_DURATION_MEASUREMENT_CALCULATION)
}

// Internal metrics
private val INTERNAL_REACH_METRIC = internalMetric {
  details = internalMetricDetails {
    reach = internalReachParams {}
    cumulative = false
  }
  namedSetOperations.add(INTERNAL_NAMED_REACH_SET_OPERATION)
}

private val INTERNAL_FREQUENCY_HISTOGRAM_METRIC = internalMetric {
  details = internalMetricDetails {
    frequencyHistogram = internalFrequencyHistogramParams { maximumFrequencyPerUser = 10 }
    cumulative = false
  }
  namedSetOperations.add(INTERNAL_NAMED_FREQUENCY_HISTOGRAM_SET_OPERATION)
}

private val INTERNAL_IMPRESSION_METRIC = internalMetric {
  details = internalMetricDetails {
    impressionCount = internalImpressionCountParams { maximumFrequencyPerUser = 10 }
    cumulative = false
  }
  namedSetOperations.add(INTERNAL_NAMED_IMPRESSION_SET_OPERATION)
}

private val INTERNAL_WATCH_DURATION_METRIC = internalMetric {
  details = internalMetricDetails {
    watchDuration = internalWatchDurationParams {
      maximumFrequencyPerUser = 10
      maximumWatchDurationPerUser = 300
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

// Internal reports
private val INTERNAL_REPORT: InternalReport = internalReport {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportId = REPORT_EXTERNAL_ID
  periodicTimeInterval = INTERNAL_PERIODIC_TIME_INTERVAL
  metrics.addAll(
    listOf(
      INTERNAL_REACH_METRIC,
      INTERNAL_FREQUENCY_HISTOGRAM_METRIC,
      INTERNAL_IMPRESSION_METRIC,
      INTERNAL_WATCH_DURATION_METRIC
    )
  )
  state = InternalReport.State.RUNNING
  measurements.putAll(
    mapOf(
      REACH_MEASUREMENT_REFERENCE_ID to INTERNAL_REACH_MEASUREMENT,
      FREQUENCY_HISTOGRAM_MEASUREMENT_REFERENCE_ID to INTERNAL_FREQUENCY_HISTOGRAM_MEASUREMENT,
      IMPRESSION_MEASUREMENT_REFERENCE_ID to INTERNAL_IMPRESSION_MEASUREMENT,
      WATCH_DURATION_MEASUREMENT_REFERENCE_ID to INTERNAL_WATCH_DURATION_MEASUREMENT,
    )
  )
  details = internalReportDetails { eventGroupFilters.putAll(EVENT_GROUP_FILTERS_MAP) }
}

@RunWith(JUnit4::class)
class ReportsServiceTest {

  private val internalReportsMock: ReportsCoroutineImplBase =
    mockService() {
      onBlocking { streamReports(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_REPORT,
          )
        )
    }
}
