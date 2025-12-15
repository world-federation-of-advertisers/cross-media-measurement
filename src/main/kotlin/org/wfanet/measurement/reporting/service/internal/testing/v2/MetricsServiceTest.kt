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

package org.wfanet.measurement.reporting.service.internal.testing.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import com.google.rpc.errorInfo
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequestKt
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequestKt
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequestKt
import org.wfanet.measurement.internal.reporting.v2.CreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.invalidateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.streamMetricsRequest
import org.wfanet.measurement.reporting.service.internal.Errors

private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"
private const val MAX_BATCH_SIZE = 1000

@RunWith(JUnit4::class)
abstract class MetricsServiceTest<T : MetricsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val metricsService: T,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val measurementsService: MeasurementsCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
  private lateinit var measurementsService: MeasurementsCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.metricsService
    reportingSetsService = services.reportingSetsService
    measurementConsumersService = services.measurementConsumersService
    measurementsService = services.measurementsService
  }

  @Test
  fun `createMetric succeeds when MetricSpec type is Reach`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reach =
          MetricSpecKt.reachParams {
            multipleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 3
          binaryRepresentation = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val createdMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          externalMetricId = "external-metric-id"
        }
      )

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)
    createdMetric.weightedMeasurementsList.forEach {
      assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
    }
  }

  @Test
  fun `createMetric succeeds when MetricSpec type is reachAndFrequency`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequency = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 3
          binaryRepresentation = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val createdMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          externalMetricId = "external-metric-id"
        }
      )

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)
    createdMetric.weightedMeasurementsList.forEach {
      assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
    }
  }

  @Test
  fun `createMetric succeeds when MetricSpec type is ImpressionCount`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        impressionCount =
          MetricSpecKt.impressionCountParams {
            params =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequencyPerUser = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 3
          binaryRepresentation = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val createdMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          externalMetricId = "external-metric-id"
        }
      )

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)
    createdMetric.weightedMeasurementsList.forEach {
      assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
    }
  }

  @Test
  fun `createMetric succeeds when MetricSpec type is WatchDuration`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        watchDuration =
          MetricSpecKt.watchDurationParams {
            params =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumWatchDurationPerUser = Durations.fromSeconds(100)
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 3
          binaryRepresentation = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val createdMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          externalMetricId = "external-metric-id"
        }
      )

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)
    createdMetric.weightedMeasurementsList.forEach {
      assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
    }
  }

  @Test
  fun `createMetric succeeds when MetricSpec type is PopulationCount`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          containingReport = "reportX"
        }
    }

    val createdMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          externalMetricId = "external-metric-id"
        }
      )

    assertThat(createdMetric.externalMetricId).isNotEmpty()
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)
    assertThat(
        createdMetric.weightedMeasurementsList.first().measurement.cmmsCreateMeasurementRequestId
      )
      .isNotEmpty()
  }

  @Test
  fun `createMetric succeeds with model line`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      cmmsModelLine = "modelProviders/123/modelSuites/123/modelLines/123"
      metricSpec = metricSpec {
        reach =
          MetricSpecKt.reachParams {
            multipleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 3
          binaryRepresentation = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val createdMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          externalMetricId = "external-metric-id"
        }
      )

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)
    createdMetric.weightedMeasurementsList.forEach {
      assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
    }
  }

  @Test
  fun `createMetric succeeds when no filters in bases in measurements`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        watchDuration =
          MetricSpecKt.watchDurationParams {
            params =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumWatchDurationPerUser = Durations.fromSeconds(100)
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val createdMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          externalMetricId = "external-metric-id"
        }
      )

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)
    createdMetric.weightedMeasurementsList.forEach {
      assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
    }
  }

  @Test
  fun `createMetric returns the same metric when using an existing request id`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequency = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 3
          binaryRepresentation = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val createdMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          requestId = "requestId"
          externalMetricId = "external-metric-id"
        }
      )

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0L)
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)

    val sameCreatedMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          requestId = "requestId"
          externalMetricId = "external-metric-id"
        }
      )

    assertThat(createdMetric).ignoringRepeatedFieldOrder().isEqualTo(sameCreatedMetric)
  }

  @Test
  fun `createMetric throws ALREADY_EXISTS when using different request ID for existing metric`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reachAndFrequency =
            MetricSpecKt.reachAndFrequencyParams {
              multipleDataProviderParams =
                MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                  reachPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  frequencyPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
              maximumFrequency = 5
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            binaryRepresentation = 1
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
            }
          }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 3
            binaryRepresentation = 2
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
            }
          }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
            containingReport = "reportX"
          }
      }

      val createdMetric =
        service.createMetric(
          createMetricRequest {
            this.metric = metric
            requestId = "requestId"
            externalMetricId = "external-metric-id"
          }
        )

      assertThat(createdMetric.externalMetricId).isNotEqualTo(0L)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createMetric(
            createMetricRequest {
              this.metric = metric
              requestId = "differentRequestId"
              externalMetricId = "external-metric-id"
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `createMetric throws NOT_FOUND when ReportingSet in basis not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequency = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = "1234"
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(
          createMetricRequest {
            this.metric = metric
            externalMetricId = "external-metric-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Reporting Set")
  }

  @Test
  fun `createMetric throws NOT_FOUND when ReportingSet in metric not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "1234"
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequency = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(
          createMetricRequest {
            this.metric = metric
            externalMetricId = "external-metric-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Reporting Set")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when request missing external ID`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        clearExternalMetricId()
      }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createMetric(createMetricRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when metric missing time interval`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      metricSpec = metricSpec {
        reach =
          MetricSpecKt.reachParams {
            multipleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(
          createMetricRequest {
            this.metric = metric
            externalMetricId = "external-metric-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("time")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when metric spec missing type`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {}
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(
          createMetricRequest {
            this.metric = metric
            externalMetricId = "external-metric-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("type")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when metric missing weighted measurements`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reach =
            MetricSpecKt.reachParams {
              multipleDataProviderParams =
                MetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
            }
        }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
            containingReport = "reportX"
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createMetric(
            createMetricRequest {
              this.metric = metric
              externalMetricId = "external-metric-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("weighted")
    }

  @Test
  fun `createMetric throws FAILED_PRECONDITION when MC not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + "2"
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequency = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(
          createMetricRequest {
            this.metric = metric
            externalMetricId = "external-metric-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            CMMS_MEASUREMENT_CONSUMER_ID + "2"
        }
      )
  }

  @Test
  fun `batchCreateMetrics succeeds for one create metric request`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequency = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val batchCreateMetricsResponse =
      service.batchCreateMetrics(
        batchCreateMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          requests += createMetricRequest {
            this.metric = metric
            externalMetricId = "external-metric-id"
          }
        }
      )

    val createdMetric = batchCreateMetricsResponse.metricsList.first()
    assertThat(batchCreateMetricsResponse.metricsList).hasSize(1)
    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)
    createdMetric.weightedMeasurementsList.forEach {
      assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
    }
  }

  @Test
  fun `batchCreateMetrics succeeds for two create metric requests`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequency = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val batchCreateMetricsResponse =
      service.batchCreateMetrics(
        batchCreateMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          requests += createMetricRequest {
            this.metric = metric
            externalMetricId = "externalMetricId1"
          }
          requests += createMetricRequest {
            this.metric = metric
            externalMetricId = "externalMetricId2"
          }
        }
      )

    val createdMetric = batchCreateMetricsResponse.metricsList.first()
    val createdMetric2 = batchCreateMetricsResponse.metricsList.last()
    assertThat(batchCreateMetricsResponse.metricsList).hasSize(2)
    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)
    assertThat(createdMetric2.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric2.hasCreateTime()).isTrue()
    assertThat(createdMetric2.state).isEqualTo(Metric.State.RUNNING)
    batchCreateMetricsResponse.metricsList.forEach {
      it.weightedMeasurementsList.forEach { weightedMeasurement ->
        assertThat(weightedMeasurement.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
      }
    }
  }

  @Test
  fun `batchCreateMetrics succeeds for two create metric requests with one already existing`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reachAndFrequency =
            MetricSpecKt.reachAndFrequencyParams {
              multipleDataProviderParams =
                MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                  reachPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  frequencyPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
              maximumFrequency = 5
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            binaryRepresentation = 1
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
            }
          }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
            containingReport = "reportX"
          }
      }

      val createdMetric =
        service.createMetric(
          createMetricRequest {
            this.metric = metric
            requestId = "one"
            externalMetricId = "externalMetricId1"
          }
        )

      val batchCreateMetricsResponse =
        service.batchCreateMetrics(
          batchCreateMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            requests += createMetricRequest {
              this.metric = metric
              requestId = "one"
              externalMetricId = "externalMetricId1"
            }
            requests += createMetricRequest {
              this.metric = metric
              externalMetricId = "externalMetricId2"
            }
          }
        )

      assertThat(batchCreateMetricsResponse.metricsList).hasSize(2)
      assertThat(batchCreateMetricsResponse.metricsList.first())
        .ignoringRepeatedFieldOrder()
        .isEqualTo(createdMetric)

      val createdMetric2 = batchCreateMetricsResponse.metricsList.last()
      assertThat(createdMetric2.externalMetricId).isNotEqualTo(0)
      assertThat(createdMetric2.hasCreateTime()).isTrue()
      assertThat(createdMetric2.state).isEqualTo(Metric.State.RUNNING)
      createdMetric2.weightedMeasurementsList.forEach {
        assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
      }
    }

  @Test
  fun `batchCreateMetrics throws ALREADY_EXISTS when using different request ID for existing metric`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reachAndFrequency =
            MetricSpecKt.reachAndFrequencyParams {
              multipleDataProviderParams =
                MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                  reachPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  frequencyPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
              maximumFrequency = 5
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            binaryRepresentation = 1
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
            }
          }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
            containingReport = "reportX"
          }
      }

      val createdMetric =
        service.createMetric(
          createMetricRequest {
            this.metric = metric
            requestId = "one"
            externalMetricId = "externalMetricId1"
          }
        )

      assertThat(createdMetric.externalMetricId).isNotEqualTo(0L)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateMetrics(
            batchCreateMetricsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              requests += createMetricRequest {
                this.metric = metric
                requestId = "different one"
                externalMetricId = "externalMetricId1"
              }
              requests += createMetricRequest {
                this.metric = metric
                externalMetricId = "externalMetricId2"
              }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when metrics have the same resource ID`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reachAndFrequency =
            MetricSpecKt.reachAndFrequencyParams {
              multipleDataProviderParams =
                MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                  reachPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  frequencyPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
              maximumFrequency = 5
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            binaryRepresentation = 1
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
            }
          }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
            containingReport = "reportX"
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateMetrics(
            batchCreateMetricsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              requests += createMetricRequest {
                this.metric = metric
                requestId = "requestId"
                externalMetricId = "externalMetricId1"
              }
              requests += createMetricRequest {
                this.metric = metric
                externalMetricId = "externalMetricId1"
              }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when metric missing time interval`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reachAndFrequency =
            MetricSpecKt.reachAndFrequencyParams {
              multipleDataProviderParams =
                MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                  reachPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  frequencyPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
              maximumFrequency = 5
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            binaryRepresentation = 1
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
            }
          }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
            containingReport = "reportX"
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateMetrics(
            batchCreateMetricsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              requests += createMetricRequest {
                this.metric = metric
                externalMetricId = "external-metric-id"
              }
              requests += createMetricRequest {
                this.metric = metric.copy { clearTimeInterval() }
                externalMetricId = "externalMetricId2"
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("time")
    }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when metric spec missing type`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequency = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateMetrics(
          batchCreateMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            requests += createMetricRequest {
              this.metric = metric
              externalMetricId = "externalMetricId1"
            }
            requests += createMetricRequest {
              this.metric = metric.copy { metricSpec = metricSpec.copy { clearType() } }
              externalMetricId = "externalMetricId2"
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("type")
  }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when metric missing weighted measurements`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reachAndFrequency =
            MetricSpecKt.reachAndFrequencyParams {
              multipleDataProviderParams =
                MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                  reachPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  frequencyPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
              maximumFrequency = 5
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            binaryRepresentation = 1
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
            }
          }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
            containingReport = "reportX"
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateMetrics(
            batchCreateMetricsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              requests += createMetricRequest {
                this.metric = metric
                externalMetricId = "externalMetricId1"
              }
              requests += createMetricRequest {
                this.metric = metric.copy { weightedMeasurements.clear() }
                externalMetricId = "externalMetricId2"
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("weighted")
    }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when cmms mc id doesn't match create request`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reachAndFrequency =
            MetricSpecKt.reachAndFrequencyParams {
              multipleDataProviderParams =
                MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                  reachPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  frequencyPrivacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
              maximumFrequency = 5
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            binaryRepresentation = 1
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
            }
          }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
            containingReport = "reportX"
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateMetrics(
            batchCreateMetricsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              requests += createMetricRequest {
                this.metric = metric
                externalMetricId = "externalMetricId1"
              }
              requests += createMetricRequest {
                this.metric =
                  metric.copy { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + "2" }
                externalMetricId = "externalMetricId2"
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("CmmsMeasurementConsumerId")
    }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when too many requests`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            multipleDataProviderParams =
              MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                reachPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                frequencyPrivacyParams =
                  MetricSpecKt.differentialPrivacyParams {
                    epsilon = 1.0
                    delta = 2.0
                  }
                vidSamplingInterval =
                  MetricSpecKt.vidSamplingInterval {
                    start = 0.1f
                    width = 0.5f
                  }
              }
            maximumFrequency = 5
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          binaryRepresentation = 1
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = interval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = createdReportingSet.externalReportingSetId
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
          containingReport = "reportX"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateMetrics(
          batchCreateMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            for (i in 1..(MAX_BATCH_SIZE + 1)) {
              requests += createMetricRequest {
                this.metric = metric
                externalMetricId = "external-metric-id-$i"
              }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Too many")
  }

  @Test
  fun `batchGetMetrics succeeds when metric spec type is reach`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        metric =
          metric.copy {
            metricSpec = metricSpec {
              reach =
                MetricSpecKt.reachParams {
                  multipleDataProviderParams =
                    MetricSpecKt.samplingAndPrivacyParams {
                      privacyParams =
                        MetricSpecKt.differentialPrivacyParams {
                          epsilon = 1.0
                          delta = 2.0
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                }
            }
          }
      }
    val createdMetric = service.createMetric(createMetricRequest)

    val retrievedMetrics =
      service.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += createdMetric.externalMetricId
        }
      )

    assertThat(retrievedMetrics.metricsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdMetric)
  }

  @Test
  fun `batchGetMetrics succeeds when metric spec type is reach and single params set`(): Unit =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val createMetricRequest =
        createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
          metric =
            metric.copy {
              metricSpec = metricSpec {
                reach =
                  MetricSpecKt.reachParams {
                    multipleDataProviderParams =
                      MetricSpecKt.samplingAndPrivacyParams {
                        privacyParams =
                          MetricSpecKt.differentialPrivacyParams {
                            epsilon = 1.0
                            delta = 2.0
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.1f
                            width = 0.5f
                          }
                      }
                    singleDataProviderParams =
                      MetricSpecKt.samplingAndPrivacyParams {
                        privacyParams =
                          MetricSpecKt.differentialPrivacyParams {
                            epsilon = 2.0
                            delta = 4.0
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.2f
                            width = 0.6f
                          }
                      }
                  }
              }
            }
        }
      val createdMetric = service.createMetric(createMetricRequest)

      assertThat(createdMetric.metricSpec.reach)
        .isEqualTo(createMetricRequest.metric.metricSpec.reach)

      val retrievedMetrics =
        service.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += createdMetric.externalMetricId
          }
        )

      assertThat(retrievedMetrics.metricsList)
        .ignoringRepeatedFieldOrder()
        .containsExactly(createdMetric)
    }

  @Test
  fun `batchGetMetrics succeeds when model line is set`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        metric = metric.copy { cmmsModelLine = "modelProviders/123/modelSuites/123/modelLines/123" }
      }
    val createdMetric = service.createMetric(createMetricRequest)

    val retrievedMetrics =
      service.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += createdMetric.externalMetricId
        }
      )

    assertThat(retrievedMetrics.metricsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdMetric)
  }

  @Test
  fun `batchGetMetrics succeeds when measurement has data_provider_count set to 1`(): Unit =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val createMetricRequest =
        createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
          val source = this
          metric =
            metric.copy {
              weightedMeasurements.clear()
              weightedMeasurements +=
                MetricKt.weightedMeasurement {
                  weight = 2
                  binaryRepresentation = 1
                  measurement = measurement {
                    this.cmmsMeasurementConsumerId = source.metric.cmmsMeasurementConsumerId
                    timeInterval = interval {
                      startTime = timestamp { seconds = 10 }
                      endTime = timestamp { seconds = 100 }
                    }
                    primitiveReportingSetBases +=
                      ReportingSetKt.primitiveReportingSetBasis {
                        externalReportingSetId = source.metric.externalReportingSetId
                        filters += "filter1"
                      }
                    details = MeasurementKt.details { dataProviderCount = 1 }
                  }
                }
            }
        }
      val createdMetric = service.createMetric(createMetricRequest)

      val retrievedMetrics =
        service.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += createdMetric.externalMetricId
          }
        )

      assertThat(
          createdMetric.weightedMeasurementsList.first().measurement.details.dataProviderCount
        )
        .isEqualTo(1)
      assertThat(retrievedMetrics.metricsList)
        .ignoringRepeatedFieldOrder()
        .containsExactly(createdMetric)
    }

  @Test
  fun `batchGetMetrics succeeds when metric spec type is reach and frequency`(): Unit =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val createMetricRequest =
        createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
          metric =
            metric.copy {
              metricSpec = metricSpec {
                reachAndFrequency =
                  MetricSpecKt.reachAndFrequencyParams {
                    multipleDataProviderParams =
                      MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                        reachPrivacyParams =
                          MetricSpecKt.differentialPrivacyParams {
                            epsilon = 1.0
                            delta = 2.0
                          }
                        frequencyPrivacyParams =
                          MetricSpecKt.differentialPrivacyParams {
                            epsilon = 1.0
                            delta = 2.0
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.1f
                            width = 0.5f
                          }
                      }
                    maximumFrequency = 5
                  }
              }
            }
        }
      val createdMetric = service.createMetric(createMetricRequest)

      val retrievedMetrics =
        service.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += createdMetric.externalMetricId
          }
        )

      assertThat(retrievedMetrics.metricsList)
        .ignoringRepeatedFieldOrder()
        .containsExactly(createdMetric)
    }

  @Test
  fun `batchGetMetrics succeeds when metric spec type is rf and single params set`(): Unit =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val createMetricRequest =
        createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
          metric =
            metric.copy {
              metricSpec = metricSpec {
                reachAndFrequency =
                  MetricSpecKt.reachAndFrequencyParams {
                    multipleDataProviderParams =
                      MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                        reachPrivacyParams =
                          MetricSpecKt.differentialPrivacyParams {
                            epsilon = 1.0
                            delta = 2.0
                          }
                        frequencyPrivacyParams =
                          MetricSpecKt.differentialPrivacyParams {
                            epsilon = 1.0
                            delta = 2.0
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.1f
                            width = 0.5f
                          }
                      }
                    singleDataProviderParams =
                      MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                        reachPrivacyParams =
                          MetricSpecKt.differentialPrivacyParams {
                            epsilon = 2.0
                            delta = 4.0
                          }
                        frequencyPrivacyParams =
                          MetricSpecKt.differentialPrivacyParams {
                            epsilon = 3.0
                            delta = 5.0
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.3f
                            width = 0.6f
                          }
                      }
                    maximumFrequency = 5
                  }
              }
            }
        }
      val createdMetric = service.createMetric(createMetricRequest)

      assertThat(createdMetric.metricSpec.reachAndFrequency)
        .isEqualTo(createMetricRequest.metric.metricSpec.reachAndFrequency)

      val retrievedMetrics =
        service.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += createdMetric.externalMetricId
          }
        )

      assertThat(retrievedMetrics.metricsList)
        .ignoringRepeatedFieldOrder()
        .containsExactly(createdMetric)
    }

  @Test
  fun `batchGetMetrics succeeds when metric spec type is impression count`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        metric =
          metric.copy {
            metricSpec = metricSpec {
              impressionCount =
                MetricSpecKt.impressionCountParams {
                  params =
                    MetricSpecKt.samplingAndPrivacyParams {
                      privacyParams =
                        MetricSpecKt.differentialPrivacyParams {
                          epsilon = 1.0
                          delta = 2.0
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                  maximumFrequencyPerUser = 5
                }
            }
          }
      }
    val createdMetric = service.createMetric(createMetricRequest)

    val retrievedMetrics =
      service.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += createdMetric.externalMetricId
        }
      )

    assertThat(retrievedMetrics.metricsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdMetric)
  }

  @Test
  fun `batchGetMetrics succeeds when metric spec type is watch duration`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        metric =
          metric.copy {
            metricSpec = metricSpec {
              watchDuration =
                MetricSpecKt.watchDurationParams {
                  params =
                    MetricSpecKt.samplingAndPrivacyParams {
                      privacyParams =
                        MetricSpecKt.differentialPrivacyParams {
                          epsilon = 1.0
                          delta = 2.0
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                  maximumWatchDurationPerUser = Durations.fromSeconds(100)
                }
            }
          }
      }
    val createdMetric = service.createMetric(createMetricRequest)

    val retrievedMetrics =
      service.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += createdMetric.externalMetricId
        }
      )

    assertThat(retrievedMetrics.metricsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdMetric)
  }

  @Test
  fun `batchGetMetrics succeeds when metric spec type is population count`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val createMetricRequest = createMetricRequest {
      externalMetricId = "external-metric-id"
      metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            binaryRepresentation = 1
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                }
            }
          }
        details =
          MetricKt.details {
            filters += "filter1"
            containingReport = "reportX"
          }
      }
    }
    val createdMetric = service.createMetric(createMetricRequest)

    val retrievedMetrics =
      service.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += createdMetric.externalMetricId
        }
      )

    assertThat(retrievedMetrics.metricsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdMetric)
  }

  @Test
  fun `batchGetMetrics succeeds when asking for two metrics`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId1",
      )
    val createdMetricRequest2 =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId2",
      )
    val createdMetric = service.createMetric(createMetricRequest)
    val createdMetric2 = service.createMetric(createdMetricRequest2)

    val retrievedMetrics =
      service.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += createdMetric.externalMetricId
          externalMetricIds += createdMetric2.externalMetricId
        }
      )

    assertThat(retrievedMetrics.metricsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdMetric, createdMetric2)
      .inOrder()
  }

  @Test
  fun `batchGetMetrics succeeds when no filters in bases in measurements`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        metric =
          metric.copy {
            val externalPrimitiveReportingSetId =
              weightedMeasurements
                .first()
                .measurement
                .primitiveReportingSetBasesList
                .first()
                .externalReportingSetId
            weightedMeasurements.clear()
            weightedMeasurements +=
              MetricKt.weightedMeasurement {
                weight = 2
                binaryRepresentation = 1
                measurement = measurement {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  timeInterval = interval {
                    startTime = timestamp { seconds = 10 }
                    endTime = timestamp { seconds = 100 }
                  }
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = externalPrimitiveReportingSetId
                    }
                }
              }
          }
      }
    val createdMetric = service.createMetric(createMetricRequest)

    val retrievedMetrics =
      service.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += createdMetric.externalMetricId
        }
      )

    assertThat(retrievedMetrics.metricsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdMetric)
  }

  @Test
  fun `batchGetMetrics returns metric with SUCCEEDED state when all measurements SUCCEEDED`():
    Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        val source = this
        metric =
          source.metric.copy {
            weightedMeasurements +=
              MetricKt.weightedMeasurement {
                weight = 2
                binaryRepresentation = 1
                measurement = measurement {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  cmmsCreateMeasurementRequestId = "1234"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 10 }
                    endTime = timestamp { seconds = 100 }
                  }
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = source.metric.externalReportingSetId
                    }
                }
              }
            weightedMeasurements +=
              MetricKt.weightedMeasurement {
                weight = 3
                binaryRepresentation = 2
                measurement = measurement {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  cmmsCreateMeasurementRequestId = "1235"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 10 }
                    endTime = timestamp { seconds = 100 }
                  }
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = source.metric.externalReportingSetId
                    }
                }
              }
          }
      }
    val createdMetric = service.createMetric(createMetricRequest)

    val suffix = "-1"
    val batchSetCmmsMeasurementIdsRequest = batchSetCmmsMeasurementIdsRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      createdMetric.weightedMeasurementsList.forEach {
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId = it.measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = it.measurement.cmmsCreateMeasurementRequestId + suffix
          }
      }
    }
    measurementsService.batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsRequest)

    val batchSetMeasurementResultsRequest = batchSetMeasurementResultsRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      createdMetric.weightedMeasurementsList.forEach {
        measurementResults +=
          BatchSetMeasurementResultsRequestKt.measurementResult {
            cmmsMeasurementId = it.measurement.cmmsCreateMeasurementRequestId + suffix
            results += MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 2 } }
          }
      }
    }
    measurementsService.batchSetMeasurementResults(batchSetMeasurementResultsRequest)

    val retrievedMetrics =
      service.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += createdMetric.externalMetricId
        }
      )

    assertThat(retrievedMetrics.metricsList.first().state).isEqualTo(Metric.State.SUCCEEDED)
  }

  @Test
  fun `batchGetMetrics returns metric with FAILED state when measurement FAILED`(): Unit =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val createMetricRequest =
        createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
          val source = this
          metric =
            source.metric.copy {
              weightedMeasurements +=
                MetricKt.weightedMeasurement {
                  weight = 2
                  binaryRepresentation = 1
                  measurement = measurement {
                    cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                    cmmsCreateMeasurementRequestId = "1234"
                    timeInterval = interval {
                      startTime = timestamp { seconds = 10 }
                      endTime = timestamp { seconds = 100 }
                    }
                    primitiveReportingSetBases +=
                      ReportingSetKt.primitiveReportingSetBasis {
                        externalReportingSetId = source.metric.externalReportingSetId
                      }
                  }
                }
              weightedMeasurements +=
                MetricKt.weightedMeasurement {
                  weight = 3
                  binaryRepresentation = 2
                  measurement = measurement {
                    cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                    cmmsCreateMeasurementRequestId = "1235"
                    timeInterval = interval {
                      startTime = timestamp { seconds = 10 }
                      endTime = timestamp { seconds = 100 }
                    }
                    primitiveReportingSetBases +=
                      ReportingSetKt.primitiveReportingSetBasis {
                        externalReportingSetId = source.metric.externalReportingSetId
                      }
                  }
                }
            }
        }
      val createdMetric = service.createMetric(createMetricRequest)

      val suffix = "-1"
      val batchSetCmmsMeasurementIdsRequest = batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        createdMetric.weightedMeasurementsList.forEach {
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId = it.measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = it.measurement.cmmsCreateMeasurementRequestId + suffix
            }
        }
      }
      measurementsService.batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsRequest)

      val batchSetMeasurementFailuresRequest = batchSetMeasurementFailuresRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementFailures +=
          BatchSetMeasurementFailuresRequestKt.measurementFailure {
            cmmsMeasurementId =
              createdMetric.weightedMeasurementsList
                .first()
                .measurement
                .cmmsCreateMeasurementRequestId + suffix
            failure = MeasurementKt.failure { message = "failure" }
          }
      }
      measurementsService.batchSetMeasurementFailures(batchSetMeasurementFailuresRequest)

      val retrievedMetrics =
        service.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += createdMetric.externalMetricId
          }
        )

      assertThat(retrievedMetrics.metricsList.first().state).isEqualTo(Metric.State.FAILED)
    }

  @Test
  fun `batchGetMetrics gets FAILED state when a measurement SUCCEEDED and other FAILED`(): Unit =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val createMetricRequest =
        createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
          val source = this
          metric =
            source.metric.copy {
              weightedMeasurements +=
                MetricKt.weightedMeasurement {
                  weight = 2
                  binaryRepresentation = 1
                  measurement = measurement {
                    cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                    cmmsCreateMeasurementRequestId = "1234"
                    timeInterval = interval {
                      startTime = timestamp { seconds = 10 }
                      endTime = timestamp { seconds = 100 }
                    }
                    primitiveReportingSetBases +=
                      ReportingSetKt.primitiveReportingSetBasis {
                        externalReportingSetId = source.metric.externalReportingSetId
                      }
                  }
                }
              weightedMeasurements +=
                MetricKt.weightedMeasurement {
                  weight = 3
                  binaryRepresentation = 2
                  measurement = measurement {
                    cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                    cmmsCreateMeasurementRequestId = "1235"
                    timeInterval = interval {
                      startTime = timestamp { seconds = 10 }
                      endTime = timestamp { seconds = 100 }
                    }
                    primitiveReportingSetBases +=
                      ReportingSetKt.primitiveReportingSetBasis {
                        externalReportingSetId = source.metric.externalReportingSetId
                      }
                  }
                }
            }
        }
      val createdMetric = service.createMetric(createMetricRequest)

      val suffix = "-1"
      val batchSetCmmsMeasurementIdsRequest = batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        createdMetric.weightedMeasurementsList.forEach {
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId = it.measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = it.measurement.cmmsCreateMeasurementRequestId + suffix
            }
        }
      }
      measurementsService.batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsRequest)

      val batchSetMeasurementResultsRequest = batchSetMeasurementResultsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementResults +=
          BatchSetMeasurementResultsRequestKt.measurementResult {
            cmmsMeasurementId =
              createdMetric.weightedMeasurementsList
                .first()
                .measurement
                .cmmsCreateMeasurementRequestId + suffix
            results += MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 2 } }
          }
      }
      measurementsService.batchSetMeasurementResults(batchSetMeasurementResultsRequest)

      val batchSetMeasurementFailuresRequest = batchSetMeasurementFailuresRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementFailures +=
          BatchSetMeasurementFailuresRequestKt.measurementFailure {
            cmmsMeasurementId =
              createdMetric.weightedMeasurementsList
                .last()
                .measurement
                .cmmsCreateMeasurementRequestId + suffix
            failure = MeasurementKt.failure { message = "failure" }
          }
      }
      measurementsService.batchSetMeasurementFailures(batchSetMeasurementFailuresRequest)

      val retrievedMetrics =
        service.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += createdMetric.externalMetricId
          }
        )

      assertThat(retrievedMetrics.metricsList.first().state).isEqualTo(Metric.State.FAILED)
    }

  @Test
  fun `batchGetMetrics gets RUNNING state when only 1 measurement SUCCEEDED`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        val source = this
        metric =
          source.metric.copy {
            weightedMeasurements +=
              MetricKt.weightedMeasurement {
                weight = 2
                binaryRepresentation = 1
                measurement = measurement {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  cmmsCreateMeasurementRequestId = "1234"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 10 }
                    endTime = timestamp { seconds = 100 }
                  }
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = source.metric.externalReportingSetId
                    }
                }
              }
            weightedMeasurements +=
              MetricKt.weightedMeasurement {
                weight = 3
                binaryRepresentation = 2
                measurement = measurement {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  cmmsCreateMeasurementRequestId = "1235"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 10 }
                    endTime = timestamp { seconds = 100 }
                  }
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = source.metric.externalReportingSetId
                    }
                }
              }
          }
      }
    val createdMetric = service.createMetric(createMetricRequest)

    val suffix = "-1"
    val batchSetCmmsMeasurementIdsRequest = batchSetCmmsMeasurementIdsRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      createdMetric.weightedMeasurementsList.forEach {
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId = it.measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = it.measurement.cmmsCreateMeasurementRequestId + suffix
          }
      }
    }
    measurementsService.batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsRequest)

    val batchSetMeasurementResultsRequest = batchSetMeasurementResultsRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      measurementResults +=
        BatchSetMeasurementResultsRequestKt.measurementResult {
          cmmsMeasurementId =
            createdMetric.weightedMeasurementsList
              .first()
              .measurement
              .cmmsCreateMeasurementRequestId + suffix
          results += MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 2 } }
        }
    }
    measurementsService.batchSetMeasurementResults(batchSetMeasurementResultsRequest)

    val retrievedMetrics =
      service.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += createdMetric.externalMetricId
        }
      )

    assertThat(retrievedMetrics.metricsList.first().state).isEqualTo(Metric.State.RUNNING)
  }

  @Test
  fun `batchGetMetrics throws NOT_FOUND when not all metrics found`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetric = service.createMetric(createMetricRequest)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += "1L"
            externalMetricIds += createdMetric.externalMetricId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.METRIC_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] = CMMS_MEASUREMENT_CONSUMER_ID
          metadata[Errors.Metadata.EXTERNAL_METRIC_ID.key] = "1L"
        }
      )
  }

  @Test
  fun `batchGetMetrics throws INVALID_ARGUMENT when missing mc id`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchGetMetrics(batchGetMetricsRequest { externalMetricIds += "1L" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("CmmsMeasurementConsumerId")
  }

  @Test
  fun `batchGetMetrics throws INVALID_ARGUMENT when too many to get`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            for (i in 1L..(MAX_BATCH_SIZE + 1)) {
              externalMetricIds += i.toString()
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Too many")
  }

  @Test
  fun `streamMetrics filters when measurement consumer filter is set`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val differentCmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + 2
    createMeasurementConsumer(differentCmmsMeasurementConsumerId, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId1",
      )
    val createMetricRequest2 =
      createCreateMetricRequest(
        differentCmmsMeasurementConsumerId,
        reportingSetsService,
        "externalMetricId2",
      )
    val createdMetric = service.createMetric(createMetricRequest)
    service.createMetric(createMetricRequest2)

    val retrievedMetrics =
      service
        .streamMetrics(
          streamMetricsRequest {
            filter =
              StreamMetricsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
        .toList()

    assertThat(retrievedMetrics).ignoringRepeatedFieldOrder().containsExactly(createdMetric)
  }

  @Test
  fun `streamMetrics filters when id after filter is set`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId1",
      )
    val createMetricRequest2 =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId2",
      )

    val createdMetric = service.createMetric(createMetricRequest)
    val createdMetric2 = service.createMetric(createMetricRequest2)

    val afterId = minOf(createdMetric.externalMetricId, createdMetric2.externalMetricId)

    val retrievedMetrics =
      service
        .streamMetrics(
          streamMetricsRequest {
            filter =
              StreamMetricsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricIdAfter = afterId
              }
          }
        )
        .toList()

    if (createdMetric.externalMetricId == afterId) {
      assertThat(retrievedMetrics).ignoringRepeatedFieldOrder().containsExactly(createdMetric2)
    } else {
      assertThat(retrievedMetrics).ignoringRepeatedFieldOrder().containsExactly(createdMetric)
    }
  }

  @Test
  fun `streamMetrics filters when both mc and after filter are set`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val differentCmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + 2
    createMeasurementConsumer(differentCmmsMeasurementConsumerId, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId1",
      )
    val createMetricRequest2 =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId2",
      )
    val createMetricRequest3 =
      createCreateMetricRequest(
        differentCmmsMeasurementConsumerId,
        reportingSetsService,
        "externalMetricId3",
      )

    val createdMetric = service.createMetric(createMetricRequest)
    val createdMetric2 = service.createMetric(createMetricRequest2)
    service.createMetric(createMetricRequest3)

    val afterId = minOf(createdMetric.externalMetricId, createdMetric2.externalMetricId)

    val retrievedMetrics =
      service
        .streamMetrics(
          streamMetricsRequest {
            filter =
              StreamMetricsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalMetricIdAfter = afterId
              }
          }
        )
        .toList()

    if (createdMetric.externalMetricId == afterId) {
      assertThat(retrievedMetrics).ignoringRepeatedFieldOrder().containsExactly(createdMetric2)
    } else {
      assertThat(retrievedMetrics).ignoringRepeatedFieldOrder().containsExactly(createdMetric)
    }
  }

  @Test
  fun `streamMetrics limits the number of results when limit is set`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId1",
      )
    val createMetricRequest2 =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId2",
      )

    val createdMetric = service.createMetric(createMetricRequest)
    val createdMetric2 = service.createMetric(createMetricRequest2)

    val retrievedMetrics =
      service
        .streamMetrics(
          streamMetricsRequest {
            filter =
              StreamMetricsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
            limit = 1
          }
        )
        .toList()

    if (createdMetric.externalMetricId < createdMetric2.externalMetricId) {
      assertThat(retrievedMetrics).ignoringRepeatedFieldOrder().containsExactly(createdMetric)
    } else {
      assertThat(retrievedMetrics).ignoringRepeatedFieldOrder().containsExactly(createdMetric2)
    }
  }

  @Test
  fun `streamMetrics returns empty flow when no metrics are found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val retrievedMetrics =
      service
        .streamMetrics(
          streamMetricsRequest {
            filter =
              StreamMetricsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
        .toList()

    assertThat(retrievedMetrics).hasSize(0)
  }

  @Test
  fun `streamMetrics throws INVALID_ARGUMENT when MC filter missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> { service.streamMetrics(streamMetricsRequest {}) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `invalidateMetric sets the state to INVALID`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "externalMetricId1",
      )

    val createdMetric = service.createMetric(createMetricRequest)

    assertThat(createdMetric.state).isEqualTo(Metric.State.RUNNING)

    val invalidatedMetric =
      service.invalidateMetric(
        invalidateMetricRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricId = createdMetric.externalMetricId
        }
      )

    assertThat(invalidatedMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(createdMetric.copy { state = Metric.State.INVALID })

    val retrievedMetric =
      service
        .streamMetrics(
          streamMetricsRequest {
            filter =
              StreamMetricsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
        .first()

    assertThat(retrievedMetric)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(createdMetric.copy { state = Metric.State.INVALID })
  }

  @Test
  fun `invalidateMetric throws NOT_FOUND when metric not found`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.invalidateMetric(
          invalidateMetricRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricId = "1L"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.METRIC_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] = CMMS_MEASUREMENT_CONSUMER_ID
          metadata[Errors.Metadata.EXTERNAL_METRIC_ID.key] = "1L"
        }
      )
  }

  @Test
  fun `invalidateMetric throws FAILED_PRECONDITION when metric FAILED`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        val source = this
        metric =
          source.metric.copy {
            weightedMeasurements +=
              MetricKt.weightedMeasurement {
                weight = 2
                binaryRepresentation = 1
                measurement = measurement {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  cmmsCreateMeasurementRequestId = "1234"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 10 }
                    endTime = timestamp { seconds = 100 }
                  }
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = source.metric.externalReportingSetId
                    }
                }
              }
            weightedMeasurements +=
              MetricKt.weightedMeasurement {
                weight = 3
                binaryRepresentation = 2
                measurement = measurement {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  cmmsCreateMeasurementRequestId = "1235"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 10 }
                    endTime = timestamp { seconds = 100 }
                  }
                  primitiveReportingSetBases +=
                    ReportingSetKt.primitiveReportingSetBasis {
                      externalReportingSetId = source.metric.externalReportingSetId
                    }
                }
              }
          }
      }
    val createdMetric = service.createMetric(createMetricRequest)

    val suffix = "-1"
    val batchSetCmmsMeasurementIdsRequest = batchSetCmmsMeasurementIdsRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      createdMetric.weightedMeasurementsList.forEach {
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId = it.measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = it.measurement.cmmsCreateMeasurementRequestId + suffix
          }
      }
    }
    measurementsService.batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsRequest)

    val batchSetMeasurementFailuresRequest = batchSetMeasurementFailuresRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      measurementFailures +=
        BatchSetMeasurementFailuresRequestKt.measurementFailure {
          cmmsMeasurementId =
            createdMetric.weightedMeasurementsList
              .first()
              .measurement
              .cmmsCreateMeasurementRequestId + suffix
          failure = MeasurementKt.failure { message = "failure" }
        }
    }
    measurementsService.batchSetMeasurementFailures(batchSetMeasurementFailuresRequest)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.invalidateMetric(
          invalidateMetricRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricId = createdMetric.externalMetricId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_METRIC_STATE_TRANSITION.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] = CMMS_MEASUREMENT_CONSUMER_ID
          metadata[Errors.Metadata.EXTERNAL_METRIC_ID.key] = createdMetric.externalMetricId
          metadata[Errors.Metadata.METRIC_STATE.key] = Metric.State.FAILED.name
          metadata[Errors.Metadata.NEW_METRIC_STATE.key] = Metric.State.INVALID.name
        }
      )
  }

  @Test
  fun `invalidateMetric throws INVALID_ARGUMENT when missing mc id`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.invalidateMetric(invalidateMetricRequest { externalMetricId = "1L" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "cmms_measurement_consumer_id"
        }
      )
  }

  @Test
  fun `invalidateMetric throws INVALID_ARGUMENT when missing metric id`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.invalidateMetric(
          invalidateMetricRequest { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "external_metric_id"
        }
      )
  }

  companion object {
    private suspend fun createCreateMetricRequest(
      cmmsMeasurementConsumerId: String,
      reportingSetsService: ReportingSetsCoroutineImplBase,
      externalMetricId: String = "external-metric-id",
    ): CreateMetricRequest {
      val createdReportingSet =
        createReportingSet(
          cmmsMeasurementConsumerId,
          reportingSetsService,
          externalMetricId + "reporting-set-id",
        )

      val metric = metric {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = interval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reach =
            MetricSpecKt.reachParams {
              multipleDataProviderParams =
                MetricSpecKt.samplingAndPrivacyParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  vidSamplingInterval =
                    MetricSpecKt.vidSamplingInterval {
                      start = 0.1f
                      width = 0.5f
                    }
                }
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            binaryRepresentation = 1
            measurement = measurement {
              this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter1"
                  filters += "filter2"
                }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter3"
                  filters += "filter4"
                }
            }
          }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 3
            binaryRepresentation = 2
            measurement = measurement {
              this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
              timeInterval = interval {
                startTime = timestamp { seconds = 10 }
                endTime = timestamp { seconds = 100 }
              }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter5"
                  filters += "filter6"
                }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = createdReportingSet.externalReportingSetId
                  filters += "filter7"
                  filters += "filter8"
                }
            }
          }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
            containingReport = "reportX"
          }
      }

      return createMetricRequest {
        this.metric = metric
        this.externalMetricId = externalMetricId
      }
    }

    private suspend fun createReportingSet(
      cmmsMeasurementConsumerId: String,
      reportingSetsService: ReportingSetsCoroutineImplBase,
      externalReportingSetId: String = "external-reporting-set-id",
    ): ReportingSet {
      val reportingSet = reportingSet {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = cmmsMeasurementConsumerId + "123"
              }
          }
      }
      return reportingSetsService.createReportingSet(
        createReportingSetRequest {
          this.reportingSet = reportingSet
          this.externalReportingSetId = externalReportingSetId
        }
      )
    }

    private suspend fun createMeasurementConsumer(
      cmmsMeasurementConsumerId: String,
      measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    ) {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
      )
    }
  }
}
