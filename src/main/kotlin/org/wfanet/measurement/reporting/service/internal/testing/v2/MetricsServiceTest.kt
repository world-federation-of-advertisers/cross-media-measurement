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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.CreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.streamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.timeInterval

private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"
private const val MAX_BATCH_SIZE = 1000

@RunWith(JUnit4::class)
abstract class MetricsServiceTest<T : MetricsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val metricsService: T,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.metricsService
    reportingSetsService = services.reportingSetsService
    measurementConsumersService = services.measurementConsumersService
  }

  @Test
  fun `createMetric succeeds when MetricSpec type is Reach`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        reach =
          MetricSpecKt.reachParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = 1.0
                delta = 2.0
              }
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val createdMetric = service.createMetric(createMetricRequest { this.metric = metric })

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    createdMetric.weightedMeasurementsList.forEach {
      assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
    }
  }

  @Test
  fun `createMetric succeeds when MetricSpec type is FrequencyHistogram`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
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
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val createdMetric = service.createMetric(createMetricRequest { this.metric = metric })

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
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
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        impressionCount =
          MetricSpecKt.impressionCountParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = 1.0
                delta = 2.0
              }
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val createdMetric = service.createMetric(createMetricRequest { this.metric = metric })

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
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
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        watchDuration =
          MetricSpecKt.watchDurationParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = 1.0
                delta = 2.0
              }
            maximumWatchDurationPerUser = 100
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val createdMetric = service.createMetric(createMetricRequest { this.metric = metric })

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
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
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        watchDuration =
          MetricSpecKt.watchDurationParams {
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = 1.0
                delta = 2.0
              }
            maximumWatchDurationPerUser = 100
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val createdMetric = service.createMetric(createMetricRequest { this.metric = metric })

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
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
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
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
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val createdMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          requestId = "requestId"
        }
      )

    assertThat(createdMetric.externalMetricId).isNotEqualTo(0L)

    val sameCreatedMetric =
      service.createMetric(
        createMetricRequest {
          this.metric = metric
          requestId = "requestId"
        }
      )

    assertThat(createdMetric).ignoringRepeatedFieldOrder().isEqualTo(sameCreatedMetric)
  }

  @Test
  fun `createMetric throws NOT_FOUND when ReportingSet in basis not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
            reachPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = 1.0
                delta = 2.0
              }
            frequencyPrivacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = 3.0
                delta = 4.0
              }
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
              startTime = timestamp { seconds = 10 }
              endTime = timestamp { seconds = 100 }
            }
            primitiveReportingSetBases +=
              ReportingSetKt.primitiveReportingSetBasis {
                externalReportingSetId = 1234
                filters += "filter1"
                filters += "filter2"
              }
          }
        }
      details =
        MetricKt.details {
          filters += "filter1"
          filters += "filter2"
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(createMetricRequest { this.metric = metric })
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
      externalReportingSetId = 1234
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
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
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(createMetricRequest { this.metric = metric })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Reporting Set")
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
            privacyParams =
              MetricSpecKt.differentialPrivacyParams {
                epsilon = 1.0
                delta = 2.0
              }
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(createMetricRequest { this.metric = metric })
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
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(createMetricRequest { this.metric = metric })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("type")
  }

  @Test
  fun `createMetric throws INVALID_ARGUMENT when metric spec missing vid sampling interval`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = timeInterval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reach =
            MetricSpecKt.reachParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = 1.0
                  delta = 2.0
                }
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = timeInterval {
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
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createMetric(createMetricRequest { this.metric = metric })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("vid")
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
        timeInterval = timeInterval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reach =
            MetricSpecKt.reachParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = 1.0
                  delta = 2.0
                }
            }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.5f
            }
        }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createMetric(createMetricRequest { this.metric = metric })
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
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
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
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createMetric(createMetricRequest { this.metric = metric })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("Measurement Consumer")
  }

  @Test
  fun `batchCreateMetrics succeeds for one create metric request`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val metric = metric {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = createdReportingSet.externalReportingSetId
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
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
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val batchCreateMetricsResponse =
      service.batchCreateMetrics(
        batchCreateMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          requests += createMetricRequest { this.metric = metric }
        }
      )

    val createdMetric = batchCreateMetricsResponse.metricsList.first()
    assertThat(batchCreateMetricsResponse.metricsList).hasSize(1)
    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
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
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
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
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val batchCreateMetricsResponse =
      service.batchCreateMetrics(
        batchCreateMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          requests += createMetricRequest { this.metric = metric }
          requests += createMetricRequest { this.metric = metric }
        }
      )

    val createdMetric = batchCreateMetricsResponse.metricsList.first()
    val createdMetric2 = batchCreateMetricsResponse.metricsList.last()
    assertThat(batchCreateMetricsResponse.metricsList).hasSize(2)
    assertThat(createdMetric.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric.hasCreateTime()).isTrue()
    assertThat(createdMetric2.externalMetricId).isNotEqualTo(0)
    assertThat(createdMetric2.hasCreateTime()).isTrue()
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
        timeInterval = timeInterval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          frequencyHistogram =
            MetricSpecKt.frequencyHistogramParams {
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
              maximumFrequencyPerUser = 5
            }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.5f
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = timeInterval {
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
          }
      }

      val createdMetric =
        service.createMetric(
          createMetricRequest {
            this.metric = metric
            requestId = "one"
          }
        )

      val batchCreateMetricsResponse =
        service.batchCreateMetrics(
          batchCreateMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            requests += createMetricRequest {
              this.metric = metric
              requestId = "one"
            }
            requests += createMetricRequest { this.metric = metric }
          }
        )

      assertThat(batchCreateMetricsResponse.metricsList).hasSize(2)
      assertThat(batchCreateMetricsResponse.metricsList.first())
        .ignoringRepeatedFieldOrder()
        .isEqualTo(createdMetric)

      val createdMetric2 = batchCreateMetricsResponse.metricsList.last()
      assertThat(createdMetric2.externalMetricId).isNotEqualTo(0)
      assertThat(createdMetric2.hasCreateTime()).isTrue()
      createdMetric2.weightedMeasurementsList.forEach {
        assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
      }
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
        timeInterval = timeInterval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          frequencyHistogram =
            MetricSpecKt.frequencyHistogramParams {
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
              maximumFrequencyPerUser = 5
            }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.5f
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = timeInterval {
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
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateMetrics(
            batchCreateMetricsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              requests += createMetricRequest { this.metric = metric }
              requests += createMetricRequest { this.metric = metric.copy { clearTimeInterval() } }
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
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
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
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateMetrics(
          batchCreateMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            requests += createMetricRequest { this.metric = metric }
            requests += createMetricRequest {
              this.metric = metric.copy { metricSpec = metricSpec.copy { clearType() } }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("type")
  }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when metric spec missing vid sampling`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

      val metric = metric {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = timeInterval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          frequencyHistogram =
            MetricSpecKt.frequencyHistogramParams {
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
              maximumFrequencyPerUser = 5
            }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.5f
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = timeInterval {
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
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateMetrics(
            batchCreateMetricsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              requests += createMetricRequest { this.metric = metric }
              requests += createMetricRequest {
                this.metric =
                  metric.copy { metricSpec = metricSpec.copy { clearVidSamplingInterval() } }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("vid")
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
        timeInterval = timeInterval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          frequencyHistogram =
            MetricSpecKt.frequencyHistogramParams {
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
              maximumFrequencyPerUser = 5
            }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.5f
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = timeInterval {
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
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateMetrics(
            batchCreateMetricsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              requests += createMetricRequest { this.metric = metric }
              requests += createMetricRequest {
                this.metric = metric.copy { weightedMeasurements.clear() }
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
        timeInterval = timeInterval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          frequencyHistogram =
            MetricSpecKt.frequencyHistogramParams {
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
              maximumFrequencyPerUser = 5
            }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.5f
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            measurement = measurement {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              timeInterval = timeInterval {
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
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateMetrics(
            batchCreateMetricsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              requests += createMetricRequest { this.metric = metric }
              requests += createMetricRequest {
                this.metric =
                  metric.copy { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + "2" }
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
      timeInterval = timeInterval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 100 }
      }
      metricSpec = metricSpec {
        frequencyHistogram =
          MetricSpecKt.frequencyHistogramParams {
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
            maximumFrequencyPerUser = 5
          }
        vidSamplingInterval =
          MetricSpecKt.vidSamplingInterval {
            start = 0.1f
            width = 0.5f
          }
      }
      weightedMeasurements +=
        MetricKt.weightedMeasurement {
          weight = 2
          measurement = measurement {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            timeInterval = timeInterval {
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
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateMetrics(
          batchCreateMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            for (i in 1..(MAX_BATCH_SIZE + 1)) {
              requests += createMetricRequest { this.metric = metric }
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
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = 0.1f
                  width = 0.5f
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
  fun `batchGetMetrics succeeds when metric spec type is frequency duration`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        metric =
          metric.copy {
            metricSpec = metricSpec {
              frequencyHistogram =
                MetricSpecKt.frequencyHistogramParams {
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
                  maximumFrequencyPerUser = 5
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = 0.1f
                  width = 0.5f
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
  fun `batchGetMetrics succeeds when metric spec type is impression count`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService).copy {
        metric =
          metric.copy {
            metricSpec = metricSpec {
              impressionCount =
                MetricSpecKt.impressionCountParams {
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  maximumFrequencyPerUser = 5
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = 0.1f
                  width = 0.5f
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
                  privacyParams =
                    MetricSpecKt.differentialPrivacyParams {
                      epsilon = 1.0
                      delta = 2.0
                    }
                  maximumWatchDurationPerUser = 100
                }
              vidSamplingInterval =
                MetricSpecKt.vidSamplingInterval {
                  start = 0.1f
                  width = 0.5f
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
  fun `batchGetMetrics succeeds when asking for two metrics`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetricRequest2 =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
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
                measurement = measurement {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  timeInterval = timeInterval {
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
            externalMetricIds += 1L
            externalMetricIds += createdMetric.externalMetricId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("not found")
  }

  @Test
  fun `batchGetMetrics throws INVALID_ARGUMENT when missing mc id`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchGetMetrics(batchGetMetricsRequest { externalMetricIds += 1L })
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
              externalMetricIds += i
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
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createMetricRequest2 =
      createCreateMetricRequest(differentCmmsMeasurementConsumerId, reportingSetsService)
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
  fun `streamReportingSets filters when id after filter is set`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val createMetricRequest =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createMetricRequest2 =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

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
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createMetricRequest2 =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createMetricRequest3 =
      createCreateMetricRequest(differentCmmsMeasurementConsumerId, reportingSetsService)

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
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createMetricRequest2 =
      createCreateMetricRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

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

  companion object {
    private suspend fun createCreateMetricRequest(
      cmmsMeasurementConsumerId: String,
      reportingSetsService: ReportingSetsCoroutineImplBase,
    ): CreateMetricRequest {
      val createdReportingSet = createReportingSet(cmmsMeasurementConsumerId, reportingSetsService)

      val metric = metric {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        externalReportingSetId = createdReportingSet.externalReportingSetId
        timeInterval = timeInterval {
          startTime = timestamp { seconds = 10 }
          endTime = timestamp { seconds = 100 }
        }
        metricSpec = metricSpec {
          reach =
            MetricSpecKt.reachParams {
              privacyParams =
                MetricSpecKt.differentialPrivacyParams {
                  epsilon = 1.0
                  delta = 2.0
                }
            }
          vidSamplingInterval =
            MetricSpecKt.vidSamplingInterval {
              start = 0.1f
              width = 0.5f
            }
        }
        weightedMeasurements +=
          MetricKt.weightedMeasurement {
            weight = 2
            measurement = measurement {
              this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
              timeInterval = timeInterval {
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
            measurement = measurement {
              this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
              timeInterval = timeInterval {
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
          }
      }

      return createMetricRequest { this.metric = metric }
    }

    private suspend fun createReportingSet(
      cmmsMeasurementConsumerId: String,
      reportingSetsService: ReportingSetsCoroutineImplBase
    ): ReportingSet {
      val reportingSet = reportingSet {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = cmmsMeasurementConsumerId + "123"
              }
          }
      }
      return reportingSetsService.createReportingSet(reportingSet)
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
