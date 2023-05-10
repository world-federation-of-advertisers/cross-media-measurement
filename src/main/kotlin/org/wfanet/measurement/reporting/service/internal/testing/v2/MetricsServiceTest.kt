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
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.timeInterval

private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"
private const val MAX_BATCH_CREATE_SIZE = 200

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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + "2"
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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + "2"
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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + "2"
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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + "2"
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
  fun `createMetric succeeds when no filters in measurements`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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

  /** TODO(tristanvuong2021): implement read methods for metric */
  @Ignore
  @Test
  fun `createMetric returns the same metric when using an existing request id`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + "2"
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

    assertThat(createdMetric).isEqualTo(sameCreatedMetric)
  }

  @Test
  fun `createMetric throws NOT_FOUND when ReportingSet in basis not found`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val reportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val reportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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

    assertThat(batchCreateMetricsResponse.metricsList).hasSize(1)
    assertThat(batchCreateMetricsResponse.metricsList.first().externalMetricId).isNotEqualTo(0)
    batchCreateMetricsResponse.metricsList.forEach {
      it.weightedMeasurementsList.forEach { weightedMeasurement ->
        assertThat(weightedMeasurement.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
      }
    }
  }

  @Test
  fun `batchCreateMetrics succeeds for two create metric requests`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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

    assertThat(batchCreateMetricsResponse.metricsList).hasSize(2)
    assertThat(batchCreateMetricsResponse.metricsList.first().externalMetricId).isNotEqualTo(0)
    assertThat(batchCreateMetricsResponse.metricsList.last().externalMetricId).isNotEqualTo(0)
    batchCreateMetricsResponse.metricsList.forEach {
      it.weightedMeasurementsList.forEach { weightedMeasurement ->
        assertThat(weightedMeasurement.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
      }
    }
  }

  /** TODO(tristanvuong2021): implement read methods for metric */
  @Ignore
  @Test
  fun `batchCreateMetrics succeeds for two create metric requests with one already existing`() =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val reportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
      assertThat(batchCreateMetricsResponse.metricsList.first().externalMetricId)
        .isEqualTo(createdMetric.externalMetricId)
      assertThat(batchCreateMetricsResponse.metricsList.last().externalMetricId).isNotEqualTo(0)
      batchCreateMetricsResponse.metricsList.last().weightedMeasurementsList.forEach {
        assertThat(it.measurement.cmmsCreateMeasurementRequestId).isNotEmpty()
      }
    }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when metric missing time interval`() =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val reportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
                this.metric = metric.copy { clearTimeInterval() }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("time")
    }

  @Test
  fun `batchCreateMetrics throws INVALID_ARGUMENT when metric spec missing type`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val reportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
                this.metric = metric.copy { metricSpec = metricSpec.copy {
                  clearVidSamplingInterval()
                } }
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
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val reportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val reportingSet = reportingSet {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = "1236"
              }
          }
      }

      val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1236"
            }
        }
    }

    val createdReportingSet = reportingSetsService.createReportingSet(reportingSet)

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
            for (i in 1..(MAX_BATCH_CREATE_SIZE + 1)) {
              requests += createMetricRequest { this.metric = metric }
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Too many")
  }
}
