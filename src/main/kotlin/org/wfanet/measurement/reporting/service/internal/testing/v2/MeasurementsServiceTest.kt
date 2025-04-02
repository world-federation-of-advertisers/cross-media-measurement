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
import com.google.rpc.errorInfo
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.util.UUID
import kotlin.random.Random
import kotlin.test.assertFailsWith
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
import org.wfanet.measurement.internal.reporting.v2.DeterministicCountDistinct
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.NoiseMechanism
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.reporting.service.internal.Errors

private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"
private const val MAX_BATCH_SIZE = 1000

@RunWith(JUnit4::class)
abstract class MeasurementsServiceTest<T : MeasurementsGrpcKt.MeasurementsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val measurementsService: T,
    val metricsService: MetricsGrpcKt.MetricsCoroutineImplBase,
    val reportingSetsService: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase,
    val measurementConsumersService:
      MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var metricsService: MetricsGrpcKt.MetricsCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
  private lateinit var measurementConsumersService:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.measurementsService
    metricsService = services.metricsService
    reportingSetsService = services.reportingSetsService
    measurementConsumersService = services.measurementConsumersService
  }

  @Test
  fun `batchSetCmmsMeasurementIds succeeds when updating one measurement`(): Unit = runBlocking {
    val metric =
      createReachMetric(
        CMMS_MEASUREMENT_CONSUMER_ID,
        metricsService,
        reportingSetsService,
        measurementConsumersService,
        1,
      )

    service.batchSetCmmsMeasurementIds(
      batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1234"
          }
      }
    )

    val readMetrics =
      metricsService.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += metric.externalMetricId
        }
      )

    assertThat(readMetrics.metricsList[0].weightedMeasurementsList[0])
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        metric.weightedMeasurementsList[0].copy {
          measurement =
            measurement.copy {
              cmmsMeasurementId = "1234"
              state = Measurement.State.PENDING
            }
        }
      )
  }

  @Test
  fun `batchSetCmmsMeasurementIds succeeds when updating two measurements`(): Unit = runBlocking {
    val metric =
      createReachMetric(
        CMMS_MEASUREMENT_CONSUMER_ID,
        metricsService,
        reportingSetsService,
        measurementConsumersService,
        2,
      )

    service.batchSetCmmsMeasurementIds(
      batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1234"
          }
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[1].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1235"
          }
      }
    )

    val readMetrics =
      metricsService.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += metric.externalMetricId
        }
      )

    assertThat(readMetrics.metricsList[0].weightedMeasurementsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        metric.weightedMeasurementsList[0].copy {
          measurement =
            measurement.copy {
              cmmsMeasurementId = "1234"
              state = Measurement.State.PENDING
            }
        },
        metric.weightedMeasurementsList[1].copy {
          measurement =
            measurement.copy {
              cmmsMeasurementId = "1235"
              state = Measurement.State.PENDING
            }
        },
      )
  }

  @Test
  fun `batchSetCmmsMeasurementIds succeeds when no filters in bases in measurements`(): Unit =
    runBlocking {
      val metric =
        createReachMetric(
          CMMS_MEASUREMENT_CONSUMER_ID,
          metricsService,
          reportingSetsService,
          measurementConsumersService,
          2,
          true,
        )

      service.batchSetCmmsMeasurementIds(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1234"
            }
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[1].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1235"
            }
        }
      )

      val readMetrics =
        metricsService.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += metric.externalMetricId
          }
        )

      assertThat(readMetrics.metricsList[0].weightedMeasurementsList)
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          metric.weightedMeasurementsList[0].copy {
            measurement =
              measurement.copy {
                cmmsMeasurementId = "1234"
                state = Measurement.State.PENDING
              }
          },
          metric.weightedMeasurementsList[1].copy {
            measurement =
              measurement.copy {
                cmmsMeasurementId = "1235"
                state = Measurement.State.PENDING
              }
          },
        )
    }

  @Test
  fun `batchSetCmmsMeasurementIds succeeds when updating the same measurement twice`(): Unit =
    runBlocking {
      val metric =
        createReachMetric(
          CMMS_MEASUREMENT_CONSUMER_ID,
          metricsService,
          reportingSetsService,
          measurementConsumersService,
          1,
        )

      val request = batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1234"
          }
      }

      service.batchSetCmmsMeasurementIds(request)
      service.batchSetCmmsMeasurementIds(request)

      val readMetrics =
        metricsService.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += metric.externalMetricId
          }
        )

      assertThat(readMetrics.metricsList[0].weightedMeasurementsList[0])
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          metric.weightedMeasurementsList[0].copy {
            measurement =
              measurement.copy {
                cmmsMeasurementId = "1234"
                state = Measurement.State.PENDING
              }
          }
        )
    }

  @Test
  fun `batchSetCmmsMeasurementIds throws NOT_FOUND when not all measurements found`(): Unit =
    runBlocking {
      val metric =
        createReachMetric(
          CMMS_MEASUREMENT_CONSUMER_ID,
          metricsService,
          reportingSetsService,
          measurementConsumersService,
          1,
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetCmmsMeasurementIds(
            batchSetCmmsMeasurementIdsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              measurementIds +=
                BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
                  cmmsCreateMeasurementRequestId =
                    metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
                  cmmsMeasurementId = "1234"
                }
              measurementIds +=
                BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
                  cmmsCreateMeasurementRequestId = UUID.randomUUID().toString()
                  cmmsMeasurementId = "1235"
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.message).contains("not found")
    }

  @Test
  fun `batchSetCmmsMeasurementIds throws NOT_FOUND when no measurements found`(): Unit =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetCmmsMeasurementIds(
            batchSetCmmsMeasurementIdsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              measurementIds +=
                BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
                  cmmsCreateMeasurementRequestId = UUID.randomUUID().toString()
                  cmmsMeasurementId = "1235"
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.message).contains("Measurement not")
    }

  @Test
  fun `batchSetCmmsMeasurementIds throws FAILED_PRECONDITION when mc not found`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetCmmsMeasurementIds(
            batchSetCmmsMeasurementIdsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              measurementIds +=
                BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
                  cmmsCreateMeasurementRequestId = UUID.randomUUID().toString()
                  cmmsMeasurementId = "1235"
                }
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
              CMMS_MEASUREMENT_CONSUMER_ID
          }
        )
    }

  @Test
  fun `batchSetCmmsMeasurementIds throws INVALID_ARGUMENT when missing mc id`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetCmmsMeasurementIds(
            batchSetCmmsMeasurementIdsRequest {
              measurementIds +=
                BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
                  cmmsCreateMeasurementRequestId = UUID.randomUUID().toString()
                  cmmsMeasurementId = "1234"
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("CmmsMeasurementConsumerId")
    }

  @Test
  fun `batchSetCmmsMeasurementIds throws INVALID_ARGUMENT when too many to get`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetCmmsMeasurementIds(
            batchSetCmmsMeasurementIdsRequest {
              for (i in 1L..(MAX_BATCH_SIZE + 1)) {
                measurementIds +=
                  BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
                    cmmsCreateMeasurementRequestId = UUID.randomUUID().toString()
                    cmmsMeasurementId = "1234"
                  }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("Too many")
    }

  @Test
  fun `batchSetMeasurementResults succeeds when updating one measurement`(): Unit = runBlocking {
    val metric =
      createReachMetric(
        CMMS_MEASUREMENT_CONSUMER_ID,
        metricsService,
        reportingSetsService,
        measurementConsumersService,
        1,
      )
    service.batchSetCmmsMeasurementIds(
      batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1234"
          }
      }
    )

    service.batchSetMeasurementResults(
      batchSetMeasurementResultsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementResults +=
          BatchSetMeasurementResultsRequestKt.measurementResult {
            cmmsMeasurementId = "1234"
            results +=
              MeasurementKt.result {
                reach =
                  MeasurementKt.ResultKt.reach {
                    value = 1
                    noiseMechanism = NoiseMechanism.GEOMETRIC
                    deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
                  }
              }
          }
      }
    )

    val readMetrics =
      metricsService.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += metric.externalMetricId
        }
      )

    assertThat(readMetrics.metricsList[0].weightedMeasurementsList[0])
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        metric.weightedMeasurementsList[0].copy {
          measurement =
            measurement.copy {
              cmmsMeasurementId = "1234"
              details =
                details.copy {
                  results +=
                    MeasurementKt.result {
                      reach =
                        MeasurementKt.ResultKt.reach {
                          value = 1
                          noiseMechanism = NoiseMechanism.GEOMETRIC
                          deterministicCountDistinct =
                            DeterministicCountDistinct.getDefaultInstance()
                        }
                    }
                }
              state = Measurement.State.SUCCEEDED
            }
        }
      )
  }

  @Test
  fun `batchSetMeasurementResults succeeds when updating two measurements`(): Unit = runBlocking {
    val metric =
      createReachMetric(
        CMMS_MEASUREMENT_CONSUMER_ID,
        metricsService,
        reportingSetsService,
        measurementConsumersService,
        2,
      )
    service.batchSetCmmsMeasurementIds(
      batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1234"
          }
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[1].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1235"
          }
      }
    )

    service.batchSetMeasurementResults(
      batchSetMeasurementResultsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementResults +=
          BatchSetMeasurementResultsRequestKt.measurementResult {
            cmmsMeasurementId = "1234"
            results +=
              MeasurementKt.result {
                reach =
                  MeasurementKt.ResultKt.reach {
                    value = 1

                    noiseMechanism = NoiseMechanism.GEOMETRIC
                    deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
                  }
              }
          }
        measurementResults +=
          BatchSetMeasurementResultsRequestKt.measurementResult {
            cmmsMeasurementId = "1235"
            results +=
              MeasurementKt.result {
                reach =
                  MeasurementKt.ResultKt.reach {
                    value = 2

                    noiseMechanism = NoiseMechanism.GEOMETRIC
                    deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
                  }
              }
          }
      }
    )

    val readMetrics =
      metricsService.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += metric.externalMetricId
        }
      )

    assertThat(readMetrics.metricsList[0].weightedMeasurementsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        metric.weightedMeasurementsList[0].copy {
          measurement =
            measurement.copy {
              cmmsMeasurementId = "1234"
              details =
                details.copy {
                  results +=
                    MeasurementKt.result {
                      reach =
                        MeasurementKt.ResultKt.reach {
                          value = 1
                          noiseMechanism = NoiseMechanism.GEOMETRIC
                          deterministicCountDistinct =
                            DeterministicCountDistinct.getDefaultInstance()
                        }
                    }
                }
              state = Measurement.State.SUCCEEDED
            }
        },
        metric.weightedMeasurementsList[1].copy {
          measurement =
            measurement.copy {
              cmmsMeasurementId = "1235"
              details =
                details.copy {
                  results +=
                    MeasurementKt.result {
                      reach =
                        MeasurementKt.ResultKt.reach {
                          value = 2
                          noiseMechanism = NoiseMechanism.GEOMETRIC
                          deterministicCountDistinct =
                            DeterministicCountDistinct.getDefaultInstance()
                        }
                    }
                }
              state = Measurement.State.SUCCEEDED
            }
        },
      )
  }

  @Test
  fun `batchSetMeasurementResults succeeds when no filters in bases in measurements`(): Unit =
    runBlocking {
      val metric =
        createReachMetric(
          CMMS_MEASUREMENT_CONSUMER_ID,
          metricsService,
          reportingSetsService,
          measurementConsumersService,
          2,
          noFiltersInMeasurementBases = true,
        )
      service.batchSetCmmsMeasurementIds(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1234"
            }
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[1].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1235"
            }
        }
      )

      service.batchSetMeasurementResults(
        batchSetMeasurementResultsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          measurementResults +=
            BatchSetMeasurementResultsRequestKt.measurementResult {
              cmmsMeasurementId = "1234"
              results +=
                MeasurementKt.result {
                  reach =
                    MeasurementKt.ResultKt.reach {
                      value = 1
                      noiseMechanism = NoiseMechanism.GEOMETRIC
                      deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
                    }
                }
            }
          measurementResults +=
            BatchSetMeasurementResultsRequestKt.measurementResult {
              cmmsMeasurementId = "1235"
              results +=
                MeasurementKt.result {
                  reach =
                    MeasurementKt.ResultKt.reach {
                      value = 2
                      noiseMechanism = NoiseMechanism.GEOMETRIC
                      deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
                    }
                }
            }
        }
      )

      val readMetrics =
        metricsService.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += metric.externalMetricId
          }
        )

      assertThat(readMetrics.metricsList[0].weightedMeasurementsList)
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          metric.weightedMeasurementsList[0].copy {
            measurement =
              measurement.copy {
                cmmsMeasurementId = "1234"
                details =
                  details.copy {
                    results +=
                      MeasurementKt.result {
                        reach =
                          MeasurementKt.ResultKt.reach {
                            value = 1
                            noiseMechanism = NoiseMechanism.GEOMETRIC
                            deterministicCountDistinct =
                              DeterministicCountDistinct.getDefaultInstance()
                          }
                      }
                  }
                state = Measurement.State.SUCCEEDED
              }
          },
          metric.weightedMeasurementsList[1].copy {
            measurement =
              measurement.copy {
                cmmsMeasurementId = "1235"
                details =
                  details.copy {
                    results +=
                      MeasurementKt.result {
                        reach =
                          MeasurementKt.ResultKt.reach {
                            value = 2

                            noiseMechanism = NoiseMechanism.GEOMETRIC
                            deterministicCountDistinct =
                              DeterministicCountDistinct.getDefaultInstance()
                          }
                      }
                  }
                state = Measurement.State.SUCCEEDED
              }
          },
        )
    }

  @Test
  fun `batchSetMeasurementResults succeeds when updating same measurement twice`(): Unit =
    runBlocking {
      val metric =
        createReachMetric(
          CMMS_MEASUREMENT_CONSUMER_ID,
          metricsService,
          reportingSetsService,
          measurementConsumersService,
          1,
        )
      service.batchSetCmmsMeasurementIds(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1234"
            }
        }
      )

      val request = batchSetMeasurementResultsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementResults +=
          BatchSetMeasurementResultsRequestKt.measurementResult {
            cmmsMeasurementId = "1234"
            results +=
              MeasurementKt.result {
                reach =
                  MeasurementKt.ResultKt.reach {
                    value = 1
                    noiseMechanism = NoiseMechanism.GEOMETRIC
                    deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
                  }
              }
          }
      }

      service.batchSetMeasurementResults(request)
      service.batchSetMeasurementResults(request)

      val readMetrics =
        metricsService.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += metric.externalMetricId
          }
        )

      assertThat(readMetrics.metricsList[0].weightedMeasurementsList[0])
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          metric.weightedMeasurementsList[0].copy {
            measurement =
              measurement.copy {
                cmmsMeasurementId = "1234"
                details =
                  details.copy {
                    results +=
                      MeasurementKt.result {
                        reach =
                          MeasurementKt.ResultKt.reach {
                            value = 1
                            noiseMechanism = NoiseMechanism.GEOMETRIC
                            deterministicCountDistinct =
                              DeterministicCountDistinct.getDefaultInstance()
                          }
                      }
                  }
                state = Measurement.State.SUCCEEDED
              }
          }
        )
    }

  @Test
  fun `batchSetMeasurementResults throws NOT_FOUND when not all measurements found`(): Unit =
    runBlocking {
      val metric =
        createReachMetric(
          CMMS_MEASUREMENT_CONSUMER_ID,
          metricsService,
          reportingSetsService,
          measurementConsumersService,
          1,
        )
      service.batchSetCmmsMeasurementIds(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1234"
            }
        }
      )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementResults(
            batchSetMeasurementResultsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              measurementResults +=
                BatchSetMeasurementResultsRequestKt.measurementResult {
                  cmmsMeasurementId = "1234"
                  results += Measurement.Result.getDefaultInstance()
                }
              measurementResults +=
                BatchSetMeasurementResultsRequestKt.measurementResult {
                  cmmsMeasurementId = "1235"
                  results += Measurement.Result.getDefaultInstance()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.message).contains("not found")
    }

  @Test
  fun `batchSetMeasurementResults throws NOT_FOUND when no measurements found`(): Unit =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementResults(
            batchSetMeasurementResultsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              measurementResults +=
                BatchSetMeasurementResultsRequestKt.measurementResult {
                  cmmsMeasurementId = "1234"
                  results += Measurement.Result.getDefaultInstance()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.message).contains("Measurement not")
    }

  @Test
  fun `batchSetMeasurementResults throws FAILED_PRECONDITION when mc not found`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementResults(
            batchSetMeasurementResultsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              measurementResults +=
                BatchSetMeasurementResultsRequestKt.measurementResult {
                  cmmsMeasurementId = "1234"
                  results += Measurement.Result.getDefaultInstance()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND.name
            metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] = "1234"
          }
        )
    }

  @Test
  fun `batchSetMeasurementResults throws INVALID_ARGUMENT when missing mc id`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementResults(
            batchSetMeasurementResultsRequest {
              measurementResults +=
                BatchSetMeasurementResultsRequestKt.measurementResult {
                  cmmsMeasurementId = "1234"
                  results += Measurement.Result.getDefaultInstance()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("CmmsMeasurementConsumerId")
    }

  @Test
  fun `batchSetMeasurementResults throws INVALID_ARGUMENT when too many to get`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementResults(
            batchSetMeasurementResultsRequest {
              for (i in 1L..(MAX_BATCH_SIZE + 1)) {
                measurementResults +=
                  BatchSetMeasurementResultsRequestKt.measurementResult {
                    cmmsMeasurementId = "1234"
                    results += Measurement.Result.getDefaultInstance()
                  }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("Too many")
    }

  @Test
  fun `batchSetMeasurementFailures succeeds when updating one measurement`(): Unit = runBlocking {
    val metric =
      createReachMetric(
        CMMS_MEASUREMENT_CONSUMER_ID,
        metricsService,
        reportingSetsService,
        measurementConsumersService,
        1,
      )
    service.batchSetCmmsMeasurementIds(
      batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1234"
          }
      }
    )

    service.batchSetMeasurementFailures(
      batchSetMeasurementFailuresRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementFailures +=
          BatchSetMeasurementFailuresRequestKt.measurementFailure {
            cmmsMeasurementId = "1234"
            failure = MeasurementKt.failure { message = "failure1" }
          }
      }
    )

    val readMetrics =
      metricsService.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += metric.externalMetricId
        }
      )

    assertThat(readMetrics.metricsList[0].weightedMeasurementsList[0])
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        metric.weightedMeasurementsList[0].copy {
          measurement =
            measurement.copy {
              cmmsMeasurementId = "1234"
              details = details.copy { failure = MeasurementKt.failure { message = "failure1" } }
              state = Measurement.State.FAILED
            }
        }
      )
  }

  @Test
  fun `batchSetMeasurementFailures succeeds when updating two measurements`(): Unit = runBlocking {
    val metric =
      createReachMetric(
        CMMS_MEASUREMENT_CONSUMER_ID,
        metricsService,
        reportingSetsService,
        measurementConsumersService,
        2,
      )
    service.batchSetCmmsMeasurementIds(
      batchSetCmmsMeasurementIdsRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1234"
          }
        measurementIds +=
          BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
            cmmsCreateMeasurementRequestId =
              metric.weightedMeasurementsList[1].measurement.cmmsCreateMeasurementRequestId
            cmmsMeasurementId = "1235"
          }
      }
    )

    service.batchSetMeasurementFailures(
      batchSetMeasurementFailuresRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementFailures +=
          BatchSetMeasurementFailuresRequestKt.measurementFailure {
            cmmsMeasurementId = "1234"
            failure = MeasurementKt.failure { message = "failure1" }
          }
        measurementFailures +=
          BatchSetMeasurementFailuresRequestKt.measurementFailure {
            cmmsMeasurementId = "1235"
            failure = MeasurementKt.failure { message = "failure2" }
          }
      }
    )

    val readMetrics =
      metricsService.batchGetMetrics(
        batchGetMetricsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalMetricIds += metric.externalMetricId
        }
      )

    assertThat(readMetrics.metricsList[0].weightedMeasurementsList)
      .ignoringRepeatedFieldOrder()
      .containsExactly(
        metric.weightedMeasurementsList[0].copy {
          measurement =
            measurement.copy {
              cmmsMeasurementId = "1234"
              details = details.copy { failure = MeasurementKt.failure { message = "failure1" } }
              state = Measurement.State.FAILED
            }
        },
        metric.weightedMeasurementsList[1].copy {
          measurement =
            measurement.copy {
              cmmsMeasurementId = "1235"
              details = details.copy { failure = MeasurementKt.failure { message = "failure2" } }
              state = Measurement.State.FAILED
            }
        },
      )
  }

  @Test
  fun `batchSetMeasurementFailures succeeds when no filters in bases in measurements`(): Unit =
    runBlocking {
      val metric =
        createReachMetric(
          CMMS_MEASUREMENT_CONSUMER_ID,
          metricsService,
          reportingSetsService,
          measurementConsumersService,
          2,
          noFiltersInMeasurementBases = true,
        )
      service.batchSetCmmsMeasurementIds(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1234"
            }
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[1].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1235"
            }
        }
      )

      service.batchSetMeasurementFailures(
        batchSetMeasurementFailuresRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          measurementFailures +=
            BatchSetMeasurementFailuresRequestKt.measurementFailure {
              cmmsMeasurementId = "1234"
              failure = MeasurementKt.failure { message = "failure1" }
            }
          measurementFailures +=
            BatchSetMeasurementFailuresRequestKt.measurementFailure {
              cmmsMeasurementId = "1235"
              failure = MeasurementKt.failure { message = "failure2" }
            }
        }
      )

      val readMetrics =
        metricsService.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += metric.externalMetricId
          }
        )

      assertThat(readMetrics.metricsList[0].weightedMeasurementsList)
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          metric.weightedMeasurementsList[0].copy {
            measurement =
              measurement.copy {
                cmmsMeasurementId = "1234"
                details = details.copy { failure = MeasurementKt.failure { message = "failure1" } }
                state = Measurement.State.FAILED
              }
          },
          metric.weightedMeasurementsList[1].copy {
            measurement =
              measurement.copy {
                cmmsMeasurementId = "1235"
                details = details.copy { failure = MeasurementKt.failure { message = "failure2" } }
                state = Measurement.State.FAILED
              }
          },
        )
    }

  @Test
  fun `batchSetMeasurementFailures succeeds when updating same measurement twice`(): Unit =
    runBlocking {
      val metric =
        createReachMetric(
          CMMS_MEASUREMENT_CONSUMER_ID,
          metricsService,
          reportingSetsService,
          measurementConsumersService,
          1,
        )
      service.batchSetCmmsMeasurementIds(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1234"
            }
        }
      )

      val request = batchSetMeasurementFailuresRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        measurementFailures +=
          BatchSetMeasurementFailuresRequestKt.measurementFailure {
            cmmsMeasurementId = "1234"
            failure = MeasurementKt.failure { message = "failure1" }
          }
      }

      service.batchSetMeasurementFailures(request)
      service.batchSetMeasurementFailures(request)

      val readMetrics =
        metricsService.batchGetMetrics(
          batchGetMetricsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalMetricIds += metric.externalMetricId
          }
        )

      assertThat(readMetrics.metricsList[0].weightedMeasurementsList[0])
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          metric.weightedMeasurementsList[0].copy {
            measurement =
              measurement.copy {
                cmmsMeasurementId = "1234"
                details = details.copy { failure = MeasurementKt.failure { message = "failure1" } }
                state = Measurement.State.FAILED
              }
          }
        )
    }

  @Test
  fun `batchSetMeasurementFailures throws NOT_FOUND when not all measurements found`(): Unit =
    runBlocking {
      val metric =
        createReachMetric(
          CMMS_MEASUREMENT_CONSUMER_ID,
          metricsService,
          reportingSetsService,
          measurementConsumersService,
          1,
        )
      service.batchSetCmmsMeasurementIds(
        batchSetCmmsMeasurementIdsRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                metric.weightedMeasurementsList[0].measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "1234"
            }
        }
      )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementFailures(
            batchSetMeasurementFailuresRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              measurementFailures +=
                BatchSetMeasurementFailuresRequestKt.measurementFailure {
                  cmmsMeasurementId = "1234"
                  failure = Measurement.Failure.getDefaultInstance()
                }
              measurementFailures +=
                BatchSetMeasurementFailuresRequestKt.measurementFailure {
                  cmmsMeasurementId = "1235"
                  failure = Measurement.Failure.getDefaultInstance()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.message).contains("not found")
    }

  @Test
  fun `batchSetMeasurementFailures throws NOT_FOUND when no measurements found`(): Unit =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementFailures(
            batchSetMeasurementFailuresRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              measurementFailures +=
                BatchSetMeasurementFailuresRequestKt.measurementFailure {
                  cmmsMeasurementId = "1234"
                  failure = Measurement.Failure.getDefaultInstance()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.message).contains("Measurement not")
    }

  @Test
  fun `batchSetMeasurementFailures throws FAILED_PRECONDITION when mc not found`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementFailures(
            batchSetMeasurementFailuresRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              measurementFailures +=
                BatchSetMeasurementFailuresRequestKt.measurementFailure {
                  cmmsMeasurementId = "1234"
                  failure = Measurement.Failure.getDefaultInstance()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND.name
            metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] = "1234"
          }
        )
    }

  @Test
  fun `batchSetMeasurementFailures throws INVALID_ARGUMENT when missing mc id`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementFailures(
            batchSetMeasurementFailuresRequest {
              measurementFailures +=
                BatchSetMeasurementFailuresRequestKt.measurementFailure {
                  cmmsMeasurementId = "1234"
                  failure = Measurement.Failure.getDefaultInstance()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("CmmsMeasurementConsumerId")
    }

  @Test
  fun `batchSetMeasurementFailures throws INVALID_ARGUMENT when too many to get`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchSetMeasurementFailures(
            batchSetMeasurementFailuresRequest {
              for (i in 1L..(MAX_BATCH_SIZE + 1)) {
                measurementFailures +=
                  BatchSetMeasurementFailuresRequestKt.measurementFailure {
                    cmmsMeasurementId = "1234"
                    failure = Measurement.Failure.getDefaultInstance()
                  }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("Too many")
    }

  companion object {
    private suspend fun createReachMetric(
      cmmsMeasurementConsumerId: String,
      metricsService: MetricsGrpcKt.MetricsCoroutineImplBase,
      reportingSetsService: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase,
      measurementConsumersService: MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase,
      numDefaultMeasurements: Int,
      noFiltersInMeasurementBases: Boolean = false,
    ): Metric {
      createMeasurementConsumer(cmmsMeasurementConsumerId, measurementConsumersService)
      val createdReportingSet =
        createPrimitiveReportingSet(cmmsMeasurementConsumerId, reportingSetsService)

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
        for (i in 0 until numDefaultMeasurements) {
          weightedMeasurements +=
            MetricKt.weightedMeasurement {
              weight = 2
              measurement = measurement {
                cmmsCreateMeasurementRequestId = UUID.randomUUID().toString()
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                timeInterval = interval {
                  startTime = timestamp { seconds = 10 }
                  endTime = timestamp { seconds = 100 }
                }
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    externalReportingSetId = createdReportingSet.externalReportingSetId
                    if (!noFiltersInMeasurementBases) {
                      filters += "filter1"
                      filters += "filter2"
                    }
                  }
                details = MeasurementKt.details { dataProviderCount = 3 }
              }
            }
        }
        details =
          MetricKt.details {
            filters += "filter1"
            filters += "filter2"
          }
      }

      return metricsService.createMetric(
        createMetricRequest {
          this.metric = metric
          externalMetricId = "externalMetricId"
        }
      )
    }

    private suspend fun createPrimitiveReportingSet(
      cmmsMeasurementConsumerId: String,
      reportingSetsService: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase,
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
          externalReportingSetId = "reportingSetId"
        }
      )
    }

    private suspend fun createMeasurementConsumer(
      cmmsMeasurementConsumerId: String,
      measurementConsumersService: MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase,
    ) {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
      )
    }
  }
}
