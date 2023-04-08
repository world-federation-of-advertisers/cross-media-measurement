package org.wfanet.measurement.reporting.service.internal.testing.v2

import com.google.common.truth.Truth
import java.time.Clock
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer

private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

@RunWith(JUnit4::class)
abstract class ReportingSetsServiceTest<T : ReportingSetsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportingSetsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportingSetsService
    measurementConsumersService = services.measurementConsumersService
  }

  @Test
  fun `createReportingSet succeeds when reporting set is primitive`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(measurementConsumer {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
    })

    val reportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive = ReportingSetKt.primitive {
        eventGroupKeys +=
          ReportingSetKt.PrimitiveKt.eventGroupKey {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            cmmsDataProviderId = "1235"
            cmmsEventGroupId = "1236"
          }

        eventGroupKeys +=
          ReportingSetKt.PrimitiveKt.eventGroupKey {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            cmmsDataProviderId = "2235"
            cmmsEventGroupId = "2236"
          }
      }
    }

    val createdReportingSet = service.createReportingSet(reportingSet)

    Truth.assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }

  @Test
  fun `createReportingSet succeeds when reporting set is composite`() = runBlocking {
    measurementConsumersService.createMeasurementConsumer(measurementConsumer {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
    })

    val primitiveReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName"
      filter = "filter"

      primitive = ReportingSetKt.primitive {
        eventGroupKeys +=
          ReportingSetKt.PrimitiveKt.eventGroupKey {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            cmmsDataProviderId = "1235"
            cmmsEventGroupId = "1236"
          }
      }
    }

    val createdPrimitiveReportingSet = service.createReportingSet(primitiveReportingSet)

    val compositeReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      displayName = "displayName2"
      filter = "filter2"

      composite = ReportingSetKt.setExpression {
        operation = ReportingSet.SetExpression.Operation.UNION
        lhs = ReportingSetKt.SetExpressionKt.operand {
          expression = ReportingSetKt.setExpression {
            operation = ReportingSet.SetExpression.Operation.DIFFERENCE
            lhs = ReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
            }
            rhs = ReportingSetKt.SetExpressionKt.operand {
              expression = ReportingSetKt.setExpression {
                operation = ReportingSet.SetExpression.Operation.INTERSECTION
                lhs = ReportingSetKt.SetExpressionKt.operand {
                  externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
                }
              }
            }
          }
        }
      }

      weightedSubsetUnions += ReportingSetKt.weightedSubsetUnion {
        primitiveReportingSetBases += ReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
          filters += "filter1"
          filters += "filter2"
        }
        primitiveReportingSetBases += ReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
          filters += "filter1"
          filters += "filter2"
        }
        weight = 5
      }

      weightedSubsetUnions += ReportingSetKt.weightedSubsetUnion {
        primitiveReportingSetBases += ReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
          filters += "filter1"
          filters += "filter2"
        }
        primitiveReportingSetBases += ReportingSetKt.primitiveReportingSetBasis {
          externalReportingSetId = createdPrimitiveReportingSet.externalReportingSetId
          filters += "filter1"
          filters += "filter2"
        }
        weight = 6
      }
    }

    val createdReportingSet = service.createReportingSet(compositeReportingSet)

    Truth.assertThat(createdReportingSet.externalReportingSetId).isNotEqualTo(0L)
  }
}
