package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.reporting.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetKt.eventGroupKey
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.reportingSet as internalReportingSet
import org.wfanet.measurement.reporting.v1alpha.ReportingSet
import org.wfanet.measurement.reporting.v1alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.reportingSet

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

// Data provider IDs and names
private const val DATA_PROVIDER_EXTERNAL_ID = 221L
private const val DATA_PROVIDER_EXTERNAL_ID_2 = 222L
private const val DATA_PROVIDER_EXTERNAL_ID_3 = 223L
private val DATA_PROVIDER_REFERENCE_ID = externalIdToApiId(DATA_PROVIDER_EXTERNAL_ID)
private val DATA_PROVIDER_REFERENCE_ID_2 = externalIdToApiId(DATA_PROVIDER_EXTERNAL_ID_2)
private val DATA_PROVIDER_REFERENCE_ID_3 = externalIdToApiId(DATA_PROVIDER_EXTERNAL_ID_3)

private val DATA_PROVIDER_NAME = DataProviderKey(DATA_PROVIDER_REFERENCE_ID).toName()
private val DATA_PROVIDER_NAME_2 = DataProviderKey(DATA_PROVIDER_REFERENCE_ID_2).toName()
private val DATA_PROVIDER_NAME_3 = DataProviderKey(DATA_PROVIDER_REFERENCE_ID_3).toName()

// Reporting set IDs and names
private val REPORTING_SET_EXTERNAL_ID = 331L
private val REPORTING_SET_EXTERNAL_ID_2 = 332L
private val REPORTING_SET_EXTERNAL_ID_3 = 333L

private val REPORTING_SET_NAME =
  ReportingSetKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORTING_SET_EXTERNAL_ID))
    .toName()
private val REPORTING_SET_NAME_2 =
  ReportingSetKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORTING_SET_EXTERNAL_ID_2))
    .toName()
private val REPORTING_SET_NAME_3 =
  ReportingSetKey(MEASUREMENT_CONSUMER_REFERENCE_ID, externalIdToApiId(REPORTING_SET_EXTERNAL_ID_3))
    .toName()

// Event group IDs and names
private val EVENT_GROUP_EXTERNAL_ID = 441L
private val EVENT_GROUP_EXTERNAL_ID_2 = 442L
private val EVENT_GROUP_EXTERNAL_ID_3 = 443L
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
private val EVENT_GROUP_NAMES = listOf(EVENT_GROUP_NAME, EVENT_GROUP_NAME_2, EVENT_GROUP_NAME_3)

// Event group keys
private val EVENT_GROUP_KEY = eventGroupKey {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  dataProviderReferenceId = DATA_PROVIDER_REFERENCE_ID
  eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
}
private val EVENT_GROUP_KEY_2 = eventGroupKey {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  dataProviderReferenceId = DATA_PROVIDER_REFERENCE_ID_2
  eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_2
}
private val EVENT_GROUP_KEY_3 = eventGroupKey {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  dataProviderReferenceId = DATA_PROVIDER_REFERENCE_ID_3
  eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_3
}
private val EVENT_GROUP_KEYS = listOf(EVENT_GROUP_KEY, EVENT_GROUP_KEY_2, EVENT_GROUP_KEY_3)

// Event filters
private const val FILTER = "AGE>20"

// Reporting sets
private val DISPLAY_NAME = REPORTING_SET_NAME + FILTER

private val REPORTING_SET: ReportingSet = reportingSet {
  name = REPORTING_SET_NAME
  eventGroups.addAll(EVENT_GROUP_NAMES)
  filter = FILTER
  displayName = DISPLAY_NAME
}

// Internal reporting sets
private val INTERNAL_REPORTING_SET: InternalReportingSet = internalReportingSet {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportingSetId = REPORTING_SET_EXTERNAL_ID
  eventGroupKeys.addAll(EVENT_GROUP_KEYS)
  filter = FILTER
  displayName = DISPLAY_NAME
}

@RunWith(JUnit4::class)
class ReportingSetsServiceTest {

  private val internalReportingSetsMock: ReportingSetsCoroutineImplBase =
    mockService() {
      onBlocking { createReportingSet(any()) }.thenReturn(INTERNAL_REPORTING_SET)
      onBlocking { streamReportingSets(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_REPORTING_SET,
            INTERNAL_REPORTING_SET.copy { externalReportingSetId = REPORTING_SET_EXTERNAL_ID_2 },
            INTERNAL_REPORTING_SET.copy { externalReportingSetId = REPORTING_SET_EXTERNAL_ID_3 }
          )
        )
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalReportingSetsMock) }

  private lateinit var service: ReportingSetsService

  @Before
  fun initService() {
    service = ReportingSetsService(ReportingSetsCoroutineStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createReportingSet returns reporting set`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportingSet = REPORTING_SET
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.createReportingSet(request) }
      }

    val expected = REPORTING_SET

    verifyProtoArgument(
        internalReportingSetsMock,
        ReportingSetsCoroutineImplBase::createReportingSet
      )
      .isEqualTo(INTERNAL_REPORTING_SET.copy { clearExternalReportingSetId() })

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createReportingSet throws UNAUTHENTICATED when no principal is found`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportingSet = REPORTING_SET
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.createReportingSet(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createReportingSet throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    val request = createReportingSetRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      reportingSet = REPORTING_SET
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME_2) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.status.description)
      .isEqualTo("Cannot create a ReportingSet for another MeasurementConsumer.")
  }

  @Test
  fun `createReportingSet throws INVALID_ARGUMENT when parent is missing`() {
    val request = createReportingSetRequest { reportingSet = REPORTING_SET }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createReportingSet(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
