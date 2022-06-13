package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
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

// Reporting set names and IDs
private const val REPORTING_SET_NAME = "reportingSet"
private const val REPORTING_SET_NAME_2 = "reportingSet2"
private const val REPORTING_SET_NAME_3 = "reportingSet3"

private val EXTERNAL_REPORTING_SET_ID =
  apiIdToExternalId(ReportingSetKey.fromName(REPORTING_SET_NAME)!!.reportingSetId)
private val EXTERNAL_REPORTING_SET_ID_2 =
  apiIdToExternalId(ReportingSetKey.fromName(REPORTING_SET_NAME_2)!!.reportingSetId)
private val EXTERNAL_REPORTING_SET_ID_3 =
  apiIdToExternalId(ReportingSetKey.fromName(REPORTING_SET_NAME_3)!!.reportingSetId)

// Measurement consumer names and IDs
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumer"
private const val MEASUREMENT_CONSUMER_NAME_2 = "measurementConsumer2"
private const val MEASUREMENT_CONSUMER_NAME_3 = "measurementConsumer3"

private val MEASUREMENT_CONSUMER_REFERENCE_ID =
  MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME)!!.measurementConsumerId
private val MEASUREMENT_CONSUMER_REFERENCE_ID_2 =
  MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME_2)!!.measurementConsumerId
private val MEASUREMENT_CONSUMER_REFERENCE_ID_3 =
  MeasurementConsumerKey.fromName(MEASUREMENT_CONSUMER_NAME_3)!!.measurementConsumerId

// Data provider names and IDs
private val DATA_PROVIDER_EXTERNAL_ID = 123L
private val DATA_PROVIDER_EXTERNAL_ID_2 = 124L
private val DATA_PROVIDER_EXTERNAL_ID_3 = 125L

private val DATA_PROVIDER_REFERENCE_ID = externalIdToApiId(DATA_PROVIDER_EXTERNAL_ID)
private val DATA_PROVIDER_REFERENCE_ID_2 = externalIdToApiId(DATA_PROVIDER_EXTERNAL_ID_2)
private val DATA_PROVIDER_REFERENCE_ID_3 = externalIdToApiId(DATA_PROVIDER_EXTERNAL_ID_3)

private val DATA_PROVIDER_NAME = makeDataProvider(DATA_PROVIDER_EXTERNAL_ID)
private val DATA_PROVIDER_NAME_2 = makeDataProvider(DATA_PROVIDER_EXTERNAL_ID_2)
private val DATA_PROVIDER_NAME_3 = makeDataProvider(DATA_PROVIDER_EXTERNAL_ID_3)

// Event group names and IDs
private val EVENT_GROUP_RESOURCE_NAME = "$DATA_PROVIDER_NAME/eventGroupResourceName"
private val EVENT_GROUP_RESOURCE_NAME_2 = "$DATA_PROVIDER_NAME_2/eventGroupResourceName2"
private val EVENT_GROUP_RESOURCE_NAME_3 = "$DATA_PROVIDER_NAME_3/eventGroupResourceName3"
private val EVENT_GROUP_RESOURCE_NAMES =
  listOf(EVENT_GROUP_RESOURCE_NAME, EVENT_GROUP_RESOURCE_NAME_2, EVENT_GROUP_RESOURCE_NAME_3)

private val EVENT_GROUP_REFERENCE_ID =
  EventGroupKey.fromName(EVENT_GROUP_RESOURCE_NAME)!!.eventGroupReferenceId
private val EVENT_GROUP_REFERENCE_ID_2 =
  EventGroupKey.fromName(EVENT_GROUP_RESOURCE_NAME_2)!!.eventGroupReferenceId
private val EVENT_GROUP_REFERENCE_ID_3 =
  EventGroupKey.fromName(EVENT_GROUP_RESOURCE_NAME_3)!!.eventGroupReferenceId

// Event group keys
private val EVENT_GROUP_KEY = eventGroupKey {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  dataProviderReferenceId = DATA_PROVIDER_REFERENCE_ID
  eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
}
private val EVENT_GROUP_KEY_2 = eventGroupKey {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID_2
  dataProviderReferenceId = DATA_PROVIDER_REFERENCE_ID_2
  eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_2
}
private val EVENT_GROUP_KEY_3 = eventGroupKey {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID_3
  dataProviderReferenceId = DATA_PROVIDER_REFERENCE_ID_3
  eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_3
}
private val EVENT_GROUP_KEYS = listOf(EVENT_GROUP_KEY, EVENT_GROUP_KEY_2, EVENT_GROUP_KEY_3)

// Event filters
private const val FILTER = "AGE>20"

// Reporting sets
private const val DISPLAY_NAME = REPORTING_SET_NAME + FILTER

private val REPORTING_SET: ReportingSet = reportingSet {
  name = REPORTING_SET_NAME
  eventGroups.addAll(EVENT_GROUP_RESOURCE_NAMES)
  filter = FILTER
  displayName = DISPLAY_NAME
}

// Internal reporting sets
private val INTERNAL_REPORTING_SET: InternalReportingSet = internalReportingSet {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportingSetId = EXTERNAL_REPORTING_SET_ID
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
            INTERNAL_REPORTING_SET.copy { externalReportingSetId = EXTERNAL_REPORTING_SET_ID_2 },
            INTERNAL_REPORTING_SET.copy { externalReportingSetId = EXTERNAL_REPORTING_SET_ID_3 }
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
      .isEqualTo(INTERNAL_REPORTING_SET)

    assertThat(result).isEqualTo(expected)
  }
}
