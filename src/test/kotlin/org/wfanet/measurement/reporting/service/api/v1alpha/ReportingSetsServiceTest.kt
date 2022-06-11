package org.wfanet.measurement.reporting.service.api.v1alpha

import kotlinx.coroutines.flow.flowOf
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.reporting.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.reportingSet as internalReportingSet

private val REPORTING_SET_NAME = "reportingSet1"
private val REPORTING_SET_NAME_2 = "reportingSet2"
private val REPORTING_SET_NAME_3 = "reportingSet3"
private val MEASUREMENT_CONSUMER_REFERENCE_ID = "measurementConsumerReferenceId1"
private val EXTERNAL_REPORTING_SET_ID =
  apiIdToExternalId(ReportingSetKey.fromName(REPORTING_SET_NAME)!!.reportingSetId)
private val EXTERNAL_REPORTING_SET_ID_2 =
  apiIdToExternalId(ReportingSetKey.fromName(REPORTING_SET_NAME_2)!!.reportingSetId)
private val EXTERNAL_REPORTING_SET_ID_3 =
  apiIdToExternalId(ReportingSetKey.fromName(REPORTING_SET_NAME_3)!!.reportingSetId)

private val INTERNAL_REPORTING_SET: InternalReportingSet = internalReportingSet {
  measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
  externalReportingSetId = EXTERNAL_REPORTING_SET_ID
}

@RunWith(JUnit4::class)
class ReportingSetsServiceTest {

  private val internalReportingSetsMock: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase =
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
}
