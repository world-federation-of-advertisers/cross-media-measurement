package org.wfanet.measurement.service.internal.kingdom

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.testing.FakeKingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionTemplate
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class ReportConfigStorageServiceTest {

  companion object {
    const val EXTERNAL_REPORT_CONFIG_ID = 1L

    val REQUISITION_TEMPLATE1: RequisitionTemplate = RequisitionTemplate.newBuilder().apply {
      externalDataProviderId = 2
      externalCampaignId = 3
      requisitionDetailsBuilder.metricDefinitionBuilder.sketchBuilder.sketchConfigId = 4
    }.build()

    val REQUISITION_TEMPLATE2: RequisitionTemplate = RequisitionTemplate.newBuilder().apply {
      externalDataProviderId = 5
      externalCampaignId = 6
      requisitionDetailsBuilder.metricDefinitionBuilder.sketchBuilder.sketchConfigId = 7
    }.build()
  }

  private val fakeKingdomRelationalDatabase = FakeKingdomRelationalDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(ReportConfigStorageService(fakeKingdomRelationalDatabase))
  }

  private val stub: ReportConfigStorageCoroutineStub by lazy {
    ReportConfigStorageCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun listRequisitionTemplates() = runBlocking<Unit> {
    var capturedExternalReportConfigId: ExternalId? = null
    fakeKingdomRelationalDatabase.listRequisitionTemplatesFn = { externalReportConfigId ->
      capturedExternalReportConfigId = externalReportConfigId
      listOf(REQUISITION_TEMPLATE1, REQUISITION_TEMPLATE2)
    }

    val request =
      ListRequisitionTemplatesRequest.newBuilder()
        .setExternalReportConfigId(EXTERNAL_REPORT_CONFIG_ID)
        .build()

    val expectedResponse =
      ListRequisitionTemplatesResponse.newBuilder()
        .addRequisitionTemplates(REQUISITION_TEMPLATE1)
        .addRequisitionTemplates(REQUISITION_TEMPLATE2)
        .build()

    assertThat(stub.listRequisitionTemplates(request))
      .ignoringRepeatedFieldOrder()
      .isEqualTo(expectedResponse)

    assertThat(capturedExternalReportConfigId)
      .isEqualTo(ExternalId(EXTERNAL_REPORT_CONFIG_ID))
  }
}
