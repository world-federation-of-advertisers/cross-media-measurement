package org.wfanet.measurement.service.internal.kingdom.testing

import org.wfanet.measurement.common.testing.ServiceMocker
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineImplBase

class FakeReportConfigStorage : ReportConfigStorageCoroutineImplBase() {
  val mocker = ServiceMocker<ReportConfigStorageCoroutineImplBase>()

  override suspend fun listRequisitionTemplates(
    request: ListRequisitionTemplatesRequest
  ): ListRequisitionTemplatesResponse {
    return mocker.handleCall(request)
  }
}
