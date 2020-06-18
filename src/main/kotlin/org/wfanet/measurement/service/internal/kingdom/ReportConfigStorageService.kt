package org.wfanet.measurement.service.internal.kingdom

import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.ReportConfigStorageGrpcKt.ReportConfigStorageCoroutineImplBase

class ReportConfigStorageService(
  private val kingdomRelationalDatabase: KingdomRelationalDatabase
) : ReportConfigStorageCoroutineImplBase() {

  override suspend fun listRequisitionTemplates(
    request: ListRequisitionTemplatesRequest
  ): ListRequisitionTemplatesResponse {
    val id = ExternalId(request.externalReportConfigId)
    val requisitionTemplates = kingdomRelationalDatabase.listRequisitionTemplates(id)
    return ListRequisitionTemplatesResponse.newBuilder()
      .addAllRequisitionTemplates(requisitionTemplates)
      .build()
  }
}
