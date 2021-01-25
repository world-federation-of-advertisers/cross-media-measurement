// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.service.internal

import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesRequest
import org.wfanet.measurement.internal.kingdom.ListRequisitionTemplatesResponse
import org.wfanet.measurement.internal.kingdom.ReportConfigsGrpcKt.ReportConfigsCoroutineImplBase
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase

class ReportConfigsService(
  private val kingdomRelationalDatabase: KingdomRelationalDatabase
) : ReportConfigsCoroutineImplBase() {

  override suspend fun listRequisitionTemplates(
    request: ListRequisitionTemplatesRequest
  ): ListRequisitionTemplatesResponse {
    val id = ExternalId(request.externalReportConfigId)
    val requisitionTemplates = kingdomRelationalDatabase.listRequisitionTemplates(id)
    return ListRequisitionTemplatesResponse.newBuilder()
      .addAllRequisitionTemplates(requisitionTemplates.toList())
      .build()
  }
}
