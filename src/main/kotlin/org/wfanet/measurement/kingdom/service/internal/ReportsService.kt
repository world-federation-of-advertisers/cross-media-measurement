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

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionResponse
import org.wfanet.measurement.internal.kingdom.ConfirmDuchyReadinessRequest
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.FinishReportRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.kingdom.db.streamReportsFilter

class ReportsService(
  private val kingdomRelationalDatabase: KingdomRelationalDatabase
) : ReportsCoroutineImplBase() {
  override suspend fun getReport(request: GetReportRequest): Report {
    return kingdomRelationalDatabase.getReport(ExternalId(request.externalReportId))
  }

  override suspend fun createNextReport(request: CreateNextReportRequest): Report {
    return kingdomRelationalDatabase.createNextReport(
      ExternalId(request.externalScheduleId),
      request.combinedPublicKeyResourceId
    )
  }

  override fun streamReports(request: StreamReportsRequest): Flow<Report> {
    return kingdomRelationalDatabase.streamReports(
      streamReportsFilter(
        externalAdvertiserIds = request.filter.externalAdvertiserIdsList.map(::ExternalId),
        externalReportConfigIds = request.filter.externalReportConfigIdsList.map(::ExternalId),
        externalScheduleIds = request.filter.externalScheduleIdsList.map(::ExternalId),
        states = request.filter.statesList,
        updatedAfter = request.filter.updatedAfter.toInstant()
      ),
      request.limit
    )
  }

  override fun streamReadyReports(request: StreamReadyReportsRequest): Flow<Report> {
    return kingdomRelationalDatabase.streamReadyReports(request.limit)
  }

  override suspend fun updateReportState(request: UpdateReportStateRequest): Report {
    return kingdomRelationalDatabase.updateReportState(
      ExternalId(request.externalReportId),
      request.state
    )
  }

  override suspend fun associateRequisition(
    request: AssociateRequisitionRequest
  ): AssociateRequisitionResponse {
    kingdomRelationalDatabase.associateRequisitionToReport(
      externalRequisitionId = ExternalId(request.externalRequisitionId),
      externalReportId = ExternalId(request.externalReportId)
    )
    return AssociateRequisitionResponse.getDefaultInstance()
  }

  override suspend fun confirmDuchyReadiness(
    request: ConfirmDuchyReadinessRequest
  ): Report {
    return kingdomRelationalDatabase.confirmDuchyReadiness(
      ExternalId(request.externalReportId),
      request.duchyId,
      request.externalRequisitionIdsList.map(::ExternalId).toSet()
    )
  }

  override suspend fun finishReport(request: FinishReportRequest): Report {
    return kingdomRelationalDatabase.finishReport(
      ExternalId(request.externalReportId),
      request.result
    )
  }
}
