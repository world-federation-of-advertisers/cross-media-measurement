// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.postgres

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.BatchGetReportingSetRequest
import org.wfanet.measurement.internal.reporting.GetReportingSetRequest
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.postgres.SerializableErrors.withSerializableErrorRetries
import org.wfanet.measurement.reporting.deploy.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.deploy.postgres.writers.CreateReportingSet
import org.wfanet.measurement.reporting.service.internal.ReportingSetAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

class PostgresReportingSetsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : ReportingSetsCoroutineImplBase() {
  override suspend fun createReportingSet(request: ReportingSet): ReportingSet {
    return try {
      CreateReportingSet(request).execute(client, idGenerator)
    } catch (e: ReportingSetAlreadyExistsException) {
      e.throwStatusRuntimeException(Status.ALREADY_EXISTS) {
        "IDs generated for Reporting Set already exist"
      }
    }
  }

  override suspend fun getReportingSet(request: GetReportingSetRequest): ReportingSet {
    return try {
      SerializableErrors.retrying {
        ReportingSetReader()
          .readReportingSetByExternalId(
            client.singleUse(),
            request.measurementConsumerReferenceId,
            ExternalId(request.externalReportingSetId)
          )
          .reportingSet
      }
    } catch (e: ReportingSetNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Reporting Set not found" }
    }
  }

  override fun streamReportingSets(request: StreamReportingSetsRequest): Flow<ReportingSet> {
    return ReportingSetReader()
      .listReportingSets(client, request.filter, request.limit)
      .map { result -> result.reportingSet }
      .withSerializableErrorRetries()
  }

  override fun batchGetReportingSet(request: BatchGetReportingSetRequest): Flow<ReportingSet> {
    return ReportingSetReader()
      .getReportingSetsByExternalIds(
        client,
        request.measurementConsumerReferenceId,
        request.externalReportingSetIdsList
      )
      .map { result -> result.reportingSet }
      .withSerializableErrorRetries()
  }
}
