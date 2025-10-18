/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.internal.reporting.v2.CreateReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt
import org.wfanet.measurement.reporting.service.internal.RequiredFieldNotSetException

/** Spanner implementation of ReportResults gRPC service. */
class SpannerReportResultsService(coroutineContext: CoroutineContext = EmptyCoroutineContext) :
  ReportResultsGrpcKt.ReportResultsCoroutineImplBase(coroutineContext) {
  override suspend fun createReportResult(request: CreateReportResultRequest): ReportResult {
    if (!request.hasReportResult()) {
      throw RequiredFieldNotSetException("report_result")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    return super.createReportResult(request)
  }
}
