/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.mcp.grpc

import org.wfanet.measurement.common.grpc.BearerTokenCallCredentials
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub

/**
 * Wrapper around gRPC stubs for the Reporting v2alpha public API.
 *
 * Per-request bearer tokens are attached via [BearerTokenCallCredentials].
 */
class ReportingPublicApiClient(
  val basicReports: BasicReportsCoroutineStub,
  val eventGroups: EventGroupsCoroutineStub,
  val reportingSets: ReportingSetsCoroutineStub,
  val impressionQualificationFilters: ImpressionQualificationFiltersCoroutineStub,
) {
  /** Returns stubs with the given bearer token attached as call credentials. */
  fun withBearerToken(bearerToken: String): AuthenticatedStubs {
    // requirePrivacy = false: the caller's token is forwarded over the channel, whose transport
    // privacy is established at construction (mTLS in production, in-process for tests).
    val credentials = BearerTokenCallCredentials(bearerToken, requirePrivacy = false)
    return AuthenticatedStubs(
      basicReports = basicReports.withCallCredentials(credentials),
      eventGroups = eventGroups.withCallCredentials(credentials),
      reportingSets = reportingSets.withCallCredentials(credentials),
      impressionQualificationFilters =
        impressionQualificationFilters.withCallCredentials(credentials),
    )
  }

  data class AuthenticatedStubs(
    val basicReports: BasicReportsCoroutineStub,
    val eventGroups: EventGroupsCoroutineStub,
    val reportingSets: ReportingSetsCoroutineStub,
    val impressionQualificationFilters: ImpressionQualificationFiltersCoroutineStub,
  )
}
