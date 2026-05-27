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

package org.wfanet.measurement.reporting.mcp.grpc

import io.grpc.Channel
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub

/**
 * Wrapper around gRPC stubs for the Reporting v2alpha public API.
 *
 * Each stub connects to the existing public API server over mTLS. Per-request bearer tokens are
 * attached via [BearerPassthroughCallCredentials].
 */
class ReportingPublicApiClient(channel: Channel) {
  val basicReports = BasicReportsCoroutineStub(channel)
  val eventGroups = EventGroupsCoroutineStub(channel)
  val reportingSets = ReportingSetsCoroutineStub(channel)
  val impressionQualificationFilters = ImpressionQualificationFiltersCoroutineStub(channel)

  /** Returns stubs with the given bearer token attached as call credentials. */
  fun withBearerToken(bearerToken: String): AuthenticatedStubs {
    val credentials = BearerPassthroughCallCredentials(bearerToken)
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
