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

package org.wfanet.measurement.reporting.service.api.v1alpha

import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.reportingSet as internalReportingSet
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.reporting.ReportingSetKt.eventGroupKey
import org.wfanet.measurement.reporting.v1alpha.CreateReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.ReportingSet
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.reportingSet

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 100

class ReportingSetsService(private val internalReportingSetsStub: ReportingSetsCoroutineStub) :
  ReportingSetsCoroutineImplBase() {
  override suspend fun createReportingSet(request: CreateReportingSetRequest): ReportingSet {
    val parentKey = grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }

    val principal = principalFromCurrentContext

    grpcRequire(request.parent == principal.resourceKey.toName()) {
      "Cannot create a ReportingSet for another MeasurementConsumer."
    }

    grpcRequire(request.hasReportingSet()) {
      "ReportingSet is not specified."
    }

    grpcRequire(request.reportingSet.eventGroupsList.isNotEmpty()) {
      "EventGroups in ReportingSet cannot be empty."
    }

    return internalReportingSetsStub.createReportingSet(
      request.reportingSet.toInternal(parentKey)
    ).toReportingSet()
  }
}

/** Converts a public [ReportingSet] to an internal [InternalReportingSet]. */
private fun ReportingSet.toInternal(
  measurementConsumerKey: MeasurementConsumerKey,
): InternalReportingSet {
  return internalReportingSet {
    measurementConsumerReferenceId = measurementConsumerKey.measurementConsumerId
    eventGroupKeys.addAll(
      this@toInternal.eventGroupsList.map {
        eventGroupKey {
          measurementConsumerReferenceId = measurementConsumerKey.measurementConsumerId
          val eventGroupKey = grpcRequireNotNull(EventGroupKey.fromName(it)) {
            "EventGroup is either unspecified or invalid."
          }
          dataProviderReferenceId = eventGroupKey.dataProviderId
          eventGroupReferenceId = eventGroupKey.eventGroupId
        }
      }
    )
    filter = this@toInternal.filter
    displayName = this@toInternal.displayName
  }
}

/** Converts an internal [InternalReportingSet] to a public [ReportingSet]. */
private fun InternalReportingSet.toReportingSet(): ReportingSet {
  return reportingSet {
    name = ReportingSetKey(
      measurementConsumerId = this@toReportingSet.measurementConsumerReferenceId,
      reportingSetId = externalIdToApiId(this@toReportingSet.externalReportingSetId)
    ).toName()
    eventGroups.addAll(
      eventGroupKeysList.map {
        it.eventGroupReferenceId
      }
    )
    filter = this@toReportingSet.filter
    displayName = this@toReportingSet.displayName
  }
}
