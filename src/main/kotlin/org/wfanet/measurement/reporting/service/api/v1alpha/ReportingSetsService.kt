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

import io.grpc.Status
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2.alpha.ListReportingSetsPageToken
import org.wfanet.measurement.api.v2.alpha.ListReportingSetsPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2.alpha.copy
import org.wfanet.measurement.api.v2.alpha.listReportingSetsPageToken
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.reporting.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSet.EventGroupKey as InternalEventGroupKey
import org.wfanet.measurement.internal.reporting.ReportingSetKt.eventGroupKey as internalEventGroupKey
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequestKt.filter
import org.wfanet.measurement.internal.reporting.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.streamReportingSetsRequest
import org.wfanet.measurement.reporting.v1alpha.CreateReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.ListReportingSetsRequest
import org.wfanet.measurement.reporting.v1alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v1alpha.ReportingSet
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v1alpha.listReportingSetsResponse
import org.wfanet.measurement.reporting.v1alpha.reportingSet

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ReportingSetsService(private val internalReportingSetsStub: ReportingSetsCoroutineStub) :
  ReportingSetsCoroutineImplBase() {
  override suspend fun createReportingSet(request: CreateReportingSetRequest): ReportingSet {
    val parentKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a ReportingSet for another MeasurementConsumer."
          }
        }
      }
    }

    grpcRequire(request.hasReportingSet()) { "ReportingSet is not specified." }

    grpcRequire(request.reportingSet.eventGroupsList.isNotEmpty()) {
      "EventGroups in ReportingSet cannot be empty."
    }

    return internalReportingSetsStub
      .createReportingSet(request.reportingSet.toInternal(parentKey))
      .toReportingSet()
  }

  override suspend fun listReportingSets(
    request: ListReportingSetsRequest
  ): ListReportingSetsResponse {
    val listReportingSetsPageToken = request.toListReportingSetsPageToken()

    // Based on AIP-132#Errors
    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (request.parent != principal.resourceKey.toName()) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list ReportingSets belonging to other MeasurementConsumers."
          }
        }
      }
    }

    val results: List<InternalReportingSet> =
      internalReportingSetsStub
        .streamReportingSets(listReportingSetsPageToken.toStreamReportingSetsRequest())
        .toList()

    if (results.isEmpty()) {
      return ListReportingSetsResponse.getDefaultInstance()
    }

    return listReportingSetsResponse {
      reportingSets +=
        results
          .subList(0, min(results.size, listReportingSetsPageToken.pageSize))
          .map(InternalReportingSet::toReportingSet)

      if (results.size > listReportingSetsPageToken.pageSize) {
        val pageToken =
          listReportingSetsPageToken.copy {
            lastReportingSet = previousPageEnd {
              measurementConsumerReferenceId =
                results[results.lastIndex - 1].measurementConsumerReferenceId
              externalReportingSetId = results[results.lastIndex - 1].externalReportingSetId
            }
          }
        nextPageToken = pageToken.toByteString().base64UrlEncode()
      }
    }
  }
}

/**
 * Converts an internal [ListReportingSetsPageToken] to an internal [StreamReportingSetsRequest].
 */
private fun ListReportingSetsPageToken.toStreamReportingSetsRequest(): StreamReportingSetsRequest {
  val source = this
  return streamReportingSetsRequest {
    // get 1 more than the actual page size for deciding whether or not to set page token
    limit = pageSize + 1
    filter = filter {
      measurementConsumerReferenceId = source.measurementConsumerReferenceId
      externalReportingSetIdAfter = source.lastReportingSet.externalReportingSetId
    }
  }
}

/** Converts a public [ListReportingSetsRequest] to an internal [ListReportingSetsPageToken]. */
private fun ListReportingSetsRequest.toListReportingSetsPageToken(): ListReportingSetsPageToken {
  grpcRequire(pageSize >= 0) { "Page size cannot be less than 0" }

  val source = this
  val parentKey: MeasurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(parent)) {
      "Parent is either unspecified or invalid."
    }
  val measurementConsumerReferenceId = parentKey.measurementConsumerId

  return if (pageToken.isNotBlank()) {
    ListReportingSetsPageToken.parseFrom(pageToken.base64UrlDecode()).copy {
      grpcRequire(this.measurementConsumerReferenceId == measurementConsumerReferenceId) {
        "Arguments must be kept the same when using a page token"
      }

      if (
        source.pageSize != 0 && source.pageSize >= MIN_PAGE_SIZE && source.pageSize <= MAX_PAGE_SIZE
      ) {
        pageSize = source.pageSize
      }
    }
  } else {
    listReportingSetsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
    }
  }
}

/** Converts a public [ReportingSet] to an internal [InternalReportingSet]. */
private fun ReportingSet.toInternal(
  measurementConsumerKey: MeasurementConsumerKey,
): InternalReportingSet {
  val source = this

  return internalReportingSet {
    measurementConsumerReferenceId = measurementConsumerKey.measurementConsumerId

    for (eventGroup: String in source.eventGroupsList) {
      eventGroupKeys +=
        grpcRequireNotNull(EventGroupKey.fromName(eventGroup)) {
            "EventGroup is either unspecified or invalid."
          }
          .toInternal(measurementConsumerKey.measurementConsumerId)
    }
    filter = source.filter
    displayName = source.displayName
  }
}

/** Converts a public [EventGroupKey] to an internal [InternalEventGroupKey] */
private fun EventGroupKey.toInternal(
  measurementConsumerReferenceId: String,
): InternalEventGroupKey {
  val source = this
  return internalEventGroupKey {
    dataProviderReferenceId = source.dataProviderReferenceId
    eventGroupReferenceId = source.eventGroupReferenceId
    this.measurementConsumerReferenceId = measurementConsumerReferenceId
  }
}

/** Converts an internal [InternalReportingSet] to a public [ReportingSet]. */
private fun InternalReportingSet.toReportingSet(): ReportingSet {
  val source = this
  return reportingSet {
    name =
      ReportingSetKey(
          measurementConsumerId = source.measurementConsumerReferenceId,
          reportingSetId = externalIdToApiId(source.externalReportingSetId)
        )
        .toName()
    eventGroups.addAll(
      eventGroupKeysList.map {
        EventGroupKey(
            measurementConsumerReferenceId = it.measurementConsumerReferenceId,
            dataProviderReferenceId = it.dataProviderReferenceId,
            eventGroupReferenceId = it.eventGroupReferenceId
          )
          .toName()
      }
    )
    filter = source.filter
    displayName = source.displayName
  }
}
