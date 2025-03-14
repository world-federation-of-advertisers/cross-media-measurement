// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import io.grpc.Status
import kotlin.math.abs
import org.wfanet.measurement.internal.reporting.v2.GetImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersRequest
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersResponse
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersPageToken
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersResponse
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.service.internal.RequiredFieldNotSetException

class SpannerImpressionQualificationFiltersService(
  private val impressionQualificationFilterMapping: ImpressionQualificationFilterMapping
) : ImpressionQualificationFiltersCoroutineImplBase() {
  override suspend fun getImpressionQualificationFilter(
    request: GetImpressionQualificationFilterRequest
  ): ImpressionQualificationFilter {
    if (request.impressionQualificationFilterResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("impression_qualification_filter_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val mappingImpressionQualificationFilter: ImpressionQualificationFilter? =
      try {
        impressionQualificationFilterMapping.getImpressionQualificationFilter(
          request.impressionQualificationFilterResourceId
        )
      } catch (e: ImpressionQualificationFilterNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    return mappingImpressionQualificationFilter!!
  }

  override suspend fun listImpressionQualificationFilters(
    request: ListImpressionQualificationFiltersRequest
  ): ListImpressionQualificationFiltersResponse {
    if (request.pageSize < 0) {
      throw Status.INVALID_ARGUMENT.withDescription("page_size must be non-negative")
        .asRuntimeException()
    }
    val pageSize: Int =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val mappingImpressionQualificationFilters: List<ImpressionQualificationFilter> =
      impressionQualificationFilterMapping.impressionQualificationFilters

    val fromIndex: Int =
      if (request.hasPageToken()) {
        val searchResult: Int =
          mappingImpressionQualificationFilters.binarySearchBy(
            request.pageToken.after.impressionQualificationFilterResourceId
          ) {
            it.impressionQualificationFilterResourceId
          }
        abs(searchResult + 1)
      } else {
        0
      }
    val toIndex: Int =
      (fromIndex + pageSize).coerceAtMost(mappingImpressionQualificationFilters.size)
    val impressionQualificationFilters: List<ImpressionQualificationFilter> =
      mappingImpressionQualificationFilters.subList(fromIndex, toIndex)

    return listImpressionQualificationFiltersResponse {
      this.impressionQualificationFilters += impressionQualificationFilters
      if (toIndex < impressionQualificationFilters.size) {
        nextPageToken = listImpressionQualificationFiltersPageToken {
          after =
            ListImpressionQualificationFiltersPageTokenKt.after {
              impressionQualificationFilterResourceId =
                impressionQualificationFilters.last().impressionQualificationFilterResourceId
            }
        }
      }
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
