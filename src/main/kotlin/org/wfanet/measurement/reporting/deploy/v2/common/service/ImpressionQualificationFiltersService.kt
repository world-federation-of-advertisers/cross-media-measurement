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

package org.wfanet.measurement.reporting.deploy.v2.common.service

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
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
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping.Companion.toImpressionQualificationFilter
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.service.internal.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.internal.RequiredFieldNotSetException

/**
 * Internal impression qualification filter service.
 *
 * This implementation is based on a config file.
 */
class ImpressionQualificationFiltersService(
  private val impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ImpressionQualificationFiltersCoroutineImplBase(coroutineContext) {
  override suspend fun getImpressionQualificationFilter(
    request: GetImpressionQualificationFilterRequest
  ): ImpressionQualificationFilter {
    if (request.externalImpressionQualificationFilterId.isEmpty()) {
      throw RequiredFieldNotSetException("external_impression_qualification_filter_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val mappingImpressionQualificationFilter: ImpressionQualificationFilter =
      impressionQualificationFilterMapping
        .getImpressionQualificationByExternalId(request.externalImpressionQualificationFilterId)
        ?.toImpressionQualificationFilter()
        ?: throw ImpressionQualificationFilterNotFoundException(
            request.externalImpressionQualificationFilterId
          )
          .asStatusRuntimeException(Status.Code.NOT_FOUND)

    return mappingImpressionQualificationFilter
  }

  override suspend fun listImpressionQualificationFilters(
    request: ListImpressionQualificationFiltersRequest
  ): ListImpressionQualificationFiltersResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val pageSize: Int =
      if (request.pageSize == 0) DEFAULT_PAGE_SIZE else request.pageSize.coerceAtMost(MAX_PAGE_SIZE)

    val mappingImpressionQualificationFilters: List<ImpressionQualificationFilter> =
      impressionQualificationFilterMapping.impressionQualificationFilters.map {
        it.toImpressionQualificationFilter()
      }

    val fromIndex: Int =
      if (request.hasPageToken()) {
        val searchResult: Int =
          mappingImpressionQualificationFilters.binarySearchBy(
            request.pageToken.after.externalImpressionQualificationFilterId
          ) {
            it.externalImpressionQualificationFilterId
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
      if (toIndex < mappingImpressionQualificationFilters.size) {
        nextPageToken = listImpressionQualificationFiltersPageToken {
          after =
            ListImpressionQualificationFiltersPageTokenKt.after {
              externalImpressionQualificationFilterId =
                impressionQualificationFilters.last().externalImpressionQualificationFilterId
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
