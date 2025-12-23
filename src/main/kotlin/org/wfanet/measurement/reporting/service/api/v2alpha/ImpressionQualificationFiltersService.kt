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
package org.wfanet.measurement.reporting.service.api.v2alpha

import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilter as InternalImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersPageToken as InternalListImpressionQualificationFiltersPageToken
import org.wfanet.measurement.internal.reporting.v2.getImpressionQualificationFilterRequest as internalGetImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersRequest as internalListImpressionQualificationFiltersRequest
import org.wfanet.measurement.reporting.service.api.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.internal.Errors as InternalErrors
import org.wfanet.measurement.reporting.v2alpha.GetImpressionQualificationFilterRequest
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ListImpressionQualificationFiltersRequest
import org.wfanet.measurement.reporting.v2alpha.ListImpressionQualificationFiltersResponse
import org.wfanet.measurement.reporting.v2alpha.listImpressionQualificationFiltersResponse

class ImpressionQualificationFiltersService(
  private val internalImpressionQualificationFiltersStub:
    ImpressionQualificationFiltersCoroutineStub,
  private val authorization: Authorization,
) : ImpressionQualificationFiltersCoroutineImplBase() {
  override suspend fun getImpressionQualificationFilter(
    request: GetImpressionQualificationFilterRequest
  ): ImpressionQualificationFilter {
    authorization.check(listOf(request.name, Authorization.ROOT_RESOURCE_NAME), Permission.GET)

    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val impressionQualificationFilterKey =
      ImpressionQualificationFilterKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalImpressionQualificationFilter: InternalImpressionQualificationFilter =
      try {
        internalImpressionQualificationFiltersStub.getImpressionQualificationFilter(
          internalGetImpressionQualificationFilterRequest {
            externalImpressionQualificationFilterId =
              impressionQualificationFilterKey.impressionQualificationFilterId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND ->
            ImpressionQualificationFilterNotFoundException(request.name, e)
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          InternalErrors.Reason.BASIC_REPORT_NOT_FOUND,
          InternalErrors.Reason.METRIC_NOT_FOUND,
          InternalErrors.Reason.INVALID_METRIC_STATE_TRANSITION,
          InternalErrors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND,
          InternalErrors.Reason.BASIC_REPORT_ALREADY_EXISTS,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.REPORT_RESULT_NOT_FOUND,
          InternalErrors.Reason.REPORTING_SET_RESULT_NOT_FOUND,
          InternalErrors.Reason.REPORTING_WINDOW_RESULT_NOT_FOUND,
          InternalErrors.Reason.BASIC_REPORT_STATE_INVALID,
          InternalErrors.Reason.INVALID_BASIC_REPORT,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalImpressionQualificationFilter.toImpressionQualificationFilter()
  }

  override suspend fun listImpressionQualificationFilters(
    request: ListImpressionQualificationFiltersRequest
  ): ListImpressionQualificationFiltersResponse {
    authorization.check(Authorization.ROOT_RESOURCE_NAME, Permission.LIST)

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalPageToken: InternalListImpressionQualificationFiltersPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          InternalListImpressionQualificationFiltersPageToken.parseFrom(
            request.pageToken.base64UrlDecode()
          )
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalListImpressionQualificationFiltersResponse =
      try {
        internalImpressionQualificationFiltersStub.listImpressionQualificationFilters(
          internalListImpressionQualificationFiltersRequest {
            pageSize = request.pageSize
            if (internalPageToken != null) {
              pageToken = internalPageToken
            }
          }
        )
      } catch (e: StatusException) {
        throw Status.INTERNAL.withCause(e).asRuntimeException()
      }

    return listImpressionQualificationFiltersResponse {
      impressionQualificationFilters +=
        internalListImpressionQualificationFiltersResponse.impressionQualificationFiltersList.map {
          it.toImpressionQualificationFilter()
        }
      if (internalListImpressionQualificationFiltersResponse.hasNextPageToken()) {
        nextPageToken =
          internalListImpressionQualificationFiltersResponse.nextPageToken
            .toByteString()
            .base64UrlEncode()
      }
    }
  }

  object Permission {
    private const val TYPE = "reporting.impressionQualificationFilters"
    const val GET = "$TYPE.get"
    const val LIST = "$TYPE.list"
  }
}
