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

package org.wfanet.measurement.reporting.service.internal.testing.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.GetImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec.MediaType
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersRequest
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.getImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersPageToken
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersRequest
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersResponse
import org.wfanet.measurement.reporting.service.internal.Errors

@RunWith(JUnit4::class)
abstract class ImpressionQualificationFiltersServiceTest<
  T : ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineImplBase
> {

  /** Instance of the service under test. */
  private lateinit var service: T

  /** Constructs the service being tested. */
  protected abstract fun newService(): T

  @Before
  fun initService() {
    service = newService()
  }

  @Test
  fun `getImpressionQualificationFilter returns ImpressionQualificationFilter`() = runBlocking {
    val result =
      service.getImpressionQualificationFilter(
        getImpressionQualificationFilterRequest { externalImpressionQualificationFilterId = "ami" }
      )

    assertThat(result).isEqualTo(AMI_IQF)
  }

  @Test
  fun `getImpressionQualificationFilter throws INVALID_ARGUMENT when ImpressionQualificationFilter resource id is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getImpressionQualificationFilter(
            GetImpressionQualificationFilterRequest.getDefaultInstance()
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "external_impression_qualification_filter_id"
          }
        )
    }

  @Test
  fun `getImpressionQualificationFilter throws NOT_FOUND when ImpressionQualificationFilter not found`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getImpressionQualificationFilter(
            getImpressionQualificationFilterRequest {
              externalImpressionQualificationFilterId = "abc"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND.name
            metadata[Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER_ID.key] = "abc"
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter returns ImpressionQualificationFilters when no page size and page token is specified`() =
    runBlocking {
      val result =
        service.listImpressionQualificationFilters(
          ListImpressionQualificationFiltersRequest.getDefaultInstance()
        )

      assertThat(result)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += AMI_IQF
            impressionQualificationFilters += MRC_IQF
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter returns AMI IQF when page size is one and no page token is specified`() =
    runBlocking {
      val result =
        service.listImpressionQualificationFilters(
          listImpressionQualificationFiltersRequest { pageSize = 1 }
        )

      assertThat(result)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += AMI_IQF
            nextPageToken = listImpressionQualificationFiltersPageToken {
              after =
                ListImpressionQualificationFiltersPageTokenKt.after {
                  externalImpressionQualificationFilterId =
                    AMI_IQF.externalImpressionQualificationFilterId
                }
            }
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter returns MRC IQF and no next page token when page size is one and page token is AMI`() =
    runBlocking {
      val result =
        service.listImpressionQualificationFilters(
          listImpressionQualificationFiltersRequest {
            pageSize = 1
            pageToken = listImpressionQualificationFiltersPageToken {
              after =
                ListImpressionQualificationFiltersPageTokenKt.after {
                  externalImpressionQualificationFilterId =
                    AMI_IQF.externalImpressionQualificationFilterId
                }
            }
          }
        )

      assertThat(result)
        .isEqualTo(
          listImpressionQualificationFiltersResponse { impressionQualificationFilters += MRC_IQF }
        )
    }

  @Test
  fun `listImpressionQualificationFilter throws INVALID_ARGUMENT when pageSize is negative `() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listImpressionQualificationFilters(
            listImpressionQualificationFiltersRequest { pageSize = -1 }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "page_size"
          }
        )
    }

  companion object {
    private val AMI_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters += eventFilter {
          terms += eventTemplateField {
            path = "banner_ad.viewable"
            value = EventTemplateFieldKt.fieldValue { boolValue = false }
          }
        }
      }
    }

    private val MRC_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "mrc"
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters += eventFilter {
          terms += eventTemplateField {
            path = "banner_ad.viewable"
            value = EventTemplateFieldKt.fieldValue { boolValue = true }
          }
        }
      }
    }
  }
}
