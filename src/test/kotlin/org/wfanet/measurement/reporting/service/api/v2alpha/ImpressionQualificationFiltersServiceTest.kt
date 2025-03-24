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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt as InternalEventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec.MediaType as InternalMediaType
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt as InternalImpressionQualificationFiltersGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersPageTokenKt as InternalListImpressionQualificationFiltersPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.eventFilter as internalEventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField as internalEventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilter as internalImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec as internalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersPageToken as internalListImpressionQualificationFiltersPageToken
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersResponse as internalListImpressionQualificationFiltersResponse
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterNotFoundException
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.GetImpressionQualificationFilterRequest
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ListImpressionQualificationFiltersRequest
import org.wfanet.measurement.reporting.v2alpha.ListImpressionQualificationFiltersResponse
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getImpressionQualificationFilterRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.listImpressionQualificationFiltersRequest
import org.wfanet.measurement.reporting.v2alpha.listImpressionQualificationFiltersResponse

@RunWith(JUnit4::class)
class ImpressionQualificationFiltersServiceTest {
  private val internalServiceMock =
    mockService<
      InternalImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineImplBase
    >()

  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: ImpressionQualificationFiltersService

  @Before
  fun initService() {
    service =
      ImpressionQualificationFiltersService(
        InternalImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub(
          grpcTestServer.channel
        )
      )
  }

  @Test
  fun `getImpressionQualificationFilter returns ImpressionQualificationFilter`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { getImpressionQualificationFilter(any()) } doReturn INTERNAL_AMI_IQF
    }

    val request = getImpressionQualificationFilterRequest {
      name = "impressionQualificationFilters/ami"
    }
    val response: ImpressionQualificationFilter = service.getImpressionQualificationFilter(request)

    assertThat(response).isEqualTo(PUBLIC_AMI_IQF)
  }

  @Test
  fun `getImpressionQualificationFilter throws IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND from backend`() =
    runBlocking {
      internalServiceMock.stub {
        onBlocking { getImpressionQualificationFilter(any()) } doThrow
          ImpressionQualificationFilterNotFoundException("impressionQualificationFilters/abc")
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getImpressionQualificationFilter(
            getImpressionQualificationFilterRequest { name = "impressionQualificationFilters/abc" }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND.name
            metadata[Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER.key] =
              "impressionQualificationFilters/abc"
          }
        )
    }

  @Test
  fun `getImpressionQualificationFilter throws INVALID_ARGUMENT if name not set`() = runBlocking {
    val request = GetImpressionQualificationFilterRequest.getDefaultInstance()
    val exception =
      assertFailsWith<StatusRuntimeException> { service.getImpressionQualificationFilter(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `getImpressionQualificationFilter throws INVALID_ARGUMENT if name is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getImpressionQualificationFilter(
            getImpressionQualificationFilterRequest { name = "123" }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "name"
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter without page size and page token returns ImpressionQualificationFilters`() =
    runBlocking {
      internalServiceMock.stub {
        onBlocking { listImpressionQualificationFilters(any()) } doReturn
          internalListImpressionQualificationFiltersResponse {
            impressionQualificationFilters += INTERNAL_MRC_IQF
            impressionQualificationFilters += INTERNAL_AMI_IQF
          }
      }

      val response: ListImpressionQualificationFiltersResponse =
        service.listImpressionQualificationFilters(
          ListImpressionQualificationFiltersRequest.getDefaultInstance()
        )

      assertThat(response)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += PUBLIC_MRC_IQF
            impressionQualificationFilters += PUBLIC_AMI_IQF
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter with page size returns ImpressionQualificationFilter`() =
    runBlocking {
      val internalListImpressionQualificationFiltersResponse =
        internalListImpressionQualificationFiltersResponse {
          impressionQualificationFilters += INTERNAL_AMI_IQF
          nextPageToken = internalListImpressionQualificationFiltersPageToken {
            after =
              InternalListImpressionQualificationFiltersPageTokenKt.after {
                externalImpressionQualificationFilterId =
                  INTERNAL_AMI_IQF.externalImpressionQualificationFilterId
              }
          }
        }
      internalServiceMock.stub {
        onBlocking { listImpressionQualificationFilters(any()) } doReturn
          internalListImpressionQualificationFiltersResponse
      }

      val response: ListImpressionQualificationFiltersResponse =
        service.listImpressionQualificationFilters(
          listImpressionQualificationFiltersRequest { pageSize = 1 }
        )

      assertThat(response)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += PUBLIC_AMI_IQF
            nextPageToken =
              internalListImpressionQualificationFiltersResponse.nextPageToken.after
                .toByteString()
                .base64UrlEncode()
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter with page token returns ImpressionQualificationFilter`() =
    runBlocking {
      val internalListImpressionQualificationFiltersResponse =
        internalListImpressionQualificationFiltersResponse {
          impressionQualificationFilters += INTERNAL_MRC_IQF
        }

      internalServiceMock.stub {
        onBlocking { listImpressionQualificationFilters(any()) } doReturn
          internalListImpressionQualificationFiltersResponse
      }

      val internalPageToken = internalListImpressionQualificationFiltersPageToken {
        after =
          InternalListImpressionQualificationFiltersPageTokenKt.after {
            externalImpressionQualificationFilterId =
              INTERNAL_MRC_IQF.externalImpressionQualificationFilterId
          }
      }

      val response: ListImpressionQualificationFiltersResponse =
        service.listImpressionQualificationFilters(
          listImpressionQualificationFiltersRequest {
            pageToken = internalPageToken.toByteString().base64UrlEncode()
          }
        )

      assertThat(response)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += PUBLIC_MRC_IQF
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter throws INVALID_ARGUMENT if page size is negative`() =
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

  @Test
  fun `listImpressionQualificationFilter throws INVALID_ARGUMENT if page token is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listImpressionQualificationFilters(
            listImpressionQualificationFiltersRequest { pageToken = "mayhem" }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "page_token"
          }
        )
    }

  companion object {
    private val INTERNAL_AMI_IQF = internalImpressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      filterSpecs += internalImpressionQualificationFilterSpec {
        mediaType = InternalMediaType.VIDEO
      }
      filterSpecs += internalImpressionQualificationFilterSpec {
        mediaType = InternalMediaType.DISPLAY
      }
      filterSpecs += internalImpressionQualificationFilterSpec {
        mediaType = InternalMediaType.OTHER
      }
    }

    private val PUBLIC_AMI_IQF = impressionQualificationFilter {
      name = "impressionQualificationFilters/ami"
      displayName = "ami"
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.VIDEO }
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.DISPLAY }
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.OTHER }
    }

    private val INTERNAL_MRC_IQF = internalImpressionQualificationFilter {
      externalImpressionQualificationFilterId = "mrc"
      filterSpecs += internalImpressionQualificationFilterSpec {
        mediaType = InternalMediaType.DISPLAY
        filters += internalEventFilter {
          terms += internalEventTemplateField {
            path = "banner_ad.viewable_fraction_1_second"
            value = InternalEventTemplateFieldKt.fieldValue { floatValue = 0.5F }
          }
        }
      }
      filterSpecs += internalImpressionQualificationFilterSpec {
        mediaType = InternalMediaType.VIDEO
        filters += internalEventFilter {
          terms += internalEventTemplateField {
            path = "video.viewable_fraction_1_second"
            value = InternalEventTemplateFieldKt.fieldValue { floatValue = 1.0F }
          }
        }
      }
    }

    private val PUBLIC_MRC_IQF = impressionQualificationFilter {
      name = "impressionQualificationFilters/mrc"
      displayName = "mrc"
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters += eventFilter {
          terms += eventTemplateField {
            path = "banner_ad.viewable_fraction_1_second"
            value = EventTemplateFieldKt.fieldValue { floatValue = 0.5F }
          }
        }
      }
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.VIDEO
        filters += eventFilter {
          terms += eventTemplateField {
            path = "video.viewable_fraction_1_second"
            value = EventTemplateFieldKt.fieldValue { floatValue = 1.0F }
          }
        }
      }
    }
  }
}
