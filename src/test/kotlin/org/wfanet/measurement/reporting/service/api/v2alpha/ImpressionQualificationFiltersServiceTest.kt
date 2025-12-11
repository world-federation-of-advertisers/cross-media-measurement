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
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt as InternalEventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec.MediaType as InternalMediaType
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt as InternalImpressionQualificationFiltersGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersPageTokenKt as InternalListImpressionQualificationFiltersPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.eventFilter as internalEventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField as internalEventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilter as internalImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec as internalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersPageToken as internalListImpressionQualificationFiltersPageToken
import org.wfanet.measurement.reporting.deploy.v2.common.service.ImpressionQualificationFiltersService as InternalImpressionQualificationFiltersService
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
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
  private lateinit var internalService: InternalImpressionQualificationFiltersService
  private lateinit var service: ImpressionQualificationFiltersService

  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    onBlocking { checkPermissions(any()) } doReturn CheckPermissionsResponse.getDefaultInstance()

    // Grant all permissions to PRINCIPAL.
    onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doAnswer
      { invocation ->
        val request: CheckPermissionsRequest = invocation.getArgument(0)
        checkPermissionsResponse { permissions += request.permissionsList }
      }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    internalService =
      InternalImpressionQualificationFiltersService(IMPRESSION_QUALIFICATION_FILTER_MAPPING)
    addService(internalService)
    addService(permissionsServiceMock)
  }

  @Before
  fun initService() {
    service =
      ImpressionQualificationFiltersService(
        InternalImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub(
          grpcTestServerRule.channel
        ),
        Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel)),
      )
  }

  @Test
  fun `getImpressionQualificationFilter returns ImpressionQualificationFilter`() = runBlocking {
    val request = getImpressionQualificationFilterRequest {
      name = "impressionQualificationFilters/ami"
    }
    val response: ImpressionQualificationFilter =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.getImpressionQualificationFilter(request)
      }

    assertThat(response).isEqualTo(PUBLIC_AMI_IQF)
  }

  @Test
  fun `getImpressionQualificationFilter throws PERMISSION_DENIED when principal does not have required permissions`() =
    runBlocking {
      val request = getImpressionQualificationFilterRequest {
        name = "impressionQualificationFilters/ami"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
            service.getImpressionQualificationFilter(request)
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    }

  @Test
  fun `getImpressionQualificationFilter throws IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND from backend`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            service.getImpressionQualificationFilter(
              getImpressionQualificationFilterRequest {
                name = "impressionQualificationFilters/abc"
              }
            )
          }
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
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.getImpressionQualificationFilter(request)
        }
      }

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
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            service.getImpressionQualificationFilter(
              getImpressionQualificationFilterRequest { name = "123" }
            )
          }
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
      val response: ListImpressionQualificationFiltersResponse =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.listImpressionQualificationFilters(
            ListImpressionQualificationFiltersRequest.getDefaultInstance()
          )
        }

      assertThat(response)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += PUBLIC_AMI_IQF
            impressionQualificationFilters += PUBLIC_MRC_IQF
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter with page size returns ImpressionQualificationFilter`() =
    runBlocking {
      val internalPageToken = internalListImpressionQualificationFiltersPageToken {
        after =
          InternalListImpressionQualificationFiltersPageTokenKt.after {
            externalImpressionQualificationFilterId =
              INTERNAL_AMI_IQF.externalImpressionQualificationFilterId
          }
      }

      val response: ListImpressionQualificationFiltersResponse =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.listImpressionQualificationFilters(
            listImpressionQualificationFiltersRequest { pageSize = 1 }
          )
        }

      assertThat(response)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += PUBLIC_AMI_IQF
            nextPageToken = internalPageToken.toByteString().base64UrlEncode()
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter with page token returns ImpressionQualificationFilter`() =
    runBlocking {
      val internalPageToken = internalListImpressionQualificationFiltersPageToken {
        after =
          InternalListImpressionQualificationFiltersPageTokenKt.after {
            externalImpressionQualificationFilterId =
              INTERNAL_AMI_IQF.externalImpressionQualificationFilterId
          }
      }

      val response: ListImpressionQualificationFiltersResponse =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.listImpressionQualificationFilters(
            listImpressionQualificationFiltersRequest {
              pageToken = internalPageToken.toByteString().base64UrlEncode()
            }
          )
        }

      assertThat(response)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += PUBLIC_MRC_IQF
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter with page token and page size returns ImpressionQualificationFilter`() =
    runBlocking {
      val internalPageToken = internalListImpressionQualificationFiltersPageToken {
        after =
          InternalListImpressionQualificationFiltersPageTokenKt.after {
            externalImpressionQualificationFilterId =
              INTERNAL_AMI_IQF.externalImpressionQualificationFilterId
          }
      }

      val response: ListImpressionQualificationFiltersResponse =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.listImpressionQualificationFilters(
            listImpressionQualificationFiltersRequest {
              pageSize = 1
              pageToken = internalPageToken.toByteString().base64UrlEncode()
            }
          )
        }

      assertThat(response)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += PUBLIC_MRC_IQF
          }
        )
    }

  @Test
  fun `listImpressionQualificationFilter throws PERMISSION_DENIED when principal does not have required permissions`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/mc-2-user" }, SCOPES) {
            service.listImpressionQualificationFilters(
              ListImpressionQualificationFiltersRequest.getDefaultInstance()
            )
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    }

  @Test
  fun `listImpressionQualificationFilter throws INVALID_ARGUMENT if page size is negative`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            service.listImpressionQualificationFilters(
              listImpressionQualificationFiltersRequest { pageSize = -1 }
            )
          }
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
          withPrincipalAndScopes(PRINCIPAL, SCOPES) {
            service.listImpressionQualificationFilters(
              listImpressionQualificationFiltersRequest { pageToken = "mayhem" }
            )
          }
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
        mediaType = InternalMediaType.DISPLAY
        filters += internalEventFilter {
          terms += internalEventTemplateField {
            path = "banner_ad.viewable"
            value = InternalEventTemplateFieldKt.fieldValue { boolValue = false }
          }
        }
      }
    }

    private val PUBLIC_AMI_IQF = impressionQualificationFilter {
      name = "impressionQualificationFilters/ami"
      displayName = "ami"
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

    private val PUBLIC_MRC_IQF = impressionQualificationFilter {
      name = "impressionQualificationFilters/mrc"
      displayName = "mrc"
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

    private val PRINCIPAL = principal { name = "principals/mc-user" }
    private val SCOPES = setOf("*")

    private val CONFIG_PATH =
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    private val IMPRESSION_QUALIFICATION_FILTER_CONFIG:
      ImpressionQualificationFilterConfig by lazy {
      val configFile =
        getRuntimePath(CONFIG_PATH.resolve("impression_qualification_filter_config.textproto"))!!
          .toFile()
      parseTextProto(configFile, ImpressionQualificationFilterConfig.getDefaultInstance())
    }
    private val IMPRESSION_QUALIFICATION_FILTER_MAPPING =
      ImpressionQualificationFilterMapping(
        IMPRESSION_QUALIFICATION_FILTER_CONFIG,
        TestEvent.getDescriptor(),
      )
  }
}
