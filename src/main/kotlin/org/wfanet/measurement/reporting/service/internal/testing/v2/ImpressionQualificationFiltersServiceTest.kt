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
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersRequest
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.getImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec
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
  fun `getImpressionQualificationFilter succeeds`() = runBlocking {
    val result =
      service.getImpressionQualificationFilter(
        getImpressionQualificationFilterRequest { impressionQualificationFilterResourceId = "ami" }
      )

    assertThat(result).isEqualTo(AMI_IQF)
  }

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
            metadata[Errors.Metadata.FIELD_NAME.key] = "impressionQualificationFilterResourceId"
          }
        )
    }

  fun `getImpressionQualificationFilter throws NOT_FOUND when ImpressionQualificationFilter not found`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getImpressionQualificationFilter(
            getImpressionQualificationFilterRequest {
              impressionQualificationFilterResourceId = "abc"
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

  fun `listImpressionQualificationFilter succeeds`() = runBlocking {
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "pageSize"
          }
        )
    }

  companion object {
    private val AMI_IQF = impressionQualificationFilter {
      impressionQualificationFilterResourceId = "ami"
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.VIDEO }
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.DISPLAY }
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.OTHER }
    }

    private val MRC_IQF = impressionQualificationFilter {
      impressionQualificationFilterResourceId = "mrc"
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
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.OTHER }
    }
  }
}
