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

package org.wfanet.measurement.edpaggregator.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
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
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata as InternalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase as InternalRequisitionMetadataService
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub as InternalRequisitionMetadataServiceStub
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createRequisitionMetadataRequest as internalCreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest as internalGetRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata as internalRequisitionMetadata
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey

private const val DATA_PROVIDER_ID = "AAAAAAAAALs"
private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
private const val REQUISITION_METADATA_ID = "AAAAAAAAAHs"
private val REQUISITION_METADATA_KEY =
  RequisitionMetadataKey(DATA_PROVIDER_KEY, REQUISITION_METADATA_ID)
private const val CMMS_REQUISITION_ID = "AAAAAAAAAJs"
private val CMMS_REQUISITION_KEY = CanonicalRequisitionKey(DATA_PROVIDER_KEY, CMMS_REQUISITION_ID)
private const val MEASUREMENT_CONSUMER_ID = "AAAAAAAAAKs"
private const val REPORT_ID = "AAAAAAAAAOs"
private val REPORT_KEY = ReportKey(MEASUREMENT_CONSUMER_ID, REPORT_ID)
private const val REQUEST_ID = "some_request_id"
private const val REQUISITION_METADATA_BLOB_URI = "path/to/blob"
private const val REQUISITION_METADATA_BLOB_TYPE = "blob.type"
private const val REQUISITION_METADATA_GROUP_ID = "group_id"
private val REQUISITION_METADATA_CMMS_CREATE_TIME = timestamp { seconds = 12345 }
private val REQUISITION_METADATA_CREATE_TIME = timestamp { seconds = 12345 }
private val REQUISITION_METADATA_UPDATE_TIME = timestamp { seconds = 12345 }

private val NEW_REQUISITION_METADATA = requisitionMetadata {
  // name not set
  cmmsRequisition = CMMS_REQUISITION_KEY.toName()
  report = REPORT_KEY.toName()
  blobUri = REQUISITION_METADATA_BLOB_URI
  blobTypeUrl = REQUISITION_METADATA_BLOB_TYPE
  groupId = REQUISITION_METADATA_GROUP_ID
  cmmsCreateTime = REQUISITION_METADATA_CMMS_CREATE_TIME
  // work_item not set
  // error_message not set
}

private val REQUISITION_METADATA =
  NEW_REQUISITION_METADATA.copy {
    name = REQUISITION_METADATA_KEY.toName()
    state = RequisitionMetadata.State.STORED
    createTime = REQUISITION_METADATA_CREATE_TIME
    updateTime = REQUISITION_METADATA_UPDATE_TIME
    etag = REQUISITION_METADATA_UPDATE_TIME.toString()
  }

private val INTERNAL_REQUISITION_METADATA: InternalRequisitionMetadata =
  internalRequisitionMetadata {
    externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
    externalRequisitionMetadataId = apiIdToExternalId(REQUISITION_METADATA_ID)
    cmmsRequisition = CMMS_REQUISITION_KEY.toName()
    report = REPORT_KEY.toName()
    blobUri = REQUISITION_METADATA_BLOB_URI
    blobTypeUrl = REQUISITION_METADATA_BLOB_TYPE
    groupId = REQUISITION_METADATA_GROUP_ID
    cmmsCreateTime = REQUISITION_METADATA_CMMS_CREATE_TIME
    state = InternalRequisitionMetadata.State.STORED
    // work_item not set
    createTime = REQUISITION_METADATA_CREATE_TIME
    updateTime = REQUISITION_METADATA_UPDATE_TIME
    // errorMessage not set
    etag = updateTime.toString()
  }

@RunWith(JUnit4::class)
class RequisitionMetadataServiceTest {
  private val internalService: InternalRequisitionMetadataService = mockService {
    onBlocking { createRequisitionMetadata(any()) }.thenReturn(INTERNAL_REQUISITION_METADATA)
    onBlocking { getRequisitionMetadata(any()) }.thenReturn(INTERNAL_REQUISITION_METADATA)
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalService) }

  private lateinit var service: RequisitionMetadataService

  @Before
  fun initService() {
    service =
      RequisitionMetadataService(InternalRequisitionMetadataServiceStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createRequisitionMetadata returns a RequisitionMetadata`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      requisitionMetadata = NEW_REQUISITION_METADATA
      requestId = REQUEST_ID
    }

    val requisitionMetadata = service.createRequisitionMetadata(request)

    assertThat(requisitionMetadata)
      .isEqualTo(
        REQUISITION_METADATA.copy {
          name = REQUISITION_METADATA_KEY.toName()
          state = RequisitionMetadata.State.STORED
          createTime = REQUISITION_METADATA_CREATE_TIME
          updateTime = REQUISITION_METADATA_UPDATE_TIME
          etag = REQUISITION_METADATA_UPDATE_TIME.toString()
        }
      )

    verifyProtoArgument(
        internalService,
        InternalRequisitionMetadataService::createRequisitionMetadata,
      )
      .isEqualTo(
        internalCreateRequisitionMetadataRequest {
          requestId = REQUEST_ID
          this.requisitionMetadata =
            INTERNAL_REQUISITION_METADATA.copy {
              clearExternalRequisitionMetadataId()
              clearState()
              clearCreateTime()
              clearUpdateTime()
              clearEtag()
            }
        }
      )
  }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      // missing parent
      requisitionMetadata = NEW_REQUISITION_METADATA
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Parent is either unspecified or invalid")
  }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT when name is specified`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      requisitionMetadata =
        NEW_REQUISITION_METADATA.copy { name = REQUISITION_METADATA_KEY.toName() }
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("name must be empty")
  }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for invalid cmms_requisition`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA.copy { cmmsRequisition = "foo" }
        requestId = REQUEST_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("cmms_requisition is either unspecified or invalid")
    }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for mismatched cmms_requisition parent`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata =
          NEW_REQUISITION_METADATA.copy {
            cmmsRequisition = CanonicalRequisitionKey("other-data-provider", "123").toName()
          }
        requestId = REQUEST_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("does not match parent DataProvider")
    }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for invalid report`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      requisitionMetadata = NEW_REQUISITION_METADATA.copy { report = "foo" }
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("report is either unspecified or invalid")
  }

  @Test
  fun `getRequisitionMetadata succeeds`() = runBlocking {
    val request = getRequisitionMetadataRequest { name = REQUISITION_METADATA_KEY.toName() }

    val requisitionMetadata = service.getRequisitionMetadata(request)

    assertThat(requisitionMetadata)
      .isEqualTo(REQUISITION_METADATA.copy { state = RequisitionMetadata.State.STORED })
    verifyProtoArgument(internalService, InternalRequisitionMetadataService::getRequisitionMetadata)
      .isEqualTo(
        internalGetRequisitionMetadataRequest {
          externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
          externalRequisitionMetadataId = apiIdToExternalId(REQUISITION_METADATA_ID)
        }
      )
  }

  @Test
  fun `getRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() = runBlocking {
    val request = getRequisitionMetadataRequest { name = "foo" }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.getRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
