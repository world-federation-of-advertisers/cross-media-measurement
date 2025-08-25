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
import io.grpc.StatusException
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
import org.wfanet.measurement.api.v2alpha.RequisitionKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata as InternalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase as InternalRequisitionMetadataService
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub as InternalRequisitionMetadataServiceStub
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createRequisitionMetadataRequest as internalCreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest as internalGetRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata as internalRequisitionMetadata
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey

private const val DATA_PROVIDER_ID = "111"
private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
private const val REQUISITION_METADATA_ID = "222"
private val REQUISITION_METADATA_KEY =
  RequisitionMetadataKey(DATA_PROVIDER_KEY, REQUISITION_METADATA_ID)
private const val CMMS_REQUISITION_ID = "333"
private val CMMS_REQUISITION_KEY = CanonicalRequisitionKey(DATA_PROVIDER_KEY, CMMS_REQUISITION_ID)
private const val MEASUREMENT_CONSUMER_ID = "444"
private const val REPORT_ID = "555"
private val REPORT_KEY = ReportKey(MEASUREMENT_CONSUMER_ID, REPORT_ID)
private const val REQUEST_ID = "some_request_id"
private const val REQUISITION_METADATA_BLOB_URI = "path/to/blob"
private const val REQUISITION_METADATA_GROUP_ID = "group_id"
private val REQUISITION_METADATA_CMMS_CREATE_TIME = timestamp { seconds = 12345 }
private val REQUISITION_METADATA_CREATE_TIME = timestamp { seconds = 12345 }
private val REQUISITION_METADATA_UPDATE_TIME = timestamp { seconds = 12345 }

private val REQUISITION_METADATA = requisitionMetadata {
  // name not set
  cmmsRequisition = CMMS_REQUISITION_KEY.toName()
  report = REPORT_KEY.toName()
  blobUri = REQUISITION_METADATA_BLOB_URI
  groupId = REQUISITION_METADATA_GROUP_ID
  cmmsCreateTime = REQUISITION_METADATA_CMMS_CREATE_TIME
  // work_item not set
  // error_message not set
}

private val INTERNAL_REQUISITION_METADATA: InternalRequisitionMetadata =
  internalRequisitionMetadata {
    externalDataProviderId = DATA_PROVIDER_ID.toLong()
    externalRequisitionMetadataId = REQUISITION_METADATA_ID.toLong()
    cmmsRequisition = CMMS_REQUISITION_KEY.toName()
    report = REPORT_KEY.toName()
    blobUri = REQUISITION_METADATA_BLOB_URI
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
      requisitionMetadata = REQUISITION_METADATA
      requestId = REQUEST_ID
    }

    val requisitionMetadata = service.createRequisitionMetadata(request)

    assertThat(requisitionMetadata)
      .isEqualTo(
        REQUISITION_METADATA.copy {
          name = REQUISITION_METADATA_KEY.toName()
          state = RequisitionMetadata.State.STORED
        }
      )

    verifyProtoArgument(
        internalService,
        InternalRequisitionMetadataService::createRequisitionMetadata,
      )
      .isEqualTo(
        internalCreateRequisitionMetadataRequest {
          requestId = REQUEST_ID
          this.requisitionMetadata = INTERNAL_REQUISITION_METADATA.copy {}
        }
      )
  }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val request =
      CreateRequisitionMetadataRequest.newBuilder()
        .apply {
          requisitionMetadata = REQUISITION_METADATA_REQUEST
          requestId = REQUEST_ID
        }
        .build()

    val exception = assertFailsWith<StatusException> { service.createRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("Parent is either unspecified or invalid")
  }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT when name is specified`() = runBlocking {
    val request =
      CreateRequisitionMetadataRequest.newBuilder()
        .apply {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata =
            REQUISITION_METADATA_REQUEST.toBuilder().apply { name = "foo" }.build()
          requestId = REQUEST_ID
        }
        .build()

    val exception = assertFailsWith<StatusException> { service.createRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("name must be empty")
  }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for invalid cmms_requisition`() =
    runBlocking {
      val request =
        CreateRequisitionMetadataRequest.newBuilder()
          .apply {
            parent = DATA_PROVIDER_KEY.toName()
            requisitionMetadata =
              REQUISITION_METADATA_REQUEST.toBuilder().apply { cmmsRequisition = "foo" }.build()
            requestId = REQUEST_ID
          }
          .build()

      val exception =
        assertFailsWith<StatusException> { service.createRequisitionMetadata(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("cmms_requisition is either unspecified or invalid")
    }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for mismatched cmms_requisition parent`() =
    runBlocking {
      val request =
        CreateRequisitionMetadataRequest.newBuilder()
          .apply {
            parent = DATA_PROVIDER_KEY.toName()
            requisitionMetadata =
              REQUISITION_METADATA_REQUEST.toBuilder()
                .apply { cmmsRequisition = RequisitionKey("other-data-provider", "123").toName() }
                .build()
            requestId = REQUEST_ID
          }
          .build()

      val exception =
        assertFailsWith<StatusException> { service.createRequisitionMetadata(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("does not match parent DataProvider")
    }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for invalid report`() = runBlocking {
    val request =
      CreateRequisitionMetadataRequest.newBuilder()
        .apply {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata =
            REQUISITION_METADATA_REQUEST.toBuilder().apply { report = "foo" }.build()
          requestId = REQUEST_ID
        }
        .build()

    val exception = assertFailsWith<StatusException> { service.createRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("report is either unspecified or invalid")
  }

  @Test
  fun `createRequisitionMetadata rethrows ALREADY_EXISTS from internal`() = runBlocking {
    every { idGenerator.generateExternalId() } returns ExternalId(REQUISITION_METADATA_ID)
    coEvery { internalService.createRequisitionMetadata(any()) }
      .throws(Status.ALREADY_EXISTS.asRuntimeException())

    val request =
      CreateRequisitionMetadataRequest.newBuilder()
        .apply {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = REQUISITION_METADATA_REQUEST
          requestId = REQUEST_ID
        }
        .build()

    val exception = assertFailsWith<StatusException> { service.createRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `getRequisitionMetadata succeeds`() = runBlocking {
    val requisitionMetadataName =
      org.wfanet.measurement.api.v2alpha
        .RequisitionMetadataKey(DATA_PROVIDER_ID, REQUISITION_METADATA_ID.toString())
        .toName()
    val request =
      GetRequisitionMetadataRequest.newBuilder().apply { name = requisitionMetadataName }.build()

    val result = service.getRequisitionMetadata(request)

    assertThat(result).isEqualTo(INTERNAL_REQUISITION_METADATA.toRequisitionMetadata())
    verifyProtoArgument(internalService, InternalRequisitionMetadataService::getRequisitionMetadata)
      .isEqualTo(
        internalGetRequisitionMetadataRequest {
          externalDataProviderId = DATA_PROVIDER_ID.toLong()
          externalRequisitionMetadataId = REQUISITION_METADATA_ID
        }
      )
  }

  @Test
  fun `getRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() = runBlocking {
    val request = GetRequisitionMetadataRequest.newBuilder().apply { name = "foo" }.build()
    val exception = assertFailsWith<StatusException> { service.getRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
