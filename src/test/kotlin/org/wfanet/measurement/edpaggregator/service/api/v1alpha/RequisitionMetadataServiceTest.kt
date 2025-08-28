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
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.fetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.edpaggregator.v1alpha.fulfillRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.lookupRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.queueRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.refuseRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.startProcessingRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata as InternalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase as InternalRequisitionMetadataService
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub as InternalRequisitionMetadataServiceStub
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createRequisitionMetadataRequest as internalCreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.fetchLatestCmmsCreateTimeRequest as internalFetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.internal.edpaggregator.fulfillRequisitionMetadataRequest as internalFulfillRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest as internalGetRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.lookupRequisitionMetadataRequest as internalLookupRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.queueRequisitionMetadataRequest as internalQueueRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.refuseRequisitionMetadataRequest as internalRefuseRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata as internalRequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.startProcessingRequisitionMetadataRequest as internalStartProcessingRequisitionMetadataRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey

// IDs are ApiIds of strings.
private val DATA_PROVIDER_ID = externalIdToApiId(111L)
private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
private val REQUISITION_METADATA_ID = externalIdToApiId(222L)
private val REQUISITION_METADATA_KEY =
  RequisitionMetadataKey(DATA_PROVIDER_KEY, REQUISITION_METADATA_ID)
private val CMMS_REQUISITION_ID = externalIdToApiId(333L)
private val CMMS_REQUISITION_KEY = CanonicalRequisitionKey(DATA_PROVIDER_KEY, CMMS_REQUISITION_ID)
private val MEASUREMENT_CONSUMER_ID = externalIdToApiId(444L)
private val REPORT_ID = externalIdToApiId(555L)
private val REPORT_KEY = ReportKey(MEASUREMENT_CONSUMER_ID, REPORT_ID)
private const val REQUEST_ID = "some_request_id"
private const val BLOB_URI = "path/to/blob"
private const val BLOB_TYPE = "blob.type"
private const val GROUP_ID = "group_id"
private val CMMS_CREATE_TIME = timestamp { seconds = 12345 }
private val CREATE_TIME = timestamp { seconds = 12345 }
private val UPDATE_TIME_1 = timestamp { seconds = 12345 }
private val UPDATE_TIME_2 = timestamp { seconds = 23456 }
private val WORK_ITEM = "workItems/${externalIdToApiId(666L)}"
private val ETAG_1 = UPDATE_TIME_1.toString()
private val ETAG_2 = UPDATE_TIME_2.toString()
private val REFUSAL_MESSAGE = "Somehow refused."

private val NEW_REQUISITION_METADATA = requisitionMetadata {
  // name not set
  cmmsRequisition = CMMS_REQUISITION_KEY.toName()
  report = REPORT_KEY.toName()
  blobUri = BLOB_URI
  blobTypeUrl = BLOB_TYPE
  groupId = GROUP_ID
  cmmsCreateTime = CMMS_CREATE_TIME
  // work_item not set
  // error_message not set
}

private val REQUISITION_METADATA =
  NEW_REQUISITION_METADATA.copy {
    name = REQUISITION_METADATA_KEY.toName()
    state = RequisitionMetadata.State.STORED
    createTime = CREATE_TIME
    updateTime = UPDATE_TIME_1
    etag = ETAG_1
  }

private val INTERNAL_REQUISITION_METADATA: InternalRequisitionMetadata =
  internalRequisitionMetadata {
    externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
    externalRequisitionMetadataId = apiIdToExternalId(REQUISITION_METADATA_ID)
    cmmsRequisition = CMMS_REQUISITION_KEY.toName()
    report = REPORT_KEY.toName()
    blobUri = BLOB_URI
    blobTypeUrl = BLOB_TYPE
    groupId = GROUP_ID
    cmmsCreateTime = CMMS_CREATE_TIME
    state = InternalRequisitionMetadata.State.STORED
    // work_item not set
    createTime = CREATE_TIME
    updateTime = UPDATE_TIME_1
    // errorMessage not set
    etag = updateTime.toString()
  }

@RunWith(JUnit4::class)
class RequisitionMetadataServiceTest {
  private val internalService: InternalRequisitionMetadataService = mockService {
    onBlocking { createRequisitionMetadata(any()) }.thenReturn(INTERNAL_REQUISITION_METADATA)
    onBlocking { getRequisitionMetadata(any()) }.thenReturn(INTERNAL_REQUISITION_METADATA)
    onBlocking { lookupRequisitionMetadata(any()) }.thenReturn(INTERNAL_REQUISITION_METADATA)
    onBlocking { fetchLatestCmmsCreateTime(any()) }.thenReturn(CMMS_CREATE_TIME)
    onBlocking { queueRequisitionMetadata(any()) }
      .thenReturn(
        INTERNAL_REQUISITION_METADATA.copy {
          state = InternalRequisitionMetadata.State.QUEUED
          updateTime = UPDATE_TIME_2
          etag = ETAG_2
          workItem = WORK_ITEM
        }
      )
    onBlocking { startProcessingRequisitionMetadata(any()) }
      .thenReturn(
        INTERNAL_REQUISITION_METADATA.copy {
          state = InternalRequisitionMetadata.State.PROCESSING
          updateTime = UPDATE_TIME_2
          etag = ETAG_2
        }
      )
    onBlocking { fulfillRequisitionMetadata(any()) }
      .thenReturn(
        INTERNAL_REQUISITION_METADATA.copy {
          state = InternalRequisitionMetadata.State.FULFILLED
          updateTime = UPDATE_TIME_2
          etag = ETAG_2
        }
      )
    onBlocking { refuseRequisitionMetadata(any()) }
      .thenReturn(
        INTERNAL_REQUISITION_METADATA.copy {
          state = InternalRequisitionMetadata.State.REFUSED
          updateTime = UPDATE_TIME_2
          etag = ETAG_2
          refusalMessage = REFUSAL_MESSAGE
        }
      )
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalService) }

  private lateinit var service: RequisitionMetadataService

  @Before
  fun initService() {
    service =
      RequisitionMetadataService(InternalRequisitionMetadataServiceStub(grpcTestServerRule.channel))
  }

  @Test
  fun `createRequisitionMetadata returns a RequisitionMetadata successfully`() = runBlocking {
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
          createTime = CREATE_TIME
          updateTime = UPDATE_TIME_1
          etag = ETAG_1
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
  }

  @Test
  fun `getRequisitionMetadata returns a RequisitionMetadata successfully`() = runBlocking {
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

  @Test
  fun `lookupRequisitionMetadata by cmmsRequisition returns a RequisitionMetadata successfully`() =
    runBlocking {
      val request = lookupRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        cmmsRequisition = CMMS_REQUISITION_KEY.toName()
      }

      val result = service.lookupRequisitionMetadata(request)

      assertThat(result).isEqualTo(INTERNAL_REQUISITION_METADATA.toRequisitionMetadata())
      verifyProtoArgument(
          internalService,
          InternalRequisitionMetadataService::lookupRequisitionMetadata,
        )
        .isEqualTo(
          internalLookupRequisitionMetadataRequest {
            externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
            cmmsRequisition = CMMS_REQUISITION_KEY.toName()
          }
        )
    }

  @Test
  fun `lookupRequisitionMetadata by blobUri returns a RequisitionMetadata successfully`() =
    runBlocking {
      val request = lookupRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        blobUri = BLOB_URI
      }

      val result = service.lookupRequisitionMetadata(request)

      assertThat(result).isEqualTo(INTERNAL_REQUISITION_METADATA.toRequisitionMetadata())
      verifyProtoArgument(
          internalService,
          InternalRequisitionMetadataService::lookupRequisitionMetadata,
        )
        .isEqualTo(
          internalLookupRequisitionMetadataRequest {
            externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
            blobUri = BLOB_URI
          }
        )
    }

  @Test
  fun `lookupRequisitionMetadata throws INVALID_ARGUMENT for missing parent`() = runBlocking {
    val request = lookupRequisitionMetadataRequest {
      // missing parent
      cmmsRequisition = CMMS_REQUISITION_KEY.toName()
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.lookupRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `lookupRequisitionMetadata throws INVALID_ARGUMENT for missing lookup key`() = runBlocking {
    val request = lookupRequisitionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      // missing lookup key
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.lookupRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `fetchLatestCmmsCreateTime updates state successfully`() = runBlocking {
    val request = fetchLatestCmmsCreateTimeRequest { parent = DATA_PROVIDER_KEY.toName() }

    val result = service.fetchLatestCmmsCreateTime(request)

    assertThat(result).isEqualTo(CMMS_CREATE_TIME)
    verifyProtoArgument(
        internalService,
        InternalRequisitionMetadataService::fetchLatestCmmsCreateTime,
      )
      .isEqualTo(
        internalFetchLatestCmmsCreateTimeRequest {
          externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
        }
      )
  }

  @Test
  fun `fetchLatestCmmsCreateTime throws INVALID_ARGUMENT for missing parent`() = runBlocking {
    val request = fetchLatestCmmsCreateTimeRequest {
      // missing parent
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.fetchLatestCmmsCreateTime(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `queueRequisitionMetadata updates state successfully`() = runBlocking {
    val request = queueRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      etag = ETAG_1
      workItem = WORK_ITEM
    }

    val requisitionMetadata = service.queueRequisitionMetadata(request)

    assertThat(requisitionMetadata)
      .isEqualTo(
        REQUISITION_METADATA.copy {
          state = RequisitionMetadata.State.QUEUED
          workItem = WORK_ITEM
          updateTime = UPDATE_TIME_2
          etag = ETAG_2
        }
      )
    verifyProtoArgument(
        internalService,
        InternalRequisitionMetadataService::queueRequisitionMetadata,
      )
      .isEqualTo(
        internalQueueRequisitionMetadataRequest {
          externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
          externalRequisitionMetadataId = apiIdToExternalId(REQUISITION_METADATA_ID)
          this.etag = ETAG_1
          this.workItem = WORK_ITEM
        }
      )
  }

  @Test
  fun `queueRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() = runBlocking {
    val request = queueRequisitionMetadataRequest {
      name = "foo"
      etag = ETAG_1
      workItem = WORK_ITEM
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.queueRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `queueRequisitionMetadata throws INVALID_ARGUMENT for invalid work_item`() = runBlocking {
    val request = queueRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      etag = ETAG_1
      workItem = "foo"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.queueRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `queueRequisitionMetadata throws INVALID_ARGUMENT for missing etag`() = runBlocking {
    val request = queueRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      // etag is missing
      workItem = WORK_ITEM
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.queueRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `startProcessingRequisitionMetadata updates state successfully`() = runBlocking {
    val request = startProcessingRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      etag = ETAG_1
    }

    val requisitionMetadata = service.startProcessingRequisitionMetadata(request)

    assertThat(requisitionMetadata)
      .isEqualTo(
        REQUISITION_METADATA.copy {
          state = RequisitionMetadata.State.PROCESSING
          updateTime = UPDATE_TIME_2
          etag = ETAG_2
        }
      )
    verifyProtoArgument(
        internalService,
        InternalRequisitionMetadataService::startProcessingRequisitionMetadata,
      )
      .isEqualTo(
        internalStartProcessingRequisitionMetadataRequest {
          externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
          externalRequisitionMetadataId = apiIdToExternalId(REQUISITION_METADATA_ID)
          this.etag = ETAG_1
        }
      )
  }

  @Test
  fun `startProcessingRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() =
    runBlocking {
      val request = startProcessingRequisitionMetadataRequest { name = "foo" }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.startProcessingRequisitionMetadata(request)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `startProcessingRequisitionMetadata throws INVALID_ARGUMENT for missing etag`() =
    runBlocking {
      val request = startProcessingRequisitionMetadataRequest {
        name = REQUISITION_METADATA_KEY.toName()
        // etag is missing
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.startProcessingRequisitionMetadata(request)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `fulfillRequisitionMetadata updates state successfully`() = runBlocking {
    val request = fulfillRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      etag = ETAG_1
    }

    val result = service.fulfillRequisitionMetadata(request)

    assertThat(result.state).isEqualTo(RequisitionMetadata.State.FULFILLED)
    verifyProtoArgument(
        internalService,
        InternalRequisitionMetadataService::fulfillRequisitionMetadata,
      )
      .isEqualTo(
        internalFulfillRequisitionMetadataRequest {
          externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
          externalRequisitionMetadataId = apiIdToExternalId(REQUISITION_METADATA_ID)
          this.etag = ETAG_1
        }
      )
  }

  @Test
  fun `fulfillRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() = runBlocking {
    val request = fulfillRequisitionMetadataRequest { name = "foo" }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.fulfillRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `fulfillRequisitionMetadata throws INVALID_ARGUMENT for missing etag`() = runBlocking {
    val request = fulfillRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      // etag is missing
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.fulfillRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisitionMetadata updates state successfully`() = runBlocking {
    val request = refuseRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      etag = ETAG_1
      refusalMessage = REFUSAL_MESSAGE
    }

    val requisitionMetadata = service.refuseRequisitionMetadata(request)

    assertThat(requisitionMetadata)
      .isEqualTo(
        REQUISITION_METADATA.copy {
          state = RequisitionMetadata.State.REFUSED
          updateTime = UPDATE_TIME_2
          etag = ETAG_2
          refusalMessage = REFUSAL_MESSAGE
        }
      )
    verifyProtoArgument(
        internalService,
        InternalRequisitionMetadataService::refuseRequisitionMetadata,
      )
      .isEqualTo(
        internalRefuseRequisitionMetadataRequest {
          externalDataProviderId = apiIdToExternalId(DATA_PROVIDER_ID)
          externalRequisitionMetadataId = apiIdToExternalId(REQUISITION_METADATA_ID)
          this.refusalMessage = REFUSAL_MESSAGE
          etag = ETAG_1
        }
      )
  }

  @Test
  fun `refuseRequisitionMetadata throws INVALID_ARGUMENT for invalid name`() = runBlocking {
    val request = refuseRequisitionMetadataRequest {
      name = "foo"
      etag = ETAG_1
      refusalMessage = REFUSAL_MESSAGE
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.refuseRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisitionMetadata throws INVALID_ARGUMENT for missing etag`() = runBlocking {
    val request = refuseRequisitionMetadataRequest {
      name = REQUISITION_METADATA_KEY.toName()
      // missing etag
      refusalMessage = REFUSAL_MESSAGE
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.refuseRequisitionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }
}
