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

package org.wfanet.measurement.edpaggregator.service.api.v1alpha.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import java.time.Instant
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.api.v1alpha.RequisitionMetadataKey
import org.wfanet.measurement.edpaggregator.service.api.v1alpha.RequisitionMetadataService
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
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase as InternalRequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub as InternalRequisitionMetadataServiceStub
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey

@RunWith(JUnit4::class)
abstract class RequisitionMetadataServiceTest {
  private lateinit var internalService: InternalRequisitionMetadataServiceCoroutineImplBase

  protected abstract fun newInternalService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): InternalRequisitionMetadataServiceCoroutineImplBase

  private lateinit var internalServer: Server
  private lateinit var internalChannel: ManagedChannel

  private lateinit var service: RequisitionMetadataService

  @Before
  fun initService() {
    internalService = newInternalService()
    internalServer =
      InProcessServerBuilder.forName(INTERNAL_SERVER_NAME)
        .directExecutor() // Use direct executor for tests
        .addService(internalService) // No forwarder needed!
        .build()
        .start()

    internalChannel = InProcessChannelBuilder.forName(INTERNAL_SERVER_NAME).directExecutor().build()

    service = RequisitionMetadataService(InternalRequisitionMetadataServiceStub(internalChannel))
  }

  @After
  fun tearDown() {
    internalChannel.shutdownNow()
    internalServer.shutdownNow()
  }

  @Test
  fun `createRequisitionMetadata with requestId returns a RequisitionMetadata successfully`() =
    runBlocking {
      val startTime = Instant.now()
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA
        requestId = REQUEST_ID
      }

      val requisitionMetadata = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = CMMS_CREATE_TIME
            cmmsRequisition = CMMS_REQUISITION_KEY.toName()
            blobUri = BLOB_URI
            blobTypeUrl = BLOB_TYPE
            groupId = GROUP_ID
            report = REPORT_KEY.toName()
          }
        )
      val requisitionMetadataKey =
        assertNotNull(RequisitionMetadataKey.fromName(requisitionMetadata.name))
      assertThat(requisitionMetadataKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(requisitionMetadataKey.requisitionMetadataId).isNotEmpty()
      assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
      assertThat(requisitionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `createRequisitionMetadata without requestId returns a RequisitionMetadata successfully`() =
    runBlocking {
      val startTime = Instant.now()
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA
        // no request_id
      }

      val requisitionMetadata = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          requisitionMetadata {
            state = RequisitionMetadata.State.STORED
            cmmsCreateTime = CMMS_CREATE_TIME
            cmmsRequisition = CMMS_REQUISITION_KEY.toName()
            blobUri = BLOB_URI
            blobTypeUrl = BLOB_TYPE
            groupId = GROUP_ID
            report = REPORT_KEY.toName()
          }
        )
      val requisitionMetadataKey =
        assertNotNull(RequisitionMetadataKey.fromName(requisitionMetadata.name))
      assertThat(requisitionMetadataKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(requisitionMetadataKey.requisitionMetadataId).isNotEmpty()
      assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
      assertThat(requisitionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `createRequisitionMetadata with existing requestId returns the existing RequisitionMetadata`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA
        requestId = REQUEST_ID
      }
      val existingRequisitionMetadata = service.createRequisitionMetadata(request)

      val requisitionMetadata = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata).isEqualTo(existingRequisitionMetadata)
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
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = getRequisitionMetadataRequest { name = existingRequisitionMetadata.name }
    val requisitionMetadata = service.getRequisitionMetadata(request)

    assertThat(requisitionMetadata).isEqualTo(existingRequisitionMetadata)
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
      val existingRequisitionMetadata =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            requisitionMetadata = NEW_REQUISITION_METADATA
            requestId = REQUEST_ID
          }
        )

      val request = lookupRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        cmmsRequisition = CMMS_REQUISITION_KEY.toName()
      }
      val result = service.lookupRequisitionMetadata(request)

      assertThat(result).isEqualTo(existingRequisitionMetadata)
    }

  @Test
  fun `lookupRequisitionMetadata by blobUri returns a RequisitionMetadata successfully`() =
    runBlocking {
      val existingRequisitionMetadata =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            requisitionMetadata = NEW_REQUISITION_METADATA
            requestId = REQUEST_ID
          }
        )

      val request = lookupRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        blobUri = BLOB_URI
      }
      val result = service.lookupRequisitionMetadata(request)

      assertThat(result).isEqualTo(existingRequisitionMetadata)
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
  fun `fetchLatestCmmsCreateTime returns default Timestamp when no requisition metadata`() =
    runBlocking {
      // No requisition metadata created

      val request = fetchLatestCmmsCreateTimeRequest { parent = DATA_PROVIDER_KEY.toName() }
      val result = service.fetchLatestCmmsCreateTime(request)

      assertThat(result).isEqualTo(Timestamp.getDefaultInstance())
    }

  @Test
  fun `fetchLatestCmmsCreateTime returns latest Timestamp successfully`() = runBlocking {
    val earlyTimestamp = timestamp { seconds = 1 }
    val lateTimestamp = timestamp { seconds = 2 }

    service.createRequisitionMetadata(
      createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata = NEW_REQUISITION_METADATA.copy { cmmsCreateTime = earlyTimestamp }
      }
    )
    service.createRequisitionMetadata(
      createRequisitionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        requisitionMetadata =
          NEW_REQUISITION_METADATA.copy {
            cmmsCreateTime = lateTimestamp
            cmmsRequisition = CMMS_REQUISITION_KEY.toName() + "second"
            blobUri = BLOB_URI + "second"
          }
      }
    )

    val request = fetchLatestCmmsCreateTimeRequest { parent = DATA_PROVIDER_KEY.toName() }
    val result = service.fetchLatestCmmsCreateTime(request)
    assertThat(result).isEqualTo(lateTimestamp)
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
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = queueRequisitionMetadataRequest {
      name = existingRequisitionMetadata.name
      etag = existingRequisitionMetadata.etag
      workItem = WORK_ITEM
    }
    val requisitionMetadata = service.queueRequisitionMetadata(request)

    assertThat(requisitionMetadata.state).isEqualTo(RequisitionMetadata.State.QUEUED)
    assertThat(requisitionMetadata.workItem).isEqualTo(WORK_ITEM)
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(existingRequisitionMetadata.etag)
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
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = startProcessingRequisitionMetadataRequest {
      name = existingRequisitionMetadata.name
      etag = existingRequisitionMetadata.etag
    }
    val requisitionMetadata = service.startProcessingRequisitionMetadata(request)

    assertThat(requisitionMetadata.state).isEqualTo(RequisitionMetadata.State.PROCESSING)
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(existingRequisitionMetadata.etag)
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
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = fulfillRequisitionMetadataRequest {
      name = existingRequisitionMetadata.name
      etag = existingRequisitionMetadata.etag
    }
    val requisitionMetadata = service.fulfillRequisitionMetadata(request)

    assertThat(requisitionMetadata.state).isEqualTo(RequisitionMetadata.State.FULFILLED)
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(existingRequisitionMetadata.etag)
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
    val existingRequisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          requisitionMetadata = NEW_REQUISITION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = refuseRequisitionMetadataRequest {
      name = existingRequisitionMetadata.name
      etag = existingRequisitionMetadata.etag
      refusalMessage = REFUSAL_MESSAGE
    }
    val requisitionMetadata = service.refuseRequisitionMetadata(request)

    assertThat(requisitionMetadata.state).isEqualTo(RequisitionMetadata.State.REFUSED)
    assertThat(requisitionMetadata.refusalMessage).isEqualTo(REFUSAL_MESSAGE)
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(existingRequisitionMetadata.etag)
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

  companion object {
    private const val INTERNAL_SERVER_NAME = "internal-server"

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private val REQUISITION_METADATA_ID = externalIdToApiId(222L)
    private val REQUISITION_METADATA_KEY =
      RequisitionMetadataKey(DATA_PROVIDER_KEY, REQUISITION_METADATA_ID)
    private val CMMS_REQUISITION_ID = externalIdToApiId(333L)
    private val CMMS_REQUISITION_KEY =
      CanonicalRequisitionKey(DATA_PROVIDER_KEY, CMMS_REQUISITION_ID)
    private val MEASUREMENT_CONSUMER_ID = externalIdToApiId(444L)
    private val REPORT_ID = externalIdToApiId(555L)
    private val REPORT_KEY = ReportKey(MEASUREMENT_CONSUMER_ID, REPORT_ID)
    private val REQUEST_ID = UUID.randomUUID().toString()
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
  }
}
