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

package org.wfanet.measurement.edpaggregator.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.internal.Errors
import org.wfanet.measurement.internal.edpaggregator.BatchCreateRequisitionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRequisitionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.batchCreateRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.batchCreateRequisitionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.fetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.internal.edpaggregator.fulfillRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.listRequisitionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.listRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.listRequisitionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.lookupRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.markWithdrawnRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.queueRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.refuseRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.startProcessingRequisitionMetadataRequest

@RunWith(JUnit4::class)
abstract class RequisitionMetadataServiceTest {
  private lateinit var service: RequisitionMetadataServiceCoroutineImplBase

  protected abstract fun newService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): RequisitionMetadataServiceCoroutineImplBase

  @Before
  fun initService() {
    service = newService()
  }

  @Test
  fun `createRequisitionMetadata without request_id returns created requisition metadata`() =
    runBlocking {
      val startTime = Instant.now()

      val request = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA
        // no request_id
      }
      val requisitionMetadata = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata)
        .ignoringFields(
          RequisitionMetadata.CREATE_TIME_FIELD_NUMBER,
          RequisitionMetadata.UPDATE_TIME_FIELD_NUMBER,
          RequisitionMetadata.ETAG_FIELD_NUMBER,
        )
        .isEqualTo(REQUISITION_METADATA.copy { state = State.REQUISITION_METADATA_STATE_STORED })
      assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
      assertThat(requisitionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `createRequisitionMetadata with request_id returns created requisition metadata`() =
    runBlocking {
      val startTime = Instant.now()

      val request = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA
        requestId = CREATE_REQUEST_ID
      }
      val requisitionMetadata = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata)
        .ignoringFields(
          RequisitionMetadata.CREATE_TIME_FIELD_NUMBER,
          RequisitionMetadata.UPDATE_TIME_FIELD_NUMBER,
          RequisitionMetadata.ETAG_FIELD_NUMBER,
        )
        .isEqualTo(REQUISITION_METADATA.copy { state = State.REQUISITION_METADATA_STATE_STORED })
      assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
      assertThat(requisitionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `createRequisitionMetadata with refusal message returns a REFUSED requisition metadata`() =
    runBlocking {
      val startTime = Instant.now()

      val request = createRequisitionMetadataRequest {
        requisitionMetadata =
          REQUISITION_METADATA.copy {
            state = State.REQUISITION_METADATA_STATE_REFUSED
            refusalMessage = REFUSAL_MESSAGE
          }
      }
      val requisitionMetadata = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata)
        .ignoringFields(
          RequisitionMetadata.CREATE_TIME_FIELD_NUMBER,
          RequisitionMetadata.UPDATE_TIME_FIELD_NUMBER,
          RequisitionMetadata.ETAG_FIELD_NUMBER,
        )
        .isEqualTo(
          REQUISITION_METADATA.copy {
            state = State.REQUISITION_METADATA_STATE_REFUSED
            refusalMessage = REFUSAL_MESSAGE
          }
        )
      assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
      assertThat(requisitionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `createRequisitionMetadata with existing request_id returns existing requisition metadata`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA
        requestId = CREATE_REQUEST_ID
      }
      val requisitionMetadata = service.createRequisitionMetadata(request)

      val requisitionMetadata2 = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata2).isEqualTo(requisitionMetadata)
    }

  @Test
  fun `multiple createRequisitionMetadata return multiple requisition metadata`(): Unit =
    runBlocking {
      val requisitionMetadata1 =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest {
            requisitionMetadata =
              REQUISITION_METADATA.copy {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                clearRequisitionMetadataResourceId()
                cmmsRequisition = CMMS_REQUISITION + "1"
                blobUri = BLOB_URI + "1"
              }
          }
        )
      // The second call should not raise ALREADY_EXISTS error.
      val requisitionMetadata2 =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest {
            requisitionMetadata =
              REQUISITION_METADATA.copy {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                clearRequisitionMetadataResourceId()
                cmmsRequisition = CMMS_REQUISITION + "2"
                blobUri = BLOB_URI + "2"
              }
          }
        )

      assertThat(requisitionMetadata1.requisitionMetadataResourceId)
        .isNotEqualTo(requisitionMetadata2.requisitionMetadataResourceId)
    }

  @Test
  fun `createRequisitionMetadata fails when dataProviderResourceName is missing`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      requisitionMetadata = REQUISITION_METADATA.copy { clearDataProviderResourceId() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createRequisitionMetadata fails when cmmsRequisition is missing`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      requisitionMetadata = REQUISITION_METADATA.copy { clearCmmsRequisition() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createRequisitionMetadata fails when cmmsRequisition is duplicated`() = runBlocking {
    service.createRequisitionMetadata(
      createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA.copy { blobUri = "uri_1" }
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest {
            requisitionMetadata =
              REQUISITION_METADATA.copy {
                // same cmmsRequisition
                blobUri = "uri_2"
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `createRequisitionMetadata fails when blobUri is missing`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      requisitionMetadata = REQUISITION_METADATA.copy { clearBlobUri() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createRequisitionMetadata fails when blobUri is duplicated`() = runBlocking {
    service.createRequisitionMetadata(
      createRequisitionMetadataRequest {
        requisitionMetadata =
          REQUISITION_METADATA.copy {
            cmmsRequisition = "dataProviders/data-provider-1/requisitions/requisition-1"
          }
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest {
            requisitionMetadata =
              REQUISITION_METADATA.copy {
                // same blobUri
                cmmsRequisition = "dataProviders/data-provider-1/requisitions/requisition-2"
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `createRequisitionMetadata fails when blobTypeUrl is missing`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      requisitionMetadata = REQUISITION_METADATA.copy { clearBlobTypeUrl() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createRequisitionMetadata fails when groupId is missing`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      requisitionMetadata = REQUISITION_METADATA.copy { clearGroupId() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createRequisitionMetadata fails when report is missing`() = runBlocking {
    val request = createRequisitionMetadataRequest {
      requisitionMetadata = REQUISITION_METADATA.copy { clearReport() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createRequisitionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `batchCreateRequisitionMetadata returns multiple requisition metadata`() = runBlocking {
    val request1 = createRequisitionMetadataRequest {
      requisitionMetadata = REQUISITION_METADATA
      requestId = UUID.randomUUID().toString()
    }
    val request2 = createRequisitionMetadataRequest {
      requisitionMetadata = REQUISITION_METADATA_2
      requestId = UUID.randomUUID().toString()
    }

    val response =
      service.batchCreateRequisitionMetadata(
        batchCreateRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requests += request1
          requests += request2
        }
      )

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        batchCreateRequisitionMetadataResponse {
          requisitionMetadata += request1.requisitionMetadata
          requisitionMetadata += request2.requisitionMetadata
        }
      )
    assertThat(response.requisitionMetadataList.all { it.hasCreateTime() }).isTrue()
  }

  @Test
  fun `batchCreateRequisitionMetadata without subrequests returns default response`() =
    runBlocking {
      val response =
        service.batchCreateRequisitionMetadata(
          batchCreateRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          }
        )

      assertThat(response)
        .comparingExpectedFieldsOnly()
        .isEqualTo(BatchCreateRequisitionMetadataResponse.getDefaultInstance())
    }

  @Test
  fun `batchCreateRequisitionMetadata is idempotent and creates new items in same request`() =
    runBlocking {
      val idempotentRequest = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA
        requestId = UUID.randomUUID().toString()
      }
      val initialResponse =
        service.batchCreateRequisitionMetadata(
          batchCreateRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requests += idempotentRequest
          }
        )

      assertThat(initialResponse)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          batchCreateRequisitionMetadataResponse {
            requisitionMetadata += idempotentRequest.requisitionMetadata
          }
        )

      val existingRequisitionMetadata = initialResponse.requisitionMetadataList.single()

      val newRequest = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA_2
        requestId = UUID.randomUUID().toString()
      }
      val secondResponse =
        service.batchCreateRequisitionMetadata(
          batchCreateRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requests += idempotentRequest // Idempotent request
            requests += newRequest // New request
          }
        )

      assertThat(secondResponse)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          batchCreateRequisitionMetadataResponse {
            requisitionMetadata += REQUISITION_METADATA
            requisitionMetadata += newRequest.requisitionMetadata
          }
        )

      val newRequisitionMetadata = secondResponse.requisitionMetadataList.last()
      assertThat(newRequisitionMetadata.updateTime.toInstant())
        .isGreaterThan(existingRequisitionMetadata.updateTime.toInstant())
    }

  @Test
  fun `batchCreateRequisitionMetadata throws ALREADY_EXISTS for existing blobUri`() = runBlocking {
    val duplicateBlobUri = "duplicate-blob-uri"
    service.batchCreateRequisitionMetadata(
      batchCreateRequisitionMetadataRequest {
        requests += createRequisitionMetadataRequest {
          requisitionMetadata = REQUISITION_METADATA.copy { blobUri = duplicateBlobUri }
          requestId = UUID.randomUUID().toString()
        }
      }
    )

    val conflictingRequest = createRequisitionMetadataRequest {
      requisitionMetadata = REQUISITION_METADATA_2.copy { blobUri = duplicateBlobUri }
      requestId = UUID.randomUUID().toString()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateRequisitionMetadata(
          batchCreateRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requests += conflictingRequest
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI.name
          metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] =
            REQUISITION_METADATA.dataProviderResourceId
          metadata[Errors.Metadata.BLOB_URI.key] = duplicateBlobUri
        }
      )
  }

  @Test
  fun `batchCreateRequisitionMetadata throws ALREADY_EXISTS for existing cmmsRequisition`() =
    runBlocking {
      val duplicateCmmsRequisition = "duplicate-cmms-requisition"
      service.batchCreateRequisitionMetadata(
        batchCreateRequisitionMetadataRequest {
          requests += createRequisitionMetadataRequest {
            requisitionMetadata =
              REQUISITION_METADATA.copy { cmmsRequisition = duplicateCmmsRequisition }
            requestId = UUID.randomUUID().toString()
          }
        }
      )

      val conflictingRequest = createRequisitionMetadataRequest {
        requisitionMetadata =
          REQUISITION_METADATA_2.copy { cmmsRequisition = duplicateCmmsRequisition }
        requestId = UUID.randomUUID().toString()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              requests += conflictingRequest
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] =
              REQUISITION_METADATA.dataProviderResourceId
            metadata[Errors.Metadata.CMMS_REQUISITION.key] = duplicateCmmsRequisition
          }
        )
    }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for inconsistent DataProviderId`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA.copy { dataProviderResourceId = "different-dp" }
        requestId = UUID.randomUUID().toString()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID // Mismatch with request inside
              requests += request
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)

      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.DATA_PROVIDER_MISMATCH.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = "different-dp"
            metadata[Errors.Metadata.EXPECTED_DATA_PROVIDER_RESOURCE_ID.key] =
              REQUISITION_METADATA.dataProviderResourceId
          }
        )
    }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for duplicate cmms requisition in the batch requests`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA
        requestId = UUID.randomUUID().toString()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              requests += request
              requests +=
                request.copy {
                  requisitionMetadata = requisitionMetadata.copy { blobUri = "different-blob-uri" }
                  requestId = UUID.randomUUID().toString()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)

      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "requests.1.requisition_metadata.cmms_requisition"
          }
        )
    }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for duplicate blob uri in the batch requests`() =
    runBlocking {
      val request = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA
        requestId = UUID.randomUUID().toString()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              requests += request
              requests +=
                request.copy {
                  requisitionMetadata =
                    requisitionMetadata.copy { cmmsRequisition = "different-cmms-requisition" }
                  requestId = UUID.randomUUID().toString()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)

      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.requisition_metadata.blob_uri"
          }
        )
    }

  @Test
  fun `batchCreateRequisitionMetadata throws INVALID_ARGUMENT for duplicate request id in the batch requests`() =
    runBlocking {
      val requestId = UUID.randomUUID().toString()
      val request1 = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA
        this.requestId = requestId
      }

      val request2 = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA_2
        this.requestId = requestId
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRequisitionMetadata(
            batchCreateRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              requests += request1
              requests += request2
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)

      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.request_id"
          }
        )
    }

  @Test
  fun `getRequisitionMetadata returns a requisition metadata`() = runBlocking {
    val startTime = Instant.now()
    service.createRequisitionMetadata(
      createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
    )

    val requisitionMetadata =
      service.getRequisitionMetadata(
        getRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
        }
      )

    assertThat(requisitionMetadata)
      .ignoringFields(
        RequisitionMetadata.CREATE_TIME_FIELD_NUMBER,
        RequisitionMetadata.UPDATE_TIME_FIELD_NUMBER,
        RequisitionMetadata.ETAG_FIELD_NUMBER,
      )
      .isEqualTo(REQUISITION_METADATA.copy { state = State.REQUISITION_METADATA_STATE_STORED })
    assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
    assertThat(requisitionMetadata.etag).isNotEmpty()
  }

  @Test
  fun `getRequisitionMetadata fails when the requisition metadata does not exist`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRequisitionMetadata(
          getRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getRequisitionMetadata fails when dataProviderResourceId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRequisitionMetadata(
          getRequisitionMetadataRequest {
            // missing dataProviderResourceId
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getRequisitionMetadata fails when requisitionMetadataResourceId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRequisitionMetadata(
          getRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            // requisitionMetadataResourceId is missing
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `lookupRequisitionMetadata by cmms requisition returns a requisition metadata`() =
    runBlocking {
      val startTime = Instant.now()
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

      val requisitionMetadata =
        service.lookupRequisitionMetadata(
          lookupRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            cmmsRequisition = CMMS_REQUISITION
          }
        )

      assertThat(requisitionMetadata)
        .ignoringFields(
          RequisitionMetadata.CREATE_TIME_FIELD_NUMBER,
          RequisitionMetadata.UPDATE_TIME_FIELD_NUMBER,
          RequisitionMetadata.ETAG_FIELD_NUMBER,
        )
        .isEqualTo(REQUISITION_METADATA.copy { state = State.REQUISITION_METADATA_STATE_STORED })
      assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
      assertThat(requisitionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `lookupRequisitionMetadata fails when requisition metadata does not exist`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.lookupRequisitionMetadata(
          lookupRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            cmmsRequisition = CMMS_REQUISITION
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `lookupRequisitionMetadata fails when dataProviderResourceId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.lookupRequisitionMetadata(
          lookupRequisitionMetadataRequest {
            // missing dataProviderResourceId
            cmmsRequisition = CMMS_REQUISITION
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `lookupRequisitionMetadata fails when lookup key is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.lookupRequisitionMetadata(
          lookupRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            // missing lookup key
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `fetchLatestCmmsCreateTime returns the latest timestamp`() = runBlocking {
    service.createRequisitionMetadata(
      createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
    )

    val latestTimestamp =
      service.fetchLatestCmmsCreateTime(
        fetchLatestCmmsCreateTimeRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(latestTimestamp).isEqualTo(CMMS_CREATE_TIME)
  }

  @Test
  fun `fetchLatestCmmsCreateTime returns the default timestamp with none requisition metadata`() =
    runBlocking {
      val latestTimestamp =
        service.fetchLatestCmmsCreateTime(
          fetchLatestCmmsCreateTimeRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )

      assertThat(latestTimestamp).isEqualTo(Timestamp.getDefaultInstance())
    }

  @Test
  fun `fetchLatestCmmsCreateTime failes when dataProviderResourceId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.fetchLatestCmmsCreateTime(
          fetchLatestCmmsCreateTimeRequest {
            // missing dataProviderResourceId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `queueRequisitionMetadata returns an updated requisition metadata`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val response =
      service.queueRequisitionMetadata(
        queueRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          workItem = WORK_ITEM
          etag = requisitionMetadata.etag
        }
      )

    assertThat(response.state).isEqualTo(State.REQUISITION_METADATA_STATE_QUEUED)
    assertThat(response.workItem).isEqualTo(WORK_ITEM)
    assertThat(response)
      .isEqualTo(
        service.getRequisitionMetadata(
          getRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          }
        )
      )
  }

  @Test
  fun `queueRequisitionMetadata fails when requisition metadata does not exist`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.queueRequisitionMetadata(
          queueRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            workItem = WORK_ITEM
            etag = "etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `queueRequisitionMetadata fails when dataProviderResourceId is missing`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.queueRequisitionMetadata(
          queueRequisitionMetadataRequest {
            // missing dataProviderResourceId
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            workItem = WORK_ITEM
            etag = requisitionMetadata.etag
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `queueRequisitionMetadata fails when requisitionMetadataResourceId is missing`() =
    runBlocking {
      val requisitionMetadata =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.queueRequisitionMetadata(
            queueRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              // missing requisitionMetadataResourceId
              workItem = WORK_ITEM
              etag = requisitionMetadata.etag
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `queueRequisitionMetadata fails when workItem is missing`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.queueRequisitionMetadata(
          queueRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            // missing workItem
            etag = requisitionMetadata.etag
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `queueRequisitionMetadata fails when etags mismatch`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.queueRequisitionMetadata(
          queueRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            workItem = WORK_ITEM
            etag = requisitionMetadata.etag + "mismatch"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `startProcessingRequisitionMetadata returns an updated requisition metadata`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val response =
      service.startProcessingRequisitionMetadata(
        startProcessingRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          etag = requisitionMetadata.etag
        }
      )

    assertThat(response.state).isEqualTo(State.REQUISITION_METADATA_STATE_PROCESSING)
    assertThat(response)
      .isEqualTo(
        service.getRequisitionMetadata(
          getRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          }
        )
      )
  }

  @Test
  fun `startProcessingRequisitionMetadata fails when requisition metadata does not exist`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.startProcessingRequisitionMetadata(
            startProcessingRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
              etag = "etag"
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `startProcessingRequisitionMetadata fails when dataProviderResourceId is missing`() =
    runBlocking {
      val requisitionMetadata =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.startProcessingRequisitionMetadata(
            startProcessingRequisitionMetadataRequest {
              // missing dataProviderResourceId
              requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
              etag = requisitionMetadata.etag
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `startProcessingRequisitionMetadata fails when requisitionMetadataResourceId is missing`() =
    runBlocking {
      val requisitionMetadata =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.startProcessingRequisitionMetadata(
            startProcessingRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              // missing requisitionMetadataResourceId
              etag = requisitionMetadata.etag
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `startProcessingRequisitionMetadata fails when etags mismatch`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.startProcessingRequisitionMetadata(
          startProcessingRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            etag = requisitionMetadata.etag + "mismatch"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `fulfillRequisitionMetadata returns an updated requisition metadata`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val response =
      service.fulfillRequisitionMetadata(
        fulfillRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          etag = requisitionMetadata.etag
        }
      )

    assertThat(response.state).isEqualTo(State.REQUISITION_METADATA_STATE_FULFILLED)
    assertThat(response)
      .isEqualTo(
        service.getRequisitionMetadata(
          getRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          }
        )
      )
  }

  @Test
  fun `fulfillRequisitionMetadata fails when requisition metadata does not exist`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.fulfillRequisitionMetadata(
          fulfillRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            etag = "etag"
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `fulfillRequisitionMetadata fails when dataProviderResourceId is missing`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.fulfillRequisitionMetadata(
          fulfillRequisitionMetadataRequest {
            // missing dataProviderResourceId
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            etag = requisitionMetadata.etag
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `fulfillRequisitionMetadata fails when requisitionMetadataResourceId is missing`() =
    runBlocking {
      val requisitionMetadata =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.fulfillRequisitionMetadata(
            fulfillRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              // missing requisitionMetadataResourceId
              etag = requisitionMetadata.etag
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `fulfillRequisitionMetadata fails when etags mismatch`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.fulfillRequisitionMetadata(
          fulfillRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            etag = requisitionMetadata.etag + "mismatch"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markWithdrawnRequisitionMetadata returns an updated requisition metadata`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val response =
      service.markWithdrawnRequisitionMetadata(
        markWithdrawnRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          etag = requisitionMetadata.etag
        }
      )

    assertThat(response.state).isEqualTo(State.REQUISITION_METADATA_STATE_WITHDRAWN)
    assertThat(response)
      .isEqualTo(
        service.getRequisitionMetadata(
          getRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          }
        )
      )
  }

  @Test
  fun `refuseRequisitionMetadata returns an updated requisition metadata`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val response =
      service.refuseRequisitionMetadata(
        refuseRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          refusalMessage = REFUSAL_MESSAGE
          etag = requisitionMetadata.etag
        }
      )

    assertThat(response.state).isEqualTo(State.REQUISITION_METADATA_STATE_REFUSED)
    assertThat(response.refusalMessage).isEqualTo(REFUSAL_MESSAGE)
    assertThat(response)
      .isEqualTo(
        service.getRequisitionMetadata(
          getRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          }
        )
      )
  }

  @Test
  fun `refuseRequisitionMetadata fails when requisition metadata does not exist`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.refuseRequisitionMetadata(
          refuseRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            refusalMessage = REFUSAL_MESSAGE
            etag = "etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `refuseRequisitionMetadata fails when dataProviderResourceId is missing`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.refuseRequisitionMetadata(
          refuseRequisitionMetadataRequest {
            // missing dataProviderResourceId
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            refusalMessage = REFUSAL_MESSAGE
            etag = requisitionMetadata.etag
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisitionMetadata fails when requisitionMetadataResourceId is missing`() =
    runBlocking {
      val requisitionMetadata =
        service.createRequisitionMetadata(
          createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.refuseRequisitionMetadata(
            refuseRequisitionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              // missing requisitionMetadataResourceId
              refusalMessage = REFUSAL_MESSAGE
              etag = requisitionMetadata.etag
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `refuseRequisitionMetadata fails when refusalMessage is missing`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.refuseRequisitionMetadata(
          refuseRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            // missing refusalMessage
            etag = requisitionMetadata.etag
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `refuseRequisitionMetadata fails when etags mismatch`() = runBlocking {
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.refuseRequisitionMetadata(
          refuseRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
            refusalMessage = REFUSAL_MESSAGE
            etag = requisitionMetadata.etag + "mismatch"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `listRequisitionMetadata returns empty when no RequisitionMetadata exist`() = runBlocking {
    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(response.requisitionMetadataList).isEmpty()
    assertThat(response.nextPageToken)
      .isEqualTo(ListRequisitionMetadataPageToken.getDefaultInstance())
  }

  @Test
  fun `listRequisitionMetadata returns all items when no filter`() = runBlocking {
    val created = createRequisitionMetadata(REQUISITION_METADATA_2, REQUISITION_METADATA_3)

    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(response)
      .isEqualTo(listRequisitionMetadataResponse { requisitionMetadata += created })
  }

  @Test
  fun `listRequisitionMetadata with page size returns first page`() = runBlocking {
    val created =
      createRequisitionMetadata(
        REQUISITION_METADATA_2,
        REQUISITION_METADATA_3,
        REQUISITION_METADATA_4,
      )

    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(response)
      .isEqualTo(
        listRequisitionMetadataResponse {
          requisitionMetadata += created.subList(0, 2)
          nextPageToken = listRequisitionMetadataPageToken {
            after =
              ListRequisitionMetadataPageTokenKt.after {
                requisitionMetadataResourceId = created[1].requisitionMetadataResourceId
                updateTime = created[1].updateTime
              }
          }
        }
      )
  }

  @Test
  fun `listRequisitionMetadata with page token returns next page`() = runBlocking {
    val created =
      createRequisitionMetadata(
        REQUISITION_METADATA_2,
        REQUISITION_METADATA_3,
        REQUISITION_METADATA_4,
      )

    val firstResponse =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
        }
      )

    val secondResponse =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
          pageToken = firstResponse.nextPageToken
        }
      )
    assertThat(secondResponse)
      .isEqualTo(listRequisitionMetadataResponse { requisitionMetadata += created[2] })
  }

  @Test
  fun `listRequisitionMetadata returns empty when filter matches nothing`() = runBlocking {
    createRequisitionMetadata(REQUISITION_METADATA_2)

    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter = ListRequisitionMetadataRequestKt.filter { groupId = "a-different-group-id" }
        }
      )

    assertThat(response).isEqualTo(ListRequisitionMetadataResponse.getDefaultInstance())
  }

  @Test
  fun `listRequisitionMetadata filters by groupID`(): Unit = runBlocking {
    val created =
      createRequisitionMetadata(
        REQUISITION_METADATA_2,
        REQUISITION_METADATA_3,
        REQUISITION_METADATA_4,
      )

    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter = ListRequisitionMetadataRequestKt.filter { groupId = GROUP_ID_2 }
        }
      )

    assertThat(response)
      .isEqualTo(listRequisitionMetadataResponse { requisitionMetadata += created.last() })
  }

  @Test
  fun `listRequisitionMetadata filters by report`(): Unit = runBlocking {
    val created =
      createRequisitionMetadata(
        REQUISITION_METADATA_2,
        REQUISITION_METADATA_3,
        REQUISITION_METADATA_4,
      )

    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter = ListRequisitionMetadataRequestKt.filter { report = REPORT_2 }
        }
      )

    assertThat(response)
      .isEqualTo(listRequisitionMetadataResponse { requisitionMetadata += created.last() })
  }

  @Test
  fun `listRequisitionMetadata filters by requisitionMetadataState`() = runBlocking {
    val created =
      createRequisitionMetadata(
        REQUISITION_METADATA_2,
        REQUISITION_METADATA_3,
        REQUISITION_METADATA_4,
      )

    val queuedRequisition =
      service.queueRequisitionMetadata(
        queueRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID_4
          workItem = WORK_ITEM
          etag = created.last().etag
        }
      )

    val response =
      service.listRequisitionMetadata(
        listRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter = ListRequisitionMetadataRequestKt.filter { state = queuedRequisition.state }
        }
      )

    assertThat(response)
      .isEqualTo(listRequisitionMetadataResponse { requisitionMetadata += queuedRequisition })
  }

  @Test
  fun `listRequisitionMetadata throws INVALID_ARGUMENT if dataProviderResourceId not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRequisitionMetadata(ListRequisitionMetadataRequest.getDefaultInstance())
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "data_provider_resource_id"
          }
        )
    }

  @Test
  fun `listRequisitionMetadata throws INVALID_ARGUMENT if pageSize is negative`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listRequisitionMetadata(
          listRequisitionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            pageSize = -1
          }
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

  private suspend fun createRequisitionMetadata(
    vararg requisitionMetadata: RequisitionMetadata
  ): List<RequisitionMetadata> {
    return requisitionMetadata.map { metadata ->
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest {
          this.requisitionMetadata = metadata
          requestId = UUID.randomUUID().toString()
        }
      )
    }
  }

  companion object {
    private val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private val REQUISITION_METADATA_RESOURCE_ID = "requisition-metadata-1"
    private val REQUISITION_METADATA_RESOURCE_ID_2 = "requisition-metadata-2"
    private val REQUISITION_METADATA_RESOURCE_ID_3 = "requisition-metadata-3"
    private val REQUISITION_METADATA_RESOURCE_ID_4 = "requisition-metadata-4"
    private val CMMS_REQUISITION = "dataProviders/data-provider-1/requisitions/requisition-1"
    private val CMMS_REQUISITION_2 = "dataProviders/data-provider-1/requisitions/requisition-2"
    private val CMMS_REQUISITION_3 = "dataProviders/data-provider-1/requisitions/requisition-3"
    private val CMMS_REQUISITION_4 = "dataProviders/data-provider-1/requisitions/requisition-4"
    private val BLOB_URI = "path/to/blob"
    private val BLOB_URI_2 = "path/to/blob2"
    private val BLOB_TYPE_URL = "blob.type.url"
    private val GROUP_ID = "group-1"
    private val GROUP_ID_2 = "group-2"
    private val CMMS_CREATE_TIME = timestamp { seconds = 12345 }
    private val REPORT = "measurementConsumers/measurement-consumer-1/reports/report-1"
    private val REPORT_2 = "measurementConsumers/measurement-consumer-1/reports/report-2"
    private val WORK_ITEM = "workItems/work-item-1"
    private val REFUSAL_MESSAGE = "refused by a reason"
    private val CREATE_REQUEST_ID = UUID.randomUUID().toString()

    private val REQUISITION_METADATA = requisitionMetadata {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
      cmmsRequisition = CMMS_REQUISITION
      blobUri = BLOB_URI
      blobTypeUrl = BLOB_TYPE_URL
      groupId = GROUP_ID
      cmmsCreateTime = CMMS_CREATE_TIME
      report = REPORT
    }

    private val REQUISITION_METADATA_2 = requisitionMetadata {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID_2
      cmmsRequisition = CMMS_REQUISITION_2
      blobUri = BLOB_URI_2
      blobTypeUrl = BLOB_TYPE_URL
      groupId = GROUP_ID
      cmmsCreateTime = CMMS_CREATE_TIME
      report = REPORT
    }

    private val REQUISITION_METADATA_3 = requisitionMetadata {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID_3
      cmmsRequisition = CMMS_REQUISITION_3
      blobUri = BLOB_URI
      blobTypeUrl = BLOB_TYPE_URL
      groupId = GROUP_ID
      cmmsCreateTime = CMMS_CREATE_TIME
      report = REPORT
    }

    private val REQUISITION_METADATA_4 = requisitionMetadata {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID_4
      cmmsRequisition = CMMS_REQUISITION_4
      blobUri = BLOB_URI
      blobTypeUrl = BLOB_TYPE_URL
      groupId = GROUP_ID_2
      cmmsCreateTime = CMMS_CREATE_TIME
      report = REPORT_2
    }
  }
}
