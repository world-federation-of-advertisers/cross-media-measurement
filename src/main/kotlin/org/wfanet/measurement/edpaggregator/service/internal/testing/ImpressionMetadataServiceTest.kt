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
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.interval
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
import org.wfanet.measurement.internal.edpaggregator.BatchCreateImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.ComputeModelLineBoundsResponse
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.batchCreateImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.batchDeleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.batchDeleteImpressionMetadataResponse
import org.wfanet.measurement.internal.edpaggregator.computeModelLineBoundsRequest
import org.wfanet.measurement.internal.edpaggregator.computeModelLineBoundsResponse
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.deleteImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.impressionMetadata
import org.wfanet.measurement.internal.edpaggregator.listImpressionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.listImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.listImpressionMetadataResponse

@RunWith(JUnit4::class)
abstract class ImpressionMetadataServiceTest {
  private lateinit var service: ImpressionMetadataServiceCoroutineImplBase

  protected abstract fun newService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): ImpressionMetadataServiceCoroutineImplBase

  @Before
  fun initService() {
    service = newService()
  }

  @Test
  fun `getImpressionMetadata returns an impression metadata`() = runBlocking {
    val startTime = Instant.now()
    service.createImpressionMetadata(
      createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
    )

    val impressionMetadata =
      service.getImpressionMetadata(
        getImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
        }
      )

    assertThat(impressionMetadata)
      .ignoringFields(
        ImpressionMetadata.CREATE_TIME_FIELD_NUMBER,
        ImpressionMetadata.UPDATE_TIME_FIELD_NUMBER,
        ImpressionMetadata.ETAG_FIELD_NUMBER,
      )
      .isEqualTo(IMPRESSION_METADATA.copy { state = State.IMPRESSION_METADATA_STATE_ACTIVE })
    assertThat(impressionMetadata.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(impressionMetadata.updateTime).isEqualTo(impressionMetadata.createTime)
    assertThat(impressionMetadata.etag).isNotEmpty()
  }

  @Test
  fun `getImpressionMetadata throws NOT_FOUND when ImpressionMetadata not found`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getImpressionMetadata(
          getImpressionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND.name
          metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
          metadata[Errors.Metadata.IMPRESSION_METADATA_RESOURCE_ID.key] =
            IMPRESSION_METADATA_RESOURCE_ID
        }
      )
  }

  @Test
  fun `getImpressionMetadata throws INVALID_ARGUMENT if dataProviderResourceId not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getImpressionMetadata(
            getImpressionMetadataRequest {
              impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
            }
          )
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
  fun `getImpressionMetadata throws INVALID_ARGUMENT if impressionMetadataResourceId not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getImpressionMetadata(
            getImpressionMetadataRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata_resource_id"
          }
        )
    }

  @Test
  fun `create impression metadata without request_id returns created impression metadata`() =
    runBlocking {
      val startTime = Instant.now()

      val impressionMetadata =
        service.createImpressionMetadata(
          createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
        )

      assertThat(impressionMetadata)
        .comparingExpectedFieldsOnly()
        .isEqualTo(IMPRESSION_METADATA.copy { State.IMPRESSION_METADATA_STATE_ACTIVE })
      assertThat(impressionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(impressionMetadata.updateTime).isEqualTo(impressionMetadata.createTime)
      assertThat(impressionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `create impression metadata with request_id returns created impression metadata`() =
    runBlocking {
      val startTime = Instant.now()

      val impressionMetadata =
        service.createImpressionMetadata(
          createImpressionMetadataRequest {
            impressionMetadata = IMPRESSION_METADATA
            requestId = CREATE_REQUEST_ID
          }
        )

      assertThat(impressionMetadata)
        .comparingExpectedFieldsOnly()
        .isEqualTo(IMPRESSION_METADATA.copy { State.IMPRESSION_METADATA_STATE_ACTIVE })
      assertThat(impressionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(impressionMetadata.updateTime).isEqualTo(impressionMetadata.createTime)
      assertThat(impressionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `create impression metadata with existing request_id returns existing impression metadata`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA
        requestId = CREATE_REQUEST_ID
      }

      val impressionMetadata = service.createImpressionMetadata(request)

      val impressionMetadata2 = service.createImpressionMetadata(request)

      assertThat(impressionMetadata2).isEqualTo(impressionMetadata)
    }

  @Test
  fun `multiple createImpressionMetadata return multiple impression metadata`() = runBlocking {
    val impressionMetadata1 =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              cmmsModelLine = MODEL_LINE_1
              clearImpressionMetadataResourceId()
              blobUri = "blobs/1"
            }
        }
      )

    val impressionMetadata2 =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              cmmsModelLine = MODEL_LINE_1
              clearImpressionMetadataResourceId()
              blobUri = "blobs/2"
            }
        }
      )

    assertThat(impressionMetadata1.impressionMetadataResourceId)
      .isNotEqualTo(impressionMetadata2.impressionMetadataResourceId)
  }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT if request id is malformed`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA
        requestId = "invalid-request-id"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "request_id"
          }
        )
    }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT if impressionMetadata is not set`() =
    runBlocking {
      val request = createImpressionMetadataRequest { requestId = CREATE_REQUEST_ID }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata"
          }
        )
    }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT if dataProviderId not set`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearDataProviderResourceId() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata.data_provider_resource_id"
        }
      )
  }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT if blobUri not set`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearBlobUri() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata.blob_uri"
        }
      )
  }

  @Test
  fun `createImpressionMetadata throws ALREADY_EXISTS if blobUri already exists`() = runBlocking {
    service.createImpressionMetadata(
      createImpressionMetadataRequest {
        impressionMetadata =
          IMPRESSION_METADATA.copy {
            cmmsModelLine =
              "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines/model-line-1"
          }
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createImpressionMetadata(
          createImpressionMetadataRequest {
            impressionMetadata =
              IMPRESSION_METADATA.copy {
                // same blobUri
                cmmsModelLine =
                  "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines/model-line-1"
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS.name
          metadata[Errors.Metadata.BLOB_URI.key] = IMPRESSION_METADATA.blobUri
        }
      )
  }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT if blobTypeUrl not set`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearBlobTypeUrl() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata.blob_type_url"
        }
      )
  }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT if eventGroupReferenceId not set`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA.copy { clearEventGroupReferenceId() }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "impression_metadata.event_group_reference_id"
          }
        )
    }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT if cmmsModelLine not set`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearCmmsModelLine() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata.cmms_model_line"
        }
      )
  }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT if interval not set`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearInterval() }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata.interval"
        }
      )
  }

  @Test
  fun `batchCreateImpressionMetadata returns created ImpressionMetadata`() = runBlocking {
    val request1 = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA
      requestId = UUID.randomUUID().toString()
    }
    val request2 = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA_2
      requestId = UUID.randomUUID().toString()
    }

    val response =
      service.batchCreateImpressionMetadata(
        batchCreateImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requests += request1
          requests += request2
        }
      )

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        batchCreateImpressionMetadataResponse {
          impressionMetadata += request1.impressionMetadata
          impressionMetadata += request2.impressionMetadata
        }
      )
    assertThat(response.impressionMetadataList.all { it.hasCreateTime() }).isTrue()
  }

  @Test
  fun `batchCreateImpressionMetadata without subrequests returns default response`() = runBlocking {
    val response =
      service.batchCreateImpressionMetadata(
        batchCreateImpressionMetadataRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(BatchCreateImpressionMetadataResponse.getDefaultInstance())
  }

  @Test
  fun `batchCreateImpressionMetadata is idempotent and creates new items in same request`() =
    runBlocking {
      val idempotentRequest = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA
        requestId = UUID.randomUUID().toString()
      }
      val initialResponse =
        service.batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requests += idempotentRequest
          }
        )

      assertThat(initialResponse)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          batchCreateImpressionMetadataResponse {
            impressionMetadata += idempotentRequest.impressionMetadata
          }
        )

      val existingImpressionMetadata = initialResponse.impressionMetadataList.single()

      val newRequest = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA_2
        requestId = UUID.randomUUID().toString()
      }
      val secondResponse =
        service.batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            requests += idempotentRequest // Idempotent request
            requests += newRequest // New request
          }
        )

      assertThat(secondResponse)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          batchCreateImpressionMetadataResponse {
            impressionMetadata += IMPRESSION_METADATA
            impressionMetadata += newRequest.impressionMetadata
          }
        )

      val newImpressionMetadata = secondResponse.impressionMetadataList[1]
      assertThat(newImpressionMetadata.updateTime.toInstant())
        .isGreaterThan(existingImpressionMetadata.updateTime.toInstant())
    }

  @Test
  fun `batchCreateImpressionMetadata throws ALREADY_EXISTS for existing blobUri`() = runBlocking {
    val duplicateBlobUri = "duplicate-blob-uri"
    service.batchCreateImpressionMetadata(
      batchCreateImpressionMetadataRequest {
        requests += createImpressionMetadataRequest {
          impressionMetadata = IMPRESSION_METADATA.copy { blobUri = duplicateBlobUri }
          requestId = UUID.randomUUID().toString()
        }
      }
    )

    val conflictingRequest = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA_2.copy { blobUri = duplicateBlobUri }
      requestId = UUID.randomUUID().toString()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
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
          reason = Errors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS.name
          metadata[Errors.Metadata.BLOB_URI.key] = duplicateBlobUri
        }
      )
  }

  @Test
  fun `batchCreateImpressionMetadata throws INVALID_ARGUMENT for inconsistent DataProviderId`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA.copy { dataProviderResourceId = "different-dp" }
        requestId = UUID.randomUUID().toString()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateImpressionMetadata(
            batchCreateImpressionMetadataRequest {
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
              IMPRESSION_METADATA.dataProviderResourceId
          }
        )
    }

  @Test
  fun `batchCreateImpressionMetadata throws INVALID_ARGUMENT for duplicate blob uri in the batch requests`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA
        requestId = UUID.randomUUID().toString()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateImpressionMetadata(
            batchCreateImpressionMetadataRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              requests += request
              requests += request.copy { requestId = UUID.randomUUID().toString() }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)

      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.impression_metadata.blob_uri"
          }
        )
    }

  @Test
  fun `batchCreateImpressionMetadata throws INVALID_ARGUMENT for duplicate request id in the batch requests`() =
    runBlocking {
      val requestId = UUID.randomUUID().toString()
      val request1 = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA
        this.requestId = requestId
      }

      val request2 = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA_2
        this.requestId = requestId
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateImpressionMetadata(
            batchCreateImpressionMetadataRequest {
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
  fun `deleteImpressionMetadata soft deletes and returns updated ImpressionMetadata`() =
    runBlocking {
      val created =
        service
          .batchCreateImpressionMetadata(
            batchCreateImpressionMetadataRequest {
              requests += createImpressionMetadataRequest {
                impressionMetadata = IMPRESSION_METADATA
              }
            }
          )
          .impressionMetadataList
          .single()

      val deleted =
        service.deleteImpressionMetadata(
          deleteImpressionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
          }
        )

      assertThat(deleted.updateTime.toInstant()).isGreaterThan(created.updateTime.toInstant())
      assertThat(deleted)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          created.copy {
            state = State.IMPRESSION_METADATA_STATE_DELETED
            clearUpdateTime()
            clearEtag()
          }
        )

      val got =
        service.getImpressionMetadata(
          getImpressionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
          }
        )
      assertThat(got).isEqualTo(deleted)
    }

  @Test
  fun `deleteImpressionMetadata throws INVALID_ARGUMENT when dataProviderResourceId is missing`() =
    runBlocking {
      val request = deleteImpressionMetadataRequest { impressionMetadataResourceId = "not-found" }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.deleteImpressionMetadata(request) }

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
  fun `deleteImpressionMetadata throws INVALID_ARGUMENT when impressioMetadataResourceId is missing`() =
    runBlocking {
      val request = deleteImpressionMetadataRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.deleteImpressionMetadata(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata_resource_id"
          }
        )
    }

  @Test
  fun `deleteImpressionMetadata throws NOT_FOUND when ImpressionMetadata not found`() =
    runBlocking {
      val request = deleteImpressionMetadataRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        impressionMetadataResourceId = "not-found"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.deleteImpressionMetadata(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = request.dataProviderResourceId
            metadata[Errors.Metadata.IMPRESSION_METADATA_RESOURCE_ID.key] =
              request.impressionMetadataResourceId
          }
        )
    }

  @Test
  fun `deleteImpressionMetadata throws NOT_FOUND when ImpressionMetadata was already deleted`() =
    runBlocking {
      val created =
        service.createImpressionMetadata(
          createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
        )

      val deleteRequest = deleteImpressionMetadataRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
      }

      val deleted = service.deleteImpressionMetadata(deleteRequest)

      assertThat(deleted.updateTime.toInstant()).isGreaterThan(created.updateTime.toInstant())
      assertThat(deleted)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          created.copy {
            state = State.IMPRESSION_METADATA_STATE_DELETED
            clearUpdateTime()
            clearEtag()
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> { service.deleteImpressionMetadata(deleteRequest) }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] =
              deleteRequest.dataProviderResourceId
            metadata[Errors.Metadata.IMPRESSION_METADATA_RESOURCE_ID.key] =
              deleteRequest.impressionMetadataResourceId
          }
        )
    }

  @Test
  fun `batchDeleteImpressionMetadata returns deleted ImpressionMetadata`() = runBlocking {
    val created1 =
      service.createImpressionMetadata(
        createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
      )
    val created2 =
      service.createImpressionMetadata(
        createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA_2 }
      )

    val startTime = Instant.now()

    val response =
      service.batchDeleteImpressionMetadata(
        batchDeleteImpressionMetadataRequest {
          requests += deleteImpressionMetadataRequest {
            dataProviderResourceId = created1.dataProviderResourceId
            impressionMetadataResourceId = created1.impressionMetadataResourceId
          }
          requests += deleteImpressionMetadataRequest {
            dataProviderResourceId = created2.dataProviderResourceId
            impressionMetadataResourceId = created2.impressionMetadataResourceId
          }
        }
      )

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        batchDeleteImpressionMetadataResponse {
          impressionMetadata +=
            created1.copy {
              state = State.IMPRESSION_METADATA_STATE_DELETED
              clearUpdateTime()
              clearEtag()
            }
          impressionMetadata +=
            created2.copy {
              state = State.IMPRESSION_METADATA_STATE_DELETED
              clearUpdateTime()
              clearEtag()
            }
        }
      )

    assertThat(response.impressionMetadataList.first().updateTime.toInstant())
      .isGreaterThan(startTime)
    assertThat(response.impressionMetadataList.last().updateTime.toInstant())
      .isGreaterThan(startTime)
  }

  @Test
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for missing ImpressionMetadata's dataProviderResourceId`() =
    runBlocking {
      val created1 =
        service.createImpressionMetadata(
          createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteImpressionMetadata(
            batchDeleteImpressionMetadataRequest {
              requests += deleteImpressionMetadataRequest {
                // missing dataProviderResourceId
                impressionMetadataResourceId = created1.impressionMetadataResourceId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.0.data_provider_resource_id"
          }
        )
    }

  @Test
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for inconsistent dataProviderResourceId`() =
    runBlocking {
      val created1 =
        service.createImpressionMetadata(
          createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
        )

      val created2 =
        service.createImpressionMetadata(
          createImpressionMetadataRequest {
            impressionMetadata =
              IMPRESSION_METADATA.copy { dataProviderResourceId = "data-provider-2" }
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteImpressionMetadata(
            batchDeleteImpressionMetadataRequest {
              requests += deleteImpressionMetadataRequest {
                dataProviderResourceId = created1.dataProviderResourceId
                impressionMetadataResourceId = created1.impressionMetadataResourceId
              }
              requests += deleteImpressionMetadataRequest {
                dataProviderResourceId = created2.dataProviderResourceId
                impressionMetadataResourceId = created1.impressionMetadataResourceId
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.data_provider_resource_id"
          }
        )
    }

  @Test
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for missing impressionMetadataResourceId`() =
    runBlocking {
      val created1 =
        service.createImpressionMetadata(
          createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteImpressionMetadata(
            batchDeleteImpressionMetadataRequest {
              requests += deleteImpressionMetadataRequest {
                dataProviderResourceId = created1.dataProviderResourceId
                // missing impressionMetadataResourceId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.0.impression_metadata_resource_id"
          }
        )
    }

  @Test
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for duplicate impressionMetadataResourceId`() =
    runBlocking {
      val created1 =
        service.createImpressionMetadata(
          createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteImpressionMetadata(
            batchDeleteImpressionMetadataRequest {
              requests += deleteImpressionMetadataRequest {
                dataProviderResourceId = created1.dataProviderResourceId
                impressionMetadataResourceId = created1.impressionMetadataResourceId
              }
              requests += deleteImpressionMetadataRequest {
                dataProviderResourceId = created1.dataProviderResourceId
                impressionMetadataResourceId = created1.impressionMetadataResourceId
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.impression_metadata_resource_id"
          }
        )
    }

  @Test
  fun `batchDeleteImpressionMetadata throws NOT_FOUND when ImpressionMetadata not found`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteImpressionMetadata(
            batchDeleteImpressionMetadataRequest {
              requests += deleteImpressionMetadataRequest {
                dataProviderResourceId = IMPRESSION_METADATA.dataProviderResourceId
                impressionMetadataResourceId = IMPRESSION_METADATA.impressionMetadataResourceId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] =
              IMPRESSION_METADATA.dataProviderResourceId
            metadata[Errors.Metadata.IMPRESSION_METADATA_RESOURCE_ID.key] =
              IMPRESSION_METADATA.impressionMetadataResourceId
          }
        )
    }

  @Test
  fun `batchDeleteImpressionMetadata throws NOT_FOUND when ImpressionMetadata already deleted`() =
    runBlocking {
      service.createImpressionMetadata(
        createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
      )

      service.deleteImpressionMetadata(
        deleteImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
        }
      )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteImpressionMetadata(
            batchDeleteImpressionMetadataRequest {
              requests += deleteImpressionMetadataRequest {
                dataProviderResourceId = IMPRESSION_METADATA.dataProviderResourceId
                impressionMetadataResourceId = IMPRESSION_METADATA.impressionMetadataResourceId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] =
              IMPRESSION_METADATA.dataProviderResourceId
            metadata[Errors.Metadata.IMPRESSION_METADATA_RESOURCE_ID.key] =
              IMPRESSION_METADATA.impressionMetadataResourceId
          }
        )
    }

  @Test
  fun `listImpressionMetadata returns empty when no ImpressionMetadata exist`() = runBlocking {
    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(response.impressionMetadataList).isEmpty()
    assertThat(response.nextPageToken)
      .isEqualTo(ListImpressionMetadataPageToken.getDefaultInstance())
  }

  @Test
  fun `listImpressionMetadata returns all items when no filter`() = runBlocking {
    val created =
      service
        .batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_2
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_3
            }
          }
        )
        .impressionMetadataList

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(response).isEqualTo(listImpressionMetadataResponse { impressionMetadata += created })
  }

  @Test
  fun `listImpressionMetadata with page size returns first page`() = runBlocking {
    val created =
      service
        .batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_2
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_3
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_4
            }
          }
        )
        .impressionMetadataList

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(response)
      .isEqualTo(
        listImpressionMetadataResponse {
          impressionMetadata += created.subList(0, 2)
          nextPageToken = listImpressionMetadataPageToken {
            after =
              ListImpressionMetadataPageTokenKt.after {
                impressionMetadataResourceId = created[1].impressionMetadataResourceId
              }
          }
        }
      )
  }

  @Test
  fun `listImpressionMetadata with page token returns next page`() = runBlocking {
    val created =
      service
        .batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_2
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_3
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_4
            }
          }
        )
        .impressionMetadataList

    val firstResponse =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
        }
      )

    val secondResponse =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          pageSize = 2
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse)
      .isEqualTo(listImpressionMetadataResponse { impressionMetadata += created[2] })
  }

  @Test
  fun `listImpressionMetadata returns empty when filter matches nothing`() = runBlocking {
    service.batchCreateImpressionMetadata(
      batchCreateImpressionMetadataRequest {
        requests += createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA_2 }
      }
    )

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter =
            ListImpressionMetadataRequestKt.filter { cmmsModelLine = "a-different-model-line" }
        }
      )

    assertThat(response).isEqualTo(ListImpressionMetadataResponse.getDefaultInstance())
  }

  @Test
  fun `listImpressionMetadata filters by cmmsModelLine`(): Unit = runBlocking {
    val created =
      service
        .batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            requests += createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_2
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_3
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_4
            }
          }
        )
        .impressionMetadataList

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter = ListImpressionMetadataRequestKt.filter { cmmsModelLine = MODEL_LINE_3 }
        }
      )

    assertThat(response)
      .isEqualTo(listImpressionMetadataResponse { impressionMetadata += created.last() })
  }

  @Test
  fun `listImpressionMetadata filters by eventGroupReferenceId`() = runBlocking {
    val created =
      service
        .batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_2
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_3
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_4
            }
          }
        )
        .impressionMetadataList

    val expected =
      created
        .filter { it.eventGroupReferenceId == "group-2" }
        .sortedBy { it.impressionMetadataResourceId }

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter = ListImpressionMetadataRequestKt.filter { eventGroupReferenceId = "group-2" }
        }
      )

    assertThat(response)
      .isEqualTo(listImpressionMetadataResponse { impressionMetadata += expected })
  }

  @Test
  fun `listImpressionMetadata filters by intervalOverlaps`(): Unit = runBlocking {
    val created =
      service
        .batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_2
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_3
            }
            requests += createImpressionMetadataRequest {
              impressionMetadata = IMPRESSION_METADATA_4
            }
          }
        )
        .impressionMetadataList

    val expected =
      created.filter { it.interval.startTime.seconds <= 450 && it.interval.endTime.seconds >= 350 }

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          filter =
            ListImpressionMetadataRequestKt.filter {
              intervalOverlaps = interval {
                startTime = timestamp { seconds = 350 }
                endTime = timestamp { seconds = 450 }
              }
            }
        }
      )

    assertThat(response)
      .isEqualTo(listImpressionMetadataResponse { impressionMetadata += expected })
  }

  @Test
  fun `listImpressionMetadata without state filter returns both active and deleted ImpressionMetadata`() =
    runBlocking {
      val (created1, created2) =
        service
          .batchCreateImpressionMetadata(
            batchCreateImpressionMetadataRequest {
              requests += createImpressionMetadataRequest {
                impressionMetadata = IMPRESSION_METADATA_2
              }
              requests += createImpressionMetadataRequest {
                impressionMetadata = IMPRESSION_METADATA_3
              }
            }
          )
          .impressionMetadataList
          .sortedBy { it.impressionMetadataResourceId }

      val deleted1 =
        service.deleteImpressionMetadata(
          deleteImpressionMetadataRequest {
            dataProviderResourceId = created1.dataProviderResourceId
            impressionMetadataResourceId = created1.impressionMetadataResourceId
          }
        )

      val response =
        service.listImpressionMetadata(
          listImpressionMetadataRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )

      assertThat(response)
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          listImpressionMetadataResponse {
            impressionMetadata += deleted1
            impressionMetadata += created2
          }
        )
    }

  @Test
  fun `listImpressionMetadata with state ACTIVE filter returns active ImpressionMetadata`() =
    runBlocking {
      val created =
        service
          .batchCreateImpressionMetadata(
            batchCreateImpressionMetadataRequest {
              requests += createImpressionMetadataRequest {
                impressionMetadata = IMPRESSION_METADATA_2
              }
              requests += createImpressionMetadataRequest {
                impressionMetadata = IMPRESSION_METADATA_3
              }
            }
          )
          .impressionMetadataList

      val response =
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            filter =
              ListImpressionMetadataRequestKt.filter {
                state = State.IMPRESSION_METADATA_STATE_ACTIVE
              }
          }
        )

      assertThat(response)
        .isEqualTo(listImpressionMetadataResponse { impressionMetadata += created })
    }

  @Test
  fun `listImpressionMetadata with state DELETED filter returns deleted ImpressionMetadata`() =
    runBlocking {
      val (created1, _) =
        service
          .batchCreateImpressionMetadata(
            batchCreateImpressionMetadataRequest {
              requests += createImpressionMetadataRequest {
                impressionMetadata = IMPRESSION_METADATA_2
              }
              requests += createImpressionMetadataRequest {
                impressionMetadata = IMPRESSION_METADATA_3
              }
            }
          )
          .impressionMetadataList

      val deleted1 =
        service.deleteImpressionMetadata(
          deleteImpressionMetadataRequest {
            dataProviderResourceId = created1.dataProviderResourceId
            impressionMetadataResourceId = created1.impressionMetadataResourceId
          }
        )

      val response =
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            filter =
              ListImpressionMetadataRequestKt.filter {
                state = State.IMPRESSION_METADATA_STATE_DELETED
              }
          }
        )

      assertThat(response)
        .isEqualTo(listImpressionMetadataResponse { impressionMetadata += deleted1 })
    }

  @Test
  fun `listImpressionMetadata throws INVALID_ARGUMENT if dataProviderResourceId not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listImpressionMetadata(ListImpressionMetadataRequest.getDefaultInstance())
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
  fun `listImpressionMetadata throws INVALID_ARGUMENT if pageSize is negative`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
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

  @Test
  fun `computeModelLineBounds returns bounds`() = runBlocking {
    service.batchCreateImpressionMetadata(
      batchCreateImpressionMetadataRequest {
        requests += createImpressionMetadataRequest {
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              cmmsModelLine = MODEL_LINE_1
              interval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
              clearImpressionMetadataResourceId()
              blobUri = "blobs/1"
            }
        }

        requests += createImpressionMetadataRequest {
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              cmmsModelLine = MODEL_LINE_1
              interval = interval {
                startTime = timestamp { seconds = 300 }
                endTime = timestamp { seconds = 400 }
              }
              clearImpressionMetadataResourceId()
              blobUri = "blobs/2"
            }
        }

        requests += createImpressionMetadataRequest {
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              cmmsModelLine = MODEL_LINE_2
              interval = interval {
                startTime = timestamp { seconds = 500 }
                endTime = timestamp { seconds = 700 }
              }
              clearImpressionMetadataResourceId()
              blobUri = "blobs/3"
            }
        }
      }
    )

    val request = computeModelLineBoundsRequest {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      cmmsModelLine += MODEL_LINE_1
      cmmsModelLine += MODEL_LINE_2
    }
    val response = service.computeModelLineBounds(request)

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        computeModelLineBoundsResponse {
          modelLineBounds.putAll(
            mapOf(
              MODEL_LINE_1 to
                interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 400 }
                },
              MODEL_LINE_2 to
                interval {
                  startTime = timestamp { seconds = 500 }
                  endTime = timestamp { seconds = 700 }
                },
            )
          )
        }
      )
  }

  @Test
  fun `ComputeModelLineBounds throws INVALID_ARGUMENT if dataProviderResourceId not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.computeModelLineBounds(
            computeModelLineBoundsRequest {
              // dataProviderResourceId not set
              cmmsModelLine += MODEL_LINE_1
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `ComputeModelLineBounds throws INVALID_ARGUMENT if cmmsModelLine not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.computeModelLineBounds(
          computeModelLineBoundsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            // cmmsModelLine not set
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `ComputeModelLineBounds returns empty for non-existent model lines`() = runBlocking {
    service.createImpressionMetadata(
      createImpressionMetadataRequest {
        impressionMetadata =
          IMPRESSION_METADATA.copy {
            cmmsModelLine = MODEL_LINE_1
            clearImpressionMetadataResourceId()
            blobUri = "blobs/1"
          }
      }
    )

    val request = computeModelLineBoundsRequest {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      cmmsModelLine += "non-existent-model-line"
    }
    val response = service.computeModelLineBounds(request)

    assertThat(response).isEqualTo(ComputeModelLineBoundsResponse.getDefaultInstance())
  }

  @Test
  fun `ComputeModelLineBounds returns partial for mix of existing and non-existent`() =
    runBlocking {
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              cmmsModelLine = MODEL_LINE_1
              interval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
              clearImpressionMetadataResourceId()
              blobUri = "blobs/1"
            }
        }
      )

      val request = computeModelLineBoundsRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        cmmsModelLine += MODEL_LINE_1
        cmmsModelLine += "non-existent-model-line"
      }
      val response = service.computeModelLineBounds(request)
      assertThat(response)
        .isEqualTo(
          computeModelLineBoundsResponse {
            modelLineBounds.putAll(
              mapOf(
                MODEL_LINE_1 to
                  interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 200 }
                  }
              )
            )
          }
        )
    }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private const val IMPRESSION_METADATA_RESOURCE_ID = "impression-metadata-1"
    private const val MODEL_LINE_PREFIX =
      "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines/"
    private const val MODEL_LINE_1 = MODEL_LINE_PREFIX + "model-line-1"
    private const val MODEL_LINE_2 = MODEL_LINE_PREFIX + "model-line-2"
    private const val MODEL_LINE_3 = MODEL_LINE_PREFIX + "model-line-3"
    private const val BLOB_URI = "path/to/blob"
    private const val BLOB_TYPE_URL = "blob.type.url"
    private const val EVENT_GROUP_REFERENCE_ID = "group-1"

    private val CREATE_REQUEST_ID = UUID.randomUUID().toString()

    private val IMPRESSION_METADATA = impressionMetadata {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
      cmmsModelLine = MODEL_LINE_1
      blobUri = BLOB_URI
      blobTypeUrl = BLOB_TYPE_URL
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
      interval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
    }

    private val IMPRESSION_METADATA_2 = impressionMetadata {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      impressionMetadataResourceId = "impression-metadata-2"
      cmmsModelLine = MODEL_LINE_2
      eventGroupReferenceId = "group-1"
      blobUri = "uri-1"
      blobTypeUrl = BLOB_TYPE_URL
      interval = interval {
        startTime = timestamp { seconds = 100 }
        endTime = timestamp { seconds = 200 }
      }
    }

    private val IMPRESSION_METADATA_3 = impressionMetadata {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      impressionMetadataResourceId = "impression-metadata-3"
      cmmsModelLine = MODEL_LINE_2
      eventGroupReferenceId = "group-2"
      blobUri = "uri-2"
      blobTypeUrl = BLOB_TYPE_URL
      interval = interval {
        startTime = timestamp { seconds = 300 }
        endTime = timestamp { seconds = 400 }
      }
    }

    private val IMPRESSION_METADATA_4 = impressionMetadata {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      impressionMetadataResourceId = "impression-metadata-4"
      cmmsModelLine = MODEL_LINE_3
      eventGroupReferenceId = "group-2"
      blobUri = "uri-3"
      blobTypeUrl = BLOB_TYPE_URL
      interval = interval {
        startTime = timestamp { seconds = 500 }
        endTime = timestamp { seconds = 600 }
      }
    }
  }
}
