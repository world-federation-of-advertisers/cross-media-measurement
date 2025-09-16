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
import java.util.*
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
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataResponse
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
  fun `getImpressionMetadata returns a impression metadata`() = runBlocking {
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
  fun `getImpressionMetadata fails when the impression metadata does not exist`() = runBlocking {
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
  }

  @Test
  fun `getImpressionMetadata fails when dataProviderResourceId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getImpressionMetadata(
          getImpressionMetadataRequest {
            impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getImpressionMetadata fails when impressionMetadataResourceId is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getImpressionMetadata(
          getImpressionMetadataRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `create impression metadata without request_id returns created impression metadata`() =
    runBlocking {
      val startTime = Instant.now()

      val request = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA
        // no request_id
      }
      val impressionMetadata = service.createImpressionMetadata(request)

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
  fun `create impression metadata with request_id returns created impression metadata`() =
    runBlocking {
      val startTime = Instant.now()

      val request = createImpressionMetadataRequest {
        impressionMetadata = IMPRESSION_METADATA
        requestId = CREATE_REQUEST_ID
      }
      val impressionMetadata = service.createImpressionMetadata(request)

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
  fun `createImpressionMetadata fails when dataProviderResourceName is missing`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearDataProviderResourceId() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createImpressionMetadata fails when blobUri is missing`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearBlobUri() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createImpressionMetadata fails when blobTypeUrl is missing`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearBlobTypeUrl() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createImpressionMetadata fails when blobUri is duplicated`() = runBlocking {
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
  }

  @Test
  fun `createImpressionMetadata fails when eventGroupReferenceId is missing`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearEventGroupReferenceId() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createImpressionMetadata fails when cmmsModelLine is missing`() = runBlocking {
    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearCmmsModelLine() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `deleteImpressionMetadata deletes ImpressionMetadata`() = runBlocking {
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
        service.getImpressionMetadata(
          getImpressionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
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
  fun `listImpressionMetadata returns ImpressionMetadata ordered by resource ID`() = runBlocking {
    val impressionMetadata: List<ImpressionMetadata> = createImpressionMetadata(service, 10)

    val response: ListImpressionMetadataResponse =
      service.listImpressionMetadata(ListImpressionMetadataRequest.getDefaultInstance())

    assertThat(response)
      .isEqualTo(listImpressionMetadataResponse { this.impressionMetadata += impressionMetadata })
  }

  @Test
  fun `listImpressionMetadata returns ImpressionMetadata when page size is specified`() =
    runBlocking {
      val impressionMetadata: List<ImpressionMetadata> = createImpressionMetadata(service, 10)

      val response: ListImpressionMetadataResponse =
        service.listImpressionMetadata(listImpressionMetadataRequest { pageSize = 10 })

      assertThat(response)
        .isEqualTo(listImpressionMetadataResponse { this.impressionMetadata += impressionMetadata })
    }

  @Test
  fun `listImpressionMetadata returns next page token when there are more results`() = runBlocking {
    val impressionMetadata: List<ImpressionMetadata> = createImpressionMetadata(service, 10)

    val request = listImpressionMetadataRequest { pageSize = 5 }
    val response: ListImpressionMetadataResponse = service.listImpressionMetadata(request)

    assertThat(response)
      .isEqualTo(
        listImpressionMetadataResponse {
          this.impressionMetadata += impressionMetadata.take(request.pageSize)
          nextPageToken = listImpressionMetadataPageToken {
            after =
              ListImpressionMetadataPageTokenKt.after {
                impressionMetadataResourceId = "impressionMetadata-0000000005"
              }
          }
        }
      )
  }

  @Test
  fun `listImpressionMetadata returns results after page token`() = runBlocking {
    val impressionMetadata: List<ImpressionMetadata> = createImpressionMetadata(service, 10)

    val request = listImpressionMetadataRequest {
      pageSize = 2
      pageToken = listImpressionMetadataPageToken {
        after =
          ListImpressionMetadataPageTokenKt.after {
            impressionMetadataResourceId = "impressionMetadata-0000000005"
          }
      }
    }
    val response: ListImpressionMetadataResponse = service.listImpressionMetadata(request)

    assertThat(response)
      .isEqualTo(
        listImpressionMetadataResponse {
          this.impressionMetadata += impressionMetadata.subList(5, 7)
          nextPageToken = listImpressionMetadataPageToken {
            after =
              ListImpressionMetadataPageTokenKt.after {
                impressionMetadataResourceId = "impressionMetadata-0000000007"
              }
          }
        }
      )
  }

  private suspend fun createImpressionMetadata(
    service: ImpressionMetadataServiceCoroutineImplBase,
    count: Int,
  ): List<ImpressionMetadata> {
    return (1..count).map {
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          requestId = UUID.randomUUID().toString()
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              impressionMetadataResourceId = String.format("impressionMetadata-%010d", it)
              blobUri = String.format("path/to/blob-%010d", it)
            }
        }
      )
    }
  }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private const val IMPRESSION_METADATA_RESOURCE_ID = "impression-metadata-1"
    private const val CMMS_MODEL_LINE =
      "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines/model-line-1"
    private const val BLOB_URI = "path/to/blob"
    private const val BLOB_TYPE_URL = "blob.type.url"
    private const val EVENT_GROUP_REFERENCE_ID = "group-1"

    private val CREATE_REQUEST_ID = UUID.randomUUID().toString()

    private val IMPRESSION_METADATA = impressionMetadata {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
      cmmsModelLine = CMMS_MODEL_LINE
      blobUri = BLOB_URI
      blobTypeUrl = BLOB_TYPE_URL
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
      interval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
    }
  }
}
