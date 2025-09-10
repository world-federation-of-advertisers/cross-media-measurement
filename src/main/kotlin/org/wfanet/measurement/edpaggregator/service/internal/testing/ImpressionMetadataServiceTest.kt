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
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.*
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.impressionMetadata

@RunWith(JUnit4::class)
abstract class ImpressionMetadataServiceTest {
  protected abstract fun initService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): ImpressionMetadataServiceCoroutineImplBase

  @Test
  fun `getImpressionMetadata returns a impression metadata`() = runBlocking {
    val service = initService()
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
    val service = initService()

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
    val service = initService()

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
    val service = initService()

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
      val service = initService()
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
      val service = initService()
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
      val service = initService()

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
    val service = initService()

    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearDataProviderResourceId() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createImpressionMetadata fails when blobUri is missing`() = runBlocking {
    val service = initService()

    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearBlobUri() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createImpressionMetadata fails when blobTypeUrl is missing`() = runBlocking {
    val service = initService()

    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearBlobTypeUrl() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createImpressionMetadata fails when blobUri is duplicated`() = runBlocking {
    val service = initService()

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
  fun `createRequisitionMetadata fails when eventGroupReferenceId is missing`() = runBlocking {
    val service = initService()

    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearEventGroupReferenceId() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createImpressionMetadata fails when cmmsModelLine is missing`() = runBlocking {
    val service = initService()

    val request = createImpressionMetadataRequest {
      impressionMetadata = IMPRESSION_METADATA.copy { clearCmmsModelLine() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  companion object {
    private val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private val IMPRESSION_METADATA_RESOURCE_ID = "impression-metadata-1"
    private val CMMS_MODEL_LINE =
      "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines/model-line-1"
    private val BLOB_URI = "path/to/blob"
    private val BLOB_TYPE_URL = "blob.type.url"
    private val EVENT_GROUP_REFERENCE_ID = "group-1"

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
