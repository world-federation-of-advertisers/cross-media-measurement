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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Empty
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.*
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerImpressionMetadataService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.ImpressionMetadataKey
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.deleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase as InternalImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub as InternalImpressionMetadataServiceCoroutineStub

@RunWith(JUnit4::class)
class ImpressionMetadataServiceTest {
  private lateinit var internalService: InternalImpressionMetadataServiceCoroutineImplBase
  private lateinit var service: ImpressionMetadataService

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    val idGenerator = IdGenerator.Default
    internalService =
      SpannerImpressionMetadataService(spannerDatabaseClient, EmptyCoroutineContext, idGenerator)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service =
      ImpressionMetadataService(
        InternalImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
      )
  }

  @Test
  fun `createImpressionMetadata with requestId returns a ImpressionMetadata successfully`() =
    runBlocking {
      val startTime = Instant.now()
      val request = createImpressionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata = IMPRESSION_METADATA
        requestId = REQUEST_ID
      }

      val impressionMetadata = service.createImpressionMetadata(request)

      assertThat(impressionMetadata).comparingExpectedFieldsOnly().isEqualTo(IMPRESSION_METADATA)

      val requisitionMetadataKey =
        assertNotNull(ImpressionMetadataKey.fromName(impressionMetadata.name))
      assertThat(requisitionMetadataKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(requisitionMetadataKey.impressionMetadataId).isNotEmpty()
      assertThat(impressionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(impressionMetadata.updateTime).isEqualTo(impressionMetadata.createTime)
      assertThat(impressionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `createImpressionMetadata without requestId returns a ImpressionMetadata successfully`() =
    runBlocking {
      val startTime = Instant.now()
      val request = createImpressionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata = IMPRESSION_METADATA
        // no request_id
      }

      val impressionMetadata = service.createImpressionMetadata(request)

      assertThat(impressionMetadata).comparingExpectedFieldsOnly().isEqualTo(IMPRESSION_METADATA)
      val impressionMetadataKey =
        assertNotNull(ImpressionMetadataKey.fromName(impressionMetadata.name))
      assertThat(impressionMetadataKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(impressionMetadataKey.impressionMetadataId).isNotEmpty()
      assertThat(impressionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(impressionMetadata.updateTime).isEqualTo(impressionMetadata.createTime)
      assertThat(impressionMetadata.etag).isNotEmpty()
    }

  @Test
  fun `createImpressionMetadata with existing requestId returns the existing ImpressionMetadata`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata = IMPRESSION_METADATA
        requestId = REQUEST_ID
      }
      val existingRequisitionMetadata = service.createImpressionMetadata(request)

      val requisitionMetadata = service.createImpressionMetadata(request)

      assertThat(requisitionMetadata).isEqualTo(existingRequisitionMetadata)
    }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val request = createImpressionMetadataRequest {
      // missing parent
      impressionMetadata = IMPRESSION_METADATA
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for invalid parent`() = runBlocking {
    val request = createImpressionMetadataRequest {
      parent = "invalid-parent-name"
      impressionMetadata = IMPRESSION_METADATA
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for invalid model_line`() = runBlocking {
    val request = createImpressionMetadataRequest {
      parent = DATA_PROVIDER_KEY.toName()
      impressionMetadata = IMPRESSION_METADATA.copy { modelLine = "invalid-model-line" }
      requestId = REQUEST_ID
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "model_line"
        }
      )
  }

  @Test
  fun `createImpressionMetadata throws IMPRESSION_METADATA_ALREADY_EXISTS from backend`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata = IMPRESSION_METADATA
        requestId = REQUEST_ID
      }

      service.createImpressionMetadata(request)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createImpressionMetadata(
            request.copy { requestId = UUID.randomUUID().toString() }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS.name
            metadata[Errors.Metadata.IMPRESSION_METADATA.key] = request.impressionMetadata.name
          }
        )
    }

  @Test
  fun `getImpressionMetadata returns a ImpressionMetadata successfully`() = runBlocking {
    val createdImpressionMetadata =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = getImpressionMetadataRequest { name = createdImpressionMetadata.name }
    val requisitionMetadata = service.getImpressionMetadata(request)

    assertThat(requisitionMetadata).isEqualTo(createdImpressionMetadata)
  }

  @Test
  fun `getRequisitionMetadata throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getImpressionMetadata(GetImpressionMetadataRequest.getDefaultInstance())
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
  fun `getRequisitionMetadata throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val request = getImpressionMetadataRequest { name = "invalid-name" }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.getImpressionMetadata(request) }
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
  fun `getRequisitionMetadata throws IMPRESSION_METADATA_NOT_FOUND from backend`() = runBlocking {
    val request = getImpressionMetadataRequest {
      name = "dataProviders/asdf/impressionMetadata/123"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.getImpressionMetadata(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND.name
          metadata[Errors.Metadata.IMPRESSION_METADATA.key] = request.name
        }
      )
  }

  @Test
  fun `deleteRole returns empty`() = runBlocking {
    val createdImpressionMetadata =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = deleteImpressionMetadataRequest { name = createdImpressionMetadata.name }
    val response = service.deleteImpressionMetadata(request)

    assertThat(response).isEqualTo(Empty.getDefaultInstance())
  }

  @Test
  fun `deleteRequisitionMetadata throws REQUIRED_FIELD_NOT_SET when name is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.deleteImpressionMetadata(DeleteImpressionMetadataRequest.getDefaultInstance())
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
  fun `deleteRequisitionMetadata throws INVALID_FEILD_VALUE when name is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.deleteImpressionMetadata(
            deleteImpressionMetadataRequest { name = "invalid-name" }
          )
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
  fun `deleteRequisitionMetadata throws IMPRESSION_METADATA_NOT_FOUND from backend`() =
    runBlocking {
      val request = deleteImpressionMetadataRequest {
        name = "dataProviders/data-provider-1/impressionMetadata/impression-metadata-1"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.deleteImpressionMetadata(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND.name
            metadata[Errors.Metadata.IMPRESSION_METADATA.key] = request.name
          }
        )
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private const val BLOB_URI = "path/to/blob"
    private const val BLOB_TYPE = "blob.type"
    private const val EVENT_GROUP_REFERENCE_ID = "event-group-1"
    private const val MODEL_LINE =
      "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines/model-line-1"
    private val REQUEST_ID = UUID.randomUUID().toString()

    private val IMPRESSION_METADATA = impressionMetadata {
      // name not set
      blobUri = BLOB_URI
      blobTypeUrl = BLOB_TYPE
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
      modelLine = MODEL_LINE
      interval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
      // state not set
    }
  }
}
