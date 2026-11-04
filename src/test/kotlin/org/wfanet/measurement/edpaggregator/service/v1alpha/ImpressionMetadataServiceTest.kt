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
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
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
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerDataAvailabilitySyncLeaseService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerImpressionMetadataService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.ImpressionMetadataKey
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLineBoundsResponseKt.modelLineBoundMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.DeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.UndeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchDeleteImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.batchUndeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchUpdateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLineBoundsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.deleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.getImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.undeleteImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.updateImpressionMetadataRequest
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase as InternalImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub as InternalImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageTokenKt as InternalListImpressionMetadataPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.acquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.internal.edpaggregator.listImpressionMetadataPageToken as internalListImpressionMetadataPageToken

@RunWith(JUnit4::class)
class ImpressionMetadataServiceTest {
  private lateinit var internalService: InternalImpressionMetadataServiceCoroutineImplBase
  private lateinit var service: ImpressionMetadataService

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalService = SpannerImpressionMetadataService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    runBlocking {
      SpannerDataAvailabilitySyncLeaseService(spannerDatabase.databaseClient)
        .acquireDataAvailabilitySyncLease(
          acquireDataAvailabilitySyncLeaseRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            synchronizationAttemptId = SYNCHRONIZATION_ATTEMPT_ID
            requestId = LEASE_REQUEST_ID
          }
        )
    }
    service =
      ImpressionMetadataService(
        InternalImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel)
      )
  }

  @Test
  fun `createImpressionMetadata requires synchronization lease`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createImpressionMetadata(
          createImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            impressionMetadata = IMPRESSION_METADATA
          }
        )
      }

    assertMissingLease(exception)
  }

  @Test
  fun `batchCreateImpressionMetadata requires synchronization lease`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            requests += createImpressionMetadataRequest {
              dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME
              this.parent = DATA_PROVIDER_KEY.toName()
              impressionMetadata = IMPRESSION_METADATA
            }
          }
        )
      }

    assertMissingLease(exception)
  }

  @Test
  fun `updateImpressionMetadata requires synchronization lease`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.updateImpressionMetadata(
          updateImpressionMetadataRequest {
            impressionMetadata =
              IMPRESSION_METADATA.copy {
                name = "${DATA_PROVIDER_KEY.toName()}/impressionMetadata/test"
              }
          }
        )
      }

    assertMissingLease(exception)
  }

  @Test
  fun `batchUpdateImpressionMetadata requires synchronization lease`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchUpdateImpressionMetadata(
          batchUpdateImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            requests += updateImpressionMetadataRequest {
              dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME
              impressionMetadata =
                IMPRESSION_METADATA.copy {
                  name = "${DATA_PROVIDER_KEY.toName()}/impressionMetadata/test"
                }
            }
          }
        )
      }

    assertMissingLease(exception)
  }

  @Test
  fun `undeleteImpressionMetadata requires synchronization lease`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.undeleteImpressionMetadata(
          undeleteImpressionMetadataRequest {
            name = "${DATA_PROVIDER_KEY.toName()}/impressionMetadata/test"
          }
        )
      }

    assertMissingLease(exception)
  }

  @Test
  fun `batchUndeleteImpressionMetadata requires synchronization lease`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchUndeleteImpressionMetadata(
          batchUndeleteImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            names += "${DATA_PROVIDER_KEY.toName()}/impressionMetadata/test"
          }
        )
      }

    assertMissingLease(exception)
  }

  private fun assertMissingLease(exception: StatusRuntimeException) {
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "data_availability_sync_lease"
        }
      )
  }

  @Test
  fun `createImpressionMetadata with requestId returns an ImpressionMetadata successfully`() =
    runBlocking {
      val startTime = Instant.now()
      val request = createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata = IMPRESSION_METADATA
        requestId = REQUEST_ID
      }

      val impressionMetadata = service.createImpressionMetadata(request)

      assertThat(impressionMetadata).comparingExpectedFieldsOnly().isEqualTo(IMPRESSION_METADATA)

      val impressionMetadataKey =
        assertNotNull(ImpressionMetadataKey.fromName(impressionMetadata.name))
      assertThat(impressionMetadataKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(impressionMetadataKey.impressionMetadataId).isNotEmpty()
      assertThat(impressionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(impressionMetadata.updateTime).isEqualTo(impressionMetadata.createTime)
    }

  @Test
  fun `createImpressionMetadata without requestId returns an ImpressionMetadata successfully`() =
    runBlocking {
      val startTime = Instant.now()
      val request = createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

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
    }

  @Test
  fun `createImpressionMetadata accepts done generation without raw upload`() = runBlocking {
    val expected = IMPRESSION_METADATA.copy { outputDoneBlobGeneration = 77L }
    val impressionMetadata =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          this.impressionMetadata = expected
          requestId = "33333333-3333-4333-8333-333333333333"
        }
      )

    assertThat(impressionMetadata).comparingExpectedFieldsOnly().isEqualTo(expected)
    assertThat(impressionMetadata.rawImpressionUpload).isEmpty()
    assertThat(impressionMetadata.outputDoneBlobGeneration).isEqualTo(77L)
  }

  @Test
  fun `updateImpressionMetadata accepts done generation without raw upload`() = runBlocking {
    val created =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
        }
      )
    val updated =
      service.updateImpressionMetadata(
        updateImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          impressionMetadata = created.copy { outputDoneBlobGeneration = 88L }
          requestId = "44444444-4444-4444-8444-444444444444"
        }
      )

    assertThat(updated.rawImpressionUpload).isEmpty()
    assertThat(updated.outputDoneBlobGeneration).isEqualTo(88L)
  }

  @Test
  fun `updateImpressionMetadata moves deterministic output to corrected upload`() = runBlocking {
    val sourceUploadA = DATA_PROVIDER_KEY.toName() + "/rawImpressionUploads/upload-a"
    val sourceUploadB = DATA_PROVIDER_KEY.toName() + "/rawImpressionUploads/upload-b"
    val created =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              rawImpressionUpload = sourceUploadA
              outputDoneBlobGeneration = 77L
            }
        }
      )
    val corrected =
      service.updateImpressionMetadata(
        updateImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          impressionMetadata =
            created.copy {
              rawImpressionUpload = sourceUploadB
              outputDoneBlobGeneration = 88L
            }
          requestId = "22222222-2222-4222-8222-222222222222"
        }
      )

    val oldUploadResponse =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter = ListImpressionMetadataRequestKt.filter { rawImpressionUpload = sourceUploadA }
        }
      )
    val correctedUploadResponse =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter = ListImpressionMetadataRequestKt.filter { rawImpressionUpload = sourceUploadB }
        }
      )

    assertThat(corrected.rawImpressionUpload).isEqualTo(sourceUploadB)
    assertThat(corrected.outputDoneBlobGeneration).isEqualTo(88L)
    assertThat(oldUploadResponse.impressionMetadataList).isEmpty()
    assertThat(correctedUploadResponse.impressionMetadataList).containsExactly(corrected)
    Unit
  }

  @Test
  fun `createImpressionMetadata with existing requestId returns the existing ImpressionMetadata`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

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
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

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
  fun `createImpressionMetadata throws INVALID_ARGUMENT for invalid parent`() = runBlocking {
    val request = createImpressionMetadataRequest {
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

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
  fun `createRequisitionMetadata throws INVALID_ARGUMENT for invalid request id`() = runBlocking {
    val request = createImpressionMetadataRequest {
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

      parent = DATA_PROVIDER_KEY.toName()
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
  fun `createImpressionMetadata throws INVALID_ARGUMENT when impressionMetadata is missing`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
        // missing impressionMetadata
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata"
          }
        )
    }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT for invalid model_line`() = runBlocking {
    val request = createImpressionMetadataRequest {
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

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
          metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata.model_line"
        }
      )
  }

  @Test
  fun `createImpressionMetadata throws IMPRESSION_METADATA_ALREADY_EXISTS from backend`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

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
            metadata[Errors.Metadata.BLOB_URI.key] = IMPRESSION_METADATA.blobUri
          }
        )
    }

  @Test
  fun `batchCreateImpressionMetadata returns created ImpressionMetadata`() = runBlocking {
    val request1 = createImpressionMetadataRequest {
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

      parent = DATA_PROVIDER_KEY.toName()
      impressionMetadata = IMPRESSION_METADATA
      requestId = UUID.randomUUID().toString()
    }
    val request2 = createImpressionMetadataRequest {
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

      parent = DATA_PROVIDER_KEY.toName()
      impressionMetadata = IMPRESSION_METADATA_2
      requestId = UUID.randomUUID().toString()
    }

    val startTime = Instant.now()

    val response =
      service.batchCreateImpressionMetadata(
        batchCreateImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
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

    assertThat(response.impressionMetadataList.all { it.name.isNotEmpty() }).isTrue()
    assertThat(response.impressionMetadataList.all { it.createTime.toInstant() >= startTime })
      .isTrue()
    assertThat(
        response.impressionMetadataList.all {
          it.updateTime.toInstant() == it.createTime.toInstant()
        }
      )
      .isTrue()
    assertThat(response.impressionMetadataList.all { it.state == ImpressionMetadata.State.ACTIVE })
      .isTrue()
  }

  @Test
  fun `batchCreateImpressionMetadata is idempotent`() = runBlocking {
    val idempotentRequest = createImpressionMetadataRequest {
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

      parent = DATA_PROVIDER_KEY.toName()
      impressionMetadata = IMPRESSION_METADATA
      requestId = UUID.randomUUID().toString()
    }
    val initialResponse =
      service.batchCreateImpressionMetadata(
        batchCreateImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          requests += idempotentRequest
        }
      )
    val existingImpressionMetadata = initialResponse.impressionMetadataList.single()

    val newRequest = createImpressionMetadataRequest {
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

      parent = DATA_PROVIDER_KEY.toName()
      impressionMetadata = IMPRESSION_METADATA_2
      requestId = UUID.randomUUID().toString()
    }
    val secondResponse =
      service.batchCreateImpressionMetadata(
        batchCreateImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          requests += idempotentRequest
          requests += newRequest
        }
      )

    assertThat(secondResponse.impressionMetadataList.first()).isEqualTo(existingImpressionMetadata)

    val newImpressionMetadata = secondResponse.impressionMetadataList.last()
    assertThat(newImpressionMetadata.name).isNotEqualTo(existingImpressionMetadata.name)
    assertThat(newImpressionMetadata.createTime.toInstant())
      .isGreaterThan(existingImpressionMetadata.createTime.toInstant())
    assertThat(newImpressionMetadata.updateTime.toInstant())
      .isGreaterThan(existingImpressionMetadata.updateTime.toInstant())
    assertThat(newImpressionMetadata.state).isEqualTo(ImpressionMetadata.State.ACTIVE)
  }

  @Test
  fun `batchCreateImpressionMetadata throws INVALID_ARGUMENT for missing parent`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            requests += createImpressionMetadataRequest {
              dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

              impressionMetadata = IMPRESSION_METADATA
              requestId = REQUEST_ID
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
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `batchCreateImpressionMetadata throws INVALID_ARGUMENT for malformed parent`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            parent = "invalid-parent"
            requests += createImpressionMetadataRequest {
              dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

              impressionMetadata = IMPRESSION_METADATA
              requestId = REQUEST_ID
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
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `batchCreateImpressionMetadata throws INVALID_ARGUMENT for malformed request id`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateImpressionMetadata(
            batchCreateImpressionMetadataRequest {
              dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

              parent = DATA_PROVIDER_KEY.toName()
              requests += createImpressionMetadataRequest {
                dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

                parent = DATA_PROVIDER_KEY.toName()
                impressionMetadata = IMPRESSION_METADATA
                requestId = "invalid-request-id"
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.0.request_id"
          }
        )
    }

  @Test
  fun `batchCreateImpressionMetadata throws INVALID_ARGUMENT for duplicate request id`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateImpressionMetadata(
            batchCreateImpressionMetadataRequest {
              dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

              parent = DATA_PROVIDER_KEY.toName()
              requests += createImpressionMetadataRequest {
                dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

                parent = DATA_PROVIDER_KEY.toName()
                impressionMetadata = IMPRESSION_METADATA
                requestId = REQUEST_ID
              }
              requests += createImpressionMetadataRequest {
                dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

                parent = DATA_PROVIDER_KEY.toName()
                impressionMetadata = IMPRESSION_METADATA_2
                requestId = REQUEST_ID
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.request_id"
          }
        )
    }

  @Test
  fun `batchCreateImpressionMetadata throws ALREADY_EXISTS for duplicate blobUri`() = runBlocking {
    val duplicateBlobUri = "duplicate-blob-uri"
    val request1 = createImpressionMetadataRequest {
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

      parent = DATA_PROVIDER_KEY.toName()
      impressionMetadata = IMPRESSION_METADATA.copy { blobUri = duplicateBlobUri }
      requestId = UUID.randomUUID().toString()
    }
    val request2 = createImpressionMetadataRequest {
      dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

      parent = DATA_PROVIDER_KEY.toName()
      impressionMetadata = IMPRESSION_METADATA_2.copy { blobUri = duplicateBlobUri }
      requestId = UUID.randomUUID().toString()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateImpressionMetadata(
          batchCreateImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            parent = DATA_PROVIDER_KEY.toName()
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
          metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.impression_metadata.blob_uri"
        }
      )
  }

  @Test
  fun `getImpressionMetadata returns an ImpressionMetadata successfully`() = runBlocking {
    val createdImpressionMetadata =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = getImpressionMetadataRequest { name = createdImpressionMetadata.name }
    val impressionMetadata = service.getImpressionMetadata(request)

    assertThat(impressionMetadata).isEqualTo(createdImpressionMetadata)
  }

  @Test
  fun `getImpressionMetadata returns a deleted ImpressionMetadata successfully`() = runBlocking {
    val createdImpressionMetadata =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )

    val deleted =
      service.deleteImpressionMetadata(
        deleteImpressionMetadataRequest { name = createdImpressionMetadata.name }
      )

    val got =
      service.getImpressionMetadata(
        getImpressionMetadataRequest { name = createdImpressionMetadata.name }
      )

    assertThat(got)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        createdImpressionMetadata.copy {
          state = ImpressionMetadata.State.DELETED
          clearUpdateTime()
        }
      )
    assertThat(got.updateTime.toInstant()).isEqualTo(deleted.updateTime.toInstant())
  }

  @Test
  fun `getImpressionMetadata throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
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
  fun `getImpressionMetadata throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
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
  fun `getImpressionMetadata throws IMPRESSION_METADATA_NOT_FOUND from backend`() = runBlocking {
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
  fun `deleteImpressionMetadata returns ImpressionMetadata`() = runBlocking {
    val created =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )

    val request = deleteImpressionMetadataRequest { name = created.name }
    val response = service.deleteImpressionMetadata(request)

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        created.copy {
          state = ImpressionMetadata.State.DELETED
          clearUpdateTime()
        }
      )
    assertThat(response.updateTime.toInstant()).isGreaterThan(created.updateTime.toInstant())
  }

  @Test
  fun `deleteImpressionMetadata throws REQUIRED_FIELD_NOT_SET when name is not set`() =
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
  fun `deleteImpressionMetadata throws INVALID_FEILD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.deleteImpressionMetadata(deleteImpressionMetadataRequest { name = "invalid-name" })
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
  fun `deleteImpressionMetadata throws IMPRESSION_METADATA_NOT_FOUND for non-existent ImpressionMetadata from backend`() =
    runBlocking {
      val created =
        service.createImpressionMetadata(
          createImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            parent = DATA_PROVIDER_KEY.toName()
            impressionMetadata = IMPRESSION_METADATA
            requestId = REQUEST_ID
          }
        )

      val request = deleteImpressionMetadataRequest { name = created.name }
      service.deleteImpressionMetadata(request)

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

  @Test
  fun `deleteImpressionMetadata throws IMPRESSION_METADATA_NOT_FOUND for already deleted ImpressionMetadata from backend`() =
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

  @Test
  fun `undeleteImpressionMetadata returns restored ImpressionMetadata`() = runBlocking {
    val created =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )
    val deleted =
      service.deleteImpressionMetadata(deleteImpressionMetadataRequest { name = created.name })

    val response =
      service.undeleteImpressionMetadata(
        undeleteImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME
          name = created.name
        }
      )

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        created.copy {
          state = ImpressionMetadata.State.ACTIVE
          clearUpdateTime()
        }
      )
    assertThat(response.updateTime.toInstant()).isGreaterThan(deleted.updateTime.toInstant())
  }

  @Test
  fun `undeleteImpressionMetadata throws REQUIRED_FIELD_NOT_SET when name is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.undeleteImpressionMetadata(UndeleteImpressionMetadataRequest.getDefaultInstance())
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
  fun `undeleteImpressionMetadata throws INVALID_FIELD_VALUE when name is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.undeleteImpressionMetadata(
            undeleteImpressionMetadataRequest {
              dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME
              name = "invalid-name"
            }
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
  fun `undeleteImpressionMetadata throws IMPRESSION_METADATA_NOT_FOUND when resource does not exist`() =
    runBlocking {
      val request = undeleteImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        name = "${DATA_PROVIDER_KEY.toName()}/impressionMetadata/not-found"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.undeleteImpressionMetadata(request) }

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
  fun `undeleteImpressionMetadata throws ALREADY_EXISTS when resource is active`() = runBlocking {
    val created =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.undeleteImpressionMetadata(
          undeleteImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME
            name = created.name
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS.name
          metadata[Errors.Metadata.IMPRESSION_METADATA.key] = created.name
        }
      )
  }

  @Test
  fun `batchDeleteImpressionMetadata returns ImpressionMetadata`() = runBlocking {
    val created1 =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )
    val created2 =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA_2
          requestId = UUID.randomUUID().toString()
        }
      )

    val beforeDelete = Instant.now()
    val response =
      service.batchDeleteImpressionMetadata(
        batchDeleteImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          names += created1.name
          names += created2.name
        }
      )

    assertThat(response)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        batchDeleteImpressionMetadataResponse {
          impressionMetadata +=
            created1.copy {
              state = ImpressionMetadata.State.DELETED
              clearUpdateTime()
            }
          impressionMetadata +=
            created2.copy {
              state = ImpressionMetadata.State.DELETED
              clearUpdateTime()
            }
        }
      )
    assertThat(response.impressionMetadataList.first().updateTime.toInstant())
      .isGreaterThan(beforeDelete)
    assertThat(response.impressionMetadataList.last().updateTime.toInstant())
      .isGreaterThan(beforeDelete)
  }

  @Test
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for missing parent`() = runBlocking {
    val created1 =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchDeleteImpressionMetadata(
          batchDeleteImpressionMetadataRequest {
            // missing parent
            names += created1.name
          }
        )
      }

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
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for malformed parent`() = runBlocking {
    val created1 =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchDeleteImpressionMetadata(
          batchDeleteImpressionMetadataRequest {
            parent = "invalid-parent"
            names += created1.name
          }
        )
      }

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
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for missing names`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchDeleteImpressionMetadata(
          batchDeleteImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            // missing names
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "names"
        }
      )
  }

  @Test
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for empty name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchDeleteImpressionMetadata(
          batchDeleteImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            names += ""
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "names.0"
        }
      )
  }

  @Test
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for malformed name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchDeleteImpressionMetadata(
          batchDeleteImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            names += "invalid-name"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "names.0"
        }
      )
  }

  @Test
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for ImpressionMetadata that doesn't belong to parent`() =
    runBlocking {
      val created1 =
        service.createImpressionMetadata(
          createImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            parent = DATA_PROVIDER_KEY.toName()
            impressionMetadata = IMPRESSION_METADATA
            requestId = REQUEST_ID
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteImpressionMetadata(
            batchDeleteImpressionMetadataRequest {
              parent = DataProviderKey(externalIdToApiId(222L)).toName()
              names += created1.name
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "names.0"
          }
        )
    }

  @Test
  fun `batchDeleteImpressionMetadata throws INVALID_ARGUMENT for duplicate names`() = runBlocking {
    val created1 =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA
          requestId = REQUEST_ID
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchDeleteImpressionMetadata(
          batchDeleteImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            names += created1.name
            names += created1.name
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "names.1"
        }
      )
  }

  @Test
  fun `batchDeleteImpressionMetadata throws NOT_FOUND for non-existent ImpressionMetadata`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteImpressionMetadata(
            batchDeleteImpressionMetadataRequest {
              parent = DATA_PROVIDER_KEY.toName()
              names += DATA_PROVIDER_KEY.toName() + "/impressionMetadata/impression-metadata-999"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND.name
            metadata[Errors.Metadata.IMPRESSION_METADATA.key] =
              DATA_PROVIDER_KEY.toName() + "/impressionMetadata/impression-metadata-999"
          }
        )
    }

  @Test
  fun `batchDeleteImpressionMetadata throws NOT_FOUND for already deleted ImpressionMetadata`() =
    runBlocking {
      val created =
        service.createImpressionMetadata(
          createImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            parent = DATA_PROVIDER_KEY.toName()
            impressionMetadata = IMPRESSION_METADATA
          }
        )

      service.deleteImpressionMetadata(deleteImpressionMetadataRequest { name = created.name })

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchDeleteImpressionMetadata(
            batchDeleteImpressionMetadataRequest {
              parent = DATA_PROVIDER_KEY.toName()
              names += created.name
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_METADATA_NOT_FOUND.name
            metadata[Errors.Metadata.IMPRESSION_METADATA.key] = created.name
          }
        )
    }

  @Test
  fun `listImpressionMetadata returns ImpressionMetadata`() = runBlocking {
    val created = createImpressionMetadata(IMPRESSION_METADATA)

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest { parent = DATA_PROVIDER_KEY.toName() }
      )

    assertThat(response).isEqualTo(listImpressionMetadataResponse { impressionMetadata += created })
  }

  @Test
  fun `listImpressionMetadata with page size returns ImpressionMetadata`() = runBlocking {
    val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)
    val sortedCreated = created.sortedBy { it.name }

    val internalPageToken = internalListImpressionMetadataPageToken {
      after =
        InternalListImpressionMetadataPageTokenKt.after {
          impressionMetadataResourceId =
            ImpressionMetadataKey.fromName(sortedCreated[0].name)!!.impressionMetadataId
        }
    }

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
        }
      )

    assertThat(response)
      .isEqualTo(
        listImpressionMetadataResponse {
          impressionMetadata += sortedCreated[0]
          nextPageToken = internalPageToken.toByteString().base64UrlEncode()
        }
      )
  }

  @Test
  fun `listImpressionMetadata with page token returns ImpressionMetadata`() = runBlocking {
    val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)
    val sortedCreated = created.sortedBy { it.name }

    val firstResponse =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
        }
      )

    val secondResponse =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          pageSize = 1
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse)
      .isEqualTo(listImpressionMetadataResponse { impressionMetadata += sortedCreated[1] })
  }

  @Test
  fun `listImpressionMetadata with ModelLine filter returns ImpressionMetadata`() = runBlocking {
    val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter = ListImpressionMetadataRequestKt.filter { modelLine = created[0].modelLine }
        }
      )

    assertThat(response)
      .isEqualTo(listImpressionMetadataResponse { impressionMetadata += created[0] })
  }

  @Test
  fun `listImpressionMetadata with event group reference ids filter returns ImpressionMetadata`() =
    runBlocking {
      val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)

      // The batched filter forwards both reference ids to the internal service.
      val response =
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            filter =
              ListImpressionMetadataRequestKt.filter {
                eventGroupReferenceIds +=
                  listOf(created[0].eventGroupReferenceId, created[1].eventGroupReferenceId)
              }
          }
        )

      assertThat(response)
        .isEqualTo(
          listImpressionMetadataResponse { impressionMetadata += created.sortedBy { it.name } }
        )
    }

  @Test
  fun `listImpressionMetadata with interval overlaps filter returns ImpressionMetadata`() =
    runBlocking {
      val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)

      val response =
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            filter =
              ListImpressionMetadataRequestKt.filter {
                intervalOverlaps = interval {
                  startTime = timestamp { seconds = created[0].interval.startTime.seconds + 1 }
                  endTime = timestamp { seconds = created[0].interval.startTime.seconds + 1 }
                }
              }
          }
        )

      assertThat(response)
        .isEqualTo(listImpressionMetadataResponse { impressionMetadata += created[0] })
    }

  @Test
  fun `listImpressionMetadata with blob_uri_prefix filter returns ImpressionMetadata`() =
    runBlocking {
      val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)

      // "path/" prefix should match "path/to/blob" but not "uri-2"
      val response =
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            filter = ListImpressionMetadataRequestKt.filter { blobUriPrefix = "path/" }
          }
        )

      assertThat(response)
        .isEqualTo(listImpressionMetadataResponse { impressionMetadata += created[0] })
    }

  @Test
  fun `listImpressionMetadata includes deleted ImpressionMetadata when show deleted is true`() =
    runBlocking {
      val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)
      val deleted =
        service.deleteImpressionMetadata(deleteImpressionMetadataRequest { name = created[0].name })

      val response =
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            showDeleted = true
          }
        )

      assertThat(response)
        .isEqualTo(
          listImpressionMetadataResponse {
            impressionMetadata += listOf(deleted, created[1]).sortedBy { it.name }
          }
        )
    }

  @Test
  fun `listImpressionMetadata filters deleted ImpressionMetadata by state`() = runBlocking {
    val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)
    val deleted =
      service.deleteImpressionMetadata(deleteImpressionMetadataRequest { name = created[0].name })

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          showDeleted = true
          filter =
            ListImpressionMetadataRequestKt.filter { state = ImpressionMetadata.State.DELETED }
        }
      )

    assertThat(response).isEqualTo(listImpressionMetadataResponse { impressionMetadata += deleted })
  }

  @Test
  fun `listImpressionMetadata filters active ImpressionMetadata when show deleted is true`() =
    runBlocking {
      val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)
      service.deleteImpressionMetadata(deleteImpressionMetadataRequest { name = created[0].name })

      val response =
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            showDeleted = true
            filter =
              ListImpressionMetadataRequestKt.filter { state = ImpressionMetadata.State.ACTIVE }
          }
        )

      assertThat(response)
        .isEqualTo(listImpressionMetadataResponse { impressionMetadata += created[1] })
    }

  @Test
  fun `listImpressionMetadata rejects deleted state without show deleted`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            filter =
              ListImpressionMetadataRequestKt.filter { state = ImpressionMetadata.State.DELETED }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "filter.state"
        }
      )
  }

  @Test
  fun `batchUndeleteImpressionMetadata restores deleted metadata`() = runBlocking {
    val created = createImpressionMetadata(IMPRESSION_METADATA, IMPRESSION_METADATA_2)
    service.batchDeleteImpressionMetadata(
      batchDeleteImpressionMetadataRequest {
        parent = DATA_PROVIDER_KEY.toName()
        names += created.map { it.name }
      }
    )

    val response =
      service.batchUndeleteImpressionMetadata(
        batchUndeleteImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          names += created.map { it.name }
        }
      )

    assertThat(response.impressionMetadataList.map { it.state })
      .containsExactly(ImpressionMetadata.State.ACTIVE, ImpressionMetadata.State.ACTIVE)
      .inOrder()
  }

  @Test
  fun `batchUndeleteImpressionMetadata rejects name under another parent`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchUndeleteImpressionMetadata(
          batchUndeleteImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            parent = DATA_PROVIDER_KEY.toName()
            names += "dataProviders/another/impressionMetadata/impression-1"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listImpressionMetadata throws INVALID_ARGUMENT when parent is not set`() = runBlocking {
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
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `listImpressionMetadata throws INVALID_ARGUMENT when parent is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listImpressionMetadata(listImpressionMetadataRequest { parent = "invalid-parent" })
      }

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
  fun `listImpressionMetadata throws INVALID_ARGUMENT when page size is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
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
  fun `listImpressionMetadata throws INVALID_ARGUMENT when page token is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listImpressionMetadata(
            listImpressionMetadataRequest {
              parent = DATA_PROVIDER_KEY.toName()
              pageToken = "this-is-not-base64-or-a-proto"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "page_token"
          }
        )
    }

  @Test
  fun `computeModelLineBounds returns bounds`() = runBlocking {
    service.batchCreateImpressionMetadata(
      batchCreateImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
        requests += createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              modelLine = MODEL_LINE_1
              interval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
            }
        }

        requests += createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              modelLine = MODEL_LINE_1
              blobUri = "blob-2"
              interval = interval {
                startTime = timestamp { seconds = 300 }
                endTime = timestamp { seconds = 400 }
              }
            }
        }

        requests += createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              modelLine = MODEL_LINE_2
              blobUri = "blob-3"
              interval = interval {
                startTime = timestamp { seconds = 500 }
                endTime = timestamp { seconds = 700 }
              }
            }
        }
      }
    )

    val request = computeModelLineBoundsRequest { parent = DATA_PROVIDER_KEY.toName() }
    val response = service.computeModelLineBounds(request)

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        computeModelLineBoundsResponse {
          modelLineBounds += modelLineBoundMapEntry {
            key = MODEL_LINE_1
            value = interval {
              startTime = timestamp { seconds = 100 }
              endTime = timestamp { seconds = 400 }
            }
          }

          modelLineBounds += modelLineBoundMapEntry {
            key = MODEL_LINE_2
            value = interval {
              startTime = timestamp { seconds = 500 }
              endTime = timestamp { seconds = 700 }
            }
          }
        }
      )
  }

  @Test
  fun `computeModelLineBounds throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.computeModelLineBounds(computeModelLineBoundsRequest {})
      }
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
  fun `computeModelLineBounds throws INVALID_ARGUMENT when parent is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.computeModelLineBounds(computeModelLineBoundsRequest { parent += "invalid-name" })
      }
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
  fun `computeModelLineBounds returns all model lines when modelLines is missing`() = runBlocking {
    service.batchCreateImpressionMetadata(
      batchCreateImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
        requests += createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              modelLine = MODEL_LINE_1
              interval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
            }
        }
        requests += createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata =
            IMPRESSION_METADATA.copy {
              modelLine = MODEL_LINE_2
              blobUri = "blob-2"
              interval = interval {
                startTime = timestamp { seconds = 300 }
                endTime = timestamp { seconds = 400 }
              }
            }
        }
      }
    )

    val response =
      service.computeModelLineBounds(
        computeModelLineBoundsRequest { parent = DATA_PROVIDER_KEY.toName() }
      )

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        computeModelLineBoundsResponse {
          modelLineBounds += modelLineBoundMapEntry {
            key = MODEL_LINE_1
            value = interval {
              startTime = timestamp { seconds = 100 }
              endTime = timestamp { seconds = 200 }
            }
          }
          modelLineBounds += modelLineBoundMapEntry {
            key = MODEL_LINE_2
            value = interval {
              startTime = timestamp { seconds = 300 }
              endTime = timestamp { seconds = 400 }
            }
          }
        }
      )
  }

  private suspend fun createImpressionMetadata(
    vararg impressionMetadata: ImpressionMetadata
  ): List<ImpressionMetadata> {
    return impressionMetadata.map { metadata ->
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          this.impressionMetadata = metadata
          requestId = UUID.randomUUID().toString()
        }
      )
    }
  }

  @Test
  fun `createImpressionMetadata returns entity_keys`() = runBlocking {
    val response =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA_WITH_ENTITY_KEYS
        }
      )

    assertThat(response.entityKeysList)
      .containsExactly(ENTITY_KEY_AD_1, ENTITY_KEY_CAMPAIGN_1)
      .inOrder()
  }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT when neither eventGroupReferenceId nor entity_keys set`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
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
              "impression_metadata.event_group_reference_id or entity_keys"
          }
        )
    }

  @Test
  fun `createImpressionMetadata succeeds with entity_keys and no eventGroupReferenceId`() =
    runBlocking {
      val response =
        service.createImpressionMetadata(
          createImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            parent = DATA_PROVIDER_KEY.toName()
            impressionMetadata =
              IMPRESSION_METADATA_WITH_ENTITY_KEYS.copy { clearEventGroupReferenceId() }
          }
        )

      assertThat(response.eventGroupReferenceId).isEmpty()
      assertThat(response.entityKeysList)
        .containsExactly(ENTITY_KEY_AD_1, ENTITY_KEY_CAMPAIGN_1)
        .inOrder()
    }

  @Test
  fun `getImpressionMetadata returns entity_keys`() = runBlocking {
    val created =
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA_WITH_ENTITY_KEYS
        }
      )

    val fetched =
      service.getImpressionMetadata(getImpressionMetadataRequest { name = created.name })

    assertThat(fetched.entityKeysList)
      .containsExactly(ENTITY_KEY_AD_1, ENTITY_KEY_CAMPAIGN_1)
      .inOrder()
  }

  @Test
  fun `batchCreateImpressionMetadata preserves entity_keys per row`() = runBlocking {
    val response =
      service.batchCreateImpressionMetadata(
        batchCreateImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          requests += createImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            parent = DATA_PROVIDER_KEY.toName()
            impressionMetadata = IMPRESSION_METADATA_WITH_ENTITY_KEYS
          }
          requests += createImpressionMetadataRequest {
            dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

            parent = DATA_PROVIDER_KEY.toName()
            impressionMetadata = IMPRESSION_METADATA_2.copy { entityKeys += ENTITY_KEY_AD_2 }
          }
        }
      )

    assertThat(response.impressionMetadataList).hasSize(2)
    assertThat(response.impressionMetadataList[0].entityKeysList)
      .containsExactly(ENTITY_KEY_AD_1, ENTITY_KEY_CAMPAIGN_1)
      .inOrder()
    assertThat(response.impressionMetadataList[1].entityKeysList)
      .containsExactly(ENTITY_KEY_AD_2)
      .inOrder()
  }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT when entity_key entity_type is empty`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata = IMPRESSION_METADATA.copy { entityKeys += entityKey { entityId = "x" } }
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
              "impression_metadata.entity_keys.0.entity_type"
          }
        )
    }

  @Test
  fun `createImpressionMetadata throws INVALID_ARGUMENT when entity_key id is empty`() =
    runBlocking {
      val request = createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata =
          IMPRESSION_METADATA.copy { entityKeys += entityKey { entityType = "ad" } }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createImpressionMetadata(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "impression_metadata.entity_keys.0.entity_id"
          }
        )
    }

  @Test
  fun `listImpressionMetadata filters by single meta entity_key`() = runBlocking {
    service.createImpressionMetadata(
      createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata = IMPRESSION_METADATA_WITH_ENTITY_KEYS
      }
    )
    service.createImpressionMetadata(
      createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata = IMPRESSION_METADATA_2
      }
    )

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter = ListImpressionMetadataRequestKt.filter { entityKeys += ENTITY_KEY_AD_1 }
        }
      )

    assertThat(response.impressionMetadataList).hasSize(1)
    assertThat(response.impressionMetadataList[0].entityKeysList)
      .containsExactly(ENTITY_KEY_AD_1, ENTITY_KEY_CAMPAIGN_1)
      .inOrder()
  }

  @Test
  fun `listImpressionMetadata applies OR-semantics across multiple meta entity_keys`() =
    runBlocking {
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = IMPRESSION_METADATA_WITH_ENTITY_KEYS
        }
      )
      val secondImpression = IMPRESSION_METADATA_2.copy { entityKeys += ENTITY_KEY_AD_2 }
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

          parent = DATA_PROVIDER_KEY.toName()
          impressionMetadata = secondImpression
        }
      )

      val response =
        service.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = DATA_PROVIDER_KEY.toName()
            filter =
              ListImpressionMetadataRequestKt.filter {
                entityKeys += ENTITY_KEY_AD_1
                entityKeys += ENTITY_KEY_AD_2
              }
          }
        )

      assertThat(response.impressionMetadataList).hasSize(2)
    }

  @Test
  fun `listImpressionMetadata returns empty when no entity_key matches`() = runBlocking {
    service.createImpressionMetadata(
      createImpressionMetadataRequest {
        dataAvailabilitySyncLease = DATA_AVAILABILITY_SYNC_LEASE_NAME

        parent = DATA_PROVIDER_KEY.toName()
        impressionMetadata = IMPRESSION_METADATA_WITH_ENTITY_KEYS
      }
    )

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest {
          parent = DATA_PROVIDER_KEY.toName()
          filter =
            ListImpressionMetadataRequestKt.filter {
              entityKeys += entityKey {
                entityType = "ad_account"
                entityId = "no-such-account"
              }
            }
        }
      )

    assertThat(response.impressionMetadataList).isEmpty()
  }

  @Test
  fun `listImpressionMetadata throws INVALID_ARGUMENT when filter entity_key is invalid`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listImpressionMetadata(
            listImpressionMetadataRequest {
              parent = DATA_PROVIDER_KEY.toName()
              filter = ListImpressionMetadataRequestKt.filter { entityKeys += entityKey {} }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "filter.entity_keys.0.entity_type"
          }
        )
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private const val SYNCHRONIZATION_ATTEMPT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val LEASE_REQUEST_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    private val DATA_AVAILABILITY_SYNC_LEASE_NAME =
      "${DATA_PROVIDER_KEY.toName()}/dataAvailabilitySyncLeases/$SYNCHRONIZATION_ATTEMPT_ID"
    private const val BLOB_URI = "path/to/blob"
    private const val BLOB_TYPE = "blob.type"
    private const val EVENT_GROUP_REFERENCE_ID_1 = "event-group-1"
    private const val EVENT_GROUP_REFERENCE_ID_2 = "event-group-2"
    private const val MODEL_LINE_PREFIX =
      "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines"
    private const val MODEL_LINE_1 = "${MODEL_LINE_PREFIX}/model-line-1"
    private const val MODEL_LINE_2 = "${MODEL_LINE_PREFIX}/model-line-2"
    private val REQUEST_ID = UUID.randomUUID().toString()

    private val IMPRESSION_METADATA = impressionMetadata {
      // name not set
      blobUri = BLOB_URI
      blobTypeUrl = BLOB_TYPE
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_1
      modelLine = MODEL_LINE_1
      interval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 9 }
      }
      // state not set
    }

    private val IMPRESSION_METADATA_2 = impressionMetadata {
      // name not set
      blobUri = "uri-2"
      blobTypeUrl = BLOB_TYPE
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID_2
      modelLine = MODEL_LINE_2
      interval = interval {
        startTime = timestamp { seconds = 10 }
        endTime = timestamp { seconds = 20 }
      }
      // state not set
    }

    private val ENTITY_KEY_AD_1 = entityKey {
      entityType = "ad"
      entityId = "ad-1"
    }

    private val ENTITY_KEY_AD_2 = entityKey {
      entityType = "ad"
      entityId = "ad-2"
    }

    private val ENTITY_KEY_CAMPAIGN_1 = entityKey {
      entityType = "campaign"
      entityId = "campaign-1"
    }

    private val IMPRESSION_METADATA_WITH_ENTITY_KEYS =
      IMPRESSION_METADATA.copy {
        entityKeys += ENTITY_KEY_AD_1
        entityKeys += ENTITY_KEY_CAMPAIGN_1
      }
  }
}
