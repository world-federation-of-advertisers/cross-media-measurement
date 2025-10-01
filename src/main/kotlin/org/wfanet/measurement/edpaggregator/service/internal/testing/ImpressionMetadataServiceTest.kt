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
import org.wfanet.measurement.internal.edpaggregator.ComputeModelLineBoundsResponse
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadata
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageToken
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataPageTokenKt
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListImpressionMetadataResponse
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

      val request = createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
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
  fun `deleteImpressionMetadata soft deletes and returns updated ImpressionMetadata`() =
    runBlocking {
      val created =
        service.createImpressionMetadata(
          createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
        )

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
  fun `deleteImpressionMetadata throws INVALID_ARGUMENT when already deleted`() = runBlocking {
    service.createImpressionMetadata(
      createImpressionMetadataRequest { impressionMetadata = IMPRESSION_METADATA }
    )

    val deleted =
      service.deleteImpressionMetadata(
        deleteImpressionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.deleteImpressionMetadata(
          deleteImpressionMetadataRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            impressionMetadataResourceId = IMPRESSION_METADATA_RESOURCE_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo?.reason)
      .isEqualTo(Errors.Reason.IMPRESSION_METADATA_STATE_INVALID.name)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.IMPRESSION_METADATA_STATE_INVALID.name
          metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = deleted.dataProviderResourceId
          metadata[Errors.Metadata.IMPRESSION_METADATA_RESOURCE_ID.key] =
            deleted.impressionMetadataResourceId
          metadata[Errors.Metadata.IMPRESSION_METADATA_STATE.key] = deleted.state.name
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
    val created = createImpressionMetadata(IMPRESSION_METADATA_2, IMPRESSION_METADATA_3)

    val response =
      service.listImpressionMetadata(
        listImpressionMetadataRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(response).isEqualTo(listImpressionMetadataResponse { impressionMetadata += created })
  }

  @Test
  fun `listImpressionMetadata with page size returns first page`() = runBlocking {
    val created =
      createImpressionMetadata(IMPRESSION_METADATA_2, IMPRESSION_METADATA_3, IMPRESSION_METADATA_4)

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
      createImpressionMetadata(IMPRESSION_METADATA_2, IMPRESSION_METADATA_3, IMPRESSION_METADATA_4)

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
    createImpressionMetadata(IMPRESSION_METADATA_2)

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
      createImpressionMetadata(IMPRESSION_METADATA_2, IMPRESSION_METADATA_3, IMPRESSION_METADATA_4)

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
      createImpressionMetadata(IMPRESSION_METADATA_2, IMPRESSION_METADATA_3, IMPRESSION_METADATA_4)
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
      createImpressionMetadata(IMPRESSION_METADATA_2, IMPRESSION_METADATA_3, IMPRESSION_METADATA_4)
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
        createImpressionMetadata(IMPRESSION_METADATA_2, IMPRESSION_METADATA_3).sortedBy {
          it.impressionMetadataResourceId
        }

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
      val created = createImpressionMetadata(IMPRESSION_METADATA_2, IMPRESSION_METADATA_3)

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
      val (created1, _) = createImpressionMetadata(IMPRESSION_METADATA_2, IMPRESSION_METADATA_3)

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
    service.createImpressionMetadata(
      createImpressionMetadataRequest {
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
    )
    service.createImpressionMetadata(
      createImpressionMetadataRequest {
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

  private suspend fun createImpressionMetadata(
    vararg impressionMetadata: ImpressionMetadata
  ): List<ImpressionMetadata> {
    return impressionMetadata.map { metadata ->
      service.createImpressionMetadata(
        createImpressionMetadataRequest {
          this.impressionMetadata = metadata
          requestId = UUID.randomUUID().toString()
        }
      )
    }
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
