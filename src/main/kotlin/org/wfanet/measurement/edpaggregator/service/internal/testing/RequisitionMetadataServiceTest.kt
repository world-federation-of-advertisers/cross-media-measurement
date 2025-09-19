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
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadata
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataState as State
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.fetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.internal.edpaggregator.fulfillRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.lookupRequisitionMetadataRequest
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
  fun `lookupRequisitionMetadata by blob uri returns a requisition metadata`() = runBlocking {
    val startTime = Instant.now()
    service.createRequisitionMetadata(
      createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
    )

    val requisitionMetadata =
      service.lookupRequisitionMetadata(
        lookupRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          blobUri = BLOB_URI
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

  companion object {
    private val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private val REQUISITION_METADATA_RESOURCE_ID = "requisition-metadata-1"
    private val CMMS_REQUISITION = "dataProviders/data-provider-1/requisitions/requisition-1"
    private val BLOB_URI = "path/to/blob"
    private val BLOB_TYPE_URL = "blob.type.url"
    private val GROUP_ID = "group-1"
    private val CMMS_CREATE_TIME = timestamp { seconds = 12345 }
    private val REPORT = "measurementConsumers/measurement-consumer-1/reports/report-1"
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
  }
}
