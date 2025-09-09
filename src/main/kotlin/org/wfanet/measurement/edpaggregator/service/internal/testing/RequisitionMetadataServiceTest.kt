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
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.runBlocking
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
import org.wfanet.measurement.internal.edpaggregator.getRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.lookupRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.queueRequisitionMetadataRequest
import org.wfanet.measurement.internal.edpaggregator.requisitionMetadata

@RunWith(JUnit4::class)
abstract class RequisitionMetadataServiceTest {
  protected abstract fun initService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): RequisitionMetadataServiceCoroutineImplBase

  @Test
  fun `create requisition metadata without request_id returns created requisition metadata`() =
    runBlocking {
      val service = initService()
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
  fun `create requisition metadata with request_id returns created requisition metadata`() =
    runBlocking {
      val service = initService()
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
  fun `create requisition with refusal message returns a REFUSED requisition metadata`() =
    runBlocking {
      val service = initService()
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
  fun `create requisition metadata with existing request_id returns existing requisition metadata`() =
    runBlocking {
      val service = initService()

      val request = createRequisitionMetadataRequest {
        requisitionMetadata = REQUISITION_METADATA
        requestId = CREATE_REQUEST_ID
      }
      val requisitionMetadata = service.createRequisitionMetadata(request)

      val requisitionMetadata2 = service.createRequisitionMetadata(request)

      assertThat(requisitionMetadata2).isEqualTo(requisitionMetadata)
    }

  @Test
  fun `get requisition metadata returns a requisition metadata`() = runBlocking {
    val service = initService()
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
  fun `lookup requisition metadata by cmms requisition returns a requisition metadata`() =
    runBlocking {
      val service = initService()
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
  fun `lookup requisition metadata by blob uri returns a requisition metadata`() = runBlocking {
    val service = initService()
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
  fun `fetch latest cmms create time returns the latest timestamp`() = runBlocking {
    val service = initService()
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
  fun `fetch latest cmms create time returns the default timestamp with none requisition metadata`() =
    runBlocking {
      val service = initService()

      val latestTimestamp =
        service.fetchLatestCmmsCreateTime(
          fetchLatestCmmsCreateTimeRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
        )

      assertThat(latestTimestamp).isEqualTo(Timestamp.getDefaultInstance())
    }

  @Test
  fun `queue requisition metadata returns an updated requisition metadata`() = runBlocking {
    val service = initService()
    val requisitionMetadata =
      service.createRequisitionMetadata(
        createRequisitionMetadataRequest { requisitionMetadata = REQUISITION_METADATA }
      )

    val updatedRequisitionMetadata =
      service.queueRequisitionMetadata(
        queueRequisitionMetadataRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          requisitionMetadataResourceId = REQUISITION_METADATA_RESOURCE_ID
          workItem = WORK_ITEM
          etag = requisitionMetadata.etag
        }
      )

    assertThat(updatedRequisitionMetadata)
      .ignoringFields(
        RequisitionMetadata.UPDATE_TIME_FIELD_NUMBER,
        RequisitionMetadata.ETAG_FIELD_NUMBER,
      )
      .isEqualTo(
        requisitionMetadata.copy {
          state = State.REQUISITION_METADATA_STATE_QUEUED
          workItem = WORK_ITEM
        }
      )
    assertThat(requisitionMetadata.updateTime.toInstant())
      .isLessThan(updatedRequisitionMetadata.updateTime.toInstant())
    assertThat(requisitionMetadata.etag).isNotEqualTo(updatedRequisitionMetadata.etag)
  }

  @Test fun `start requisiton metadata returns an updated requisition metadata`() = runBlocking {}

  @Test
  fun `fulfill requisition metadata returns an updated requisition metadata`() = runBlocking {}

  @Test fun `refuse requisition metadata returns an updated requisition metadata`() = runBlocking {}

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
