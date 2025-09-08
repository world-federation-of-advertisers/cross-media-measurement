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
import java.time.Instant
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
        )
        .isEqualTo(REQUISITION_METADATA.copy { state = State.REQUISITION_METADATA_STATE_STORED })

      assertThat(requisitionMetadata.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(requisitionMetadata.updateTime).isEqualTo(requisitionMetadata.createTime)
      assertThat(requisitionMetadata.etag).isNotEmpty()
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

    private val REQUISITION_METADATA = requisitionMetadata {
      dataProviderResourceId = "edp1"
      requisitionMetadataResourceId = "req1"
      cmmsRequisition = "cmms_req1"
      blobUri = "blob1"
      groupId = "group1"
      cmmsCreateTime = CMMS_CREATE_TIME
      report = "report1"
    }
  }
}
