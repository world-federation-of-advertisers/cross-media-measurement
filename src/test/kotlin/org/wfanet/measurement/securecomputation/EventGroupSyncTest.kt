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

package org.wfanet.measurement.securecomputation

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.type.interval
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.AdMetadataKt.campaignMetadata as eventGroupCampaignMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt.adMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.securecomputation.eventgroups.EventGroupSync

// import com.google.common.truth.extensions.proto.ProtoTruth.assertThat

private fun CampaignMetadata.toEventGroup(): EventGroup {
  val campaign = this
  return eventGroup {
    name = "resource-name-for-${campaign.eventGroupReferenceId}"
    measurementConsumer = campaign.measurementConsumerName
    eventGroupReferenceId = campaign.eventGroupReferenceId
    this.eventGroupMetadata = eventGroupMetadata {
      this.adMetadata = adMetadata {
        this.campaignMetadata = eventGroupCampaignMetadata {
          brandName = campaign.brandName
          campaignName = campaign.campaignName
        }
      }
    }
    mediaTypes += campaign.mediaTypesList.map { MediaType.valueOf(it) }
    dataAvailabilityInterval = interval {
      startTime = campaign.startTime
      endTime = campaign.endTime
    }
  }
}
