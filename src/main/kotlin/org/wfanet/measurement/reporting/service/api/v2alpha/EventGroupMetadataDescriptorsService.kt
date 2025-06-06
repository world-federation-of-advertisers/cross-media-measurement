/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.BatchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.BatchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.ResourceKey

class EventGroupMetadataDescriptorsService(
  private val eventGroupMetadataDescriptorsStub: EventGroupMetadataDescriptorsCoroutineStub,
  private val authorization: Authorization,
  private val apiAuthenticationKey: String,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : EventGroupMetadataDescriptorsCoroutineImplBase(coroutineContext) {
  override suspend fun getEventGroupMetadataDescriptor(
    request: GetEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    authorization.check(Authorization.ROOT_RESOURCE_NAME, GET_DESCRIPTOR_PERMISSIONS)

    return eventGroupMetadataDescriptorsStub
      .withAuthenticationKey(apiAuthenticationKey)
      .getEventGroupMetadataDescriptor(request)
  }

  override suspend fun batchGetEventGroupMetadataDescriptors(
    request: BatchGetEventGroupMetadataDescriptorsRequest
  ): BatchGetEventGroupMetadataDescriptorsResponse {
    authorization.check(Authorization.ROOT_RESOURCE_NAME, GET_DESCRIPTOR_PERMISSIONS)

    return eventGroupMetadataDescriptorsStub
      .withAuthenticationKey(apiAuthenticationKey)
      .batchGetEventGroupMetadataDescriptors(
        batchGetEventGroupMetadataDescriptorsRequest {
          parent = DataProviderKey(ResourceKey.WILDCARD_ID).toName()
          names += request.namesList
        }
      )
  }

  companion object {
    private const val GET_DESCRIPTOR_PERMISSION = "reporting.eventGroupMetadataDescriptors.get"
    val GET_DESCRIPTOR_PERMISSIONS = setOf(GET_DESCRIPTOR_PERMISSION)
  }
}
