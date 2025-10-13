/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import io.grpc.BindableService
import io.grpc.Channel
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.ImpressionMetadataServiceGrpcKt as InternalImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt as InternalRequisitionMetadataServiceGrpcKt

data class Services(
  val requisitionMetadata: RequisitionMetadataServiceCoroutineImplBase,
  val impressionMetadata: ImpressionMetadataServiceCoroutineImplBase,
) {
  fun toList(): List<BindableService> = listOf(requisitionMetadata, impressionMetadata)

  companion object {
    fun build(
      internalApiChannel: Channel,
      coroutineContext: CoroutineContext = EmptyCoroutineContext,
    ): Services {
      val internalRequisitionMetadataStub =
        InternalRequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub(
          internalApiChannel
        )
      val internalImpressionMetadataStub =
        InternalImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(
          internalApiChannel
        )

      return Services(
        RequisitionMetadataService(internalRequisitionMetadataStub, coroutineContext),
        ImpressionMetadataService(internalImpressionMetadataStub, coroutineContext),
      )
    }
  }
}
