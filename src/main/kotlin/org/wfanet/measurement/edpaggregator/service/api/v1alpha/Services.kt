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

package org.wfanet.measurement.edpaggregator.service.api.v1alpha

import io.grpc.BindableService
import io.grpc.Channel
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RequisitionMetadataServiceGrpcKt as InternalRequisitionMetadataServiceGrpcKt

data class Services(
  val requisitionMetadata:
    RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
) {
  fun toList(): List<BindableService> = listOf(requisitionMetadata)

  companion object {
    fun build(internalApiChannel: Channel): Services {
      val internalRequisitionMetadataStub =
        InternalRequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub(
          internalApiChannel
        )
      return Services(RequisitionMetadataService(internalRequisitionMetadataStub))
    }
  }
}
