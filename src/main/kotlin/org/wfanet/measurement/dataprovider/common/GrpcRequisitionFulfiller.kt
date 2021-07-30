// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.dataprovider.common

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub

/**
 * Implementation of [RequisitionFulfiller] for gRPC.
 */
class GrpcRequisitionFulfiller(
  private val stub: RequisitionFulfillmentCoroutineStub
) : RequisitionFulfiller {
  override suspend fun fulfillRequisition(key: Requisition.Key, data: Flow<ByteString>) {
    stub.fulfillRequisition(
      flow {
        emit(makeFulfillRequisitionHeader(key))
        emitAll(data.map { makeFulfillRequisitionBody(it) })
      }
    )
  }

  private fun makeFulfillRequisitionHeader(key: Requisition.Key): FulfillRequisitionRequest {
    return FulfillRequisitionRequest.newBuilder().apply {
      headerBuilder.key = key
    }.build()
  }

  private fun makeFulfillRequisitionBody(bytes: ByteString): FulfillRequisitionRequest {
    return FulfillRequisitionRequest.newBuilder().apply {
      bodyChunkBuilder.data = bytes
    }.build()
  }
}
