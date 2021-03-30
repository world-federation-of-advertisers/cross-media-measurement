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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetExchangeRequest
import org.wfanet.measurement.api.v2alpha.ListExchangesRequest
import org.wfanet.measurement.api.v2alpha.ListExchangesResponse
import org.wfanet.measurement.api.v2alpha.UploadAuditTrailRequest

class ExchangesService : ExchangesCoroutineImplBase() {
  override suspend fun getExchange(request: GetExchangeRequest): Exchange {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }

  override suspend fun listExchanges(request: ListExchangesRequest): ListExchangesResponse {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }

  override suspend fun uploadAuditTrail(requests: Flow<UploadAuditTrailRequest>): Exchange {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }
}
