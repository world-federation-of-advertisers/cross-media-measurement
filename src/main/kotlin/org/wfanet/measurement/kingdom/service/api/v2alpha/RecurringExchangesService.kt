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

import org.wfanet.measurement.api.v2alpha.CreateRecurringExchangeRequest
import org.wfanet.measurement.api.v2alpha.GetRecurringExchangeRequest
import org.wfanet.measurement.api.v2alpha.ListRecurringExchangesRequest
import org.wfanet.measurement.api.v2alpha.ListRecurringExchangesResponse
import org.wfanet.measurement.api.v2alpha.RecurringExchange
import org.wfanet.measurement.api.v2alpha.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RetireRecurringExchangeRequest

class RecurringExchangesService : RecurringExchangesCoroutineImplBase() {
  override suspend fun createRecurringExchange(
    request: CreateRecurringExchangeRequest
  ): RecurringExchange {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }

  override suspend fun getRecurringExchange(
    request: GetRecurringExchangeRequest
  ): RecurringExchange {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }

  override suspend fun listRecurringExchanges(
    request: ListRecurringExchangesRequest
  ): ListRecurringExchangesResponse {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }

  override suspend fun retireRecurringExchange(
    request: RetireRecurringExchangeRequest
  ): RecurringExchange {
    TODO("world-federation-of-advertisers/cross-media-measurement#3: implement this")
  }
}
