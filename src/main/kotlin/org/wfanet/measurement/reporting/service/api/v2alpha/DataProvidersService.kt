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

import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.GetDataProviderRequest
import org.wfanet.measurement.api.withAuthenticationKey

class DataProvidersService(private val dataProvidersStub: DataProvidersCoroutineStub) :
  DataProvidersCoroutineImplBase() {
  override suspend fun getDataProvider(request: GetDataProviderRequest): DataProvider {
    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {}
    }

    val apiAuthenticationKey: String = principal.config.apiKey

    return dataProvidersStub.withAuthenticationKey(apiAuthenticationKey).getDataProvider(request)
  }
}
