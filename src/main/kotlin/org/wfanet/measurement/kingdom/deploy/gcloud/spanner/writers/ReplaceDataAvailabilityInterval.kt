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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.ReplaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

class ReplaceDataAvailabilityInterval(private val request: ReplaceDataAvailabilityIntervalRequest) :
  SpannerWriter<DataProvider, DataProvider>() {
  override suspend fun TransactionScope.runTransaction(): DataProvider {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val dataProviderResult =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    val dataProvider = dataProviderResult.dataProvider
    val dataProviderId = dataProviderResult.dataProviderId

    val updatedDetails =
      dataProvider.details.copy { dataAvailabilityInterval = request.dataAvailabilityInterval }

    transactionContext.bufferUpdateMutation("DataProviders") {
      set("DataProviderId" to dataProviderId)
      set("DataProviderDetails").to(updatedDetails)
    }

    return dataProvider.copy { details = updatedDetails }
  }

  override fun ResultScope<DataProvider>.buildResult(): DataProvider {
    return checkNotNull(transactionResult)
  }
}
