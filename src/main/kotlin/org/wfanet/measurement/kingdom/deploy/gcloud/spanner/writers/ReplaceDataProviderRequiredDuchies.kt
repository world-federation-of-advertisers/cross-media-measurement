// Copyright 2020 The Cross-Media Measurement Authors
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
package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.ReplaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

class ReplaceDataProviderRequiredDuchies(
  private val request: ReplaceDataProviderRequiredDuchiesRequest
) : SpannerWriter<DataProvider, DataProvider>() {
  override suspend fun TransactionScope.runTransaction(): DataProvider {
    val externalDataProviderId = ExternalId(request.externalDataProviderId)
    val dataProviderResult =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    val dataProvider = dataProviderResult.dataProvider
    val dataProviderId = dataProviderResult.dataProviderId
    val desiredRequiredDuchyList = request.requiredExternalDuchyIdsList

    // Delete old duchy list.
    transactionContext.buffer(
      Mutation.delete("DataProviderRequiredDuchies", KeySet.prefixRange(Key.of(dataProviderId)))
    )

    // Write new duchy list.
    for (externalDuchyId in desiredRequiredDuchyList) {
      val duchyId =
        InternalId(
          DuchyIds.getInternalId(externalDuchyId.toString())
            ?: throw DuchyNotFoundException(externalDuchyId.toString())
        )
      transactionContext.bufferInsertMutation("DataProviderRequiredDuchies") {
        set("DataProviderId" to dataProviderId)
        set("DuchyId" to duchyId)
      }
    }

    return dataProvider
      .toBuilder()
      .clearRequiredExternalDuchyIds()
      .addAllRequiredExternalDuchyIds(desiredRequiredDuchyList)
      .build()
  }

  override fun ResultScope<DataProvider>.buildResult(): DataProvider {
    return checkNotNull(transactionResult)
  }
}
