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

import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.DataProvider

class CreateDataProvider(private val dataProvider: DataProvider) :
  SpannerWriter<DataProvider, DataProvider>() {
  override suspend fun TransactionScope.runTransaction(): DataProvider {
    val internalCertificateId = idGenerator.generateInternalId()

    dataProvider.certificate.toInsertMutation(internalCertificateId).bufferTo(transactionContext)

    val internalDataProviderId = idGenerator.generateInternalId()
    val externalDataProviderId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("DataProviders") {
      set("DataProviderId" to internalDataProviderId)
      set("PublicKeyCertificateId" to internalCertificateId)
      set("ExternalDataProviderId" to externalDataProviderId)
      set("DataProviderDetails" to dataProvider.details)
      setJson("DataProviderDetailsJson" to dataProvider.details)
    }

    val externalDataProviderCertificateId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("DataProviderCertificates") {
      set("DataProviderId" to internalDataProviderId)
      set("CertificateId" to internalCertificateId)
      set("ExternalDataProviderCertificateId" to externalDataProviderCertificateId)
    }

    return dataProvider
      .toBuilder()
      .also {
        it.externalDataProviderId = externalDataProviderId.value
        it.certificateBuilder.also {
          it.externalDataProviderId = externalDataProviderId.value
          it.externalCertificateId = externalDataProviderCertificateId.value
        }
      }
      .build()
  }

  override fun ResultScope<DataProvider>.buildResult(): DataProvider {
    return checkNotNull(transactionResult)
  }
}
