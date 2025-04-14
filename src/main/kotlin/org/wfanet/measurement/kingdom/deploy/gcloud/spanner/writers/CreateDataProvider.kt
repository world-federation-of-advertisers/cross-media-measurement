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

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.ModelLineKey
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuchyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineInternalKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

/**
 * [SpannerWriter] for creating a [DataProvider].
 *
 * Throws one of the following on [execute]:
 * * [ModelLineNotFoundException]
 */
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
      set("DataProviderDetails").to(dataProvider.details)
    }

    insertRequiredDuchies(internalDataProviderId)
    insertDataAvailabilityIntervals(internalDataProviderId)

    val externalDataProviderCertificateId = idGenerator.generateExternalId()
    transactionContext.bufferInsertMutation("DataProviderCertificates") {
      set("DataProviderId" to internalDataProviderId)
      set("CertificateId" to internalCertificateId)
      set("ExternalDataProviderCertificateId" to externalDataProviderCertificateId)
    }

    return dataProvider.copy {
      this.externalDataProviderId = externalDataProviderId.value
      certificate =
        certificate.copy {
          this.externalDataProviderId = externalDataProviderId.value
          externalCertificateId = externalDataProviderCertificateId.value
        }
    }
  }

  private fun TransactionScope.insertRequiredDuchies(internalDataProviderId: InternalId) {
    for (externalDuchyId in dataProvider.requiredExternalDuchyIdsList) {
      val duchyId =
        InternalId(
          DuchyIds.getInternalId(externalDuchyId.toString())
            ?: throw DuchyNotFoundException(externalDuchyId.toString())
        )

      transactionContext.bufferInsertMutation("DataProviderRequiredDuchies") {
        set("DataProviderId" to internalDataProviderId)
        set("DuchyId" to duchyId)
      }
    }
  }

  private suspend fun TransactionScope.insertDataAvailabilityIntervals(dataProviderId: InternalId) {
    val modelLineInternalKeys: Map<ModelLineKey, ModelLineInternalKey> =
      ModelLineReader.readInternalIds(
        transactionContext,
        dataProvider.dataAvailabilityIntervalsList.map { it.key },
      )
    for (entry: DataProvider.DataAvailabilityMapEntry in
      dataProvider.dataAvailabilityIntervalsList) {
      val internalKey: ModelLineInternalKey =
        modelLineInternalKeys[entry.key]
          ?: throw ModelLineNotFoundException(
            ExternalId(entry.key.externalModelProviderId),
            ExternalId(entry.key.externalModelSuiteId),
            ExternalId(entry.key.externalModelLineId),
          )
      transactionContext.bufferInsertMutation("DataProviderAvailabilityIntervals") {
        set("DataProviderId").to(dataProviderId)
        set("ModelProviderId").to(internalKey.modelProviderId)
        set("ModelSuiteId").to(internalKey.modelSuiteId)
        set("ModelLineId").to(internalKey.modelLineId)
        set("StartTime").to(entry.value.startTime.toGcloudTimestamp())
        set("EndTime").to(entry.value.endTime.toGcloudTimestamp())
      }
    }
  }

  override fun ResultScope<DataProvider>.buildResult(): DataProvider {
    return checkNotNull(transactionResult)
  }
}
