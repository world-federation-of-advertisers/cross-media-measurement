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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.protobuf.ByteString
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.setJson
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountActivationStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PermissionDeniedException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerCreationTokenReader

/**
 * Creates a measurement consumer in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [PermissionDeniedException] MeasurementConsumer CreationToken not found
 * @throws [AccountNotFoundException] Account not found
 * @throws [AccountActivationStateIllegalException] Account is not in state of ACTIVATED
 */
class CreateMeasurementConsumer(
  private val measurementConsumer: MeasurementConsumer,
  private val externalAccountId: ExternalId,
  private val measurementConsumerCreationTokenHash: ByteString,
) : SpannerWriter<MeasurementConsumer, MeasurementConsumer>() {
  override suspend fun TransactionScope.runTransaction(): MeasurementConsumer {
    val internalCertificateId = idGenerator.generateInternalId()

    val measurementConsumerCreationTokenResult =
      readMeasurementConsumerCreationToken(measurementConsumerCreationTokenHash)
    deleteMeasurementConsumerCreationToken(
      measurementConsumerCreationTokenResult.measurementConsumerCreationTokenId
    )

    measurementConsumer.certificate
      .toInsertMutation(internalCertificateId)
      .bufferTo(transactionContext)

    val internalMeasurementConsumerId = idGenerator.generateInternalId()
    val externalMeasurementConsumerId = idGenerator.generateExternalId()

    val accountResult = readAccount(externalAccountId)
    if (accountResult.account.activationState != Account.ActivationState.ACTIVATED) {
      throw AccountActivationStateIllegalException(
        ExternalId(accountResult.account.externalAccountId),
        accountResult.account.activationState
      )
    }

    transactionContext.bufferInsertMutation("MeasurementConsumerOwners") {
      set("MeasurementConsumerId" to internalMeasurementConsumerId)
      set("AccountId" to accountResult.accountId)
    }

    transactionContext.bufferInsertMutation("MeasurementConsumers") {
      set("MeasurementConsumerId" to internalMeasurementConsumerId)
      set("PublicKeyCertificateId" to internalCertificateId)
      set("ExternalMeasurementConsumerId" to externalMeasurementConsumerId)
      set("MeasurementConsumerDetails" to measurementConsumer.details)
      setJson("MeasurementConsumerDetailsJson" to measurementConsumer.details)
    }

    val externalMeasurementConsumerCertificateId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("MeasurementConsumerCertificates") {
      set("MeasurementConsumerId" to internalMeasurementConsumerId)
      set("CertificateId" to internalCertificateId)
      set(
        "ExternalMeasurementConsumerCertificateId" to externalMeasurementConsumerCertificateId.value
      )
    }

    return measurementConsumer.copy {
      this.externalMeasurementConsumerId = externalMeasurementConsumerId.value
      certificate =
        certificate.copy {
          this.externalMeasurementConsumerId = externalMeasurementConsumerId.value
          externalCertificateId = externalMeasurementConsumerCertificateId.value
        }
    }
  }

  override fun ResultScope<MeasurementConsumer>.buildResult(): MeasurementConsumer {
    return checkNotNull(transactionResult)
  }

  private suspend fun TransactionScope.readMeasurementConsumerCreationToken(
    measurementConsumerCreationTokenHash: ByteString
  ): MeasurementConsumerCreationTokenReader.Result =
    MeasurementConsumerCreationTokenReader()
      .readByMeasurementConsumerCreationTokenHash(
        transactionContext,
        measurementConsumerCreationTokenHash
      )
      ?: throw PermissionDeniedException()

  private suspend fun TransactionScope.readAccount(
    externalAccountId: ExternalId
  ): AccountReader.Result =
    AccountReader().readByExternalAccountId(transactionContext, externalAccountId)
      ?: throw AccountNotFoundException(externalAccountId)
}
