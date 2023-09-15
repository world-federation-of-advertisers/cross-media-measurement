// Copyright 2023 The Cross-Media Measurement Authors
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
package org.wfanet.measurement.loadtest.panelmatchresourcesetup

import com.google.protobuf.kotlin.toByteString
import com.google.type.Date
import java.time.LocalDate
import io.grpc.StatusException
import java.util.logging.Logger
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.RecurringExchangeKey
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.internal.kingdom.recurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails
import org.wfanet.measurement.kingdom.service.api.v2alpha.parseCertificateDer
import org.wfanet.measurement.kingdom.service.api.v2alpha.toInternal
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent

private val API_VERSION = Version.V2_ALPHA

class PanelMatchResourceSetup(private val internalDataProvidersClient: DataProvidersGrpcKt.DataProvidersCoroutineStub,
                              private val internalModelProvidersClient: ModelProvidersGrpcKt.ModelProvidersCoroutineStub,
                              private val recurringExchangesStub: RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub
) {
  suspend fun process(dataProviderContent: EntityContent,
                      exchangeDate: Date,
                      exchangeWorkflow: ExchangeWorkflow,
                      exchangeSchedule: String,
                      publicApiVersion: String,): PanelMatchResourceKeys {
    logger.info("Starting resource setup ...")
    val externalDataProviderId = createDataProvider(dataProviderContent)
    val dataProviderKey = DataProviderKey(externalIdToApiId(externalDataProviderId))
    val externalModelProviderId = createModelProvider()
    val modelProviderKey = ModelProviderKey(externalIdToApiId(externalModelProviderId))
    val externalRecurringExchangeId = createRecurringExchange(externalDataProviderId, externalModelProviderId, exchangeDate, exchangeSchedule, publicApiVersion, exchangeWorkflow)

    val recurringExchangeKey = RecurringExchangeKey(externalIdToApiId(externalRecurringExchangeId))
    logger.info("Resource setup was successful.")
    return PanelMatchResourceKeys(dataProviderKey, modelProviderKey, recurringExchangeKey)
  }

  suspend fun createDataProvider(dataProviderContent: EntityContent): Long {
    val encryptionPublicKey = dataProviderContent.encryptionPublicKey
    val signedPublicKey =
      signEncryptionPublicKey(encryptionPublicKey, dataProviderContent.signingKey)
    val internalDataProvider =
      try {
        internalDataProvidersClient.createDataProvider(
          dataProvider {
            certificate =
              parseCertificateDer(dataProviderContent.signingKey.certificate.encoded.toByteString())
            details =
              DataProviderKt.details {
                apiVersion = API_VERSION.string
                publicKey = signedPublicKey.data
                publicKeySignature = signedPublicKey.signature
              }
          }
        )
      } catch (e: StatusException) {
        throw Exception("Error creating DataProvider", e)
      }
    logger.info("InternalDataProvider: ${internalDataProvider.externalDataProviderId}")
    return internalDataProvider.externalDataProviderId
  }

  suspend fun createModelProvider(): Long {
    val internalModelProvider =
      try {
        internalModelProvidersClient.createModelProvider(
          modelProvider {}
        )
      } catch (e: StatusException) {
        throw Exception("Error creating ModelProvider", e)
      }
    logger.info("InternalModelProvider: ${internalModelProvider.externalModelProviderId}")
    return internalModelProvider.externalModelProviderId
  }

  private suspend fun createRecurringExchange(
    externalDataProvider: Long,
    externalModelProvider: Long,
    exchangeDate: Date,
    exchangeSchedule: String,
    publicApiVersion: String,
    exchangeWorkflow: ExchangeWorkflow
  ): Long {
    val recurringExchangeId = recurringExchangesStub
      .createRecurringExchange(
        createRecurringExchangeRequest {
          recurringExchange = recurringExchange {
            externalDataProviderId = externalDataProvider
            externalModelProviderId = externalModelProvider
            state = RecurringExchange.State.ACTIVE
            details = recurringExchangeDetails {
              this.exchangeWorkflow = exchangeWorkflow.toInternal()
              cronSchedule = exchangeSchedule
              externalExchangeWorkflow = exchangeWorkflow.toByteString()
              apiVersion = publicApiVersion
            }
            nextExchangeDate = exchangeDate
          }
        }
      )
      .externalRecurringExchangeId
    logger.info("recurringExchangeId: ${recurringExchangeId}")
    return recurringExchangeId
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }

}

data class PanelMatchResourceKeys(
  val dataProviderKey: DataProviderKey,
  val modelProviderKey: ModelProviderKey,
  val recurringExchangeKey: RecurringExchangeKey
)
