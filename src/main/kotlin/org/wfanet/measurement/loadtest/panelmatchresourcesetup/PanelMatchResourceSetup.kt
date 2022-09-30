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

package org.wfanet.measurement.loadtest.panelmatchresourcesetup

import com.google.protobuf.kotlin.toByteString
import com.google.type.Date
import io.grpc.Channel
import io.grpc.ManagedChannel
import java.time.LocalDate
import java.util.logging.Logger
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.RecurringExchangeKey
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchange as InternalRecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.dataProvider as internalDataProvider
import org.wfanet.measurement.internal.kingdom.modelProvider as internalModelProvider
import org.wfanet.measurement.internal.kingdom.recurringExchange as internalRecurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails
import org.wfanet.measurement.kingdom.service.api.v2alpha.parseCertificateDer
import org.wfanet.measurement.kingdom.service.api.v2alpha.toInternal
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent

private val API_VERSION = Version.V2_ALPHA

/** Prepares resources for Panel Match integration tests using internal APIs. */
class PanelMatchResourceSetup(
  private val dataProvidersStub: DataProvidersCoroutineStub,
  private val modelProvidersStub: ModelProvidersCoroutineStub,
  private val recurringExchangesStub: RecurringExchangesCoroutineStub
) {

  /** The Channel can be used in the in-process integration test. */
  constructor(
    kingdomInternalApiChannel: Channel
  ) : this(
    DataProvidersCoroutineStub(kingdomInternalApiChannel),
    ModelProvidersCoroutineStub(kingdomInternalApiChannel),
    RecurringExchangesCoroutineStub(kingdomInternalApiChannel)
  )

  /** The ManagedChannel can be used in the deployed integration test. */
  constructor(
    kingdomInternalApiChannel: ManagedChannel
  ) : this(
    DataProvidersCoroutineStub(kingdomInternalApiChannel),
    ModelProvidersCoroutineStub(kingdomInternalApiChannel),
    RecurringExchangesCoroutineStub(kingdomInternalApiChannel)
  )

  /** Process to create resources with actual certificates. */
  suspend fun process(
    dataProviderContent: EntityContent,
    exchangeSchedule: String,
    apiVersion: String,
    exchangeWorkflow: ExchangeWorkflow,
    exchangeDate: Date,
    runId: String = LocalDate.now().toString(),
  ) {
    logger.info("Starting with RunID: $runId ...")

    // Step 2a: Create the MP.
    val externalModelProviderId = createModelProvider()
    val modelProviderName = ModelProviderKey(externalIdToApiId(externalModelProviderId)).toName()
    logger.info("Successfully created model provider: $modelProviderName")

    // Step 2b: Create the EDP.
    val externalDataProviderId = createDataProvider(dataProviderContent)
    val dataProviderName = DataProviderKey(externalIdToApiId(externalDataProviderId)).toName()
    logger.info("Successfully created data provider: $dataProviderName")

    val externalRecurringExchangeId =
      createRecurringExchange(
        externalDataProvider = externalDataProviderId,
        externalModelProvider = externalModelProviderId,
        exchangeDate = exchangeDate,
        exchangeSchedule = exchangeSchedule,
        publicApiVersion = apiVersion,
        exchangeWorkflow = exchangeWorkflow
      )
    val recurringExchangeName =
      RecurringExchangeKey(externalIdToApiId(externalRecurringExchangeId)).toName()
    logger.info("Successfully created Recurring Exchange: $recurringExchangeName.")
  }

  /** Create an internal dataProvider, and return its corresponding public API resource name. */
  suspend fun createDataProvider(dataProviderContent: EntityContent): Long {
    val encryptionPublicKey = dataProviderContent.encryptionPublicKey
    val signedPublicKey =
      signEncryptionPublicKey(encryptionPublicKey, dataProviderContent.signingKey)
    val certificateDer =
      parseCertificateDer(dataProviderContent.signingKey.certificate.encoded.toByteString())

    return dataProvidersStub
      .createDataProvider(
        internalDataProvider {
          certificate = certificateDer
          details =
            DataProviderKt.details {
              apiVersion = API_VERSION.string
              publicKey = signedPublicKey.data
              publicKeySignature = signedPublicKey.signature
            }
        }
      )
      .externalDataProviderId
  }

  private suspend fun createModelProvider(): Long {
    return modelProvidersStub.createModelProvider(internalModelProvider {}).externalModelProviderId
  }

  private suspend fun createRecurringExchange(
    externalDataProvider: Long,
    externalModelProvider: Long,
    exchangeDate: Date,
    exchangeSchedule: String,
    publicApiVersion: String,
    exchangeWorkflow: ExchangeWorkflow
  ): Long {
    return recurringExchangesStub
      .createRecurringExchange(
        createRecurringExchangeRequest {
          recurringExchange = internalRecurringExchange {
            externalDataProviderId = externalDataProvider
            externalModelProviderId = externalModelProvider
            state = InternalRecurringExchange.State.ACTIVE
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
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
