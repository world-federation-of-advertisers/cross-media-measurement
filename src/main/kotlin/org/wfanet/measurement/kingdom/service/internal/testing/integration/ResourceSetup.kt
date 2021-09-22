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

package org.wfanet.measurement.kingdom.service.internal.testing.integration

import com.google.protobuf.ByteString
import com.google.type.Date
import java.time.Instant
import java.time.LocalDate
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow as InternalExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflowKt.step as internalStep
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RecurringExchange as InternalRecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.dataProvider as internalDataProvider
import org.wfanet.measurement.internal.kingdom.exchangeWorkflow as internalExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.modelProvider as internalModelProvider
import org.wfanet.measurement.internal.kingdom.recurringExchange as internalRecurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails

/**
 * Class preparing resources required for the Panel Match Integration test using internal services.
 */
class ResourceSetup(
  private val dataProvidersService: DataProvidersCoroutineImplBase,
  private val modelProvidersService: ModelProvidersCoroutineImplBase,
  private val recurringExchangesService: RecurringExchangesCoroutineImplBase
) {

  /** Process to create resources. */
  suspend fun process(exchangeSchedule: String, exchangeWorkflow: ExchangeWorkflow): ProviderKey {

    val externalDataProviderId = createDataProvider()
    logger.info("Successfully created data provider: $externalDataProviderId.")
    val externalModelProviderId = createModelProvider()
    logger.info("Successfully created model provider: $externalModelProviderId.")

    val externalRecurringExchangeId =
      createRecurringExchange(
        externalDataProvider = externalDataProviderId,
        externalModelProvider = externalModelProviderId,
        exchangeDate = LocalDate.now().toProtoDate(),
        exchangeSchedule = exchangeSchedule,
        externalExchangeWorkflow = exchangeWorkflow
      )
    logger.info("Successfully created Recurring Exchange $externalRecurringExchangeId")

    return ProviderKey(
      DataProviderKey(externalIdToApiId(externalDataProviderId)).toName(),
      ModelProviderKey(externalIdToApiId(externalModelProviderId)).toName()
    )
  }

  private suspend fun createDataProvider(): Long {
    // TODO(@yunyeng): Get the certificate and details from client side and verify.
    return dataProvidersService.createDataProvider(
        internalDataProvider {
          certificate =
            certificate {
              notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
              notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
              details =
                CertificateKt.details {
                  x509Der = ByteString.copyFromUtf8("This is a certificate der.")
                }
            }
          details =
            DataProviderKt.details {
              apiVersion = "2"
              publicKey = ByteString.copyFromUtf8("This is a  public key.")
              publicKeySignature = ByteString.copyFromUtf8("This is a  public key signature.")
            }
        }
      )
      .externalDataProviderId
  }

  suspend fun createModelProvider(): Long {
    return modelProvidersService.createModelProvider(internalModelProvider {})
      .externalModelProviderId
  }

  suspend fun createRecurringExchange(
    externalDataProvider: Long,
    externalModelProvider: Long,
    exchangeDate: Date,
    exchangeSchedule: String,
    externalExchangeWorkflow: ExchangeWorkflow
  ): Long {
    return recurringExchangesService.createRecurringExchange(
        createRecurringExchangeRequest {
          recurringExchange =
            internalRecurringExchange {
              externalDataProviderId = externalDataProvider
              externalModelProviderId = externalModelProvider
              state = InternalRecurringExchange.State.ACTIVE
              details =
                recurringExchangeDetails {
                  cronSchedule = exchangeSchedule
                  exchangeWorkflow = externalExchangeWorkflow.toInternal()
                }
              nextExchangeDate = exchangeDate
            }
        }
      )
      .externalRecurringExchangeId
  }

  private fun ExchangeWorkflow.toInternal(): InternalExchangeWorkflow {
    val prerequisiteList =
      stepsList.map { step ->
        step.stepId.toInt() to
          Pair(step.partyValue, step.sharedInputLabelsMap.keys.map { it.toInt() })
      }
    return internalExchangeWorkflow {
      prerequisiteList.forEach {
        steps +=
          internalStep {
            stepIndex = it.first
            party = InternalExchangeWorkflow.Party.forNumber(it.second.first)
            prerequisiteStepIndices.addAll(it.second.second)
          }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

data class ProviderKey(val dataProviderKey: String, val modelProviderKey: String)
