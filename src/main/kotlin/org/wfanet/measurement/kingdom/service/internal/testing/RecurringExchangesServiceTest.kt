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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFails
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.CreateRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.GetRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase

private const val EXTERNAL_RECURRING_EXCHANGE_ID = 111L
private const val INTERNAL_RECURRING_EXCHANGE_ID = 222L

private const val EXTERNAL_DATA_PROVIDER_ID = 333L
private const val INTERNAL_DATA_PROVIDER_ID = 444L

private const val EXTERNAL_MODEL_PROVIDER_ID = 555L
private const val INTERNAL_MODEL_PROVIDER_ID = 666L

private val RECURRING_EXCHANGE: RecurringExchange =
  RecurringExchange.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      state = RecurringExchange.State.ACTIVE
      detailsBuilder.apply {
        cronSchedule = "some arbitrary cron_schedule"
        exchangeWorkflow = ByteString.copyFromUtf8("some arbitrary exchange_workflow")
      }
      nextExchangeDateBuilder.apply {
        year = 2021
        month = 8
        day = 5
      }
    }
    .build()

private val PUBLIC_KEY = ByteString.copyFromUtf8("This is a  public key.")
private val PUBLIC_KEY_SIGNATURE = ByteString.copyFromUtf8("This is a  public key signature.")
private val PREFERRED_CERTIFICATE_DER = ByteString.copyFromUtf8("This is a certificate der.")
private val DATA_PROVIDER: DataProvider =
  DataProvider.newBuilder()
    .apply {
      preferredCertificateBuilder.apply {
        notValidBeforeBuilder.seconds = 12345
        notValidAfterBuilder.seconds = 23456
        detailsBuilder.x509Der = PREFERRED_CERTIFICATE_DER
      }
      detailsBuilder.apply {
        apiVersion = "2"
        publicKey = PUBLIC_KEY
        publicKeySignature = PUBLIC_KEY_SIGNATURE
      }
    }
    .build()

/** Base test class for [RecurringExchangesCoroutineImplBase] implementations. */
abstract class RecurringExchangesServiceTest {
  protected abstract fun createModelProvider(idGenerator: IdGenerator): ModelProvider
  protected abstract fun createDataProvider(
    dataProvider: DataProvider,
    idGenerator: IdGenerator
  ): DataProvider

  protected abstract fun newService(idGenerator: IdGenerator): RecurringExchangesCoroutineImplBase

  protected lateinit var service: RecurringExchangesCoroutineImplBase

  private fun createRecurringExchange(request: CreateRecurringExchangeRequest): RecurringExchange {
    return runBlocking { service.createRecurringExchange(request) }
  }

  private fun getRecurringExchange(): RecurringExchange {
    val request =
      GetRecurringExchangeRequest.newBuilder()
        .apply { externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID }
        .build()
    return runBlocking { service.getRecurringExchange(request) }
  }

  @Before
  fun initService() {
    service =
      newService(
        FixedIdGenerator(
          InternalId(INTERNAL_RECURRING_EXCHANGE_ID),
          ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID)
        )
      )
    createModelProvider(
      FixedIdGenerator(
        InternalId(INTERNAL_MODEL_PROVIDER_ID),
        ExternalId(EXTERNAL_MODEL_PROVIDER_ID)
      )
    )
    createDataProvider(
      DATA_PROVIDER,
      FixedIdGenerator(InternalId(INTERNAL_DATA_PROVIDER_ID), ExternalId(EXTERNAL_DATA_PROVIDER_ID))
    )
  }

  @Test
  fun `createRecurringExchange and getRecurringExchange roundTrip succeeds`() {
    val createRequest =
      CreateRecurringExchangeRequest.newBuilder()
        .apply {
          recurringExchange = RECURRING_EXCHANGE
          recurringExchangeBuilder.clearExternalRecurringExchangeId()
          recurringExchangeBuilder.clearState()
        }
        .build()

    assertThat(createRecurringExchange(createRequest)).isEqualTo(RECURRING_EXCHANGE)
    assertThat(getRecurringExchange()).isEqualTo(RECURRING_EXCHANGE)
  }

  @Test
  fun `createRecurringExchange ignores state and id`() {
    val createRequest =
      CreateRecurringExchangeRequest.newBuilder()
        .apply {
          recurringExchange = RECURRING_EXCHANGE
          recurringExchangeBuilder.externalRecurringExchangeId += 12345
          recurringExchangeBuilder.state = RecurringExchange.State.RETIRED
        }
        .build()

    assertThat(createRecurringExchange(createRequest)).isEqualTo(RECURRING_EXCHANGE)
    assertThat(getRecurringExchange()).isEqualTo(RECURRING_EXCHANGE)
  }

  @Test
  fun `createRecurringExchange requires foreign keys`() {
    val createRequest =
      CreateRecurringExchangeRequest.newBuilder()
        .apply {
          recurringExchange = RECURRING_EXCHANGE
          recurringExchangeBuilder.clearExternalRecurringExchangeId()
          recurringExchangeBuilder.clearExternalModelProviderId()
        }
        .build()

    assertFails { createRecurringExchange(createRequest) }
    assertFails { getRecurringExchange() }
  }

  @Test
  fun `getRecurringExchange for missing recurringExchange fails`() {
    assertFails { getRecurringExchange() }
  }
}
