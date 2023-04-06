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
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.CreateRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.GetRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

private const val INTERNAL_RECURRING_EXCHANGE_ID = 111L
private const val EXTERNAL_RECURRING_EXCHANGE_ID = 222L
private val RECURRING_EXCHANGE_ID_GENERATOR =
  FixedIdGenerator(
    InternalId(INTERNAL_RECURRING_EXCHANGE_ID),
    ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID)
  )

private const val INTERNAL_DATA_PROVIDER_ID = 333L
private const val EXTERNAL_DATA_PROVIDER_ID = 444L
private val DATA_PROVIDER_ID_GENERATOR =
  FixedIdGenerator(InternalId(INTERNAL_DATA_PROVIDER_ID), ExternalId(EXTERNAL_DATA_PROVIDER_ID))

private const val INTERNAL_MODEL_PROVIDER_ID = 555L
private const val EXTERNAL_MODEL_PROVIDER_ID = 666L
private val MODEL_ID_GENERATOR =
  FixedIdGenerator(InternalId(INTERNAL_MODEL_PROVIDER_ID), ExternalId(EXTERNAL_MODEL_PROVIDER_ID))

private val RECURRING_EXCHANGE: RecurringExchange =
  RecurringExchange.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      state = RecurringExchange.State.ACTIVE
      detailsBuilder.apply {
        cronSchedule = "some arbitrary cron_schedule"
        exchangeWorkflow = ExchangeWorkflow.getDefaultInstance()
      }
      nextExchangeDateBuilder.apply {
        year = 2021
        month = 8
        day = 5
      }
    }
    .build()

private val DATA_PROVIDER: DataProvider =
  DataProvider.newBuilder()
    .apply {
      certificateBuilder.apply {
        notValidBeforeBuilder.seconds = 12345
        notValidAfterBuilder.seconds = 23456
        detailsBuilder.x509Der = ByteString.copyFromUtf8("This is a certificate der.")
      }
      detailsBuilder.apply {
        apiVersion = "2"
        publicKey = ByteString.copyFromUtf8("This is a  public key.")
        publicKeySignature = ByteString.copyFromUtf8("This is a  public key signature.")
      }
      addAllRequiredExternalDuchyIds(DUCHIES.map { it.externalDuchyId })
    }
    .build()

/** Base test class for [RecurringExchangesCoroutineImplBase] implementations. */
abstract class RecurringExchangesServiceTest {

  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES)

  /** Creates a /RecurringExchanges service implementation using [idGenerator]. */
  protected abstract fun newRecurringExchangesService(
    idGenerator: IdGenerator
  ): RecurringExchangesCoroutineImplBase

  /** Creates a test subject. */
  protected abstract fun newDataProvidersService(
    idGenerator: IdGenerator
  ): DataProvidersCoroutineImplBase

  /** Creates a test subject. */
  protected abstract fun newModelProvidersService(
    idGenerator: IdGenerator
  ): ModelProvidersCoroutineImplBase

  private lateinit var recurringExchanges: RecurringExchangesCoroutineImplBase

  @Before
  fun createDataProvider() {
    val service = newDataProvidersService(DATA_PROVIDER_ID_GENERATOR)
    runBlocking { service.createDataProvider(DATA_PROVIDER) }
  }

  @Before
  fun createModelProvider() {
    val service = newModelProvidersService(MODEL_ID_GENERATOR)
    runBlocking { service.createModelProvider(modelProvider {}) }
  }

  @Before
  fun makeRecurringExchanges() {
    recurringExchanges = newRecurringExchangesService(RECURRING_EXCHANGE_ID_GENERATOR)
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

  private fun createRecurringExchange(request: CreateRecurringExchangeRequest): RecurringExchange {
    return runBlocking { recurringExchanges.createRecurringExchange(request) }
  }

  private fun getRecurringExchange(): RecurringExchange {
    val request =
      GetRecurringExchangeRequest.newBuilder()
        .apply { externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID }
        .build()
    return runBlocking { recurringExchanges.getRecurringExchange(request) }
  }
}
