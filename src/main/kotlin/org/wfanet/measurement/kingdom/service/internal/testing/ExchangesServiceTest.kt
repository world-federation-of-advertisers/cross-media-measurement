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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.CreateExchangeRequest
import org.wfanet.measurement.internal.kingdom.CreateRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.DeleteExchangeRequest
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetDataProviderRequest
import org.wfanet.measurement.internal.kingdom.GetExchangeRequest
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamExchangesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.batchDeleteExchangesRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createExchangeRequest
import org.wfanet.measurement.internal.kingdom.deleteExchangeRequest
import org.wfanet.measurement.internal.kingdom.streamExchangesRequest
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

private const val MAX_BATCH_DELETE = 1000
private const val INTERNAL_RECURRING_EXCHANGE_ID = 111L
private const val EXTERNAL_RECURRING_EXCHANGE_ID = 222L
private val idGenerator =
  FixedIdGenerator(
    InternalId(INTERNAL_RECURRING_EXCHANGE_ID),
    ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID),
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
        apiVersion = Version.V2_ALPHA.string
        cronSchedule = "some arbitrary cron_schedule"
        exchangeWorkflow = ExchangeWorkflow.getDefaultInstance()
      }
      nextExchangeDateBuilder.apply {
        year = 2021
        month = 3
        day = 15
      }
    }
    .build()

private val EXCHANGE: Exchange =
  Exchange.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      state = Exchange.State.ACTIVE
      date = EXCHANGE_DATE
      detailsBuilder.apply {
        auditTrailHash = ByteString.copyFromUtf8("some arbitrary audit_trail_hash")
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
        apiVersion = Version.V2_ALPHA.string
        publicKey = ByteString.copyFromUtf8("This is a  public key.")
        publicKeySignature = ByteString.copyFromUtf8("This is a  public key signature.")
      }
      addAllRequiredExternalDuchyIds(DUCHIES.map { it.externalDuchyId })
    }
    .build()

/** Base test class for [ExchangesCoroutineImplBase] implementations. */
abstract class ExchangesServiceTest {

  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES)

  /**
   * Creates a ModelProvider using [idGenerator] to generate internal and external ids.
   *
   * TODO: replace with /ModelProviders service once that exists.
   */
  protected abstract fun createModelProvider(idGenerator: IdGenerator): ModelProvider

  /** Creates a /Exchanges service implementation using [idGenerator]. */
  protected abstract fun newExchangesService(idGenerator: IdGenerator): ExchangesCoroutineImplBase

  /** Creates a test subject. */
  protected abstract fun newDataProvidersService(
    idGenerator: IdGenerator
  ): DataProvidersCoroutineImplBase

  /** Creates a test subject. */
  protected abstract fun newRecurringExchangesService(
    idGenerator: IdGenerator
  ): RecurringExchangesCoroutineImplBase

  private lateinit var exchanges: ExchangesCoroutineImplBase

  @Before
  fun createRecurringExchange() =
    runBlocking<Unit> {
      createModelProvider(MODEL_ID_GENERATOR)

      val dataProvidersService = newDataProvidersService(DATA_PROVIDER_ID_GENERATOR)
      dataProvidersService.createDataProvider(DATA_PROVIDER)
      dataProvidersService.getDataProvider(
        GetDataProviderRequest.newBuilder()
          .apply { externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID }
          .build()
      )

      val recurringExchangesService = newRecurringExchangesService(idGenerator)
      val createRequest =
        CreateRecurringExchangeRequest.newBuilder()
          .apply {
            recurringExchange = RECURRING_EXCHANGE
            recurringExchangeBuilder.clearExternalRecurringExchangeId()
            recurringExchangeBuilder.clearState()
          }
          .build()

      recurringExchangesService.createRecurringExchange(createRequest)
    }

  @Before
  fun makeExchanges() {
    exchanges = newExchangesService(idGenerator)
  }

  @Test
  fun `createExchange and getExchange roundTrip succeeds`() {
    val createRequest = CreateExchangeRequest.newBuilder().apply { exchange = EXCHANGE }.build()

    assertThat(createExchange(createRequest)).isEqualTo(EXCHANGE)
    assertThat(getExchange()).isEqualTo(EXCHANGE)
  }

  @Test
  fun `createExchange ignores state`() {
    val createRequest =
      CreateExchangeRequest.newBuilder()
        .apply {
          exchange = EXCHANGE
          exchangeBuilder.state = Exchange.State.FAILED
        }
        .build()

    assertThat(createExchange(createRequest)).isEqualTo(EXCHANGE)
    assertThat(getExchange()).isEqualTo(EXCHANGE)
  }

  @Test
  fun `createExchange requires foreign keys`() {
    val createRequest =
      CreateExchangeRequest.newBuilder()
        .apply {
          exchange = EXCHANGE
          exchangeBuilder.clearExternalRecurringExchangeId()
        }
        .build()

    assertFails { createExchange(createRequest) }
    assertFails { getExchange() }
  }

  @Test
  fun `getExchange for missing exchange fails`() {
    assertFails { getExchange() }
  }

  @Test
  fun `streamExchange returns all exchanges`(): Unit = runBlocking {
    val createRequest1 = createExchangeRequest { exchange = EXCHANGE }
    val createRequest2 = createExchangeRequest {
      exchange =
        EXCHANGE.copy {
          date = date {
            year = 2021
            month = 1
            day = 1
          }
        }
    }

    val exchange1 = createExchange(createRequest1)
    val exchange2 = createExchange(createRequest2)

    val response = exchanges.streamExchanges(streamExchangesRequest {}).toList()

    assertThat(response).hasSize(2)
    assertThat(response).containsExactly(exchange1, exchange2)
  }

  @Test
  fun `streamExchange respects filter before date`(): Unit = runBlocking {
    val oldExchangeRequest = createExchangeRequest {
      exchange =
        EXCHANGE.copy {
          date = date {
            year = 2021
            month = 1
            day = 1
          }
        }
    }
    val newExchangeRequest = createExchangeRequest {
      exchange =
        EXCHANGE.copy {
          date = date {
            year = 2023
            month = 1
            day = 1
          }
        }
    }

    val oldExchange = createExchange(oldExchangeRequest)
    createExchange(newExchangeRequest)

    val response =
      exchanges
        .streamExchanges(
          streamExchangesRequest {
            filter = filter {
              dateBefore = date {
                year = 2022
                month = 1
                day = 1
              }
            }
          }
        )
        .toList()

    assertThat(response).containsExactly(oldExchange)
  }

  @Test
  fun `streamExchange respects limit`(): Unit = runBlocking {
    val createRequest1 = createExchangeRequest { exchange = EXCHANGE }
    val createRequest2 = createExchangeRequest {
      exchange =
        EXCHANGE.copy {
          date = date {
            year = 2021
            month = 1
            day = 1
          }
        }
    }

    createExchange(createRequest1)
    createExchange(createRequest2)

    val response = exchanges.streamExchanges(streamExchangesRequest { limit = 1 }).toList()

    assertThat(response).hasSize(1)
  }

  @Test
  fun `batchDeleteExchanges deletes all requested Exchanges`(): Unit = runBlocking {
    val exchangeRequest1 = createExchangeRequest { exchange = EXCHANGE }
    val exchangeRequest2 = createExchangeRequest {
      exchange =
        EXCHANGE.copy {
          date = date {
            year = 2021
            month = 1
            day = 1
          }
        }
    }
    createExchange(exchangeRequest1)
    createExchange(exchangeRequest2)

    val deleteExchangeRequest1 = deleteExchangeRequest {
      externalRecurringExchangeId = exchangeRequest1.exchange.externalRecurringExchangeId
      date = exchangeRequest1.exchange.date
    }
    val deleteExchangeRequest2 = deleteExchangeRequest {
      externalRecurringExchangeId = exchangeRequest2.exchange.externalRecurringExchangeId
      date = exchangeRequest2.exchange.date
    }

    exchanges.batchDeleteExchanges(
      batchDeleteExchangesRequest {
        requests += listOf(deleteExchangeRequest1, deleteExchangeRequest2)
      }
    )

    val allExchanges: List<Exchange> = exchanges.streamExchanges(streamExchangesRequest {}).toList()
    assertThat(allExchanges).hasSize(0)
  }

  @Test
  fun `batchDeleteExchanges does not delete any Exchange when any is missing`(): Unit =
    runBlocking {
      val exchangeRequest1 = createExchangeRequest { exchange = EXCHANGE }
      val validExchange = createExchange(exchangeRequest1)

      val validDeleteExchangeRequest = deleteExchangeRequest {
        externalRecurringExchangeId = exchangeRequest1.exchange.externalRecurringExchangeId
        date = exchangeRequest1.exchange.date
      }
      val missingExchangeRequest = deleteExchangeRequest {
        externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
        date = date {
          year = 2021
          month = 1
          day = 1
        }
      }

      assertFailsWith<StatusRuntimeException> {
        exchanges.batchDeleteExchanges(
          batchDeleteExchangesRequest {
            requests += listOf(validDeleteExchangeRequest, missingExchangeRequest)
          }
        )
      }

      val allExchanges: List<Exchange> =
        exchanges.streamExchanges(streamExchangesRequest { limit = 1 }).toList()

      assertThat(allExchanges).containsExactly(validExchange)
    }

  @Test
  fun `batchDeleteExchanges throws NOT_FOUND when Exchange is missing`(): Unit = runBlocking {
    val missingExchangeRequest = deleteExchangeRequest {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      date = date {
        year = 2021
        month = 1
        day = 1
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchanges.batchDeleteExchanges(
          batchDeleteExchangesRequest { requests += listOf(missingExchangeRequest) }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception).hasMessageThat().contains("Exchange not found")
  }

  @Test
  fun `batchDeleteExchanges throws INVALID_ARGUMENT when external recurring Exchange ID is missing`():
    Unit = runBlocking {
    val invalidExchangeRequest = deleteExchangeRequest {
      date = date {
        year = 2021
        month = 1
        day = 1
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchanges.batchDeleteExchanges(
          batchDeleteExchangesRequest { requests += listOf(invalidExchangeRequest) }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("not specified")
  }

  @Test
  fun `batchDeleteExchanges deletes throws INVALID_ARGUMENT when limit is exceeded`(): Unit =
    runBlocking {
      val deletionRequests = mutableListOf<DeleteExchangeRequest>()
      for (i in 1..MAX_BATCH_DELETE + 1) {
        deletionRequests.add(
          deleteExchangeRequest {
            externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
            date = date {
              year = 1 + 2 * i
              month = 1
              day = 31
            }
          }
        )
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          exchanges.batchDeleteExchanges(
            batchDeleteExchangesRequest { requests += deletionRequests }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("exceeds limit")
    }

  private fun createExchange(request: CreateExchangeRequest): Exchange {
    return runBlocking { exchanges.createExchange(request) }
  }

  private fun getExchange(): Exchange {
    val request =
      GetExchangeRequest.newBuilder()
        .apply {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = EXCHANGE_DATE
        }
        .build()
    return runBlocking { exchanges.getExchange(request) }
  }
}
