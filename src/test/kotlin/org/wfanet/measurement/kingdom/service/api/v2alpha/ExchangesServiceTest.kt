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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.GetExchangeRequestKt
import org.wfanet.measurement.api.v2alpha.ListExchangesRequest
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.exchange
import org.wfanet.measurement.api.v2alpha.getExchangeRequest
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.internal.kingdom.Exchange.State
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineImplBase as InternalExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineStub as InternalExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase as InternalRecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub as InternalRecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.exchange as internalExchange
import org.wfanet.measurement.internal.kingdom.exchangeDetails
import org.wfanet.measurement.internal.kingdom.getExchangeRequest as internalGetExchangeRequest
import org.wfanet.measurement.internal.kingdom.recurringExchange as internalRecurringExchange

@RunWith(JUnit4::class)
class ExchangesServiceTest {

  private val internalRecurringExchangesServiceMock: InternalRecurringExchangesCoroutineImplBase =
    mockService {
      onBlocking { getRecurringExchange(any()) }.thenReturn(INTERNAL_RECURRING_EXCHANGE)
    }
  private val internalExchangesServiceMock: InternalExchangesCoroutineImplBase = mockService {
    onBlocking { getExchange(any()) }.thenReturn(INTERNAL_EXCHANGE)
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalRecurringExchangesServiceMock)
    addService(internalExchangesServiceMock)
  }

  private val service =
    ExchangesService(
      InternalRecurringExchangesCoroutineStub(grpcTestServerRule.channel),
      InternalExchangesCoroutineStub(grpcTestServerRule.channel)
    )

  private fun getExchange(fillRequest: GetExchangeRequestKt.Dsl.() -> Unit): Exchange =
    runBlocking {
      service.getExchange(getExchangeRequest(fillRequest))
    }

  @Test
  fun `getExchange throws UNAUTHENTICATED for no principal`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        getExchange {
          name = EXCHANGE_KEY.toName()
          dataProvider = DATA_PROVIDER_KEY.toName()
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getExchange returns Exchange for DataProvider principal`() = runBlocking {
    val principal = DataProviderPrincipal(DATA_PROVIDER_KEY)

    val response = withPrincipal(principal) { getExchange { name = EXCHANGE_KEY.toName() } }

    assertThat(response)
      .isEqualTo(
        exchange {
          name = EXCHANGE_KEY.toName()
          date = EXCHANGE_DATE
          state = Exchange.State.ACTIVE
          auditTrailHash = AUDIT_TRAIL_HASH
        }
      )

    verifyProtoArgument(
        internalExchangesServiceMock,
        InternalExchangesCoroutineImplBase::getExchange
      )
      .isEqualTo(
        internalGetExchangeRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID.value
          date = EXCHANGE_DATE
        }
      )
  }

  @Test
  fun `getExchange returns Exchange for ModelProvider principal`() = runBlocking {
    val principal = ModelProviderPrincipal(MODEL_PROVIDER_KEY)

    val response = withPrincipal(principal) { getExchange { name = EXCHANGE_KEY.toName() } }

    assertThat(response)
      .isEqualTo(
        exchange {
          name = EXCHANGE_KEY.toName()
          date = EXCHANGE_DATE
          state = Exchange.State.ACTIVE
          auditTrailHash = AUDIT_TRAIL_HASH
        }
      )

    verifyProtoArgument(
        internalExchangesServiceMock,
        InternalExchangesCoroutineImplBase::getExchange
      )
      .isEqualTo(
        internalGetExchangeRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID.value
          date = EXCHANGE_DATE
        }
      )
  }

  @Test
  fun `getExchange throws PERMISSION_DENIED for incorrect principal`() {
    val principal = DataProviderPrincipal(DataProviderKey(ExternalId(404).apiId.value))

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(principal) { getExchange { name = EXCHANGE_KEY.toName() } }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listExchanges throws UNIMPLEMENTED`() = runBlocking {
    val exception =
      assertFailsWith(StatusException::class) {
        service.listExchanges(ListExchangesRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNIMPLEMENTED)
  }

  @Test
  fun `uploadAuditTrail throws UNIMPLEMENTED`() = runBlocking {
    val exception =
      assertFailsWith(StatusException::class) { service.uploadAuditTrail(emptyFlow()) }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNIMPLEMENTED)
  }

  companion object {
    private val EXTERNAL_RECURRING_EXCHANGE_ID = ExternalId(1)
    private val EXCHANGE_DATE = date {
      year = 2021
      month = 3
      day = 14
    }
    private val EXTERNAL_DATA_PROVIDER_ID = ExternalId(12345)
    private val EXTERNAL_MODEL_PROVIDER_ID = ExternalId(23456)

    private val DATA_PROVIDER_KEY = DataProviderKey(EXTERNAL_DATA_PROVIDER_ID.apiId.value)
    private val MODEL_PROVIDER_KEY = ModelProviderKey(EXTERNAL_MODEL_PROVIDER_ID.apiId.value)
    private val EXCHANGE_KEY =
      ExchangeKey(
        EXTERNAL_RECURRING_EXCHANGE_ID.apiId.value,
        EXCHANGE_DATE.toLocalDate().toString()
      )

    private val AUDIT_TRAIL_HASH = ByteString.copyFromUtf8("some arbitrary audit_trail_hash")

    private val INTERNAL_RECURRING_EXCHANGE = internalRecurringExchange {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID.value
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID.value
    }
    private val INTERNAL_EXCHANGE = internalExchange {
      externalRecurringExchangeId = INTERNAL_RECURRING_EXCHANGE.externalRecurringExchangeId
      date = EXCHANGE_DATE
      state = State.ACTIVE
      details = exchangeDetails { auditTrailHash = AUDIT_TRAIL_HASH }
    }
  }
}
