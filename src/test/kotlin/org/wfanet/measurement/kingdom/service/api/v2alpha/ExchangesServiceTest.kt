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
import io.grpc.StatusRuntimeException
import kotlin.test.assertFails
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
import org.wfanet.measurement.api.v2alpha.exchange
import org.wfanet.measurement.api.v2alpha.getExchangeRequest
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.testing.makeModelProvider
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.kingdom.Exchange.State
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineImplBase as InternalExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineStub as InternalExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.exchange as internalExchange
import org.wfanet.measurement.internal.kingdom.exchangeDetails
import org.wfanet.measurement.internal.kingdom.getExchangeRequest as internalGetExchangeRequest

private val DATA_PROVIDER = makeDataProvider(12345L)
private val MODEL_PROVIDER = makeModelProvider(23456L)
private const val RECURRING_EXCHANGE_ID = 1L
private val DATE = date {
  year = 2021
  month = 3
  day = 14
}
private const val EXCHANGE_ID = "2021-03-14"

private val AUDIT_TRAIL_HASH = ByteString.copyFromUtf8("some arbitrary audit_trail_hash")

private val INTERNAL_EXCHANGE = internalExchange {
  externalRecurringExchangeId = RECURRING_EXCHANGE_ID
  date = DATE
  state = State.ACTIVE
  details = exchangeDetails { auditTrailHash = AUDIT_TRAIL_HASH }
}

@RunWith(JUnit4::class)
class ExchangesServiceTest {

  private val internalService: InternalExchangesCoroutineImplBase =
    mockService() { onBlocking { getExchange(any()) }.thenReturn(INTERNAL_EXCHANGE) }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalService) }

  private val service = ExchangesService(InternalExchangesCoroutineStub(grpcTestServerRule.channel))

  private fun getExchange(init: GetExchangeRequestKt.Dsl.() -> Unit): Exchange = runBlocking {
    service.getExchange(getExchangeRequest(init))
  }

  @Test
  fun `getExchange unauthenticated`() {
    val exchangeKey = ExchangeKey(null, null, externalIdToApiId(RECURRING_EXCHANGE_ID), EXCHANGE_ID)
    val e =
      assertFailsWith<StatusRuntimeException> {
        getExchange {
          name = exchangeKey.toName()
          dataProvider = DATA_PROVIDER
        }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `getExchange for DataProvider`() = runBlocking {
    val principal = DataProviderPrincipal(DataProviderKey(externalIdToApiId(12345L)))
    val provider = provider {
      type = Provider.Type.DATA_PROVIDER
      externalId = 12345L
    }

    val exchangeKey = ExchangeKey(null, null, externalIdToApiId(RECURRING_EXCHANGE_ID), EXCHANGE_ID)
    val response =
      withPrincipal(principal) {
        getExchange {
          name = exchangeKey.toName()
          dataProvider = DATA_PROVIDER
        }
      }

    assertThat(response)
      .isEqualTo(
        exchange {
          name = exchangeKey.toName()
          date = DATE
          state = Exchange.State.ACTIVE
          auditTrailHash = AUDIT_TRAIL_HASH
        }
      )

    verifyProtoArgument(internalService, InternalExchangesCoroutineImplBase::getExchange)
      .isEqualTo(
        internalGetExchangeRequest {
          externalRecurringExchangeId = RECURRING_EXCHANGE_ID
          date = DATE
          this.provider = provider
        }
      )
  }

  @Test
  fun `getExchange for DataProvider with wrong parent in Request`() {
    val principal = DataProviderPrincipal(DataProviderKey(externalIdToApiId(12345L)))

    withPrincipal(principal) { assertFails { getExchange { modelProvider = MODEL_PROVIDER } } }
  }

  @Test
  fun listExchanges() =
    runBlocking<Unit> {
      assertFailsWith(NotImplementedError::class) {
        service.listExchanges(ListExchangesRequest.getDefaultInstance())
      }
    }

  @Test
  fun uploadAuditTrail() =
    runBlocking<Unit> {
      assertFailsWith(NotImplementedError::class) { service.uploadAuditTrail(emptyFlow()) }
    }
}
