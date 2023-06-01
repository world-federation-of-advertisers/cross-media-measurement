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
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.CreateRecurringExchangeRequest
import org.wfanet.measurement.api.v2alpha.GetRecurringExchangeRequest
import org.wfanet.measurement.api.v2alpha.ListRecurringExchangesRequest
import org.wfanet.measurement.api.v2alpha.RetireRecurringExchangeRequest

@RunWith(JUnit4::class)
class RecurringExchangesServiceTest {

  val service = RecurringExchangesService()

  @Test
  fun createRecurringExchange() = runBlocking {
    val exception =
      assertFailsWith<StatusException> {
        service.createRecurringExchange(CreateRecurringExchangeRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNIMPLEMENTED)
  }

  @Test
  fun getRecurringExchange() = runBlocking {
    val exception =
      assertFailsWith<StatusException> {
        service.getRecurringExchange(GetRecurringExchangeRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNIMPLEMENTED)
  }

  @Test
  fun listRecurringExchanges() = runBlocking {
    val exception =
      assertFailsWith<StatusException> {
        service.listRecurringExchanges(ListRecurringExchangesRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNIMPLEMENTED)
  }

  @Test
  fun retireRecurringExchange() = runBlocking {
    val exception =
      assertFailsWith<StatusException> {
        service.retireRecurringExchange(RetireRecurringExchangeRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNIMPLEMENTED)
  }
}
