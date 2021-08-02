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

import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.CreateRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.GetRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase

private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L

/** Base test class for [RecurringExchangesCoroutineImplBase] implementations. */
abstract class RecurringExchangesServiceTest {
  private val idGenerator =
    FixedIdGenerator(
      InternalId(FIXED_GENERATED_INTERNAL_ID),
      ExternalId(FIXED_GENERATED_EXTERNAL_ID)
    )

  protected abstract fun newService(idGenerator: IdGenerator): RecurringExchangesCoroutineImplBase

  protected lateinit var service: RecurringExchangesCoroutineImplBase

  @Before
  fun initService() {
    service = newService(idGenerator)
  }

  @Test
  fun `is unimplemented`() {
    assertFailsWith<NotImplementedError> {
      runBlocking {
        service.createRecurringExchange(CreateRecurringExchangeRequest.getDefaultInstance())
      }
    }

    assertFailsWith<NotImplementedError> {
      runBlocking { service.getRecurringExchange(GetRecurringExchangeRequest.getDefaultInstance()) }
    }
  }
}
