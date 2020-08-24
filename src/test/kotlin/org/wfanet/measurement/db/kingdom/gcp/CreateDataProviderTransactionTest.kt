// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.FixedIdGenerator
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase

@RunWith(JUnit4::class)
class CreateDataProviderTransactionTest : KingdomDatabaseTestBase() {
  private val idGenerator = FixedIdGenerator()
  private val transaction = CreateDataProviderTransaction(idGenerator)

  @Test
  fun success() = runBlocking<Unit> {
    databaseClient.runReadWriteTransaction { transactionContext ->
      transaction.execute(transactionContext)
    }

    val advertisers = databaseClient
      .singleUse(TimestampBound.strong())
      .executeQuery(Statement.of("SELECT * FROM DataProviders"))
      .asSequence()
      .toList()

    assertThat(advertisers)
      .containsExactly(
        Struct.newBuilder()
          .set("DataProviderId").to(idGenerator.internalId.value)
          .set("ExternalDataProviderId").to(idGenerator.externalId.value)
          .set("DataProviderDetails").to(ByteArray.copyFrom(""))
          .set("DataProviderDetailsJson").to("")
          .build()
      )
  }
}
