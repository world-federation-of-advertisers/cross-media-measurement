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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

@RunWith(JUnit4::class)
class CreateDataProviderTest : KingdomDatabaseTestBase() {

  @Test
  fun success() = runBlocking<Unit> {
    val idGenerator = FixedIdGenerator()
    val dataProvider = CreateDataProvider().execute(databaseClient, idGenerator)

    assertThat(dataProvider)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        DataProvider.newBuilder().apply {
          externalDataProviderId = idGenerator.externalId.value
        }.build()
      )

    val dataProviders = databaseClient
      .singleUse(TimestampBound.strong())
      .executeQuery(Statement.of("SELECT * FROM DataProviders"))
      .toList()

    assertThat(dataProviders)
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
