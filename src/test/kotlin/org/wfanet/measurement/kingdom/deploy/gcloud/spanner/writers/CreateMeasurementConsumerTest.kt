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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.testing.DeterministicIdGenerator
import org.wfanet.measurement.common.identity.testing.copy
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val ID_SEED = 1L

@RunWith(JUnit4::class)
class CreateMeasurementConsumerTest : KingdomDatabaseTestBase() {

  // private val clock = TestClockWithNamedInstants(Instant.now())

  @Test
  fun success() =
    runBlocking<Unit> {
      val measurementConsumer = MeasurementConsumer.newBuilder().build()
      val usedIdGenerator = DeterministicIdGenerator(ID_SEED)
      CreateMeasurementConsumer(measurementConsumer).execute(databaseClient, usedIdGenerator.copy())

      val internalId = usedIdGenerator.generateInternalId()
      val externalId = usedIdGenerator.generateExternalId()

      val measurementConsumers =
        databaseClient
          .singleUse(TimestampBound.strong())
          .executeQuery(Statement.of("SELECT * FROM MeasurementConsumers"))
          .toList()

      assertThat(measurementConsumers)
        .containsExactly(
          Struct.newBuilder()
            .set("MeasurementConsumerId")
            .to(internalId.value)
            .set("ExternalMeasurementConsumerId")
            .to(externalId.value)
            .set("MeasurementConsumerDetails")
            .toProtoBytes(measurementConsumer.details)
            .set("MeasurementConsumerDetailsJson")
            .toProtoJson(measurementConsumer.details)
            .build()
        )
    }
}
