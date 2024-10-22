// Copyright 2020 The Cross-Media Measurement Authors
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
import com.google.cloud.Timestamp
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFails
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

@RunWith(JUnit4::class)
// TODO: Stop using the KingdomDatabaseTestBase, use some toy schema to test.
class SpannerWriterTest : KingdomDatabaseTestBase() {
  private val idGenerator = FixedIdGenerator()

  @Test
  fun `Transaction with no writes succeeds`() = runBlocking {
    val transaction =
      object : SpannerWriter<Long, String>() {
        override suspend fun TransactionScope.runTransaction(): Long {
          // A transaction can have no writes, but must still be non-empty (i.e. interact with the
          // DB in some way).
          return txn.executeQuery(statement("SELECT 1234")).single().getLong(0)
        }

        override fun ResultScope<Long>.buildResult(): String {
          return requireNotNull(transactionResult).toString()
        }
      }
    val result = transaction.execute(databaseClient, idGenerator)
    assertThat(result).isEqualTo("1234")
  }

  @Test
  fun `Transaction with writes succeeds`() = runBlocking {
    val transaction =
      object : SpannerWriter<InternalId, Timestamp>() {
        override suspend fun TransactionScope.runTransaction(): InternalId {
          val internalId = idGenerator.generateInternalId()
          transactionContext.buffer(
            Mutation.newInsertBuilder("Certificates")
              .set("CertificateId")
              .to(internalId.value)
              .set("SubjectKeyIdentifier")
              .to(ByteArray.copyFrom("something"))
              .set("NotValidBefore")
              .to("2021-09-27T12:30:00.45Z")
              .set("NotValidAfter")
              .to("2022-09-27T12:30:00.45Z")
              .set("RevocationState")
              .to(1)
              .set("CertificateDetails")
              .to(ByteArray.copyFrom(""))
              .build()
          )
          return internalId
        }

        override fun ResultScope<InternalId>.buildResult(): Timestamp {
          return commitTimestamp
        }
      }

    val commitTimestamp = transaction.execute(databaseClient, idGenerator)

    // Verify the transaction committed.
    val bound = TimestampBound.ofReadTimestamp(commitTimestamp)
    val readContext = databaseClient.singleUse(bound)
    val key = Key.of(idGenerator.internalId.value)
    val row = readContext.readRow("Certificates", key, listOf("RevocationState"))
    assertThat(requireNotNull(row).getLong(0)).isEqualTo(1)
  }

  @Test
  fun `multiple execution fails`() =
    runBlocking<Unit> {
      val transaction =
        object : SpannerWriter<Long, String>() {
          override suspend fun TransactionScope.runTransaction(): Long {
            return txn.executeQuery(statement("SELECT 1")).single().getLong(0)
          }

          override fun ResultScope<Long>.buildResult(): String {
            return "the-result"
          }
        }

      // First execution should succeed
      val result = transaction.execute(databaseClient, idGenerator)
      assertThat(result).isEqualTo("the-result")

      // Second execution should fail
      assertFails { transaction.execute(databaseClient, idGenerator) }
    }
}
