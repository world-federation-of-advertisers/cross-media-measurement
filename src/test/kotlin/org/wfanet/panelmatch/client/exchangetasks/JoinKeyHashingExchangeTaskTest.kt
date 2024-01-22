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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.privatemembership.JniQueryPreparer
import org.wfanet.panelmatch.client.privatemembership.LookupKeyAndId
import org.wfanet.panelmatch.client.privatemembership.LookupKeyAndIdCollection
import org.wfanet.panelmatch.client.privatemembership.testing.joinKeyAndIdOf

private const val ATTEMPT_KEY = "some-arbitrary-attempt-key"

private val PLAINTEXT_JOIN_KEYS =
  listOf(
    joinKeyAndIdOf("some plaintext0", "some identifier0"),
    joinKeyAndIdOf("some plaintext1", "some identifier1"),
    joinKeyAndIdOf("some plaintext2", "some identifier2"),
    joinKeyAndIdOf("some plaintext3", "some identifier3"),
    joinKeyAndIdOf("some plaintext4", "some identifier4"),
  )

private val PEPPER = "some-pepper-value".toByteStringUtf8()

@RunWith(JUnit4::class)
class JoinKeyHashingExchangeTaskTest {
  private val storage = InMemoryStorageClient()
  private val queryPreparer = JniQueryPreparer()
  private val saltedJoinKeys = queryPreparer.prepareLookupKeys(PEPPER, PLAINTEXT_JOIN_KEYS)

  private val blobOfPepper = runBlocking { storage.writeBlob("mp-pepper", PEPPER) }
  private val blobOfJoinKeys = runBlocking {
    storage.writeBlob(
      "join-keys",
      joinKeyAndIdCollection { joinKeyAndIds += PLAINTEXT_JOIN_KEYS }.toByteString(),
    )
  }

  @Test
  fun `hash inputs`() = withTestContext {
    val result =
      JoinKeyHashingExchangeTask.forHashing(queryPreparer)
        .execute(mapOf("pepper" to blobOfPepper, "join-keys" to blobOfJoinKeys))
    assertThat(parseResults(result.getValue("lookup-keys").flatten()))
      .containsExactlyElementsIn(saltedJoinKeys)
  }
}

private fun withTestContext(block: suspend () -> Unit) {
  runBlocking { withContext(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) { block() } }
}

private fun parseResults(cryptoResult: ByteString): List<LookupKeyAndId> {
  return LookupKeyAndIdCollection.parseFrom(cryptoResult).lookupKeyAndIdsList
}
