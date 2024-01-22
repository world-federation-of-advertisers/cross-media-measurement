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

package org.wfanet.panelmatch.client.privatemembership.testing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import org.junit.Test
import org.wfanet.panelmatch.client.common.joinKeyAndIdOf
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndId
import org.wfanet.panelmatch.client.privatemembership.LookupKeyAndId
import org.wfanet.panelmatch.client.privatemembership.QueryPreparer

private val DECRYPTED_JOIN_KEYS: List<JoinKeyAndId> =
  listOf(
    joinKeyAndIdOf(
      "some-decrypted-join-key-0".toByteStringUtf8(),
      "some-identifier0".toByteStringUtf8(),
    ),
    joinKeyAndIdOf(
      "some-decrypted-join-key-11".toByteStringUtf8(),
      "some-identifier1".toByteStringUtf8(),
    ),
    joinKeyAndIdOf(
      "some-decrypted-join-key-222".toByteStringUtf8(),
      "some-identifier2".toByteStringUtf8(),
    ),
    joinKeyAndIdOf(
      "some-decrypted-join-key-3333".toByteStringUtf8(),
      "some-identifier3".toByteStringUtf8(),
    ),
    joinKeyAndIdOf(
      "some-decrypted-join-key-44444".toByteStringUtf8(),
      "some-identifier4".toByteStringUtf8(),
    ),
  )

/** Abstract base class for testing implementations of [QueryPreparer]. */
abstract class AbstractQueryPreparerTest {
  abstract val queryPreparer: QueryPreparer
  abstract val identifierHashPepper: ByteString

  @Test
  fun testCryptor() {
    val joinKeyAndIds: List<LookupKeyAndId> =
      queryPreparer.prepareLookupKeys(identifierHashPepper, DECRYPTED_JOIN_KEYS)
    assertThat(joinKeyAndIds.size).isEqualTo(DECRYPTED_JOIN_KEYS.size)
  }

  @Test
  fun hashesAreEqual() {
    val joinKeyAndIds1: List<LookupKeyAndId> =
      queryPreparer.prepareLookupKeys(identifierHashPepper, DECRYPTED_JOIN_KEYS)
    val joinKeyAndIds2: List<LookupKeyAndId> =
      queryPreparer.prepareLookupKeys(identifierHashPepper, DECRYPTED_JOIN_KEYS)
    assertThat(joinKeyAndIds1).isEqualTo(joinKeyAndIds2)
  }

  @Test
  fun hashesAreNotEqual() {
    val joinKeyAndIds1: List<LookupKeyAndId> =
      queryPreparer.prepareLookupKeys(identifierHashPepper, DECRYPTED_JOIN_KEYS)
    val joinKeyAndIds2: List<LookupKeyAndId> =
      queryPreparer.prepareLookupKeys(
        "some-other-other-pepper".toByteStringUtf8(),
        DECRYPTED_JOIN_KEYS,
      )
    assertThat(joinKeyAndIds1).containsNoneIn(joinKeyAndIds2)
  }

  @Test
  fun joinKeyIdentifiersAreEqual() {
    val joinKeyAndIds: List<LookupKeyAndId> =
      queryPreparer.prepareLookupKeys(identifierHashPepper, DECRYPTED_JOIN_KEYS)
    assertThat(joinKeyAndIds.map { it.joinKeyIdentifier })
      .isEqualTo(DECRYPTED_JOIN_KEYS.map { it.joinKeyIdentifier })
  }
}
