// Copyright 2023 The Cross-Media Measurement Authors
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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.exchangetasks.testing.executeToByteStrings

@RunWith(JUnit4::class)
class AssignJoinKeyIdsTaskTest {
  private fun runAssignIds(joinKeys: List<JoinKey> = JOIN_KEYS): List<JoinKeyAndId> {
    val joinKeyCollection = joinKeyCollection { this.joinKeys += joinKeys }
    val inputs = mutableListOf<Pair<String, ByteString>>()
    inputs.add("join-keys" to joinKeyCollection.toByteString())
    val taskOutputs = AssignJoinKeyIdsTask().executeToByteStrings(*inputs.toTypedArray())
    return JoinKeyAndIdCollection.parseFrom(taskOutputs.getValue("shuffled-join-key-and-ids"))
      .joinKeyAndIdsList
  }

  private fun List<JoinKey>.sort(): List<JoinKey> {
    return this.sortedBy { it.key.toStringUtf8() }
  }

  @Test
  fun assignIds() {
    val shuffledJoinKeyAndIds = runAssignIds()
    assertThat(shuffledJoinKeyAndIds.size).isEqualTo(JOIN_KEYS.size)

    val joinKeyIdentifiers = shuffledJoinKeyAndIds.map { requireNotNull(it.joinKeyIdentifier) }
    assertThat(joinKeyIdentifiers).containsNoDuplicates()

    val shuffledJoinKeys = shuffledJoinKeyAndIds.map { requireNotNull(it.joinKey) }
    assertThat(shuffledJoinKeys).containsExactlyElementsIn(JOIN_KEYS)
    assertThat(shuffledJoinKeys.sort()).containsExactlyElementsIn(JOIN_KEYS.sort()).inOrder()
  }

  @Test
  fun missingInputs() {
    assertFailsWith<IllegalArgumentException> { runAssignIds(joinKeys = emptyList()) }
  }

  companion object {
    private val JOIN_KEYS: List<JoinKey> =
      (1..10).map { joinKey { key = "join-key-$it".toByteStringUtf8() } }
  }
}
