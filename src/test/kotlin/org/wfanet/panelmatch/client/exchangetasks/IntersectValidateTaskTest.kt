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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.joinKeyAndIdCollectionOf
import org.wfanet.panelmatch.client.exchangetasks.testing.executeToByteStrings

private val JOIN_KEYS: List<JoinKeyAndId> =
  (1..10).map {
    joinKeyAndId {
      joinKey = joinKey { key = "join-key-$it".toByteStringUtf8() }
      joinKeyIdentifier = joinKeyIdentifier { id = "join-key-identifier-$it".toByteStringUtf8() }
    }
  }

private const val DEFAULT_MAX_SIZE = 10
private const val MAXIMUM_NEW_ITEMS_ALLOWED = 2

@RunWith(JUnit4::class)
class IntersectValidateTaskTest {
  private fun runIntersectAndValidate(
    previousData: List<JoinKeyAndId>? = JOIN_KEYS,
    currentData: List<JoinKeyAndId>? = JOIN_KEYS,
    maxSize: Int = DEFAULT_MAX_SIZE,
    isFirstExchange: Boolean = false,
  ): Map<String, ByteString> {
    val inputs = mutableListOf<Pair<String, ByteString>>()

    if (previousData != null)
      inputs.add("previous-data" to joinKeyAndIdCollectionOf(previousData).toByteString())
    if (currentData != null)
      inputs.add("current-data" to joinKeyAndIdCollectionOf(currentData).toByteString())

    return IntersectValidateTask(
        maxSize = maxSize,
        maximumNewItemsAllowed = MAXIMUM_NEW_ITEMS_ALLOWED,
        isFirstExchange = isFirstExchange,
      )
      .executeToByteStrings(*inputs.toTypedArray())
  }

  private fun assertIntersectAndValidateHasCorrectOutput(
    previousData: List<JoinKeyAndId>? = JOIN_KEYS,
    currentData: List<JoinKeyAndId>? = JOIN_KEYS,
    maxSize: Int = DEFAULT_MAX_SIZE,
    isFirstExchange: Boolean = false,
  ) {
    val outputs = runIntersectAndValidate(previousData, currentData, maxSize, isFirstExchange)
    val outputJoinKeys =
      JoinKeyAndIdCollection.parseFrom(outputs.getValue("current-data")).joinKeyAndIdsList
    assertThat(outputJoinKeys).containsExactlyElementsIn(currentData)
  }

  @Test
  fun newIsSubsetOfOld() {
    for (i in JOIN_KEYS.indices) {
      assertIntersectAndValidateHasCorrectOutput(currentData = JOIN_KEYS.take(i))
    }
  }

  @Test
  fun maximumNewItems() {
    assertIntersectAndValidateHasCorrectOutput(previousData = JOIN_KEYS.drop(2))
  }

  @Test
  fun tooManyNewItems() {
    assertFailsWith<IllegalArgumentException> {
      runIntersectAndValidate(previousData = JOIN_KEYS.drop(3))
    }
  }

  @Test
  fun tooManyItems() {
    assertFailsWith<IllegalArgumentException> { runIntersectAndValidate(maxSize = 9) }
  }

  @Test
  fun disjointSets() {
    val previousData = JOIN_KEYS.drop(5)
    val currentData = JOIN_KEYS.take(5)
    assertThat(previousData).containsNoneIn(currentData) // Sanity check that they are disjoint
    assertFailsWith<IllegalArgumentException> {
      runIntersectAndValidate(previousData = previousData, currentData = currentData)
    }
  }

  @Test
  fun missingInputs() {
    assertFailsWith<NoSuchElementException> { runIntersectAndValidate(currentData = null) }
    assertFailsWith<NoSuchElementException> { runIntersectAndValidate(previousData = null) }
  }

  @Test
  fun firstExchange() {
    assertIntersectAndValidateHasCorrectOutput(previousData = null, isFirstExchange = true)
  }
}
