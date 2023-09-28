// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.common

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.produceIn
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class BlockingIteratorTest {

  @Test
  fun `hasNext returns false for empty iterator`() = runBlockingTest {
    val iterator = blockingIteratorOf()

    assertThat(iterator.hasNext()).isFalse()
  }

  @Test
  fun `hasNext returns true for new non empty iterator`() = runBlockingTest {
    val iterator = blockingIteratorOf("cat")

    assertThat(iterator.hasNext()).isTrue()
  }

  @Test
  fun `hasNext returns true for partially consumed iterator`() = runBlockingTest {
    val iterator = blockingIteratorOf("cat", "dog", "fox")
    iterator.next()

    assertThat(iterator.hasNext()).isTrue()
  }

  @Test
  fun `hasNext returns false for consumed iterator`() = runBlockingTest {
    val iterator = blockingIteratorOf("cat", "dog", "fox")
    iterator.next()
    iterator.next()
    iterator.next()

    assertThat(iterator.hasNext()).isFalse()
  }

  @Test
  fun `hasNext does not consume elements`() = runBlockingTest {
    val iterator = blockingIteratorOf("cat", "dog", "fox")

    assertThat(iterator.hasNext()).isTrue()
    assertThat(iterator.next()).isEqualTo("cat")
  }

  @Test
  fun `next returns next item`() = runBlockingTest {
    val iterator = blockingIteratorOf("cat", "dog", "fox")
    iterator.next()
    iterator.next()

    assertThat(iterator.next()).isEqualTo("fox")
  }

  @Test
  fun `next throws no such element exception when consumed`() = runBlockingTest {
    val iterator = blockingIteratorOf("cat", "dog", "fox")
    iterator.next()
    iterator.next()
    iterator.next()

    assertFailsWith<NoSuchElementException> { iterator.next() }
  }

  private fun CoroutineScope.blockingIteratorOf(vararg items: String): BlockingIterator<String> {
    val channel = flowOf(*items).produceIn(this)
    return BlockingIterator(channel, this.coroutineContext)
  }
}
