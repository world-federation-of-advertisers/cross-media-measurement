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

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.channels.ChannelIterator
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.runBlocking

/** [Iterator] that wraps [channelIterator] using [runBlocking]. */
class BlockingIterator<T : Any>(
  channel: ReceiveChannel<T>,
  private val context: CoroutineContext = EmptyCoroutineContext,
) : Iterator<T> {
  private val channelIterator: ChannelIterator<T> = channel.iterator()
  private var next: T? = null

  /**
   * Internal version of [hasNext] which accounts for the fact that [ChannelIterator.hasNext]
   * advances the iterator.
   */
  private suspend fun hasNextInternal(): Boolean {
    if (next != null) {
      return true
    }

    return if (channelIterator.hasNext()) {
      next = channelIterator.next()
      true
    } else {
      false
    }
  }

  override fun hasNext(): Boolean = runBlocking(context) { hasNextInternal() }

  override fun next(): T {
    if (!hasNext()) {
      throw NoSuchElementException("No more elements")
    }

    val element = next!!
    next = null
    return element
  }
}
