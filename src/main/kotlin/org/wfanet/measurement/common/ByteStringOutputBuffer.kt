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

package org.wfanet.measurement.common

import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel

/**
 * Fixed capacity [ByteString.Output] buffer for outputting [ByteString]
 * instances.
 */
class ByteStringOutputBuffer(private val capacity: Int) : AutoCloseable {
  init { require(capacity > 0) }

  private val output = ByteString.newOutput(capacity)
  private val channel: WritableByteChannel = Channels.newChannel(output)

  /** Size of this buffer in bytes. */
  val size: Int
    get() = output.size()

  /** Whether the buffer is full. */
  val full: Boolean
    get() = size == capacity

  /** Clears all bytes from the buffer. */
  fun clear() = output.reset()

  /**
   * Puts the remaining bytes from the source buffer into this buffer until
   * either there are no more remaining bytes in the source or this buffer
   * is full.
   */
  fun putUntilFull(source: ByteBuffer) {
    if (full) return

    val remainingCapacity = capacity - size
    if (source.remaining() > remainingCapacity) {
      val originalLimit = source.limit()
      source.limit(source.position() + remainingCapacity)
      channel.write(source)
      source.limit(originalLimit)
    } else {
      channel.write(source)
    }
  }

  /** @see ByteString.Output.toByteString() */
  fun toByteString(): ByteString = output.toByteString()

  /** Closes the underlying [ByteString.Output]. */
  override fun close() {
    channel.close()
  }
}
