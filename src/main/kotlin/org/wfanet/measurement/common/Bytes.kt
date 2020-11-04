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

import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.withContext

const val BYTES_PER_MIB = 1024 * 1024

fun ByteArray.toByteString(): ByteString = ByteString.copyFrom(this)

fun Iterable<ByteArray>.toByteString(): ByteString {
  val totalSize = sumBy { it.size }

  return ByteString.newOutput(totalSize).use { output ->
    forEach { output.write(it) }
    output.toByteString()
  }
}

/** Returns a [ByteString] which is the concatenation of all elements. */
fun Iterable<ByteString>.flatten(): ByteString {
  return fold(ByteString.EMPTY) { acc, value -> acc.concat(value) }
}

/** Copies all bytes in a list of [ByteString]s into a [ByteArray]. */
fun Iterable<ByteString>.toByteArray(): ByteArray {
  // Allocate a ByteBuffer large enough for all the bytes in all the byte strings.
  val buffer = ByteBuffer.allocate(sumBy { it.size })
  forEach { byteString -> byteString.copyTo(buffer) }
  return buffer.array()
}

/** @see ByteString.size(). */
val ByteString.size: Int
  get() = size()

/**
 * Returns a [ByteString] with the same contents, padded with zeros in its
 * most-significant bits until it reaches the specified size.
 *
 * If this [ByteString]'s size is already at least the specified size, it will
 * be returned instead of a new one.
 *
 * @param paddedSize the size of the padded [ByteString]
 */
fun ByteString.withPadding(paddedSize: Int): ByteString {
  if (size >= paddedSize) {
    return this
  }

  return ByteString.newOutput(paddedSize).use { output ->
    repeat(paddedSize - size) { output.write(0x00) }
    output.toByteString()
  }.concat(this)
}

/** Returns a [ByteString] containing the specified elements. */
fun byteStringOf(vararg bytesAsInts: Int): ByteString {
  return ByteString.newOutput(bytesAsInts.size).use {
    for (byteAsInt in bytesAsInts) {
      it.write(byteAsInt)
    }
    it.toByteString()
  }
}

/**
 * Returns a [ByteString] which is the concatenation of the elements.
 *
 * This is a terminal [Flow] operation.
 */
suspend fun Flow<ByteString>.flatten(): ByteString {
  return fold(ByteString.EMPTY) { acc, value -> acc.concat(value) }
}

suspend fun Flow<ByteString>.toByteArray(): ByteArray = flatten().toByteArray()

/**
 * Creates a flow that produces [ByteString] values with the specified
 * [size][bufferSize] from this [ByteArray].
 *
 * The final produced value may have [size][ByteString.size] < [bufferSize].
 */
fun ByteArray.asBufferedFlow(bufferSize: Int): Flow<ByteString> =
  ByteBuffer.wrap(this).asBufferedFlow(bufferSize)

/**
 * Creates a flow that produces [ByteString] values with the specified
 * [size][bufferSize] from this [ByteBuffer].
 *
 * The final produced value may have [size][ByteString.size] < [bufferSize].
 */
fun ByteBuffer.asBufferedFlow(bufferSize: Int): Flow<ByteString> = flow {
  ByteStringOutputBuffer(bufferSize).use { outputBuffer ->
    outputBuffer.putEmittingFull(listOf(this@asBufferedFlow), this)

    // Emit a final value with whatever is left.
    if (outputBuffer.size > 0) {
      emit(outputBuffer.toByteString())
    }
  }
}

/**
 * Creates a flow that produces [ByteString] values with the specified
 * [size][bufferSize] from this [ByteString].
 *
 * The final produced value may have [size][ByteString.size] < [bufferSize].
 *
 * This will produce an empty flow if the receiver is empty.
 */
fun ByteString.asBufferedFlow(bufferSize: Int): Flow<ByteString> = flow {
  require(bufferSize > 0)
  for (begin in 0 until size() step bufferSize) {
    emit(substring(begin, minOf(size(), begin + bufferSize)))
  }
}

/**
 * Creates a flow that produces [ByteString] values with the specified
 * [size][bufferSize] from this [Flow].
 *
 * The final produced value may have [size][ByteString.size] < [bufferSize].
 */
fun Flow<ByteString>.asBufferedFlow(bufferSize: Int): Flow<ByteString> = flow {
  ByteStringOutputBuffer(bufferSize).use { outputBuffer ->
    collect {
      outputBuffer.putEmittingFull(it.asReadOnlyByteBufferList(), this)
    }

    // Emit a final value with whatever is left.
    if (outputBuffer.size > 0) {
      emit(outputBuffer.toByteString())
    }
  }
}

/**
 * Puts all of the remaining bytes from [source] into this buffer, emitting its
 * contents to [collector] and clearing it whenever it gets full.
 */
private suspend fun ByteStringOutputBuffer.putEmittingFull(
  source: Iterable<ByteBuffer>,
  collector: FlowCollector<ByteString>
) {
  for (buffer in source) {
    while (buffer.hasRemaining()) {
      putUntilFull(buffer)
      if (full) {
        collector.emit(toByteString())
        clear()
      }
    }
  }
}

/**
 * Creates a [Flow] that produces [ByteString] values from this
 * [ReadableByteChannel].
 *
 * @param bufferSize size in bytes of the buffer to use to read from the channel
 */
@OptIn(ExperimentalCoroutinesApi::class) // For `onCompletion`.
fun ReadableByteChannel.asFlow(bufferSize: Int): Flow<ByteString> = flow {
  val buffer = ByteBuffer.allocate(bufferSize)

  // Suppressed for https://youtrack.jetbrains.com/issue/IDEA-223285
  @Suppress("BlockingMethodInNonBlockingContext")
  while (read(buffer) >= 0) {
    if (buffer.position() == 0) {
      // Nothing was read, so we may have a non-blocking channel that nothing
      // can be read from right now. Suspend this coroutine to avoid
      // monopolizing the thread.
      delay(1L)
      continue
    }
    buffer.flip()
    emit(ByteString.copyFrom(buffer))
    buffer.clear()
  }
}.onCompletion { withContext(Dispatchers.IO) { close() } }.flowOn(Dispatchers.IO)

/**
 * Converts a hex string to its equivalent [ByteString].
 */
fun String.hexAsByteString(): ByteString {
  return BaseEncoding.base16().decode(this.toUpperCase()).toByteString()
}

/**
 * Converts a [ByteArray] into an upperbase hex string.
 */
fun ByteArray.toHexString(): String {
  return BaseEncoding.base16().upperCase().encode(this)
}
