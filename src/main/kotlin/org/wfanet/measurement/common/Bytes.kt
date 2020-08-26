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

/** Returns a [ByteArray] containing all of the elements in this [Sequence]. */
fun Sequence<Byte>.toByteArray(size: Int): ByteArray {
  val iter = iterator()
  return ByteArray(size) { iter.next() }
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
