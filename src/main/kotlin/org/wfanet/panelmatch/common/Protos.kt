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

package org.wfanet.panelmatch.common

import com.google.protobuf.ByteString
import com.google.protobuf.MessageLite
import java.io.InputStream
import java.time.Duration

/** Reads length-delimited [T] messages from a [ByteString]. */
fun <T : MessageLite> ByteString.parseDelimitedMessages(prototype: T): Iterable<T> =
  newInput().parseDelimitedMessages(prototype)

/** Reads length-delimited [T] messages from an [InputStream]. */
fun <T : MessageLite> InputStream.parseDelimitedMessages(prototype: T): Iterable<T> = Iterable {
  iterator {
    this@parseDelimitedMessages.use { inputStream ->
      val parser = prototype.parserForType
      while (true) {
        @Suppress("UNCHECKED_CAST") // MessageLite::getParseForType guarantees this cast is safe.
        val message = parser.parseDelimitedFrom(inputStream) as T? ?: break
        yield(message)
      }
    }
  }
}

/** Serializes a [MessageLite] with its length. */
fun MessageLite.toDelimitedByteString(): ByteString {
  val outputStream = ByteString.newOutput()
  writeDelimitedTo(outputStream)
  return outputStream.toByteString()
}

/** Converts a java.time.Duration to com.google.protobuf.Duration */
fun Duration.toProto(): com.google.protobuf.Duration {
  return com.google.protobuf.Duration.newBuilder().setSeconds(seconds).setNanos(nano).build()
}
