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

package org.wfanet.panelmatch.client.storage

import com.google.protobuf.ByteString
import java.lang.Exception
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** Interface for Storage adapter. */
interface Storage {
  class NotFoundException(filename: String) : Exception("$filename not found")

  /**
   * Reads input data from given path.
   *
   * @param path String location of input data to read from.
   * @return Input data.
   */
  @Throws(NotFoundException::class) suspend fun read(path: String): ByteString

  /**
   * Writes output data into given path.
   *
   * @param path String location of data to write to.
   */
  suspend fun write(path: String, data: ByteString)

  /**
   * Transforms values of [inputLabels] into the underlying blobs.
   *
   * If any blob can't be found, it throws [NotFoundException].
   */
  @Throws(NotFoundException::class)
  suspend fun batchRead(inputLabels: Map<String, String>): Map<String, ByteString> =
    withContext(Dispatchers.IO) {
      coroutineScope {
        inputLabels
          .mapValues { entry -> async(start = CoroutineStart.DEFAULT) { read(path = entry.value) } }
          .mapValues { entry -> entry.value.await() }
      }
    }

  /** Writes output [data] based on [outputLabels] */
  suspend fun batchWrite(outputLabels: Map<String, String>, data: Map<String, ByteString>) =
    withContext(Dispatchers.IO) {
      coroutineScope {
        for ((key, value) in outputLabels) {
          val payload = requireNotNull(data[key]) { "Key $key not found in ${data.keys}" }
          launch { write(path = value, data = payload) }
        }
      }
    }
}
