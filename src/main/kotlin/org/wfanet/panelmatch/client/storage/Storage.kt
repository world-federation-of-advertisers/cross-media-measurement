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
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext

/** Interface for Storage adapter. */
interface Storage {

  /**
   * Reads input data from given path.
   *
   * @param path String location of input data to read from.
   * @return Input data.
   */
  suspend fun read(path: String): ByteString

  /**
   * Writes output data into given path.
   *
   * @param path String location of data to write to.
   */
  suspend fun write(path: String, data: ByteString)

  /** Reads different data from [storage] and returns Map<String, ByteString> of different input */
  suspend fun batchRead(inputLabels: Map<String, String>): Map<String, ByteString> =
    withContext(Dispatchers.IO) {
      coroutineScope {
        inputLabels
          .mapValues { entry -> async(start = CoroutineStart.DEFAULT) { read(path = entry.value) } }
          .mapValues { entry -> entry.value.await() }
      }
    }

  /** Writes output [data] to [storage] based on [outputLabels] */
  suspend fun batchWrite(outputLabels: Map<String, String>, data: Map<String, ByteString>) =
    withContext(Dispatchers.IO) {
      coroutineScope {
        for ((key, value) in outputLabels) {
          async { write(path = value, data = requireNotNull(data[key])) }
        }
      }
    }
}
