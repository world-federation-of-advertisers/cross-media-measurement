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

package org.wfanet.panelmatch.client.exchangetasks.testing

import com.google.protobuf.ByteString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.logger.TaskLog

private const val ATTEMPT_KEY = "some-arbitrary-attempt-key"

/** Helper for executing an [ExchangeTask] by handling the [StorageClient]. */
fun ExchangeTask.executeToByteStrings(
  vararg inputs: Pair<String, ByteString>
): Map<String, ByteString> {
  val storage = InMemoryStorageClient()

  fun writeBlob(blobKey: String, contents: ByteString): StorageClient.Blob = runBlocking {
    storage.writeBlob(blobKey, contents)
  }

  fun writeInputs(vararg inputs: Pair<String, ByteString>): Map<String, StorageClient.Blob> {
    return inputs.toMap().mapValues { writeBlob("underlying-key-for-${it.key}", it.value) }
  }

  return runBlocking(TaskLog(ATTEMPT_KEY) + Dispatchers.Default) {
    execute(writeInputs(*inputs)).mapValues { it.value.flatten() }
  }
}
