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

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.storage.StorageClient

/** Interface for ExchangeTask. */
interface ExchangeTask {
  /**
   * Executes subclass on input map and returns the output.
   *
   * @param input input blobs as specified by the ExchangeWorkflow.
   * @return Executed output. It is a map from the labels to the payload associated with the label.
   */
  suspend fun execute(input: Map<String, StorageClient.Blob>): Map<String, Flow<ByteString>>

  /**
   * Indicates whether the executor running this task should skip reading inputs before calling
   * [execute]. If true, input blob presence is not verified and an empty map is passed to [execute]
   * . Otherwise, input blobs are read and passed to [execute], and the task will not run if any
   * input blobs are missing.
   *
   * Defaults to false.
   */
  fun skipReadInput(): Boolean = false
}
