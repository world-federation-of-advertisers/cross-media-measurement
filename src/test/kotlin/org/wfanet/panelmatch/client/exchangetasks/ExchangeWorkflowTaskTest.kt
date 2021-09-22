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

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.flowOf
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.exchangetasks.WorkflowExchangeTask.Workflow
import org.wfanet.panelmatch.client.exchangetasks.WorkflowExchangeTask.Workflow.WorkflowOutputs
import org.wfanet.panelmatch.client.storage.testing.makeTestVerifiedStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest
import org.wfanet.panelmatch.common.toByteString

private const val OUTPUT_URI_PREFIX = "some-output-uri-prefix"

@RunWith(JUnit4::class)
class ExchangeWorkflowTaskTest {

  @Test
  fun `multiple inputs and outputs`() = runBlockingTest {
    val storageClient = makeTestVerifiedStorageClient()
    storageClient.createBlob("key1", flowOf("ABC".toByteString()))
    storageClient.createBlob("key2", flowOf("DEF".toByteString()))
    val inputs = listOf("key1", "key2").associateWith { storageClient.getBlob(it) }
    val outputs = WorkflowExchangeTask(OUTPUT_URI_PREFIX, FakeWorkflow).execute(inputs)
    assertThat(outputs.mapValues { it.value.flatten().toStringUtf8() }.toList())
      .containsExactly(
        "Out: key1" to "Out: ABC",
        "Out: key2" to "Out: DEF",
        "some-blob-output-key" to "some-blob-output-value"
      )
  }
}

private object FakeWorkflow : Workflow {
  override suspend fun execute(
    outputUriPrefix: String,
    inputUris: Map<String, String>
  ): WorkflowOutputs {
    assertThat(outputUriPrefix).isEqualTo(OUTPUT_URI_PREFIX)
    return WorkflowOutputs(
      mapOf("some-blob-output-key" to flowOf("some-blob-output-value".toByteString())),
      inputUris.mapKeys { "Out: ${it.key}" }.mapValues { "Out: ${it.value}" }
    )
  }
}
