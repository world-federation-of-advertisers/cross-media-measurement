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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFails
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.copyOptions
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class SharedStorageTaskTest {
  @Test
  fun singleFile() = runBlockingTest {
    val copyOptions = copyOptions { labelType = BLOB }
    val destination = FakeWriteToDestination()
    val readFromSource = makeReadFromSource("abc" to "xyz")
    val task = SharedStorageTask(readFromSource, destination::write, copyOptions, "abc", "def")
    task.execute()
    assertThat(destination.data).containsExactly("def", "xyz")
  }

  @Test
  fun manifest() = runBlockingTest {
    val copyOptions = copyOptions { labelType = MANIFEST }
    val destination = FakeWriteToDestination()
    val readFromSource =
      makeReadFromSource(
        "abc" to "foo-?-of-2",
        "foo-0-of-2" to "123",
        "foo-1-of-2" to "456",
      )
    val task = SharedStorageTask(readFromSource, destination::write, copyOptions, "abc", "def")
    task.execute()
    assertThat(destination.data.toList())
      .containsExactly(
        "def" to "foo-?-of-2",
        "foo-0-of-2" to "123",
        "foo-1-of-2" to "456",
      )
  }

  @Test
  fun missingBlob() = runBlockingTest {
    val copyOptions = copyOptions { labelType = BLOB }
    val destination = FakeWriteToDestination()
    val readFromSource = makeReadFromSource()
    val task = SharedStorageTask(readFromSource, destination::write, copyOptions, "non-key", "abc")
    assertFails { task.execute() }
    assertThat(destination.data).isEmpty()
  }
}

private fun makeReadFromSource(
  vararg items: Pair<String, String>
): suspend (String) -> Flow<ByteString> {
  val map = mapOf(*items)
  return { key -> flowOf(map.getValue(key).toByteStringUtf8()) }
}

private class FakeWriteToDestination {
  val data = mutableMapOf<String, String>()

  suspend fun write(key: String, data: Flow<ByteString>) {
    require(key !in this.data)
    this.data[key] = data.flatten().toStringUtf8()
  }
}
