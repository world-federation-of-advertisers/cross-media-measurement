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

package org.wfanet.panelmatch.common.beam

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.protobuf.stringValue
import java.io.File
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.testing.BeamTestBase
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class WriteShardedDataTest : BeamTestBase() {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun assignToShardReturnsNonNegativeValueForIntMinValue() {
    val shardCount = 10
    val item =
      object {
        override fun hashCode(): Int = Int.MIN_VALUE
      }

    val shard = item.assignToShard(shardCount)

    assertThat(shard).isAtLeast(0)
    assertThat(shard).isLessThan(shardCount)
  }

  @Test
  fun addsAllFilesInFileSpec() = runBlockingTest {
    val shardedFileName = ShardedFileName("foo-*-of-10")
    val rootPath = temporaryFolder.root.absolutePath
    val storageFactory =
      object : StorageFactory {
        override fun build(): StorageClient {
          return FileSystemStorageClient(File(rootPath))
        }
      }
    val input = pcollectionOf("Input", stringValue { value = "some-input" })
    input.apply(WriteShardedData(shardedFileName.spec, storageFactory))
    pipeline.run()

    val client = storageFactory.build()
    for (filename in shardedFileName.fileNames) {
      assertWithMessage("Shard $filename").that(client.getBlob(filename)).isNotNull()
    }
  }
}
