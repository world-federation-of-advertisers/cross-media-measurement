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

package org.wfanet.panelmatch.common.secrets

import com.google.protobuf.ByteString
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.secrets.testing.AbstractSecretMapTest
import org.wfanet.panelmatch.common.toBase64

@RunWith(JUnit4::class)
class CsvSecretMapTest : AbstractSecretMapTest<CsvSecretMap>() {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Suppress("BlockingMethodInNonBlockingContext") // Suppress warning on TemporaryFolder operations
  override suspend fun secretMapOf(vararg items: Pair<String, ByteString>): CsvSecretMap {
    val file = temporaryFolder.newFile()
    file.writeText(items.joinToString("\n") { "${it.first},${it.second.toBase64()}" })

    return CsvSecretMap(file.toPath())
  }
}
