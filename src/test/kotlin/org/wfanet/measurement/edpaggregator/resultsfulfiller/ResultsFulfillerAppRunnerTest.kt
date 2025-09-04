/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import java.io.File
import java.nio.file.Files
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ResultsFulfillerAppRunnerTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  @Test
  fun `saveSecretToFile writes bytes to file`() {
    val testFile = tempFolder.newFile("test.pem")
    val data = "testdata".toByteArray()
    val runner = ResultsFulfillerAppRunner()

    runner.saveByteArrayToFile(data, testFile.absolutePath)

    assertThat(Files.exists(testFile.toPath())).isTrue()
    assertThat(data).isEqualTo(Files.readAllBytes(testFile.toPath()))
  }

  @Test
  fun `saveSecretToFile creates parent directories`() {
    val nestedFile = File(tempFolder.root, "nested/dir/file.pem")
    val data = "nested-data".toByteArray()
    val runner = ResultsFulfillerAppRunner()

    runner.saveByteArrayToFile(data, nestedFile.absolutePath)

    assertThat(nestedFile.exists()).isTrue()
    assertThat(data).isEqualTo(nestedFile.readBytes())
  }
}
