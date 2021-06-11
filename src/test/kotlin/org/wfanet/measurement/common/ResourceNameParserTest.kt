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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import java.lang.IllegalArgumentException
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ResourceNameParserTest {
  private val parser = ResourceNameParser("repositories/{repository}/pullRequests/{pull_request}")

  @Test
  fun `parseIdSegments returns null when name does not match pattern`() {
    val idSegments = parser.parseIdSegments("repositories/common-kotlin/foo/bar")
    assertNull(idSegments)
  }

  @Test
  fun `parseIdSegments returns ID segments when name matches pattern`() {
    val repository = "common-kotlin"
    val pullRequest = "123"
    val idSegments = parser.parseIdSegments("repositories/$repository/pullRequests/$pullRequest")
    assertThat(idSegments).containsExactly("repository", repository, "pull_request", pullRequest)
  }

  @Test
  fun `assembleName throws IllegalArgumentException when missing ID segment`() {
    assertFailsWith(IllegalArgumentException::class) {
      parser.assembleName(mapOf("repository" to "common-kotlin"))
    }
  }

  @Test
  fun `assembleName throws IllegalArgumentException when ID segment value is empty`() {
    assertFailsWith(IllegalArgumentException::class) {
      parser.assembleName(mapOf("repository" to "common-kotlin", "pull_request" to ""))
    }
  }

  @Test
  fun `assembleName return assembled resource name`() {
    val repository = "common-kotlin"
    val pullRequest = "123"
    val resourceName =
      parser.assembleName(mapOf("repository" to repository, "pull_request" to pullRequest))
    assertThat(resourceName).isEqualTo("repositories/$repository/pullRequests/$pullRequest")
  }

  @Test
  fun `throws IllegalArgumentException for UpperCamelCase literal segment`() {
    assertFailsWith(IllegalArgumentException::class) {
      ResourceNameParser("Repositories/{repository}")
    }
  }

  @Test
  fun `throws IllegalArgumentException for UpperCamelCase variable name`() {
    assertFailsWith(IllegalArgumentException::class) {
      ResourceNameParser("repositories/{Repository}")
    }
  }

  @Test
  fun `throws IllegalArgumentException for variable name ending with _id`() {
    assertFailsWith(IllegalArgumentException::class) {
      ResourceNameParser("repositories/{repository_id}")
    }
  }

  @Test
  fun `throws IllegalArgumentException for multi-variable segment`() {
    assertFailsWith(IllegalArgumentException::class) {
      ResourceNameParser("repositories/{organization}~{repository}")
    }
  }
}
