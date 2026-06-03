/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.mcp.auth

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class BearerTokenExtractorTest {

  @Test
  fun extractsTokenFromValidBearerHeader() {
    assertThat(BearerTokenExtractor.extract("Bearer my-secret-token"))
      .isEqualTo("my-secret-token")
  }

  @Test
  fun extractsTokenCaseInsensitively() {
    assertThat(BearerTokenExtractor.extract("bearer my-token")).isEqualTo("my-token")
  }

  @Test
  fun trimsWhitespaceFromToken() {
    assertThat(BearerTokenExtractor.extract("Bearer   spaced-token  "))
      .isEqualTo("spaced-token")
  }

  @Test
  fun returnsNullForMissingHeader() {
    assertThat(BearerTokenExtractor.extract(null as String?)).isNull()
  }

  @Test
  fun returnsNullForNonBearerScheme() {
    assertThat(BearerTokenExtractor.extract("Basic dXNlcjpwYXNz")).isNull()
  }

  @Test
  fun returnsNullForEmptyTokenAfterPrefix() {
    assertThat(BearerTokenExtractor.extract("Bearer ")).isNull()
  }

  @Test
  fun returnsNullForBearerPrefixOnly() {
    assertThat(BearerTokenExtractor.extract("Bearer")).isNull()
  }
}
