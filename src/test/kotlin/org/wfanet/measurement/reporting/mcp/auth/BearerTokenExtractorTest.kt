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

package org.wfanet.measurement.reporting.mcp.auth

import kotlin.test.assertEquals
import kotlin.test.assertNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class BearerTokenExtractorTest {

  @Test
  fun `extracts token from valid Bearer header`() {
    val token = BearerTokenExtractor.extract("Bearer my-secret-token")
    assertEquals("my-secret-token", token)
  }

  @Test
  fun `extracts token case-insensitively`() {
    val token = BearerTokenExtractor.extract("bearer my-token")
    assertEquals("my-token", token)
  }

  @Test
  fun `trims whitespace from token`() {
    val token = BearerTokenExtractor.extract("Bearer   spaced-token  ")
    assertEquals("spaced-token", token)
  }

  @Test
  fun `returns null for missing header`() {
    val token = BearerTokenExtractor.extract(null)
    assertNull(token)
  }

  @Test
  fun `returns null for non-Bearer auth scheme`() {
    val token = BearerTokenExtractor.extract("Basic dXNlcjpwYXNz")
    assertNull(token)
  }

  @Test
  fun `returns null for empty token after Bearer prefix`() {
    val token = BearerTokenExtractor.extract("Bearer ")
    assertNull(token)
  }

  @Test
  fun `returns null for Bearer prefix only`() {
    val token = BearerTokenExtractor.extract("Bearer")
    assertNull(token)
  }
}
