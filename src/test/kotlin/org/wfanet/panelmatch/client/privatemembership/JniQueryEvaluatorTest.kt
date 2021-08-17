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

package org.wfanet.panelmatch.client.privatemembership

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.JniException

// TODO: subclass AbstractQueryEvaluatorTest once implemented
@RunWith(JUnit4::class)
class JniQueryEvaluatorTest {
  @Test
  fun `executeQueries is unimplemented`() {
    val queryEvaluator = JniQueryEvaluator()
    val e =
      assertFailsWith<JniException> { queryEvaluator.executeQueries(emptyList(), emptyList()) }
    assertThat(e.message).ignoringCase().contains("unimplemented")
  }

  @Test
  fun `combineResults is unimplemented`() {
    val queryEvaluator = JniQueryEvaluator()
    val e = assertFailsWith<JniException> { queryEvaluator.combineResults(emptySequence()) }
    assertThat(e.message).ignoringCase().contains("unimplemented")
  }
}
