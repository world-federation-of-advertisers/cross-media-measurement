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

package org.wfanet.measurement.edpaggregator.vidlabeler

import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class VirtualPeopleVidAssignerTest {
  /**
   * Verifies that `VirtualPeopleVidAssigner.fromCompiledNodeBlob` parses a serialized
   * `CompiledNode` and produces an assigner that returns the expected VID for a known input.
   *
   * Ignored until the reference model fixture lands on main.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#3951): Once PR #3951 lands
   *   `src/main/resources/testing/labeler/reference_test_model.textproto` on main:
   *     1. Add that fixture as a `data` dependency of this target and depend on `:vid_assigner`.
   *     2. Read the fixture, serialize the parsed `CompiledNode`, and build the assigner via
   *        `VirtualPeopleVidAssigner.fromCompiledNodeBlob`.
   *     3. Assign a known `LabelerInput` and assert the expected `virtualPersonId` using the golden
   *        input/VID pairs defined alongside the fixture.
   *     4. Remove the `@Ignore`.
   */
  @Ignore("Pending reference_test_model.textproto from #3951; see TODO")
  @Test
  fun `fromCompiledNodeBlob builds an assigner that returns the expected VID`() {
    TODO("Implement once #3951 lands the reference model fixture on main")
  }
}
