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

package org.wfanet.measurement.edpaggregator.dataavailability

enum class CompletionBlobType {
  EXTERNAL,
  VID_LABELER,
}

object CompletionBlobNames {
  private val VID_LABELER_DONE_NAME = Regex("^done-[0-9a-f]{64}$")

  fun matches(type: CompletionBlobType, fileName: String): Boolean =
    when (type) {
      CompletionBlobType.EXTERNAL -> fileName == "done"
      CompletionBlobType.VID_LABELER -> VID_LABELER_DONE_NAME.matches(fileName)
    }
}
