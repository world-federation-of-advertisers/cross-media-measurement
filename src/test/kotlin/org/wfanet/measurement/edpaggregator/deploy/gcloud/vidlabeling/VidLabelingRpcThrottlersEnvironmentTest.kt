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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class VidLabelingRpcThrottlersEnvironmentTest {
  @Test
  fun `load reads all supported environment variables`() {
    val names = mutableListOf<String>()

    VidLabelingRpcThrottlersEnvironment.load { name ->
      names += name
      null
    }

    assertThat(names)
      .containsExactly(
        VidLabelingRpcThrottlersEnvironment.KINGDOM_MIN_INTERVAL_ENV,
        VidLabelingRpcThrottlersEnvironment.METADATA_READ_MIN_INTERVAL_ENV,
        VidLabelingRpcThrottlersEnvironment.METADATA_WRITE_MIN_INTERVAL_ENV,
        VidLabelingRpcThrottlersEnvironment.CONTROL_PLANE_MIN_INTERVAL_ENV,
      )
  }

  @Test
  fun `load rejects non-positive configured interval`() {
    assertFailsWith<IllegalArgumentException> {
      VidLabelingRpcThrottlersEnvironment.load { name ->
        if (name == VidLabelingRpcThrottlersEnvironment.METADATA_READ_MIN_INTERVAL_ENV) "0s"
        else null
      }
    }
  }
}
