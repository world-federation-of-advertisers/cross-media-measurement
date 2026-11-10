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

package org.wfanet.measurement.securecomputation.datawatcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Struct
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.controlPlaneQueueSink
import org.wfanet.measurement.config.securecomputation.dataWatcherConfig
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.edpaggregator.v1alpha.resultsFulfillerParams

class DataWatcherConfigValidatorTest {
  @Test
  fun `rejects legacy ResultsFulfiller route`() {
    val config = dataWatcherConfig {
      watchedPaths += watchedPath {
        identifier = "legacy-requisitions"
        this.controlPlaneQueueSink = controlPlaneQueueSink {
          appParams = Any.pack(resultsFulfillerParams {})
        }
      }
    }

    val exception =
      assertFailsWith<IllegalArgumentException> { DataWatcherConfigValidator.validate(config) }

    assertThat(exception).hasMessageThat().contains("uses legacy ResultsFulfiller dispatch")
  }

  @Test
  fun `allows other control plane routes`() {
    val config = dataWatcherConfig {
      watchedPaths += watchedPath {
        this.controlPlaneQueueSink = controlPlaneQueueSink {
          appParams = Any.pack(Struct.getDefaultInstance())
        }
      }
    }

    DataWatcherConfigValidator.validate(config)
  }
}
