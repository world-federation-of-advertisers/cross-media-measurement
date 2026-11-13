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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.TextFormat
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.controlPlaneQueueSink
import org.wfanet.measurement.config.securecomputation.dataWatcherConfig
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.edpaggregator.v1alpha.resultsFulfillerParams
import picocli.CommandLine

class ValidateDataWatcherConfigTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `valid config succeeds`() {
    assertThat(execute(config(Any.getDefaultInstance()))).isEqualTo(0)
  }

  @Test
  fun `legacy ResultsFulfiller route fails`() {
    assertThat(execute(config(Any.pack(resultsFulfillerParams {})))).isEqualTo(1)
  }

  private fun execute(config: DataWatcherConfig): Int {
    val configFile = temporaryFolder.newFile("data-watcher-config.textproto")
    configFile.writeText(TextFormat.printer().printToString(config))

    return CommandLine(ValidateDataWatcherConfig())
      .execute("--data-watcher-config=${configFile.path}")
  }

  private fun config(appParams: Any): DataWatcherConfig = dataWatcherConfig {
    watchedPaths += watchedPath {
      identifier = "requisitions"
      this.controlPlaneQueueSink = controlPlaneQueueSink { this.appParams = appParams }
    }
  }
}
