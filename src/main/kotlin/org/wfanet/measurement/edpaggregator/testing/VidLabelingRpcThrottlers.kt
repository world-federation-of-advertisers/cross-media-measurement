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

package org.wfanet.measurement.edpaggregator.testing

import java.util.concurrent.atomic.AtomicInteger
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers

/** Test utilities for process-scoped VID Labeling RPC throttlers. */
object VidLabelingRpcThrottlersTestHelper {
  /** Returns throttlers that execute every block immediately. */
  fun alwaysReady(): VidLabelingRpcThrottlers {
    val alwaysReady = RecordingThrottler()
    return VidLabelingRpcThrottlers(
      kingdom = alwaysReady,
      metadataRead = alwaysReady,
      metadataWrite = alwaysReady,
      controlPlane = alwaysReady,
    )
  }

  /** Returns independently recording throttlers for verifying RPC-to-bucket routing. */
  fun recording(): RecordingThrottlers {
    val kingdom = RecordingThrottler()
    val metadataRead = RecordingThrottler()
    val metadataWrite = RecordingThrottler()
    val controlPlane = RecordingThrottler()
    return RecordingThrottlers(
      throttlers =
        VidLabelingRpcThrottlers(
          kingdom = kingdom,
          metadataRead = metadataRead,
          metadataWrite = metadataWrite,
          controlPlane = controlPlane,
        ),
      kingdom = kingdom,
      metadataRead = metadataRead,
      metadataWrite = metadataWrite,
      controlPlane = controlPlane,
    )
  }

  class RecordingThrottler : Throttler {
    private val counter = AtomicInteger()

    val invocationCount: Int
      get() = counter.get()

    override suspend fun <T> onReady(block: suspend () -> T): T {
      counter.incrementAndGet()
      return block()
    }
  }

  data class RecordingThrottlers(
    val throttlers: VidLabelingRpcThrottlers,
    val kingdom: RecordingThrottler,
    val metadataRead: RecordingThrottler,
    val metadataWrite: RecordingThrottler,
    val controlPlane: RecordingThrottler,
  )
}
