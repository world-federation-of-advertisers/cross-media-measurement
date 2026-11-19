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

package org.wfanet.measurement.common.edpaggregator

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParamsKt.recoveryParams
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingDispatchWorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy

class VidLabelingDispatchWorkItemsTest {
  @Test
  fun `normalize accepts ordinary dispatch without recovery`() {
    val params = ordinary()
    assertThat(VidLabelingDispatchWorkItems.normalize(params)).isEqualTo(params)
    assertThat(params.hasRecovery()).isFalse()
  }

  @Test
  fun `normalize rejects a missing source`() {
    assertFailsWith<IllegalArgumentException> {
      VidLabelingDispatchWorkItems.normalize(ordinary().copy { clearSource() })
    }
  }

  @Test
  fun `normalize rejects an invalid finalized source identity`() {
    val source = ordinary().source
    for (invalid in
      listOf(
        source.copy { dataPath = "" },
        source.copy { clearGeneration() },
        source.copy { generation = 0L },
        source.copy { clearCreateTime() },
        source.copy { eventType = WorkItem.WorkItemParams.DataPathParams.StorageEventType.DELETED },
      )) {
      assertFailsWith<IllegalArgumentException> {
        VidLabelingDispatchWorkItems.normalize(ordinary().copy { this.source = invalid })
      }
    }
  }

  @Test
  fun `normalize rejects an invalid provider`() {
    assertFailsWith<IllegalArgumentException> {
      VidLabelingDispatchWorkItems.normalize(ordinary().copy { dataProvider = "provider" })
    }
  }

  @Test
  fun `normalize rejects an empty recovery message`() {
    assertFailsWith<IllegalArgumentException> {
      VidLabelingDispatchWorkItems.normalize(ordinary().copy { recovery = recoveryParams {} })
    }
  }

  @Test
  fun `normalize rejects incomplete recovery authorization`() {
    val params = recovery()
    for (invalid in
      listOf(
        params.recovery.copy { overrideModelLines.clear() },
        params.recovery.copy { sourceUpload = "" },
        params.recovery.copy { uploadHealingOperation = "" },
      )) {
      assertFailsWith<IllegalArgumentException> {
        VidLabelingDispatchWorkItems.normalize(params.copy { recovery = invalid })
      }
    }
  }

  @Test
  fun `normalize rejects malformed and mismatched upload names`() {
    val params = recovery()
    for (upload in listOf("source", "dataProviders/other/rawImpressionUploads/source")) {
      assertFailsWith<IllegalArgumentException> {
        VidLabelingDispatchWorkItems.normalize(
          params.copy { recovery = params.recovery.copy { sourceUpload = upload } }
        )
      }
    }
  }

  @Test
  fun `normalize rejects bare operation IDs and mismatched operation parents`() {
    val params = recovery()
    for (operation in
      listOf("operation", "dataProviders/other/uploadHealingOperations/operation")) {
      assertFailsWith<IllegalArgumentException> {
        VidLabelingDispatchWorkItems.normalize(
          params.copy { recovery = params.recovery.copy { uploadHealingOperation = operation } }
        )
      }
    }
  }

  @Test
  fun `normalize rejects malformed and duplicate model lines`() {
    val params = recovery()
    for (lines in listOf(listOf("first"), listOf(FIRST, FIRST))) {
      assertFailsWith<IllegalArgumentException> {
        VidLabelingDispatchWorkItems.normalize(
          params.copy {
            recovery =
              params.recovery.copy {
                overrideModelLines.clear()
                overrideModelLines += lines
              }
          }
        )
      }
    }
  }

  @Test
  fun `intentId normalizes model line order and retains the recovery identity`() {
    val params = recovery()
    val sorted = VidLabelingDispatchWorkItems.normalize(params)
    assertThat(sorted.recovery.overrideModelLinesList).containsExactly(FIRST, SECOND).inOrder()
    assertThat(sorted.recovery.sourceUpload).isEqualTo(params.recovery.sourceUpload)
    assertThat(sorted.recovery.uploadHealingOperation)
      .isEqualTo(params.recovery.uploadHealingOperation)
    assertThat(VidLabelingDispatchWorkItems.intentId(params))
      .isEqualTo(VidLabelingDispatchWorkItems.intentId(sorted))
    assertThat(VidLabelingDispatchWorkItems.intentId(params))
      .isNotEqualTo(VidLabelingDispatchWorkItems.intentId(ordinary()))
  }

  @Test
  fun `intentId distinguishes source and authorization components`() {
    val params = recovery()
    val variants =
      listOf(
        params.copy { source = params.source.copy { generation = 43L } },
        params.copy {
          recovery =
            params.recovery.copy {
              sourceUpload = "dataProviders/provider/rawImpressionUploads/other"
            }
        },
        params.copy {
          recovery =
            params.recovery.copy {
              uploadHealingOperation = "dataProviders/provider/uploadHealingOperations/other"
            }
        },
        params.copy {
          recovery =
            params.recovery.copy {
              overrideModelLines.clear()
              overrideModelLines += FIRST
            }
        },
      )
    val original = VidLabelingDispatchWorkItems.intentId(params)
    for (variant in variants) {
      assertThat(VidLabelingDispatchWorkItems.intentId(variant)).isNotEqualTo(original)
    }
  }

  private fun ordinary(): VidLabelingDispatchWorkItemParams = vidLabelingDispatchWorkItemParams {
    dataProvider = "dataProviders/provider"
    source = dataPathParams {
      dataPath = "gs://raw/day/done"
      generation = 42L
      createTime = Instant.parse("2026-01-01T00:00:00Z").toProtoTime()
      eventType = WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
    }
  }

  private fun recovery(): VidLabelingDispatchWorkItemParams =
    ordinary().copy {
      recovery = recoveryParams {
        overrideModelLines += listOf(SECOND, FIRST)
        sourceUpload = "dataProviders/provider/rawImpressionUploads/source"
        uploadHealingOperation = "dataProviders/provider/uploadHealingOperations/operation"
      }
    }

  companion object {
    private const val FIRST = "modelProviders/mp/modelSuites/ms/modelLines/first"
    private const val SECOND = "modelProviders/mp/modelSuites/ms/modelLines/second"
  }
}
