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
  fun `dispatch contract has compact tags and only one source per mode`() {
    val descriptor = VidLabelingDispatchWorkItemParams.getDescriptor()
    assertThat(descriptor.fields.map { it.number }).containsExactly(1, 2).inOrder()
    assertThat(descriptor.fields.map { it.name })
      .containsExactly("data_provider", "recovery")
      .inOrder()
    assertThat(descriptor.toProto().reservedNameCount).isEqualTo(0)
    assertThat(descriptor.toProto().reservedRangeCount).isEqualTo(0)
    val recovery = VidLabelingDispatchWorkItemParams.RecoveryParams.getDescriptor()
    assertThat(recovery.fields.map { it.number }).containsExactly(1, 2, 3).inOrder()
    assertThat(recovery.fields.map { it.name })
      .containsExactly("source", "source_upload", "upload_healing_operation")
      .inOrder()
  }

  @Test
  fun `ordinary source is supplied only by the standard WorkItem field`() {
    val params = ordinary()
    assertThat(VidLabelingDispatchWorkItems.normalize(params)).isEqualTo(params)
    assertThat(VidLabelingDispatchWorkItems.source(params, source())).isEqualTo(source())
    assertFailsWith<IllegalArgumentException> { VidLabelingDispatchWorkItems.source(params, null) }
  }

  @Test
  fun `recovery source is supplied only by nested recovery`() {
    val params = recovery()
    assertThat(VidLabelingDispatchWorkItems.normalize(params)).isEqualTo(params)
    assertThat(VidLabelingDispatchWorkItems.source(params, null)).isEqualTo(source())
    assertFailsWith<IllegalArgumentException> {
      VidLabelingDispatchWorkItems.source(params, source())
    }
  }

  @Test
  fun `source rejects incomplete or unfinalized identity`() {
    for (invalid in
      listOf(
        source().copy { dataPath = "" },
        source().copy { clearGeneration() },
        source().copy { generation = 0L },
        source().copy { clearCreateTime() },
        source().copy {
          eventType = WorkItem.WorkItemParams.DataPathParams.StorageEventType.DELETED
        },
      )) {
      assertFailsWith<IllegalArgumentException> {
        VidLabelingDispatchWorkItems.source(ordinary(), invalid)
      }
      assertFailsWith<IllegalArgumentException> {
        VidLabelingDispatchWorkItems.normalize(
          recovery().copy { recovery = recovery.copy { source = invalid } }
        )
      }
    }
  }

  @Test
  fun `normalize rejects incomplete recovery and invalid resource parents`() {
    val params = recovery()
    for (invalid in
      listOf(
        params.recovery.copy { clearSource() },
        params.recovery.copy { sourceUpload = "" },
        params.recovery.copy { sourceUpload = "dataProviders/other/rawImpressionUploads/source" },
        params.recovery.copy { uploadHealingOperation = "operation" },
        params.recovery.copy {
          uploadHealingOperation = "dataProviders/other/uploadHealingOperations/operation"
        },
      )) {
      assertFailsWith<IllegalArgumentException> {
        VidLabelingDispatchWorkItems.normalize(params.copy { recovery = invalid })
      }
    }
    assertFailsWith<IllegalArgumentException> {
      VidLabelingDispatchWorkItems.normalize(ordinary().copy { dataProvider = "provider" })
    }
  }

  @Test
  fun `intent identity distinguishes source provider and approved operation`() {
    val ordinaryId = VidLabelingDispatchWorkItems.intentId(ordinary(), source())
    for (variant in
      listOf(
        source().copy { dataPath = "gs://raw/other/done" },
        source().copy { generation = 43L },
        source().copy { createTime = Instant.parse("2026-01-02T00:00:00Z").toProtoTime() },
      )) {
      assertThat(VidLabelingDispatchWorkItems.intentId(ordinary(), variant))
        .isNotEqualTo(ordinaryId)
    }
    assertThat(
        VidLabelingDispatchWorkItems.intentId(
          ordinary().copy { dataProvider = "dataProviders/other" },
          source(),
        )
      )
      .isNotEqualTo(ordinaryId)
    val replay = recovery()
    val replayId = VidLabelingDispatchWorkItems.intentId(replay)
    assertThat(replayId).isNotEqualTo(ordinaryId)
    for (variant in
      listOf(
        replay.recovery.copy { source = source().copy { generation = 43L } },
        replay.recovery.copy { sourceUpload = "dataProviders/provider/rawImpressionUploads/other" },
        replay.recovery.copy {
          uploadHealingOperation = "dataProviders/provider/uploadHealingOperations/other"
        },
      )) {
      assertThat(VidLabelingDispatchWorkItems.intentId(replay.copy { recovery = variant }))
        .isNotEqualTo(replayId)
    }
    assertThat(VidLabelingDispatchWorkItems.intentId(replay)).isEqualTo(replayId)
  }

  private fun ordinary() = vidLabelingDispatchWorkItemParams {
    dataProvider = "dataProviders/provider"
  }

  private fun source() = dataPathParams {
    dataPath = "gs://raw/day/done"
    generation = 42L
    createTime = Instant.parse("2026-01-01T00:00:00Z").toProtoTime()
    eventType = WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
  }

  private fun recovery() =
    ordinary().copy {
      recovery = recoveryParams {
        source = source()
        sourceUpload = "dataProviders/provider/rawImpressionUploads/source"
        uploadHealingOperation = "dataProviders/provider/uploadHealingOperations/operation"
      }
    }
}
