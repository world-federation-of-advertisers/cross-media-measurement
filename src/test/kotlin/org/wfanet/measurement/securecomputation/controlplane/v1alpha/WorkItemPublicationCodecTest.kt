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

package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.StringValue
import com.google.protobuf.timestamp
import java.io.ByteArrayInputStream
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem as InternalWorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem as internalWorkItem

@RunWith(JUnit4::class)
class WorkItemPublicationCodecTest {
  @Test
  fun `parse converts internal publication to public WorkItem`() {
    val params = Any.pack(StringValue.of("params"))
    val createTime = timestamp { seconds = 123L }
    val updateTime = timestamp { seconds = 456L }
    val publication = internalWorkItem {
      workItemResourceId = "work-item-id"
      queueResourceId = "queue-id"
      workItemParams = params
      state = InternalWorkItem.State.QUEUED
      this.createTime = createTime
      this.updateTime = updateTime
      generation = 3L
      serializationKey = "serialization-key"
    }

    val actual = WorkItemPublicationCodec.parse(ByteArrayInputStream(publication.toByteArray()))

    assertThat(actual)
      .isEqualTo(
        workItem {
          name = "workItems/work-item-id"
          queue = "queue-id"
          workItemParams = params
          state = WorkItem.State.QUEUED
          this.createTime = createTime
          this.updateTime = updateTime
          generation = 3L
          serializationKey = "serialization-key"
        }
      )
  }
}
