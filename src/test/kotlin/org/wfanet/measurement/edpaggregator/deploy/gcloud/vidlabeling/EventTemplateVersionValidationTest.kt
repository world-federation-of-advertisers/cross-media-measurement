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
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.api.v2alpha.event_templates.testing.VersionedEvent
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt

class EventTemplateVersionValidationTest {
  @get:Rule val tempDir = TemporaryFolder()

  private fun modelLineConfig(): VidLabelerParams.ModelLineConfig {
    val descriptorFiles = linkedMapOf<String, Descriptors.FileDescriptor>()
    fun collect(file: Descriptors.FileDescriptor) {
      if (file.name in descriptorFiles) return
      file.dependencies.forEach(::collect)
      descriptorFiles[file.name] = file
    }
    collect(VersionedEvent.getDescriptor().file)
    val descriptorFile = tempDir.newFile("event-descriptor-set.binpb")
    descriptorFile.writeBytes(
      DescriptorProtos.FileDescriptorSet.newBuilder()
        .addAllFile(descriptorFiles.values.map { it.toProto() })
        .build()
        .toByteArray()
    )
    return VidLabelerParamsKt.modelLineConfig {
      eventTemplateDescriptorBlobUri = descriptorFile.toURI().toString()
      eventTemplateType = VersionedEvent.getDescriptor().fullName
    }
  }

  @Test
  fun `descriptor blob supports historical and current versions`() = runBlocking {
    val config = modelLineConfig()
    VidLabelingFunctionHelpers.validateEventTemplateVersion(config, 1, "test-project")
    VidLabelingFunctionHelpers.validateEventTemplateVersion(config, 3, "test-project")
  }

  @Test
  fun `version newer than descriptor is rejected`() = runBlocking {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingFunctionHelpers.validateEventTemplateVersion(
          modelLineConfig(),
          4,
          "test-project",
        )
      }
    assertThat(exception).hasMessageThat().contains("exceeds current version 3")
  }

  @Test
  fun `event message absent from descriptor blob is rejected`() = runBlocking {
    val config = modelLineConfig().toBuilder().setEventTemplateType("test.UnknownEvent").build()
    val exception =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingFunctionHelpers.validateEventTemplateVersion(config, 1, "test-project")
      }
    assertThat(exception).hasMessageThat().contains("EventTemplate descriptor not found")
  }
}
