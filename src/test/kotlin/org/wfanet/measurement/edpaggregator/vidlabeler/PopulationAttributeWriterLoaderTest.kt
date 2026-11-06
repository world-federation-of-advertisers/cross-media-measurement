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

import com.google.common.truth.Truth.assertThat
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt

@RunWith(JUnit4::class)
class PopulationAttributeWriterLoaderTest {
  private val eventDescriptor = TestEvent.getDescriptor()

  private val specTextProto =
    """
    subpopulations {
      attributes {
        [type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.Person] {
          gender: MALE
          age_group: YEARS_18_TO_34
          social_grade_group: A_B_C1
        }
      }
      vid_ranges { start_vid: 1 end_vid_inclusive: 1000 }
    }
    """
      .trimIndent()

  private fun config(specBlobUri: String = "gs://configs/spec.textproto") =
    VidLabelerParamsKt.modelLineConfig {
      eventTemplateDescriptorBlobUri = "gs://descriptors/set.binpb"
      eventTemplateType = TestEvent.getDescriptor().fullName
      populationSpecBlobUri = specBlobUri
    }

  @Test
  fun `getWriter loads the spec and builds a writer`() = runBlocking {
    val loader = PopulationAttributeWriterLoader { specTextProto.toByteArray() }

    val writer = loader.getWriter(config(), eventDescriptor)

    // Built from the loaded spec: VID 500 falls in its only subpopulation.
    val event = writer.apply(TestEvent.getDefaultInstance(), 500L).unpack(TestEvent::class.java)
    assertThat(event.person.ageGroup)
      .isEqualTo(
        org.wfanet.measurement.api.v2alpha.event_templates.testing.Person.AgeGroup.YEARS_18_TO_34
      )
  }

  @Test
  fun `getWriter reads each spec blob once`() = runBlocking {
    val reads = AtomicInteger()
    val loader = PopulationAttributeWriterLoader {
      reads.incrementAndGet()
      specTextProto.toByteArray()
    }

    val first = loader.getWriter(config(), eventDescriptor)
    val second = loader.getWriter(config(), eventDescriptor)

    assertThat(second).isSameInstanceAs(first)
    assertThat(reads.get()).isEqualTo(1)
  }

  @Test
  fun `getWriter throws when population_spec_blob_uri is empty`() = runBlocking {
    // Without the spec the labeled output would carry the DataProvider's uploaded demographics
    // instead of the model-assigned ones, so this must fail rather than skip the writer.
    val loader = PopulationAttributeWriterLoader { specTextProto.toByteArray() }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        loader.getWriter(config(specBlobUri = ""), eventDescriptor)
      }
    assertThat(exception).hasMessageThat().contains("population_spec_blob_uri must be set")
  }
}
