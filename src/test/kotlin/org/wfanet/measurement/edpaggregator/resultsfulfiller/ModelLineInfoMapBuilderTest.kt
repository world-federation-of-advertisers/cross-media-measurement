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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.TypeRegistry
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Dummy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.VidIndexMap

@RunWith(JUnit4::class)
class ModelLineInfoMapBuilderTest {

  @Test
  fun `build caches population spec and VID index by population-spec URI`() {
    runBlocking {
      val descriptorsByUri =
        mapOf(
          "descriptor-x" to listOf(TestEvent.getDescriptor()),
          "descriptor-y" to listOf(Person.getDescriptor()),
        )

      // A and B share a population-spec URI but use different descriptors; C uses a different
      // population-spec URI but shares a descriptor URI with A.
      val modelLineA =
        ModelLineSource(
          modelLine = "modelLineA",
          populationSpecFileBlobUri = "population-spec-1",
          eventTemplateDescriptorBlobUri = "descriptor-x",
          eventTemplateTypeName = TestEvent.getDescriptor().fullName,
        )
      val modelLineB =
        ModelLineSource(
          modelLine = "modelLineB",
          populationSpecFileBlobUri = "population-spec-1",
          eventTemplateDescriptorBlobUri = "descriptor-y",
          eventTemplateTypeName = Person.getDescriptor().fullName,
        )
      val modelLineC =
        ModelLineSource(
          modelLine = "modelLineC",
          populationSpecFileBlobUri = "population-spec-2",
          eventTemplateDescriptorBlobUri = "descriptor-x",
          eventTemplateTypeName = TestEvent.getDescriptor().fullName,
        )

      var loadDescriptorSetCallCount = 0
      var loadPopulationSpecCallCount = 0
      var buildVidIndexMapCallCount = 0

      val builder =
        ModelLineInfoMapBuilder(
          loadDescriptorSet = { uri ->
            loadDescriptorSetCallCount++
            descriptorsByUri.getValue(uri)
          },
          loadPopulationSpec = { uri, _ ->
            loadPopulationSpecCallCount++
            populationSpec {
              subpopulations +=
                PopulationSpecKt.subPopulation {
                  vidRanges +=
                    PopulationSpecKt.vidRange {
                      startVid = uri.hashCode().toLong()
                      endVidInclusive = uri.hashCode().toLong()
                    }
                }
            }
          },
          buildVidIndexMap = { _ ->
            buildVidIndexMapCallCount++
            object : VidIndexMap by VidIndexMap.EMPTY {}
          },
        )
      val result = builder.build(listOf(modelLineA, modelLineB, modelLineC))

      assertThat(result.keys).containsExactly("modelLineA", "modelLineB", "modelLineC")
      // Each descriptor URI ("descriptor-x", "descriptor-y") and population-spec URI
      // ("population-spec-1", "population-spec-2") is loaded exactly once, despite 3 model lines.
      assertThat(loadDescriptorSetCallCount).isEqualTo(2)
      assertThat(loadPopulationSpecCallCount).isEqualTo(2)
      assertThat(buildVidIndexMapCallCount).isEqualTo(2)

      // A and B share a population-spec URI, so they share the same PopulationSpec and VidIndexMap
      // even though they use different event descriptors.
      assertThat(result.getValue("modelLineA").populationSpec)
        .isSameInstanceAs(result.getValue("modelLineB").populationSpec)
      assertThat(result.getValue("modelLineA").vidIndexMap)
        .isSameInstanceAs(result.getValue("modelLineB").vidIndexMap)
      // ...but they still get their own, distinct event descriptors.
      assertThat(result.getValue("modelLineA").eventDescriptor.fullName)
        .isEqualTo(TestEvent.getDescriptor().fullName)
      assertThat(result.getValue("modelLineB").eventDescriptor.fullName)
        .isEqualTo(Person.getDescriptor().fullName)

      // C uses a different population-spec URI, so it gets distinct instances.
      assertThat(result.getValue("modelLineC").populationSpec)
        .isNotSameInstanceAs(result.getValue("modelLineA").populationSpec)
      assertThat(result.getValue("modelLineC").vidIndexMap)
        .isNotSameInstanceAs(result.getValue("modelLineA").vidIndexMap)

      assertThat(result.values.map { it.localAlias }).containsExactly(null, null, null)
    }
  }

  @Test
  fun `build scopes each group's TypeRegistry to only that group's descriptors`() {
    runBlocking {
      // Dummy has no message-type fields of its own, so it can't transitively pull TestEvent (or
      // vice versa) into a TypeRegistry -- unlike Person, which TestEvent itself embeds.
      val descriptorsByUri =
        mapOf(
          "descriptor-x" to listOf(TestEvent.getDescriptor()),
          "descriptor-y" to listOf(Dummy.getDescriptor()),
        )

      // A and C are in different population-spec groups and use different descriptors.
      val modelLineA =
        ModelLineSource(
          modelLine = "modelLineA",
          populationSpecFileBlobUri = "population-spec-1",
          eventTemplateDescriptorBlobUri = "descriptor-x",
          eventTemplateTypeName = TestEvent.getDescriptor().fullName,
        )
      val modelLineC =
        ModelLineSource(
          modelLine = "modelLineC",
          populationSpecFileBlobUri = "population-spec-2",
          eventTemplateDescriptorBlobUri = "descriptor-y",
          eventTemplateTypeName = Dummy.getDescriptor().fullName,
        )

      val typeRegistriesByPopulationSpecUri = mutableMapOf<String, TypeRegistry>()
      val builder =
        ModelLineInfoMapBuilder(
          loadDescriptorSet = { uri -> descriptorsByUri.getValue(uri) },
          loadPopulationSpec = { uri, typeRegistry ->
            typeRegistriesByPopulationSpecUri[uri] = typeRegistry
            populationSpec {}
          },
          buildVidIndexMap = { object : VidIndexMap by VidIndexMap.EMPTY {} },
        )
      builder.build(listOf(modelLineA, modelLineC))

      val registryForA = typeRegistriesByPopulationSpecUri.getValue("population-spec-1")
      val registryForC = typeRegistriesByPopulationSpecUri.getValue("population-spec-2")

      // Each group's TypeRegistry contains only that group's own descriptor, not the other
      // group's -- a population spec must not be resolvable against unrelated model lines'
      // message types.
      assertThat(registryForA.find(TestEvent.getDescriptor().fullName)).isNotNull()
      assertThat(registryForA.find(Dummy.getDescriptor().fullName)).isNull()
      assertThat(registryForC.find(Dummy.getDescriptor().fullName)).isNotNull()
      assertThat(registryForC.find(TestEvent.getDescriptor().fullName)).isNull()
    }
  }

  @Test
  fun `build rejects duplicate model line names`() {
    runBlocking {
      val firstModelLineSource =
        ModelLineSource(
          modelLine = "duplicateModelLine",
          populationSpecFileBlobUri = "population-spec-1",
          eventTemplateDescriptorBlobUri = "descriptor-x",
          eventTemplateTypeName = TestEvent.getDescriptor().fullName,
        )
      val secondModelLineSource =
        ModelLineSource(
          modelLine = "duplicateModelLine",
          populationSpecFileBlobUri = "population-spec-2",
          eventTemplateDescriptorBlobUri = "descriptor-x",
          eventTemplateTypeName = TestEvent.getDescriptor().fullName,
        )
      val builder =
        ModelLineInfoMapBuilder(
          loadDescriptorSet = { listOf(TestEvent.getDescriptor()) },
          loadPopulationSpec = { _, _ -> populationSpec {} },
          buildVidIndexMap = { object : VidIndexMap by VidIndexMap.EMPTY {} },
        )

      val exception =
        assertFailsWith<IllegalArgumentException> {
          builder.build(listOf(firstModelLineSource, secondModelLineSource))
        }
      assertThat(exception).hasMessageThat().contains("duplicateModelLine")
    }
  }
}
