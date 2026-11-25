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

package org.wfanet.measurement.reporting.service.internal

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.reportingSet

@RunWith(JUnit4::class)
class ReportingSetValidatorTest {
  @Test
  fun `validate batches nested references and reuses shared primitive identities`(): Unit =
    runBlocking {
      val primitives = (0 until 4).map { primitive("primitive-$it") }
      val firstIds = listOf("primitive-0", "primitive-1")
      val secondIds = listOf("primitive-2", "primitive-3")
      val allIds = firstIds + secondIds
      val stored =
        (primitives +
            listOf(
              composite("grandchild-a", firstIds, firstIds),
              composite("grandchild-b", secondIds, secondIds),
              composite("child-a", listOf("grandchild-a", "primitive-0"), firstIds),
              composite("child-b", listOf("grandchild-b", "primitive-2"), secondIds),
            ))
          .associateBy { it.externalReportingSetId }
      val reads = mutableListOf<List<String>>()
      val validator =
        ReportingSetValidator(MC_ID) { ids ->
          reads += ids
          ids.reversed().map { stored.getValue(it) }
        }
      val root = composite("root", listOf("child-a", "child-b"), allIds)

      val membership = validator.validate(root)
      assertThat(membership)
        .containsExactlyElementsIn(primitives.flatMap { it.primitive.eventGroupKeysList })
      assertThat(reads).hasSize(2)
      assertThat(reads[0]).containsExactly("child-a", "child-b", *allIds.toTypedArray())
      assertThat(reads[1]).containsExactly("grandchild-a", "grandchild-b")
      assertThat(reads.flatten()).containsExactlyElementsIn(stored.keys)
      assertThat(validator.validate(root)).isEqualTo(membership)
      assertThat(validator.getReportingSet("primitive-3")).isEqualTo(primitives[3])
      assertThat(reads).hasSize(2)
    }

  @Test
  fun `preloadReportingSets bounds batch sizes and caches returned identities`(): Unit =
    runBlocking {
      val sets =
        (0..1000).map { primitive("primitive-$it") }.associateBy { it.externalReportingSetId }
      val reads = mutableListOf<List<String>>()
      val validator =
        ReportingSetValidator(MC_ID) { ids ->
          reads += ids
          ids.reversed().map { sets.getValue(it) }
        }

      validator.preloadReportingSets(sets.keys.toList() + sets.keys)
      assertThat(reads.map { it.size }).containsExactly(1000, 1)
      assertThat(reads.flatten()).containsExactlyElementsIn(sets.keys)
      assertThat(validator.getReportingSet("primitive-1000"))
        .isEqualTo(sets.getValue("primitive-1000"))
      validator.preloadReportingSets(sets.keys)
      assertThat(reads).hasSize(2)
    }

  private fun primitive(id: String): ReportingSet = reportingSet {
    cmmsMeasurementConsumerId = MC_ID
    externalReportingSetId = id
    primitive =
      ReportingSetKt.primitive {
        eventGroupKeys +=
          ReportingSetKt.PrimitiveKt.eventGroupKey {
            cmmsDataProviderId = "provider"
            cmmsEventGroupId = id
          }
      }
  }

  private fun composite(
    id: String,
    operandIds: List<String>,
    primitiveIds: List<String>,
  ): ReportingSet = reportingSet {
    cmmsMeasurementConsumerId = MC_ID
    externalReportingSetId = id
    composite =
      ReportingSetKt.setExpression {
        operation = ReportingSet.SetExpression.Operation.UNION
        lhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = operandIds[0] }
        rhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = operandIds[1] }
      }
    weightedSubsetUnions +=
      ReportingSetKt.weightedSubsetUnion {
        weight = 1
        binaryRepresentation = (1 shl primitiveIds.size) - 1
        for (primitiveId in primitiveIds) {
          primitiveReportingSetBases +=
            ReportingSetKt.primitiveReportingSetBasis { externalReportingSetId = primitiveId }
        }
      }
  }

  private companion object {
    const val MC_ID = "measurement-consumer"
  }
}
