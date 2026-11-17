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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import java.time.Duration
import java.time.LocalDate
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.edpaggregator.ModelLineCutover

class ModelLineAliasValidatorTest {
  private val l1 = "modelProviders/mp/modelSuites/ms/modelLines/legacy"
  private val r1 = "modelProviders/mp/modelSuites/ms/modelLines/replacement"
  private val d1 = "modelProviders/mp/modelSuites/ms/modelLines/dev"
  private val d2 = "modelProviders/mp/modelSuites/ms/modelLines/dev-2"
  private val cutover =
    ModelLineCutover(
      l1,
      l1,
      r1,
      LocalDate.of(2027, 6, 1),
      LocalDate.of(2027, 6, 3),
      Duration.ofDays(2),
    )

  @Test
  fun `validate accepts independent aliases with one source each`() {
    ModelLineAliasValidator.validate(listOf(cutover), mapOf(r1 to listOf(d1, d2)))
  }

  @Test
  fun `validate rejects an alias shared by two internal sources`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        ModelLineAliasValidator.validate(listOf(cutover), mapOf(l1 to listOf(d1), r1 to listOf(d1)))
      }
    assertThat(error).hasMessageThat().contains("Duplicate external")
  }

  @Test
  fun `validate rejects repeated alias targets within one source`() {
    assertFailsWith<IllegalArgumentException> {
      ModelLineAliasValidator.validate(listOf(cutover), mapOf(r1 to listOf(d1, d1)))
    }
  }

  @Test
  fun `validate rejects replacement self alias`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        ModelLineAliasValidator.validate(listOf(cutover), mapOf(r1 to listOf(r1)))
      }
    assertThat(error).hasMessageThat().contains("must not expose")
  }

  @Test
  fun `validate rejects alias to another contributor`() {
    assertFailsWith<IllegalArgumentException> {
      ModelLineAliasValidator.validate(
        listOf(
          ModelLineCutover(
            d2,
            l1,
            r1,
            cutover.cutoverDate,
            cutover.legacyRequiredThroughDate,
            cutover.maximumReportDuration,
          )
        ),
        mapOf(r1 to listOf(l1)),
      )
    }
  }

  @Test
  fun `validate keeps explicit hidden source protected without a cutover`() {
    assertFailsWith<IllegalArgumentException> {
      ModelLineAliasValidator.validate(emptyList(), mapOf(l1 to listOf(r1)), setOf(r1))
    }
    ModelLineAliasValidator.validate(emptyList(), mapOf(r1 to listOf(d1)), setOf(r1))
  }

  @Test
  fun `validate rejects hidden advertiser visible identity`() {
    assertFailsWith<IllegalArgumentException> {
      ModelLineAliasValidator.validate(listOf(cutover), emptyMap(), setOf(l1))
    }
  }

  @Test
  fun `validate retains ordinary identity plus alias publication`() {
    ModelLineAliasValidator.validate(emptyList(), mapOf(l1 to listOf(l1, d1)))
  }

  @Test
  fun `validate rejects hidden contributor alias with padded API ID`() {
    val source = "modelProviders/AAAAAAAAAAE/modelSuites/AAAAAAAAAAI/modelLines/AAAAAAAAAAM"
    for (alias in listOf(source + "=", source.dropLast(1) + "N")) {
      assertFailsWith<IllegalArgumentException> {
        ModelLineAliasValidator.validate(emptyList(), mapOf(source to listOf(alias)), setOf(source))
      }
    }
  }

  @Test
  fun `validate rejects duplicate alias spellings of one Kingdom identity`() {
    val alias = "modelProviders/AAAAAAAAAAE/modelSuites/AAAAAAAAAAI/modelLines/AAAAAAAAAAQ"
    assertFailsWith<IllegalArgumentException> {
      ModelLineAliasValidator.validate(
        emptyList(),
        mapOf(l1 to listOf(alias), r1 to listOf(alias + "=")),
      )
    }
  }

  @Test
  fun `validate rejects noncanonical hidden source ID in each resource segment`() {
    val source = "modelProviders/AAAAAAAAAAE/modelSuites/AAAAAAAAAAI/modelLines/AAAAAAAAAAM"
    for (id in listOf("AAAAAAAAAAE", "AAAAAAAAAAI", "AAAAAAAAAAM")) {
      assertFailsWith<IllegalArgumentException> {
        ModelLineAliasValidator.validate(
          emptyList(),
          emptyMap(),
          setOf(source.replace(id, id + "=")),
        )
      }
    }
  }

  @Test
  fun `validate rejects noncanonical mapped source and storage source`() {
    val source = "modelProviders/AAAAAAAAAAE/modelSuites/AAAAAAAAAAI/modelLines/AAAAAAAAAAM="
    assertFailsWith<IllegalArgumentException> {
      ModelLineAliasValidator.validate(emptyList(), mapOf(source to listOf(d1)))
    }
    assertFailsWith<IllegalArgumentException> {
      ModelLineAliasValidator.validate(emptyList(), emptyMap(), sourceModelLines = setOf(source))
    }
  }

  @Test
  fun `validate rejects noncanonical cutover references`() {
    val source = "modelProviders/AAAAAAAAAAE/modelSuites/AAAAAAAAAAI/modelLines/AAAAAAAAAAM="
    assertFailsWith<IllegalArgumentException> {
      ModelLineAliasValidator.validate(
        listOf(cutover.copy(replacementModelLine = source)),
        emptyMap(),
      )
    }
  }

  @Test
  fun `validate accepts canonical numeric API IDs`() {
    val source = "modelProviders/AAAAAAAAAAE/modelSuites/AAAAAAAAAAI/modelLines/AAAAAAAAAAM"
    val alias = "modelProviders/AAAAAAAAAAE/modelSuites/AAAAAAAAAAI/modelLines/AAAAAAAAAAQ"
    ModelLineAliasValidator.validate(emptyList(), mapOf(source to listOf(alias)), setOf(source))
  }
}
