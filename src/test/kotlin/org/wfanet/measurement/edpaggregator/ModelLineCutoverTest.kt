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

package org.wfanet.measurement.edpaggregator

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.duration
import com.google.type.date
import java.time.Instant
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.config.edpaggregator.modelLineCutoverConfig

class ModelLineCutoverTest {
  @Test
  fun `selectModelLine uses one source from report start`() {
    val cutover = newCutover()

    assertThat(cutover.selectModelLine(Instant.parse("2026-10-04T23:59:59Z")))
      .isEqualTo(HISTORICAL_MODEL_LINE)
    assertThat(cutover.selectModelLine(Instant.parse("2026-10-05T00:00:00Z")))
      .isEqualTo(REPLACEMENT_MODEL_LINE)
  }

  @Test
  fun `from rejects legacy required date before cutover`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ModelLineCutover.from(
          modelLineCutoverConfig {
            externalModelLine = EXTERNAL_MODEL_LINE
            historicalModelLine = HISTORICAL_MODEL_LINE
            replacementModelLine = REPLACEMENT_MODEL_LINE
            cutoverDate = date {
              year = 2026
              month = 10
              day = 5
            }
            legacyRequiredThroughDate = date {
              year = 2026
              month = 10
              day = 4
            }
            maximumReportDuration = duration { seconds = 86400 }
          }
        )
      }

    assertThat(exception).hasMessageThat().contains("legacy_required_through_date")
  }

  @Test
  fun `validate rejects duplicate external model line`() {
    val cutover = newCutover()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ModelLineCutoverValidator.validate(listOf(cutover, cutover))
      }

    assertThat(exception).hasMessageThat().contains(EXTERNAL_MODEL_LINE)
  }

  @Test
  fun `validate rejects contributor shared by cutovers`() {
    val first = newCutover()
    val second =
      newCutover()
        .copy(
          externalModelLine = OTHER_EXTERNAL_MODEL_LINE,
          replacementModelLine = OTHER_REPLACEMENT_MODEL_LINE,
        )

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ModelLineCutoverValidator.validate(listOf(first, second))
      }

    assertThat(exception).hasMessageThat().contains(HISTORICAL_MODEL_LINE)
  }

  private fun newCutover(): ModelLineCutover =
    ModelLineCutover.from(
      modelLineCutoverConfig {
        externalModelLine = EXTERNAL_MODEL_LINE
        historicalModelLine = HISTORICAL_MODEL_LINE
        replacementModelLine = REPLACEMENT_MODEL_LINE
        cutoverDate = date {
          year = 2026
          month = 10
          day = 5
        }
        legacyRequiredThroughDate = date {
          year = 2027
          month = 1
          day = 3
        }
        maximumReportDuration = duration { seconds = 90L * 86400L }
      }
    )

  companion object {
    private const val EXTERNAL_MODEL_LINE =
      "modelProviders/provider/modelSuites/suite/modelLines/external"
    private const val HISTORICAL_MODEL_LINE =
      "modelProviders/provider/modelSuites/suite/modelLines/historical"
    private const val REPLACEMENT_MODEL_LINE =
      "modelProviders/provider/modelSuites/suite/modelLines/replacement"
    private const val OTHER_EXTERNAL_MODEL_LINE =
      "modelProviders/provider/modelSuites/suite/modelLines/other-external"
    private const val OTHER_REPLACEMENT_MODEL_LINE =
      "modelProviders/provider/modelSuites/suite/modelLines/other-replacement"
  }
}
