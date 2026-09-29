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
import com.google.type.date
import com.google.type.interval
import java.time.Instant
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.ModelLineCutover
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.modelLineCutover

@RunWith(JUnit4::class)
class ModelLineCutoverTest {
  @Test
  fun `routes interval before cutover to before model line`() {
    val routes =
      CUTOVER_CONFIG.routesFor(timeInterval("2026-09-01T00:00:00Z", "2026-10-01T00:00:00Z"))

    assertThat(routes)
      .containsExactly(
        ModelLineRoute(
          BEFORE_MODEL_LINE,
          timeInterval("2026-09-01T00:00:00Z", "2026-10-01T00:00:00Z"),
        )
      )
  }

  @Test
  fun `routes interval starting at cutover to on or after model line`() {
    val routes =
      CUTOVER_CONFIG.routesFor(timeInterval("2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z"))

    assertThat(routes)
      .containsExactly(
        ModelLineRoute(
          ON_OR_AFTER_MODEL_LINE,
          timeInterval("2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z"),
        )
      )
  }

  @Test
  fun `splits interval crossing cutover without gap or overlap`() {
    val routes =
      CUTOVER_CONFIG.routesFor(timeInterval("2026-09-30T12:00:00Z", "2026-10-01T12:00:00Z"))

    assertThat(routes)
      .containsExactly(
        ModelLineRoute(
          BEFORE_MODEL_LINE,
          timeInterval("2026-09-30T12:00:00Z", "2026-10-01T00:00:00Z"),
        ),
        ModelLineRoute(
          ON_OR_AFTER_MODEL_LINE,
          timeInterval("2026-10-01T00:00:00Z", "2026-10-01T12:00:00Z"),
        ),
      )
      .inOrder()
    assertThat(routes[0].interval.endTime).isEqualTo(routes[1].interval.startTime)
  }

  @Test
  fun `rejects duplicate external model line`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ModelLineCutoverValidator.validate(listOf(CUTOVER_CONFIG, CUTOVER_CONFIG))
      }

    assertThat(exception).hasMessageThat().contains("Duplicate model-line cutover")
  }

  @Test
  fun `rejects external model line in legacy map`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ModelLineCutoverValidator.validate(listOf(CUTOVER_CONFIG), setOf(EXTERNAL_MODEL_LINE))
      }

    assertThat(exception).hasMessageThat().contains("both model_line_map and model_line_cutovers")
  }

  @Test
  fun `rejects identical internal model lines`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        CUTOVER.copy { onOrAfterCutoverModelLine = beforeCutoverModelLine }
          .toModelLineCutoverConfig()
      }

    assertThat(exception).hasMessageThat().contains("must differ")
  }

  @Test
  fun `rejects invalid model line resource name`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        CUTOVER.copy { beforeCutoverModelLine = "invalid" }.toModelLineCutoverConfig()
      }

    assertThat(exception).hasMessageThat().contains("before_cutover_model_line")
  }

  @Test
  fun `rejects invalid cutover date`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        CUTOVER.copy {
            cutoverDate = date {
              year = 2026
              month = 2
              day = 30
            }
          }
          .toModelLineCutoverConfig()
      }

    assertThat(exception).hasMessageThat().contains("Invalid 'cutover_date'")
  }

  @Test
  fun `rejects partial cutover date`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        CUTOVER.copy {
            cutoverDate = date {
              month = 10
              day = 1
            }
          }
          .toModelLineCutoverConfig()
      }

    assertThat(exception).hasMessageThat().contains("a full year is required")
  }

  @Test
  fun `rejects missing cutover date`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        CUTOVER.copy { clearCutoverDate() }.toModelLineCutoverConfig()
      }

    assertThat(exception).hasMessageThat().contains("Missing 'cutover_date'")
  }

  private fun timeInterval(start: String, end: String) = interval {
    startTime = Instant.parse(start).toProtoTime()
    endTime = Instant.parse(end).toProtoTime()
  }

  companion object {
    private const val EXTERNAL_MODEL_LINE =
      "modelProviders/provider1/modelSuites/suite1/modelLines/external"
    private const val BEFORE_MODEL_LINE =
      "modelProviders/provider1/modelSuites/suite1/modelLines/before"
    private const val ON_OR_AFTER_MODEL_LINE =
      "modelProviders/provider1/modelSuites/suite1/modelLines/after"
    private val CUTOVER: ModelLineCutover = modelLineCutover {
      externalModelLine = EXTERNAL_MODEL_LINE
      beforeCutoverModelLine = BEFORE_MODEL_LINE
      onOrAfterCutoverModelLine = ON_OR_AFTER_MODEL_LINE
      cutoverDate = date {
        year = 2026
        month = 10
        day = 1
      }
    }
    private val CUTOVER_CONFIG = CUTOVER.toModelLineCutoverConfig()
  }
}
