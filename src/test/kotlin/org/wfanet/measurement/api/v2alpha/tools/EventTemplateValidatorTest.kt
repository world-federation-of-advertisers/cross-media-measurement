/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha.tools

import java.nio.file.Path
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.CommandLineTesting.assertThat
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager

@RunWith(JUnit4::class)
class EventTemplateValidatorTest {
  private fun descriptorSetArgs(paths: Iterable<Path>): List<String> {
    return paths.map { "--descriptor-set=${it}" }
  }

  private fun callCli(args: Array<String>) =
    CommandLineTesting.capturingOutput(args, EventTemplateValidator::main)

  private fun callCli(args: List<String>) = callCli(args.toTypedArray())

  @Test
  fun `runs successfully when templates are valid`() {
    val args =
      descriptorSetArgs(TEST_EVENT_DESCRIPTOR_SET_RUNTIME_PATHS) + "--event-proto=$TEST_EVENT_NAME"

    val capturedOutput: CommandLineTesting.CapturedOutput = callCli(args)

    assertThat(capturedOutput).status().isEqualTo(0)
  }

  @Test
  fun `exits with error when event not found`() {
    val args =
      descriptorSetArgs(TEST_EVENT_DESCRIPTOR_SET_RUNTIME_PATHS.dropLast(1)) +
        "--event-proto=$TEST_EVENT_NAME"

    val capturedOutput: CommandLineTesting.CapturedOutput = callCli(args)

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).out().contains(TEST_EVENT_NAME)
  }

  @Test
  fun `exits with error when template annotation is missing`() {
    val args =
      arrayOf(
        "--descriptor-set=$BAD_TEMPLATES_DESCRIPTOR_SET_RUNTIME_PATH",
        "--event-proto=$TEST_TEMPLATES_PACKAGE.MissingTemplateAnnotationEvent",
      )

    val capturedOutput: CommandLineTesting.CapturedOutput = callCli(args)

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).out().contains("missing_template_annotation")
  }

  @Test
  fun `exits with error when field name does not match template name`() {
    val args =
      arrayOf(
        "--descriptor-set=$BAD_TEMPLATES_DESCRIPTOR_SET_RUNTIME_PATH",
        "--event-proto=$TEST_TEMPLATES_PACKAGE.MismatchedTemplateNameEvent",
      )

    val capturedOutput: CommandLineTesting.CapturedOutput = callCli(args)

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).out().contains("wrong_name")
    assertThat(capturedOutput).out().contains("dummy")
  }

  @Test
  fun `exits with error when template field annotation is missing`() {
    val args =
      arrayOf(
        "--descriptor-set=$BAD_TEMPLATES_DESCRIPTOR_SET_RUNTIME_PATH",
        "--event-proto=$TEST_TEMPLATES_PACKAGE.MissingFieldAnnotationEvent",
      )

    val capturedOutput: CommandLineTesting.CapturedOutput = callCli(args)

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).out().contains("field_without_annotation")
  }

  @Test
  fun `exits with error when template field has unsupported type`() {
    val args =
      arrayOf(
        "--descriptor-set=$BAD_TEMPLATES_DESCRIPTOR_SET_RUNTIME_PATH",
        "--event-proto=$TEST_TEMPLATES_PACKAGE.UnsupportedFieldTypeEvent",
      )

    val capturedOutput: CommandLineTesting.CapturedOutput = callCli(args)

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).out().contains("custom_field")
    assertThat(capturedOutput)
      .out()
      .contains("$TEST_TEMPLATES_PACKAGE.UnsupportedFieldType.CustomMessage")
  }

  @Test
  fun `exits with error when template field is repeated`() {
    val args =
      arrayOf(
        "--descriptor-set=$BAD_TEMPLATES_DESCRIPTOR_SET_RUNTIME_PATH",
        "--event-proto=$TEST_TEMPLATES_PACKAGE.UnsupportedRepeatedFieldEvent",
      )

    val capturedOutput: CommandLineTesting.CapturedOutput = callCli(args)

    assertThat(capturedOutput).status().isNotEqualTo(0)
    assertThat(capturedOutput).out().contains("repeated_field")
  }

  companion object {
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    private const val DESCRIPTOR_SET_SUFFIX = "descriptor-set.proto.bin"
    private const val TEST_TEMPLATES_PACKAGE = "wfa.measurement.api.v2alpha.event_templates.testing"
    private const val TEST_EVENT_NAME = "$TEST_TEMPLATES_PACKAGE.TestEvent"

    private val TEST_TEMPLATES_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "api",
        "v2alpha",
        "event_templates",
        "testing",
      )
    private val TEST_TEMPLATES_RUNTIME_PATH by lazy { getRuntimePath(TEST_TEMPLATES_PATH)!! }

    private val BAD_TEMPLATES_DESCRIPTOR_SET_RUNTIME_PATH by lazy {
      TEST_TEMPLATES_RUNTIME_PATH.resolve("bad_templates_proto-$DESCRIPTOR_SET_SUFFIX")
    }
    private val TEST_EVENT_DESCRIPTOR_SET_RUNTIME_PATHS by lazy {
      listOf(
        TEST_TEMPLATES_RUNTIME_PATH.resolve("person_proto-$DESCRIPTOR_SET_SUFFIX"),
        TEST_TEMPLATES_RUNTIME_PATH.resolve("video_proto-$DESCRIPTOR_SET_SUFFIX"),
        TEST_TEMPLATES_RUNTIME_PATH.resolve("banner_proto-$DESCRIPTOR_SET_SUFFIX"),
        TEST_TEMPLATES_RUNTIME_PATH.resolve("test_event_proto-$DESCRIPTOR_SET_SUFFIX"),
      )
    }
  }
}
