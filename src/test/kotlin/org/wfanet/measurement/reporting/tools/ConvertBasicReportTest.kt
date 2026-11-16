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

package org.wfanet.measurement.reporting.tools

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Message
import com.google.protobuf.TextFormat
import com.google.protobuf.UnknownFieldSet
import com.google.protobuf.util.JsonFormat
import java.io.File
import java.util.concurrent.TimeUnit
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.reporting.v2alpha.BasicReport

class ConvertBasicReportTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `main converts JSON files and verifies exact reconstruction`() {
    val full = file(ReportFixtures.full(), "JSON")
    val compact = temporaryFolder.newFile()
    val restored = temporaryFolder.newFile()
    assertThat(
        invokeMain(listOf("--to=COMPACT", "--input=$full", "--output=$compact"), byteArrayOf())
          .status
      )
      .isEqualTo(0)
    val result =
      invokeMain(
        listOf("--to=FULL", "--input=$compact", "--output=$restored", "--verify-against=$full"),
        byteArrayOf(),
      )
    assertThat(result.status).isEqualTo(0)
    assertThat(parse(restored.readBytes(), BasicReport.newBuilder(), "JSON"))
      .isEqualTo(ReportFixtures.full())
  }

  @Test
  fun `main supports all input and output encodings`() {
    val full = ReportFixtures.full()
    val expected = file(full, "BINARY")
    for (format in listOf("JSON", "TEXTPROTO", "BINARY")) {
      val source = file(full, format)
      val compact = temporaryFolder.newFile()
      assertThat(
          invokeMain(
              listOf(
                "--to=COMPACT",
                "--input=$source",
                "--input-format=$format",
                "--output=$compact",
                "--output-format=$format",
              ),
              byteArrayOf(),
            )
            .status
        )
        .isEqualTo(0)
      val compactMessage = parse(compact.readBytes(), CompactBasicReport.newBuilder(), format)
      val compactBinary = file(compactMessage, "BINARY")
      val restored = temporaryFolder.newFile()
      val result =
        invokeMain(
          listOf(
            "--to=FULL",
            "--input=$compactBinary",
            "--input-format=BINARY",
            "--output=$restored",
            "--output-format=$format",
            "--verify-against=$expected",
          ),
          byteArrayOf(),
        )
      assertThat(result.status).isEqualTo(0)
      assertThat(parse(restored.readBytes(), BasicReport.newBuilder(), format)).isEqualTo(full)
    }
  }

  @Test
  fun `main reads standard input and writes standard output`() {
    val compact = BasicReportConverter.compact(ReportFixtures.full())
    val result =
      invokeMain(
        listOf("--to=FULL", "--output-format=BINARY"),
        JsonFormat.printer().print(compact).toByteArray(),
      )
    assertThat(result.status).isEqualTo(0)
    assertThat(BasicReport.parseFrom(result.output)).isEqualTo(ReportFixtures.full())
  }

  @Test
  fun `main verification mismatch preserves an existing output file`() {
    val compact = file(BasicReportConverter.compact(ReportFixtures.full()), "JSON")
    val other = file(ReportFixtures.full().toBuilder().setTitle("Different report").build(), "JSON")
    val destination = temporaryFolder.newFile().also { it.writeText("keep") }
    val result =
      invokeMain(
        listOf("--to=FULL", "--input=$compact", "--output=$destination", "--verify-against=$other"),
        byteArrayOf(),
      )
    assertThat(result.status).isNotEqualTo(0)
    assertThat(result.error).contains("does not match")
    assertThat(destination.readText()).isEqualTo("keep")
  }

  @Test
  fun `main rejects malformed JSON before opening output`() {
    val source = temporaryFolder.newFile().also { it.writeText("{") }
    val destination = temporaryFolder.newFile().also { it.writeText("keep") }
    val result =
      invokeMain(listOf("--to=FULL", "--input=$source", "--output=$destination"), byteArrayOf())
    assertThat(result.status).isNotEqualTo(0)
    assertThat(destination.readText()).isEqualTo("keep")
  }

  @Test
  fun `main rejects malformed compact indices before opening output`() {
    val compact = BasicReportConverter.compact(ReportFixtures.full()).toBuilder()
    compact.setGroups(0, compact.getGroups(0).toBuilder().setWindowIndices(0, 999))
    val source = file(compact.build(), "JSON")
    val destination = temporaryFolder.newFile().also { it.writeText("keep") }
    val result =
      invokeMain(listOf("--to=FULL", "--input=$source", "--output=$destination"), byteArrayOf())
    assertThat(result.status).isNotEqualTo(0)
    assertThat(destination.readText()).isEqualTo("keep")
  }

  @Test
  fun `main rejects unknown compact format version`() {
    val source =
      file(
        BasicReportConverter.compact(ReportFixtures.full()).toBuilder().setFormatVersion(2).build(),
        "JSON",
      )
    assertThat(invokeMain(listOf("--to=FULL", "--input=$source"), byteArrayOf()).status)
      .isNotEqualTo(0)
  }

  @Test
  fun `main applies expansion row budget`() {
    val source = file(BasicReportConverter.compact(ReportFixtures.full()), "JSON")
    val result =
      invokeMain(listOf("--to=FULL", "--input=$source", "--max-result-rows=1"), byteArrayOf())
    assertThat(result.status).isNotEqualTo(0)
    assertThat(result.error).contains("maxResultRows")
  }

  @Test
  fun `main verification requires FULL output`() {
    val full = file(ReportFixtures.full(), "JSON")
    val result =
      invokeMain(listOf("--to=COMPACT", "--input=$full", "--verify-against=$full"), byteArrayOf())
    assertThat(result.status).isNotEqualTo(0)
    assertThat(result.error).contains("requires --to=FULL")
  }

  @Test
  fun `main preserves unknown fields in binary conversions`() {
    val unknown =
      UnknownFieldSet.newBuilder()
        .addField(999, UnknownFieldSet.Field.newBuilder().addVarint(123).build())
        .build()
    val full = ReportFixtures.full().toBuilder().setUnknownFields(unknown).build()
    val source = file(full, "BINARY")
    val compact = temporaryFolder.newFile()
    val result =
      invokeMain(
        listOf(
          "--to=COMPACT",
          "--input=$source",
          "--input-format=BINARY",
          "--output=$compact",
          "--output-format=BINARY",
        ),
        byteArrayOf(),
      )
    assertThat(result.status).isEqualTo(0)
    val restored =
      invokeMain(
        listOf(
          "--to=FULL",
          "--input=$compact",
          "--input-format=BINARY",
          "--output-format=BINARY",
          "--verify-against=$source",
        ),
        byteArrayOf(),
      )
    assertThat(restored.status).isEqualTo(0)
    assertThat(BasicReport.parseFrom(restored.output)).isEqualTo(full)
  }

  @Test
  fun `main rejects output encodings that would discard unknown fields`() {
    val unknown =
      UnknownFieldSet.newBuilder()
        .addField(999, UnknownFieldSet.Field.newBuilder().addVarint(123).build())
        .build()
    val full = ReportFixtures.full().toBuilder().setUnknownFields(unknown).build()
    val source = file(BasicReportConverter.compact(full), "BINARY")
    val destination = temporaryFolder.newFile().also { it.writeText("keep") }
    val result =
      invokeMain(
        listOf("--to=FULL", "--input=$source", "--input-format=BINARY", "--output=$destination"),
        byteArrayOf(),
      )
    assertThat(result.status).isNotEqualTo(0)
    assertThat(result.error).contains("--output-format=BINARY")
    assertThat(destination.readText()).isEqualTo("keep")
  }

  private fun file(message: Message, format: String): File =
    temporaryFolder.newFile().also {
      when (format) {
        "JSON" -> it.writeText(JsonFormat.printer().print(message))
        "TEXTPROTO" -> it.writeText(TextFormat.printer().printToString(message))
        "BINARY" -> it.writeBytes(message.toByteArray())
        else -> error("Unsupported test format")
      }
    }

  private fun parse(bytes: ByteArray, builder: Message.Builder, format: String): Message {
    when (format) {
      "JSON" -> JsonFormat.parser().merge(bytes.toString(Charsets.UTF_8), builder)
      "TEXTPROTO" -> TextFormat.getParser().merge(bytes.toString(Charsets.UTF_8), builder)
      "BINARY" -> builder.mergeFrom(bytes)
      else -> error("Unsupported test format")
    }
    return builder.build()
  }

  private fun invokeMain(arguments: List<String>, input: ByteArray): Result {
    val out = temporaryFolder.newFile()
    val err = temporaryFolder.newFile()
    val process =
      ProcessBuilder(
          File(System.getProperty("java.home"), "bin/java").path,
          "-cp",
          System.getProperty("java.class.path"),
          ConvertBasicReport::class.java.name,
          *arguments.toTypedArray(),
        )
        .redirectOutput(out)
        .redirectError(err)
        .start()
    try {
      process.outputStream.use { it.write(input) }
      check(process.waitFor(30, TimeUnit.SECONDS)) { "Report converter did not finish" }
      return Result(process.exitValue(), out.readBytes(), err.readText())
    } finally {
      if (process.isAlive) process.destroyForcibly().waitFor()
    }
  }

  private data class Result(val status: Int, val output: ByteArray, val error: String)
}
