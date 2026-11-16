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

import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Message
import com.google.protobuf.TextFormat
import com.google.protobuf.util.JsonFormat
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import picocli.CommandLine

/** Converts local report files and optionally checks the reconstructed FULL report. */
@CommandLine.Command(
  name = "ConvertBasicReport",
  description = ["Converts FULL BasicReports and draft COMPACT report files."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class ConvertBasicReport private constructor() : Runnable {
  private enum class View {
    FULL,
    COMPACT,
  }

  private enum class Format {
    JSON,
    TEXTPROTO,
    BINARY,
  }

  @CommandLine.Spec private lateinit var spec: CommandLine.Model.CommandSpec

  @CommandLine.Option(
    names = ["--to"],
    required = true,
    description = ["Output view: FULL or COMPACT."],
  )
  private lateinit var view: View

  @CommandLine.Option(
    names = ["--input"],
    defaultValue = "-",
    description = ["Input file, or - for standard input."],
  )
  private lateinit var input: String

  @CommandLine.Option(
    names = ["--output"],
    defaultValue = "-",
    description = ["Output file, or - for standard output."],
  )
  private lateinit var output: String

  @CommandLine.Option(
    names = ["--input-format"],
    defaultValue = "JSON",
    description = ["Input encoding: JSON, TEXTPROTO or BINARY."],
  )
  private lateinit var inputFormat: Format

  @CommandLine.Option(
    names = ["--output-format"],
    defaultValue = "JSON",
    description = ["Output encoding: JSON, TEXTPROTO or BINARY."],
  )
  private lateinit var outputFormat: Format

  @CommandLine.Option(
    names = ["--verify-against"],
    description = ["Expected FULL file in --input-format encoding; requires --to=FULL."],
  )
  private lateinit var verifyAgainst: File

  @set:CommandLine.Option(
    names = ["--max-result-rows"],
    defaultValue = "1000000",
    description = ["Maximum total rows to allocate when expanding COMPACT."],
  )
  private var maxResultRows by Delegates.notNull<Int>()

  override fun run() {
    if (::verifyAgainst.isInitialized && view != View.FULL)
      invalid("--verify-against requires --to=FULL")
    if (maxResultRows < 0) invalid("--max-result-rows must not be negative")
    val source = if (input == "-") System.`in` else File(input).inputStream()
    val message =
      source.use {
        when (view) {
          View.FULL ->
            BasicReportConverter.expand(
              read(it, CompactBasicReport.newBuilder()).build() as CompactBasicReport,
              maxResultRows,
            )
          View.COMPACT ->
            BasicReportConverter.compact(read(it, BasicReport.newBuilder()).build() as BasicReport)
        }
      }
    if (::verifyAgainst.isInitialized) {
      val expected = verifyAgainst.inputStream().use { read(it, BasicReport.newBuilder()).build() }
      if (message != expected) invalid("Reconstructed report does not match --verify-against")
    }
    if (outputFormat != Format.BINARY && hasUnknownFields(message)) {
      invalid("Unknown protobuf fields require --output-format=BINARY")
    }
    val destination = if (output == "-") System.out else File(output).outputStream()
    destination.use { write(message, it) }
  }

  private fun read(input: InputStream, builder: Message.Builder): Message.Builder {
    when (inputFormat) {
      Format.JSON -> input.reader(Charsets.UTF_8).use { JsonFormat.parser().merge(it, builder) }
      Format.TEXTPROTO ->
        input.reader(Charsets.UTF_8).use { TextFormat.getParser().merge(it, builder) }
      Format.BINARY -> builder.mergeFrom(input)
    }
    return builder
  }

  private fun write(message: Message, output: OutputStream) {
    when (outputFormat) {
      Format.BINARY -> message.writeTo(output)
      Format.JSON ->
        output.writer(Charsets.UTF_8).use {
          JsonFormat.printer().appendTo(message, it)
          it.append('\n')
        }
      Format.TEXTPROTO ->
        output.writer(Charsets.UTF_8).use { TextFormat.printer().print(message, it) }
    }
  }

  private fun hasUnknownFields(message: Message): Boolean {
    if (message.unknownFields.asMap().isNotEmpty()) return true
    for ((field, value) in message.allFields) {
      if (field.javaType != FieldDescriptor.JavaType.MESSAGE) continue
      val children = if (field.isRepeated) value as List<*> else listOf(value)
      if (children.any { hasUnknownFields(it as Message) }) return true
    }
    return false
  }

  private fun invalid(message: String): Nothing =
    throw CommandLine.ParameterException(spec.commandLine(), message)

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(ConvertBasicReport(), args)
  }
}
