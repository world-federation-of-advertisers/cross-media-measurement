// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.tools

import com.google.protobuf.TextFormat
import com.google.protobuf.util.JsonFormat
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.loadtest.dataprovider.toPopulationSpec
import org.wfanet.measurement.loadtest.dataprovider.toPopulationSpecWithoutAttributes
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import java.io.File
import kotlin.system.exitProcess

@Command(
  name = "population_spec_converter",
  description = ["Convert SyntheticPopulationSpec to PopulationSpec"],
  subcommands = [CommandLine.HelpCommand::class]
)
class PopulationSpecConverterTool : Runnable {
  @Parameters(index = "0", description = ["Path to the SyntheticPopulationSpec textproto file"])
  private lateinit var inputFile: String

  @Option(names = ["-f", "--format"], description = ["Output format: text or json"], defaultValue = "text")
  private var format: String = "text"

  @Option(names = ["--with-attributes"], description = ["Include attributes in output"])
  private var withAttributes: Boolean = false

  override fun run() {
    val file = File(inputFile)
    if (!file.exists()) {
      System.err.println("File not found: $inputFile")
      exitProcess(1)
    }

    // Read and parse SyntheticPopulationSpec from textproto
    val syntheticSpec = SyntheticPopulationSpec.newBuilder().apply {
      TextFormat.Parser.newBuilder().build().merge(file.readText(), this)
    }.build()

    println("=== SyntheticPopulationSpec ===")
    println("VID Range: ${syntheticSpec.vidRange.start} - ${syntheticSpec.vidRange.endExclusive}")
    println("Event Message Type: ${syntheticSpec.eventMessageTypeUrl}")
    println("Population Fields: ${syntheticSpec.populationFieldsList.joinToString(", ")}")
    println("Non-Population Fields: ${syntheticSpec.nonPopulationFieldsList.joinToString(", ")}")
    println("Sub-Populations: ${syntheticSpec.subPopulationsCount}")
    println()

    // Convert to PopulationSpec
    val populationSpec = if (withAttributes) {
      println("Converting with attributes...")
      val eventDescriptor = TestEvent.getDescriptor()
      syntheticSpec.toPopulationSpec(eventDescriptor)
    } else {
      println("Converting without attributes...")
      syntheticSpec.toPopulationSpecWithoutAttributes()
    }

    println("=== PopulationSpec ===")
    println("Sub-Populations: ${populationSpec.subpopulationsCount}")
    println()

    val jsonPrinter = JsonFormat.printer()
      .preservingProtoFieldNames()
      .includingDefaultValueFields()

    when (format) {
      "json" -> println(jsonPrinter.print(populationSpec))
      else -> println(TextFormat.printer().printToString(populationSpec))
    }
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(PopulationSpecConverterTool(), args)
  }
}
