/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.panelmatch.client.tools

import com.google.privatemembership.batch.Shared.Parameters
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow as V2AlphaExchangeWorkflow
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.tools.ExchangeWorkflowFormat.KINGDOMLESS
import org.wfanet.panelmatch.client.tools.ExchangeWorkflowFormat.V2ALPHA
import picocli.CommandLine.Command
import picocli.CommandLine.Option

@Command(
  name = "SerializeExchangeWorkflow",
  description =
    [
      "Serializes a textproto format wfa.measurement.api.v2alpha.ExchangeWorkflow.",
      "",
      "Note that this does not handle RLWE parameter types other than " +
        "private_membership.batch.Parameters.",
    ],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
class SerializeExchangeWorkflow : Runnable {
  @Option(
    names = ["--in"],
    description = ["Input ExchangeWorkflow file in textproto format"],
    required = true,
  )
  private lateinit var input: File

  @Option(
    names = ["--in-format"],
    description = ["Format of the input ExchangeWorkflow"],
    required = true,
  )
  private lateinit var inputFormat: ExchangeWorkflowFormat

  @Option(
    names = ["--out"],
    description = ["Output ExchangeWorkflow file in binary format"],
    required = true,
  )
  private lateinit var output: File

  override fun run() {
    val typeRegistry = TypeRegistry.newBuilder().apply { add(Parameters.getDescriptor()) }.build()
    val parsed: Message =
      when (inputFormat) {
        V2ALPHA -> parseTextProto(input, V2AlphaExchangeWorkflow.getDefaultInstance(), typeRegistry)
        KINGDOMLESS -> parseTextProto(input, ExchangeWorkflow.getDefaultInstance(), typeRegistry)
      }

    output.outputStream().use { outputStream -> parsed.writeTo(outputStream) }
  }
}

fun main(args: Array<String>) = commandLineMain(SerializeExchangeWorkflow(), args)
