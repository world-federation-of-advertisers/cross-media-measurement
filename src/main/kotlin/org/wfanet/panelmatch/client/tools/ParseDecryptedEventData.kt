// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.tools

import com.google.protobuf.Message
import com.google.protobuf.Parser
import java.io.BufferedInputStream
import java.io.File
import java.io.PrintWriter
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.toJson
import org.wfanet.panelmatch.client.eventpreprocessing.CombinedEvents
import org.wfanet.panelmatch.client.privatemembership.KeyedDecryptedEventDataSet
import org.wfanet.panelmatch.client.privatemembership.isPaddingQuery
import org.wfanet.panelmatch.client.tools.DataProviderEventSetKt.entry
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.virtualpeople.common.DataProviderEvent
import picocli.CommandLine.Command
import picocli.CommandLine.Option

private const val BUFFER_SIZE_BYTES = 32 * 1024 * 1024 // 32MiB

@Command(
  name = "parse-decrypted-event-data",
  description = ["Parses the decrypted event data produced by an exchange."],
)
class ParseDecryptedEventData : Runnable {

  @Option(
    names = ["--manifest-path"],
    description = ["Path to the manifest file of the decrypted event data set."],
    required = true,
  )
  private lateinit var manifestFile: File

  @Option(
    names = ["--output-path"],
    description = ["Path to output the parsed JSON result."],
    required = true,
  )
  private lateinit var outputFile: File

  @set:Option(
    names = ["--max-shards-to-parse"],
    description = ["If specified, up to this many shards will be parsed."],
    required = false,
    defaultValue = "-1",
  )
  private var maxShardsToParse by Delegates.notNull<Int>()

  override fun run() {
    val fileSpec = manifestFile.readText().trim()
    val shardedFileName = ShardedFileName(fileSpec)
    val parsedShards = shardedFileName.parseAllShards(KeyedDecryptedEventDataSet.parser())

    PrintWriter(outputFile.outputStream()).use { printWriter ->
      for (keyedDecryptedEventDataSet in parsedShards) {
        if (!keyedDecryptedEventDataSet.hasPaddingNonce()) {
          printWriter.println(keyedDecryptedEventDataSet.toDataProviderEventSetEntry().toJson())
        }
      }
    }
  }

  private fun KeyedDecryptedEventDataSet.toDataProviderEventSetEntry(): DataProviderEventSet.Entry {
    return entry {
      joinKeyAndId = plaintextJoinKeyAndId
      events +=
        decryptedEventDataList.flatMap { plaintext ->
          val combinedEvents = CombinedEvents.parseFrom(plaintext.payload)
          combinedEvents.serializedEventsList.map { DataProviderEvent.parseFrom(it) }
        }
    }
  }

  private fun <T : Message> ShardedFileName.parseAllShards(parser: Parser<T>): Sequence<T> {
    return sequence {
      fileNames.forEachIndexed { shardNum, fileName ->
        if (maxShardsToParse in 0..shardNum) {
          return@sequence
        }

        val shardFile = manifestFile.parentFile.resolve(fileName)
        BufferedInputStream(shardFile.inputStream(), BUFFER_SIZE_BYTES).use { inputStream ->
          while (true) {
            val message: T = parser.parseDelimitedFrom(inputStream) ?: break
            yield(message)
          }
        }
      }
    }
  }

  private fun KeyedDecryptedEventDataSet.hasPaddingNonce(): Boolean {
    return plaintextJoinKeyAndId.joinKeyIdentifier.isPaddingQuery
  }
}

fun main(args: Array<String>) = commandLineMain(ParseDecryptedEventData(), args)
