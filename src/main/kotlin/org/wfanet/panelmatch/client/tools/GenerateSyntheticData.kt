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

import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.io.File
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.properties.Delegates
import kotlin.random.Random
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.panelmatch.client.eventpreprocessing.UnprocessedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.unprocessedEvent
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyAndId
import org.wfanet.panelmatch.client.exchangetasks.joinKey
import org.wfanet.panelmatch.client.exchangetasks.joinKeyAndId
import org.wfanet.panelmatch.client.exchangetasks.joinKeyAndIdCollection
import org.wfanet.panelmatch.client.exchangetasks.joinKeyIdentifier
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.brotliCompressionParameters
import org.wfanet.panelmatch.common.compression.CompressionParametersKt.noCompression
import org.wfanet.panelmatch.common.compression.compressionParameters
import org.wfanet.panelmatch.common.toDelimitedByteString
import org.wfanet.virtualpeople.common.dataProviderEvent
import org.wfanet.virtualpeople.common.labelerInput
import org.wfanet.virtualpeople.common.logEvent
import picocli.CommandLine.Command
import picocli.CommandLine.Option

private val FROM_TIME = TimeUnit.SECONDS.toMicros(Instant.parse("2022-01-01T00:00:00Z").epochSecond)
private val UNTIL_TIME =
  TimeUnit.SECONDS.toMicros(Instant.parse("2022-04-04T00:00:00Z").epochSecond)

@Command(
  name = "generate-synthetic-data",
  description = ["Generates synthetic data for Panel Match."],
)
class GenerateSyntheticData : Runnable {
  @set:Option(
    names = ["--number_of_events"],
    description = ["Number of UnprocessedEvent protos to generate."],
    required = true,
  )
  var numberOfEvents by Delegates.notNull<Int>()
    private set

  @Option(
    names = ["--unprocessed_events_output_path"],
    description = ["Path to the file where UnprocessedEvent protos will be written."],
    required = false,
    defaultValue = "edp-unprocessed-events",
  )
  private lateinit var unprocessedEventsFile: File

  @Option(
    names = ["--join_keys_output_path"],
    description = ["Path to the file where JoinKeyAndIdCollection proto will be written."],
    required = false,
    defaultValue = "mp-plaintext-join-keys",
  )
  private lateinit var joinKeysFile: File

  @set:Option(
    names = ["--join_key_sample_rate"],
    description = ["The sample rate [0, 1] used for selecting an UnprocessedEvent proto."],
    required = true,
    defaultValue = "0.0005",
  )
  var sampleRate by Delegates.notNull<Double>()
    private set

  @Option(
    names = ["--brotli_dictionary_path"],
    description = ["Path to a file containing a Brotli dictionary."],
    required = false,
  )
  private lateinit var brotliInputFile: File

  @Option(
    names = ["--compression_parameters_output_path"],
    description = ["Path to the file where the CompressionParameters proto will be written."],
    required = false,
  )
  private lateinit var brotliOutputFile: File

  @Option(
    names = ["--previous_join_keys_input_path"],
    description = ["Path to the file where the previous day's join keys are stored."],
    required = false,
  )
  private lateinit var joinKeyInputFile: File

  @Option(
    names = ["--previous_join_keys_output_path"],
    description = ["Path to the file where previous day's join keys will be copied to."],
    required = false,
  )
  private lateinit var joinKeyOutputFile: File

  /** Creates a [JoinKeyAndId] proto from the given [UnprocessedEvent] proto. */
  fun UnprocessedEvent.toJoinKeyAndId(): JoinKeyAndId {
    return joinKeyAndId {
      this.joinKey = joinKey { key = id }
      this.joinKeyIdentifier = joinKeyIdentifier { this.id = "$id-join-key-id".toByteStringUtf8() }
    }
  }

  fun generateSyntheticData(id: Int): UnprocessedEvent {
    val rawDataProviderEvent = dataProviderEvent {
      this.logEvent = logEvent {
        this.labelerInput = labelerInput {
          this.timestampUsec = Random.nextLong(FROM_TIME, UNTIL_TIME)
        }
      }
    }
    return unprocessedEvent {
      this.id = id.toString().toByteStringUtf8()
      this.data = rawDataProviderEvent.toByteString()
    }
  }

  fun writeCompressionParameters() {
    if (brotliOutputFile.name.isEmpty()) return

    val params =
      if (brotliInputFile.name.isEmpty()) {
        compressionParameters { this.uncompressed = noCompression {} }
      } else {
        compressionParameters {
          this.brotli = brotliCompressionParameters {
            this.dictionary = brotliInputFile.readBytes().toByteString()
          }
        }
      }

    brotliOutputFile.outputStream().use { outputStream ->
      outputStream.write(params.toByteString().toByteArray())
    }
  }

  @kotlin.io.path.ExperimentalPathApi
  override fun run() {
    val joinKeyAndIdProtos = mutableListOf<JoinKeyAndId>()
    val events = sequence {
      (1..numberOfEvents).forEach {
        val event = generateSyntheticData(it)

        if (Random.nextDouble(1.0) < sampleRate) {
          joinKeyAndIdProtos.add(event.toJoinKeyAndId())
        }

        yield(event)
      }
    }

    unprocessedEventsFile.outputStream().use { outputStream ->
      events.forEach { outputStream.write(it.toDelimitedByteString().toByteArray()) }
    }

    joinKeysFile.outputStream().use { outputStream ->
      outputStream.write(
        joinKeyAndIdCollection { joinKeyAndIds += joinKeyAndIdProtos }.toByteArray()
      )
    }

    writeCompressionParameters()

    joinKeyInputFile.copyTo(joinKeyOutputFile)
  }
}

fun main(args: Array<String>) = commandLineMain(GenerateSyntheticData(), args)
