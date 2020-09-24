// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.loadtest

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.io.File
import java.nio.file.Files
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.hexAsByteString
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.crypto.ElGamalPublicKey
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import picocli.CommandLine

@CommandLine.Command(
  name = "run_correctness",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: CorrectnessFlags) {
  val channel: ManagedChannel =
    ManagedChannelBuilder
      .forTarget(flags.publisherDataServiceTarget)
      .usePlaintext()
      .build()
  val publisherDataStub = PublisherDataCoroutineStub(channel)

  var runId = flags.runId
  if (flags.runId.isBlank()) {
    // Set the runId to current timestamp.
    runId = DateTimeFormatter
      .ofPattern("yyyy-MM-ddHH-mm-ss-SSS")
      .withZone(ZoneOffset.UTC)
      .format(Instant.now())
  }
  val storageClient = FileSystemStorageClient(makeFile(flags.outputDir))
  val sketchConfig = parseTextProto(flags.sketchConfigFile, SketchConfig.getDefaultInstance())
  val encryptionPublicKey = ElGamalPublicKey(
    flags.curveId,
    flags.encryptionKeyGenerator.hexAsByteString(),
    flags.encryptionKeyElement.hexAsByteString()
  )
  val clock = Clock.systemUTC()
  val spannerFromFlags = SpannerFromFlags(flags.spannerFlags)
  val relationalDatabase =
    GcpKingdomRelationalDatabase(clock, RandomIdGenerator(clock), spannerFromFlags.databaseClient)

  val correctness = CorrectnessImpl(
    dataProviderCount = flags.dataProviderCount,
    campaignCount = flags.campaignCount,
    generatedSetSize = flags.generatedSetSize,
    universeSize = flags.universeSize,
    runId = runId,
    sketchConfig = sketchConfig,
    encryptionPublicKey = encryptionPublicKey,
    storageClient = storageClient,
    combinedPublicKeyId = flags.combinedPublicKeyId,
    publisherDataStub = publisherDataStub
  )

  runBlocking {
    correctness.process(relationalDatabase)
  }
}

private fun makeFile(directory: File): File {
  val path = directory.toPath()
  return Files.createDirectories(path).toFile()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
