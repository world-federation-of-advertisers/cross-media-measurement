package org.wfanet.measurement.loadtest

import com.google.protobuf.TextFormat
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
  val sketchConfig = flags.sketchConfigFile.toSketchConfig()
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

private fun File.toSketchConfig(): SketchConfig {
  return TextFormat.parse(
    this.readText(),
    SketchConfig::class.java
  )
}

private fun makeFile(directory: File): File {
  val path = directory.toPath()
  return Files.createDirectories(path).toFile()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
