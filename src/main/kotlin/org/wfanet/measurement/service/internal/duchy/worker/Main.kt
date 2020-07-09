package org.wfanet.measurement.service.internal.duchy.worker

import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.db.duchy.gcp.newCascadingLegionsSketchAggregationGcpComputationManager
import org.wfanet.measurement.db.gcp.GoogleCloudStorageFromFlags
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import picocli.CommandLine
import kotlin.properties.Delegates
import kotlin.reflect.jvm.javaMethod
import kotlin.system.exitProcess

private class WorkerServiceFlags {
  @set:CommandLine.Option(
    names = ["--port", "-p"],
    description = ["TCP port for gRPC server."],
    required = true,
    defaultValue = "8080"
  )
  var port by Delegates.notNull<Int>()
    private set

  @CommandLine.Option(
    names = ["--server-name"],
    description = ["Name of the gRPC server for logging purposes."],
    required = true,
    defaultValue = "WorkerServer"
  )
  lateinit var nameForLogging: String
    private set
}

@ExperimentalCoroutinesApi
@CommandLine.Command(name = "gcp_worker_server", mixinStandardHelpOptions = true)
private fun run(
  @CommandLine.Mixin workerServiceFlags: WorkerServiceFlags,
  @CommandLine.Mixin spannerFlags: SpannerFromFlags.Flags,
  @CommandLine.Mixin cloudStorageFlags: GoogleCloudStorageFromFlags.Flags
) {
  // TODO: Expand flags and configuration to work on other cloud environments when available.
  val spannerFromFlags = SpannerFromFlags(spannerFlags)
  val cloudStorageFromFlags = GoogleCloudStorageFromFlags(cloudStorageFlags)

  val computationManager = newCascadingLegionsSketchAggregationGcpComputationManager(
    duchyName = workerServiceFlags.nameForLogging,
    // TODO: Pass public keys of all duchies to the computation manager
    duchyPublicKeys = mapOf(),
    databaseClient = spannerFromFlags.databaseClient,
    googleCloudStorageOptions = cloudStorageFromFlags.cloudStorageOptions,
    storageBucket = cloudStorageFromFlags.bucket
  )

  CommonServer(
    workerServiceFlags.nameForLogging,
    workerServiceFlags.port,
    WorkerServiceImpl(computationManager)
  )
    .start()
    .blockUntilShutdown()
}

@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
  exitProcess(CommandLine(::run.javaMethod).execute(*args))
}
