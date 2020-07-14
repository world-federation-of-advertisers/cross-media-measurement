package org.wfanet.measurement.tools

import com.google.devtools.build.runfiles.Runfiles
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command
import java.nio.file.Paths
import java.util.concurrent.Callable
import java.util.logging.Logger
import kotlin.system.exitProcess

@Command(
  name = "deploy_to_kind",
  description = ["Builds container images from source and deploys them to a local kind cluster."]
)
class DeployToKind() : Callable<Int> {
  private val duchyFilePath = "src/main/kotlin/org/wfanet/measurement/service/internal/duchy"

  private fun String.runAsProcess(
    // A lot of tools write things that aren't errors to stderr.
    redirectErrorStream: Boolean = true,
    exitOnFail: Boolean = true
  ) {
    logger.info("*** RUNNING: $this ***")

    val process = ProcessBuilder(this.split("\\s".toRegex()))
      .redirectErrorStream(redirectErrorStream)
      .start()

    runBlocking {
      val errJob = GlobalScope.async {
        process.errorStream.bufferedReader().use {
          it.lines().forEach { line -> logger.severe(line) }
        }
      }
      val outJob = GlobalScope.async {
        process.inputStream.bufferedReader().use {
          it.lines().forEach { line -> logger.info(line) }
        }
      }

      errJob.await()
      outJob.await()
    }

    process.waitFor()

    if (exitOnFail && process.exitValue() != 0) {
      logger.severe("*** FAILURE: Something went wrong. Aborting. ****")
      System.exit(process.exitValue())
    }
  }

  override fun call(): Int {
    logger.info("*** STARTING ***")

    val runfiles = Runfiles.create()

    // Load Duchy images.
    val duchyRunfiles = Paths.get(
      runfiles.rlocation("wfa_measurement_system/$duchyFilePath")
    ).toFile()
    duchyRunfiles
      .walk().filter { !it.isDirectory && it.extension == "tar" }
      .forEach { imageFile ->
        logger.info("*** LOADING IMAGE: ${imageFile.absolutePath} ***")

        // Figure out what the image name is.
        val repository = "bazel/$duchyFilePath/${imageFile.parentFile.relativeTo(duchyRunfiles)}"
        val tag = imageFile.nameWithoutExtension
        val imageName = "$repository:$tag"

        // Remove images from Docker if they already exist.
        // This is not strictly necessary but Docker keeps the old image in memory otherwise.
        logger.info(
          "*** FYI: If the image doesn't exist the next command fails. " +
            "This is expected and not a big deal. ***"
        )
        "docker rmi $imageName".runAsProcess(exitOnFail = false)

        // Load the image into Docker.
        "docker load -i ${imageFile.absolutePath}".runAsProcess(redirectErrorStream = false)

        // Load the image into Kind.
        "kind load docker-image $imageName".runAsProcess()

        logger.info("*** DONE LOADING IMAGE: $imageFile.absolutePath ***")
      }

    logger.info("*** DONE LOADING ALL IMAGES ***")

    val yaml = Paths.get(
      runfiles.rlocation("wfa_measurement_system/src/main/docker/deploy_to_kind.yaml")
    ).toFile()

    // kubectl apply does not necessarily overwrite previous configuration.
    // Delete existing pods/services to be safe.
    logger.info(
      "*** FYI: If the pods don't exist the next command fails. " +
        "This is expected and not a big deal. ***"
    )
    "kubectl delete -f ${yaml.absolutePath}".runAsProcess(exitOnFail = false)

    // Create the pods and services.
    "kubectl apply -f ${yaml.absolutePath}".runAsProcess()

    logger.info("*** DONE: Completed successfully. ***")

    return 0
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

fun main(args: Array<String>) {
  exitProcess(CommandLine(DeployToKind()).execute(*args))
}
