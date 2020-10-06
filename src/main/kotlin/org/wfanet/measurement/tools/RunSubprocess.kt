package org.wfanet.measurement.tools

import java.util.logging.Logger
import kotlin.system.exitProcess
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/** Runs a subprocess. */
fun runSubprocess(
  command: String,
  redirectErrorStream: Boolean = true,
  exitOnFail: Boolean = true
) {
  logger.info("*** RUNNING: $command***")

  val process =
    ProcessBuilder(command.split("\\s".toRegex()))
      .redirectErrorStream(redirectErrorStream)
      .start()

  runBlocking {
    joinAll(
      launch { process.errorStream.bufferedReader().forEachLine(logger::severe) },
      launch { process.inputStream.bufferedReader().forEachLine(logger::info) }
    )
  }

  process.waitFor()

  if (exitOnFail && process.exitValue() != 0) {
    logger.severe("*** FAILURE: Something went wrong. Aborting. ****")
    exitProcess(process.exitValue())
  }
}

private val logger: Logger = Logger.getLogger(::runSubprocess.javaClass.enclosingClass.name)
