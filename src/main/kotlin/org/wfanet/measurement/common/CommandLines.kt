package org.wfanet.measurement.common

import java.time.Duration
import kotlin.reflect.KFunction
import kotlin.reflect.jvm.javaMethod
import kotlin.system.exitProcess
import picocli.CommandLine

/**
 * A `main` function for a [CommandLine] application.
 *
 * @param command A function annotated with [@Command][CommandLine.Command] to execute.
 * @param args Command-line arguments.
 */
fun commandLineMain(command: KFunction<*>, args: Array<String>) {
  exitProcess(
    CommandLine(command.javaMethod)
      .registerConverter(Duration::class.java) { it.toDuration() }
      .execute(*args)
  )
}
