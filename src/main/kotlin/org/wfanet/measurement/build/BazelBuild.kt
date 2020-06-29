package org.wfanet.measurement.build

import com.google.devtools.build.runfiles.Runfiles
import java.io.File
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

/**
 * Support running Bazel from Kotlin.
 *
 * This writes to the workspace directory given as well as to the Bazel output root. Therefore, it
 * cannot be run from sandboxed targets.
 */
class BazelBuild() {
  companion object {
    private const val BAZEL_RUNFILES_PATH: String = "bazel_binary/bazel"

    /**
     * Null file.
     *
     * TODO(sanjayvas): Replace with [ProcessBuilder.Redirect].`DISCARD` for Java 9+.
     */
    private val NULL_FILE = File(
      if (System.getProperty("os.name").startsWith("Windows")) "NUL" else "/dev/null"
    )
  }

  /** Bazel binary file in runfiles. */
  private val bazelBinary: File

  init {
    val runfiles = Runfiles.create()
    bazelBinary = Paths.get(runfiles.rlocation(BAZEL_RUNFILES_PATH)).toFile()
    check(bazelBinary.canExecute())
  }

  /**
   * Builds a target in the specified Bazel workspace and copies the output to the output path.
   *
   * @param workspacePath Path to the Bazel workspace.
   * @param buildTarget Build target label within the workspace. The output file of this target is
   *     assumed to have the same name as the target, and be written to Bazel's `$(BINDIR)`.
   * @param output Path to copy the target output to.
   */
  fun build(workspacePath: Path, buildTarget: String, output: Path) {
    val bazelProcess = ProcessBuilder().directory(workspacePath.toFile())
      .command(
        bazelBinary.absolutePath,
        "build",
        buildTarget
      ).redirectOutput(NULL_FILE)
      .start()
    val exitCode = bazelProcess.waitFor()
    if (exitCode == 0) {
      // Gobble stderr.
      bazelProcess.errorStream.use { out ->
        out.copyTo(NullOutputStream())
      }
    } else {
      bazelProcess.errorStream.use { out ->
        out.copyTo(System.err)
      }
      throw Exception("Build error")
    }

    /** Path to build output, relative to workspace path. */
    val buildOutput =
      Paths.get(
        "bazel-bin",
        *buildTarget.removePrefix("//").replace(':', '/').split('/').toTypedArray()
      )

    Files.copy(
      workspacePath.resolve(buildOutput),
      output,
      StandardCopyOption.REPLACE_EXISTING,
      StandardCopyOption.COPY_ATTRIBUTES
    )
  }

  private fun ensureDirectory(directory: Path): Path {
    if (!directory.toFile().isDirectory) {
      return Files.createDirectory(directory)
    }
    return directory
  }

  /**
   * Null output stream.
   *
   * TODO(sanjayvas): Replace with [java.io.OutputStream]`.nullOutputStream()` for Java 9+.
   */
  class NullOutputStream() : OutputStream() {
    override fun write(b: Int) {
      // Do nothing.
    }
  }
}

fun main(args: Array<String>) {
  require(args.size == 3)
  val workspacePath = Paths.get(args[0])
  val buildTarget = args[1]
  val output = Paths.get(args[2])

  BazelBuild().build(workspacePath, buildTarget, output)
}
