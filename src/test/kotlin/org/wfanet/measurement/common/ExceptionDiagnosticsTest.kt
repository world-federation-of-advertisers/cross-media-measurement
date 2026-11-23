/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.BoolValue
import io.grpc.Status
import java.io.IOException
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.SimpleFormatter
import org.junit.Test
import org.wfanet.measurement.common.ExceptionDiagnostics.Type
import org.wfanet.measurement.common.ExceptionDiagnostics.ValidationFailure

class ExceptionDiagnosticsTest {
  @Test
  fun `forLogging excludes messages and preserves detached cause and suppressed locations`() {
    val cause = IllegalArgumentException(PRIVATE_VALUE)
    val suppressed = IllegalStateException(PRIVATE_VALUE)
    val error = IllegalStateException(PRIVATE_VALUE, cause).apply { addSuppressed(suppressed) }

    val diagnostic = ExceptionDiagnostics.forLogging(error)
    val output = format(diagnostic)

    assertThat(output).doesNotContain(PRIVATE_VALUE)
    assertThat(output).contains("java.lang.IllegalStateException")
    assertThat(output).contains("Caused by: java.lang.IllegalArgumentException")
    assertThat(output).contains("Suppressed: java.lang.IllegalStateException")
    assertThat(output).contains(ExceptionDiagnosticsTest::class.java.name)
    assertThat(diagnostic).isNotSameInstanceAs(error)
    assertThat(diagnostic.cause).isNotSameInstanceAs(cause)
    assertThat(diagnostic.suppressed.single()).isNotSameInstanceAs(suppressed)
    val location = diagnostic.stackTrace.first()
    val originalLocation = error.stackTrace.first()
    assertThat(location.className).isEqualTo(originalLocation.className)
    assertThat(location.methodName).isEqualTo(originalLocation.methodName)
    assertThat(location.fileName).isEqualTo(originalLocation.fileName)
    assertThat(location.lineNumber).isEqualTo(originalLocation.lineNumber)
    assertThat(error.message).isEqualTo(PRIVATE_VALUE)
  }

  @Test
  fun `forLogging never invokes custom message or stack rendering`() {
    val error =
      object : IllegalStateException() {
        override val message: String
          get() = throw AssertionError("message must not be read")

        override fun getLocalizedMessage(): String = PRIVATE_VALUE

        override fun toString(): String = PRIVATE_VALUE

        override fun printStackTrace(writer: java.io.PrintWriter) {
          writer.print(PRIVATE_VALUE)
        }
      }

    assertThat(format(ExceptionDiagnostics.forLogging(error))).doesNotContain(PRIVATE_VALUE)
    assertThat(ExceptionDiagnostics.summary(error)).contains(error.javaClass.name)
  }

  @Test
  fun `summary retains wrapped grpc code without its description`() {
    val error =
      IllegalStateException(
        PRIVATE_VALUE,
        Status.UNAVAILABLE.withDescription(PRIVATE_VALUE).asException(),
      )

    assertThat(ExceptionDiagnostics.summary(error)).contains("grpc.UNAVAILABLE")
    assertThat(ExceptionDiagnostics.summary(error)).doesNotContain(PRIVATE_VALUE)
    assertThat(format(ExceptionDiagnostics.forLogging(error))).contains("grpc.UNAVAILABLE")
    assertThat(format(ExceptionDiagnostics.forLogging(error))).doesNotContain(PRIVATE_VALUE)
  }

  @Test
  fun `validation failure retains category and schema metadata`() {
    val error =
      ValidationFailure.incompatibleType(
        Type.BOOLEAN,
        Type.BYTES_VALUE,
        BoolValue.getDescriptor().findFieldByName("value"),
      )

    val summary = ExceptionDiagnostics.summary(error)
    assertThat(summary).contains("INCOMPATIBLE_TYPE")
    assertThat(summary).contains("field=google.protobuf.BoolValue.value")
    assertThat(summary).contains("field=google.protobuf.BoolValue.value")
    assertThat(summary).contains("expected=BOOLEAN")
    assertThat(summary).contains("actual=BYTES_VALUE")
    assertThat(format(ExceptionDiagnostics.forLogging(error))).contains("INCOMPATIBLE_TYPE")
  }

  @Test
  fun `forLogging bounds cyclic and deep exception graphs`() {
    val first = IllegalStateException(PRIVATE_VALUE)
    val second = IllegalArgumentException(PRIVATE_VALUE)
    first.initCause(second)
    second.initCause(first)
    assertThat(format(ExceptionDiagnostics.forLogging(first))).contains("Exception graph omitted")
    assertThat(format(ExceptionDiagnostics.forLogging(first))).doesNotContain(PRIVATE_VALUE)

    var deep: Throwable = first
    repeat(100) { deep = IllegalStateException(PRIVATE_VALUE, deep) }
    val output = format(ExceptionDiagnostics.forLogging(deep))
    assertThat(output).contains("Exception graph omitted")
    assertThat(output).doesNotContain(PRIVATE_VALUE)
    assertThat(output.length).isLessThan(30_000)
  }

  @Test
  fun `forLogging preserves fatal failure kind and excludes its values`() {
    val error = LinkageError(PRIVATE_VALUE)
    val diagnostic = ExceptionDiagnostics.forLogging(error)
    assertThat(diagnostic).isInstanceOf(Error::class.java)
    assertThat(format(diagnostic)).contains("java.lang.LinkageError")
    assertThat(format(diagnostic)).doesNotContain(PRIVATE_VALUE)
  }

  @Test
  fun `summary bounds approved metadata and excludes unknown error text`() {
    val validation = ValidationFailure.invalidConfiguration(Type.SCHEMA_FIELD)
    assertThat(ExceptionDiagnostics.summary(validation).length).isAtMost(1024)
    assertThat(ExceptionDiagnostics.summary(IllegalStateException(PRIVATE_VALUE)))
      .startsWith("java.lang.IllegalStateException")
  }

  @Test
  fun `forLogging detaches unsafe additions to previously returned diagnostics`() {
    val diagnostic = ExceptionDiagnostics.forLogging(IllegalStateException("Processing failed"))
    val cause = IllegalArgumentException(PRIVATE_VALUE)
    val suppressed = IllegalArgumentException(PRIVATE_VALUE)
    diagnostic.initCause(cause)
    diagnostic.addSuppressed(suppressed)

    val copied = ExceptionDiagnostics.forLogging(diagnostic)
    assertThat(format(copied)).doesNotContain(PRIVATE_VALUE)
    assertThat(copied).isNotSameInstanceAs(diagnostic)
    assertThat(copied.cause).isNotSameInstanceAs(cause)
    assertThat(copied.suppressed.single()).isNotSameInstanceAs(suppressed)
  }

  @Test
  fun `summary retains wrapped validation category field and code location`() {
    val validation =
      ValidationFailure.outOfRange(
        Type.UINT32_AGE,
        BoolValue.getDescriptor().findFieldByName("value"),
      )
    val error = IllegalStateException(PRIVATE_VALUE, validation)
    val output = ExceptionDiagnostics.summary(error)
    assertThat(output).doesNotContain(PRIVATE_VALUE)
    assertThat(output).contains("OUT_OF_RANGE")
    assertThat(output).contains("field=google.protobuf.BoolValue.value")
    assertThat(output).contains("field=google.protobuf.BoolValue.value")
    assertThat(output).contains("ExceptionDiagnosticsTest")
    assertThat(output.length).isAtMost(1024)
  }

  @Test
  fun `forLogging bounds frame text and broad suppressed graphs`() {
    val error =
      IllegalStateException(PRIVATE_VALUE).apply {
        stackTrace =
          arrayOf(
            StackTraceElement(
              "c".repeat(1_000_000),
              "m".repeat(1_000_000),
              "f".repeat(1_000_000),
              1,
            )
          )
        repeat(100) { addSuppressed(IllegalArgumentException(PRIVATE_VALUE)) }
      }
    val output = format(ExceptionDiagnostics.forLogging(error))
    assertThat(output).doesNotContain(PRIVATE_VALUE)
    assertThat(output.length).isLessThan(100_000)
  }

  @Test
  fun `closed type metadata cannot transmit unapproved input text`() {
    val field = BoolValue.getDescriptor().findFieldByName("value")
    for (failure in
      listOf(
        ValidationFailure.incompatibleType(Type.fromName(PRIVATE_VALUE), Type.BYTES_VALUE, field),
        ValidationFailure.incompatibleType(Type.BOOLEAN, Type.fromName(PRIVATE_VALUE), field),
        ValidationFailure.outOfRange(Type.fromName(PRIVATE_VALUE), field),
        ValidationFailure.invalidConfiguration(
          Type.fromName(PRIVATE_VALUE),
          Type.fromName(PRIVATE_VALUE),
          field,
        ),
        ValidationFailure.unknownLookup(field),
        ValidationFailure.unknownEnum(field),
      )) {
      assertThat(format(ExceptionDiagnostics.forLogging(failure))).doesNotContain(PRIVATE_VALUE)
      assertThat(ExceptionDiagnostics.summary(failure)).doesNotContain(PRIVATE_VALUE)
      assertThat(failure.message).doesNotContain(PRIVATE_VALUE)
      assertThat(ExceptionDiagnostics.summary(failure)).contains("google.protobuf.BoolValue.value")
    }
    assertThat(Type.fromName(PRIVATE_VALUE)).isEqualTo(Type.UNKNOWN)
    assertThat(Type.fromName("BOOLEAN\n" + PRIVATE_VALUE)).isEqualTo(Type.UNKNOWN)
  }

  @Test
  fun `stack frames escape control characters without dropping dependency locations`() {
    val frame =
      StackTraceElement("dependency\nClass", "method\r\tname", "file\u0000\u0085\u2028.kt", 123)
    val cause = IOException(PRIVATE_VALUE).apply { stackTrace = arrayOf(frame) }
    val suppressed = IllegalArgumentException(PRIVATE_VALUE).apply { stackTrace = arrayOf(frame) }
    val error =
      IllegalStateException(PRIVATE_VALUE, cause).apply {
        stackTrace = arrayOf(frame)
        addSuppressed(suppressed)
      }
    val copy = ExceptionDiagnostics.forLogging(error)
    for (node in listOf(copy, copy.cause!!, copy.suppressed.single())) {
      val escaped = node.stackTrace.single()
      assertThat(escaped.className).isEqualTo("dependency\\nClass")
      assertThat(escaped.methodName).isEqualTo("method\\r\\tname")
      assertThat(escaped.fileName).isEqualTo("file\\u0000\\u0085\\u2028.kt")
      assertThat(escaped.lineNumber).isEqualTo(123)
    }
    val output = format(copy)
    assertThat(output).contains("dependency\\nClass")
    assertThat(output).contains("Caused by: java.io.IOException")
    assertThat(output).contains("Suppressed: java.lang.IllegalArgumentException")
    assertThat(output).doesNotContain(PRIVATE_VALUE)
    assertThat(ExceptionDiagnostics.summary(error)).doesNotContain(PRIVATE_VALUE)
  }

  @Test
  fun `summary preserves ordinary cause and suppressed types and root location`() {
    val rootFrame = StackTraceElement("root.Application", "process", "Root.kt", 7)
    val nestedFrame = StackTraceElement("nested.Dependency", "read", "Nested.kt", 99)
    val cause = IOException(PRIVATE_VALUE).apply { stackTrace = arrayOf(nestedFrame) }
    val suppressed =
      IllegalArgumentException(PRIVATE_VALUE).apply { stackTrace = arrayOf(nestedFrame) }
    val error =
      IllegalStateException(PRIVATE_VALUE, cause).apply {
        stackTrace = arrayOf(rootFrame)
        addSuppressed(suppressed)
      }
    val output = ExceptionDiagnostics.summary(error)
    assertThat(output).contains("java.lang.IllegalStateException")
    assertThat(output).contains("java.io.IOException")
    assertThat(output).contains("java.lang.IllegalArgumentException")
    assertThat(output).contains("at root.Application.process(Root.kt:7)")
    assertThat(output).doesNotContain("Nested.kt:99")
    assertThat(output).doesNotContain(PRIVATE_VALUE)
    assertThat(output.length).isAtMost(1024)
  }

  @Test
  fun `summary falls back to the first cause frame when root has none`() {
    val cause =
      IOException(PRIVATE_VALUE).apply {
        stackTrace = arrayOf(StackTraceElement("first.Dependency", "read", "First.kt", 1))
        addSuppressed(
          IllegalArgumentException(PRIVATE_VALUE).apply {
            stackTrace = arrayOf(StackTraceElement("last.Dependency", "read", "Last.kt", 2))
          }
        )
      }
    val error = IllegalStateException(PRIVATE_VALUE, cause).apply { stackTrace = emptyArray() }
    assertThat(ExceptionDiagnostics.summary(error)).contains("at first.Dependency.read(First.kt:1)")
    assertThat(ExceptionDiagnostics.summary(error)).doesNotContain("Last.kt:2")
  }

  @Test
  fun `summary reserves space for types across a bounded broad exception graph`() {
    val root = IllegalStateException(PRIVATE_VALUE)
    repeat(20) { root.addSuppressed(IOException(PRIVATE_VALUE)) }
    val output = ExceptionDiagnostics.summary(root)
    assertThat(Regex("IOException").findAll(output).count()).isEqualTo(20)
    assertThat(output.length).isAtMost(1024)
    assertThat(output).doesNotContain(PRIVATE_VALUE)
  }

  private fun format(error: Throwable): String =
    SimpleFormatter().format(LogRecord(Level.SEVERE, "Processing failed").apply { thrown = error })

  companion object {
    private const val PRIVATE_VALUE = "SYNTHETIC_PRIVATE_VALUE"
  }
}
