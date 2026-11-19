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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.BoolValue
import com.google.protobuf.ByteString
import com.google.protobuf.BytesValue
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.DoubleValue
import com.google.protobuf.Int32Value
import com.google.protobuf.StringValue
import com.google.protobuf.UInt32Value
import com.google.protobuf.Value
import com.google.protobuf.timestamp
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.SimpleFormatter
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.measurement.common.ExceptionDiagnostics
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetValue

class ProtoRowProjectorTest {
  @Test
  fun `resolvePath excludes rejected configuration paths and segments`() {
    for (path in
      listOf(
        PRIVATE_VALUE,
        "struct_value.$PRIVATE_VALUE",
        "bool_value.$PRIVATE_VALUE",
        "struct_value.fields.$PRIVATE_VALUE",
      )) {
      val failure =
        assertFailsWith<IllegalArgumentException> {
          ProtoRowProjector.resolvePath(Value.getDescriptor(), path)
        }
      assertThat(failure.message).doesNotContain(PRIVATE_VALUE)
      assertThat(failure.message).contains("INVALID_CONFIGURATION")
      assertThat(ExceptionDiagnostics.summary(failure)).doesNotContain(PRIVATE_VALUE)
      val output =
        SimpleFormatter()
          .format(LogRecord(Level.SEVERE, "Invalid mapping").apply { thrown = failure })
      assertThat(output).doesNotContain(PRIVATE_VALUE)
    }
  }

  @Test
  fun `resolvePath preserves valid scalar and allowed message paths`() {
    assertThat(ProtoRowProjector.resolvePath(Value.getDescriptor(), "bool_value"))
      .containsExactly(Value.getDescriptor().findFieldByName("bool_value"))
    assertThat(ProtoRowProjector.resolvePath(Value.getDescriptor(), "struct_value", true))
      .containsExactly(Value.getDescriptor().findFieldByName("struct_value"))
    assertFailsWith<IllegalArgumentException> {
      ProtoRowProjector.resolvePath(Value.getDescriptor(), "struct_value")
    }
    assertFailsWith<IllegalArgumentException> {
      ProtoRowProjector.resolvePath(Value.getDescriptor(), "bool_value", true)
    }
  }

  @Test
  fun `convert rejects enum names without exposing the value`() {
    assertPrivateRejection(parquetValue { stringValue = PRIVATE_VALUE }, ENUM, PRIVATE_VALUE)
  }

  @Test
  fun `convert rejects unknown and overflowing enum numbers without exposing the number`() {
    assertPrivateRejection(parquetValue { int32Value = 123456789 }, ENUM, "123456789")
    assertPrivateRejection(parquetValue { uint32Value = -1 }, ENUM, "4294967295")
  }

  @Test
  fun `convert rejects signed and unsigned ranges without exposing the number`() {
    assertPrivateRejection(parquetValue { int64Value = 9876543210L }, SIGNED, "9876543210")
    assertPrivateRejection(parquetValue { int64Value = -9876543210L }, UNSIGNED, "-9876543210")
    assertPrivateRejection(parquetValue { int64Value = 9876543210L }, UNSIGNED, "9876543210")
  }

  @Test
  fun `convert excludes bytes and strings from incompatible scalar errors`() {
    val bytes = parquetValue { bytesValue = ByteString.copyFromUtf8(PRIVATE_VALUE) }
    for (field in listOf(SIGNED, UNSIGNED, ENUM, STRING, BOOLEAN, DOUBLE)) {
      assertPrivateRejection(bytes, field, PRIVATE_VALUE)
    }
    val string = parquetValue { stringValue = PRIVATE_VALUE }
    for (field in listOf(SIGNED, UNSIGNED, BYTES, BOOLEAN, DOUBLE)) {
      assertPrivateRejection(string, field, PRIVATE_VALUE)
    }
  }

  @Test
  fun `convert excludes a rejected timestamp numeric value`() {
    val value = parquetValue { timestampValue = timestamp { seconds = 1_800_000_000L } }
    assertPrivateRejection(value, SIGNED, "1800000000000000")
  }

  @Test
  fun `convert preserves valid scalar values and unsigned bit representation`() {
    val bytes = ByteString.copyFromUtf8(PRIVATE_VALUE)
    assertThat(ProtoRowProjector.convert(parquetValue { bytesValue = bytes }, BYTES, "source"))
      .isEqualTo(bytes)
    assertThat(
        ProtoRowProjector.convert(parquetValue { stringValue = PRIVATE_VALUE }, STRING, "source")
      )
      .isEqualTo(PRIVATE_VALUE)
    assertThat(ProtoRowProjector.convert(parquetValue { boolValue = true }, BOOLEAN, "source"))
      .isEqualTo(true)
    assertThat(ProtoRowProjector.convert(parquetValue { int64Value = 12 }, DOUBLE, "source"))
      .isEqualTo(12.0)
    val unsigned =
      ProtoRowProjector.convert(parquetValue { uint32Value = -1 }, UNSIGNED, "source") as Int
    assertThat(Integer.toUnsignedLong(unsigned)).isEqualTo(4294967295L)
    assertThat(ProtoRowProjector.convert(parquetValue { int32Value = 0 }, ENUM, "source"))
      .isEqualTo(ENUM.enumType.findValueByNumber(0))
  }

  private fun assertPrivateRejection(value: ParquetValue, field: FieldDescriptor, marker: String) {
    val failure =
      assertFailsWith<IllegalArgumentException> {
        ProtoRowProjector.convert(value, field, "source")
      }
    assertThat(failure.message).doesNotContain(marker)
    assertThat(failure.message).contains("column=source")
    assertThat(ExceptionDiagnostics.summary(failure)).doesNotContain(marker)
    val output =
      SimpleFormatter()
        .format(
          LogRecord(Level.SEVERE, "Invalid impression").apply {
            thrown = ExceptionDiagnostics.forLogging(failure)
          }
        )
    assertThat(output).doesNotContain(marker)
    assertThat(output).contains("ProtoRowProjector")
  }

  companion object {
    private const val PRIVATE_VALUE = "SYNTHETIC_PRIVATE_VALUE"
    private val SIGNED = Int32Value.getDescriptor().findFieldByName("value")
    private val UNSIGNED = UInt32Value.getDescriptor().findFieldByName("value")
    private val STRING = StringValue.getDescriptor().findFieldByName("value")
    private val BYTES = BytesValue.getDescriptor().findFieldByName("value")
    private val BOOLEAN = BoolValue.getDescriptor().findFieldByName("value")
    private val DOUBLE = DoubleValue.getDescriptor().findFieldByName("value")
    private val ENUM = Value.getDescriptor().findFieldByName("null_value")
  }
}
