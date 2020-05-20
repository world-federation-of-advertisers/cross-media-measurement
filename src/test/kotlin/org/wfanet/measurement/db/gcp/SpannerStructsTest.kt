package org.wfanet.measurement.db.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Struct
import java.lang.IllegalStateException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class SpannerStructsTest {
  private val timestamp = Timestamp.now()
  private val str = "abcdefg"
  private val int64 = 405060708090100L
  private val struct = Struct.newBuilder()
    .set("nullString").to(null as String?)
    .set("stringValue").to(str)
    .set("nullInt64").to(null as Long?)
    .set("int64Value").to(int64)
    .set("nullTimestamp").to(null as Timestamp?)
    .set("timestampValue").to(timestamp)
    .build()

  @Test
  fun getNullableString() {
    assertNull(struct.getNullableString("nullString"))
    assertEquals(str, struct.getNullableString("stringValue"))
    assertFailsWith<IllegalStateException> { struct.getNullableString("timestampValue") }
    assertFailsWith<IllegalStateException> { struct.getNullableString("nullInt64") }
  }

  @Test
  fun getNullableInt64() {
    assertNull(struct.getNullableLong("nullInt64"))
    assertEquals(int64, struct.getNullableLong("int64Value"))
    assertFailsWith<IllegalStateException> { struct.getNullableLong("timestampValue") }
    assertFailsWith<IllegalStateException> { struct.getNullableLong("nullString") }
  }

  @Test
  fun getNullableTimestamp() {
    assertNull(struct.getNullableTimestamp("nullTimestamp"))
    assertEquals(timestamp, struct.getNullableTimestamp("timestampValue"))
    assertFailsWith<IllegalStateException> { struct.getNullableTimestamp("int64Value") }
    assertFailsWith<IllegalStateException> { struct.getNullableTimestamp("nullString") }
  }
}
