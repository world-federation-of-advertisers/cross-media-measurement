package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFails
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runBlockingTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@OptIn(ExperimentalCoroutinesApi::class)  // For runBlockingTest
@RunWith(JUnit4::class)
class BytesTest {
  @Test
  fun `ByteString asBufferedFlow with non-full last part`() = runBlockingTest {
    val flow = ByteString.copyFromUtf8("Hello World").asBufferedFlow(3)
    assertThat(flow.map { it.toStringUtf8() }.toList())
      .containsExactly("Hel", "lo ", "Wor", "ld")
      .inOrder()
  }

  @Test
  fun `ByteString asBufferedFlow on empty ByteString`() = runBlockingTest {
    val flow = ByteString.copyFromUtf8("").asBufferedFlow(3)
    assertThat(flow.toList()).isEmpty()
  }

  @Test
  fun `ByteString asBufferedFlow with invalid buffer size`() = runBlockingTest {
    assertFails {
      ByteString.copyFromUtf8("this should throw").asBufferedFlow(0).toList()
    }
  }
}