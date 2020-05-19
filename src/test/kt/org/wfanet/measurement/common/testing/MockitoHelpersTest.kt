package org.wfanet.measurement.common.testing

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito

class Foo

/**
 * Helper to assert that its nullable [Foo] input is null.
 */
fun consumeFoo(f: Foo?) {
  assertThat(f).isNull()
}

@RunWith(JUnit4::class)
class MockitoHelpersTest {
  /**
   * Demonstrate that the original [Mockito.any] fails in Kotlin.
   */
  @Test
  fun `mockito original any fails`() {
    assertFails {
      val foo: Foo = Mockito.any<Foo>()
      consumeFoo(foo)
    }
  }

  /**
   * Demonstrate that the new [any] succeeds in Kotlin.
   */
  @Test
  fun `custom any succeeds`() {
    // Strangely enough, even though the type here is Foo, not Foo?, it is actually null.
    val foo: Foo = any<Foo>()
    consumeFoo(foo)
  }
}
