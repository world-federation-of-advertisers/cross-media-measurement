package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import java.time.Duration
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class FlagsTest {
  @Before
  fun setup() {
    Flags.clear()
  }

  @Test
  fun `integer flag`() {
    val flag = intFlag("foo", 100)
    assertThat(flag.value).isEqualTo(100)

    flag.value = 101
    assertThat(flag.value).isEqualTo(101)

    flag.parseFrom("102")
    assertThat(flag.value).isEqualTo(102)

    Flags.parse(listOf("--foo=103"))
    assertThat(flag.value).isEqualTo(103)

    Flags.parse("-foo=104")
    assertThat(flag.value).isEqualTo(104)

    Flags.parse("--foo=105")
    assertThat(flag.value).isEqualTo(105)
  }

  @Test
  fun `hex parsing`() {
    val flag = intFlag("foo", 100)

    flag.parseFrom("0xabc")
    assertThat(flag.value).isEqualTo(0xabc)

    flag.parseFrom("0xDEf")
    assertThat(flag.value).isEqualTo(0xdef)
  }

  @Test
  fun `boolean flag`() {
    val flag = booleanFlag("foo", true)
    assertThat(flag.value).isEqualTo(true)

    flag.parseFrom("false")
    assertThat(flag.value).isEqualTo(false)

    flag.parseFrom("true")
    assertThat(flag.value).isEqualTo(true)

    flag.parseFrom("0")
    assertThat(flag.value).isEqualTo(false)

    flag.parseFrom("1")
    assertThat(flag.value).isEqualTo(true)

    Flags.parse("-foo=false")
    assertThat(flag.value).isEqualTo(false)
  }

  @Test
  fun `duration flag`() {
    val flag = durationFlag("foo", default = Duration.ZERO)

    flag.parseFrom("10s")
    assertThat(flag.value).isEqualTo(Duration.ofSeconds(10))

    flag.parseFrom("100m")
    assertThat(flag.value).isEqualTo(Duration.ofMinutes(100))

    flag.parseFrom("4h")
    assertThat(flag.value).isEqualTo(Duration.ofHours(4))

    flag.parseFrom("2d")
    assertThat(flag.value).isEqualTo(Duration.ofDays(2))
  }

  @Test
  fun `flag lookup`() {
    val flag = stringFlag("foo", "abc")
    assertThat(Flags["foo"]).isSameInstanceAs(flag)
  }

  @Test
  fun `multiple flags`() {
    stringFlag("foo", "abc")
    intFlag("bar", 123)
    longFlag("baz", 0)
    booleanFlag("qux", false)

    Flags.parse(
      listOf(
        "--foo=def",
        "-bar=456",
        "-baz=0xabcdef",
        "--qux=1"
      )
    )

    assertThat(Flags["foo"]!!.value).isEqualTo("def")
    assertThat(Flags["bar"]!!.value).isEqualTo(456)
    assertThat(Flags["baz"]!!.value).isEqualTo(0xabcdef)
    assertThat(Flags["qux"]!!.value).isEqualTo(true)
  }

  @Test
  fun `duplicate flag names`() {
    stringFlag("foo", "abc")

    assertFailsWith<IllegalArgumentException> {
      stringFlag("foo", "def")
    }
  }

  @Test
  fun `unknown flag`() {
    assertFailsWith<FlagError> { Flags.parse(listOf("--foo=10")) }
  }

  @Test
  fun `bad flag formats`() {
    stringFlag("foo", "abc")
    assertFailsWith<FlagError> { Flags.parse("--foo") }
    assertFailsWith<FlagError> { Flags.parse("=foo") }
    assertFailsWith<FlagError> { Flags.parse("no flag name") }
    assertFailsWith<FlagError> { Flags.parse("bar--foo=baz") }
  }

  @Test
  fun `bad flag value`() {
    val f = intFlag("foo", 0)
    assertFailsWith<IllegalArgumentException> { f.parseFrom("not a valid number") }
  }
}
