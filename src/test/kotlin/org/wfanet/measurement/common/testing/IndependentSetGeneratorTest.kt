package org.wfanet.measurement.common.testing

import com.google.common.truth.Truth.assertThat
import kotlin.random.Random
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.lang.IllegalArgumentException
import kotlin.test.assertFailsWith

@RunWith(JUnit4::class)
class IndependentSetGeneratorTest {
  private val DEFAULT_SEED = 1L

  @Test
  fun set_with_same_seed_first_value_match_succeeds() {
    val random = Random(DEFAULT_SEED)
    val universeSize: Long = 1000000
    val setGenerator = generateIndependentSets(
      universeSize,
      1,
      1,
      random)

    // Generate new Random object with the same seed to generate same numbers.
    val random2 = Random(DEFAULT_SEED)
    val expected = List(1) { random2.nextLong(universeSize) }
    assertThat(setGenerator.next()).isEqualTo(expected)
  }

  @Test
  fun set_contains_0_to_n_values_succeeds() {
    val setGenerator = generateIndependentSets(
      100,
      100,
      1)
    assertThat(setGenerator.next()).containsExactlyElementsIn(0L..99L)
  }

  @Test
  fun set_size_succeeds() {
    val random = Random(DEFAULT_SEED)
    val setGenerator = generateIndependentSets(
      99,
      33,
      10,
      random)
    var size = 0
    while (setGenerator.hasNext()) {
      size++
      val set = setGenerator.next()
      assertThat(set.size).isEqualTo(33)
    }
    assertThat(size).isEqualTo(10)
  }

  @Test
  fun set_contains_no_duplicates_succeeds() {
    var random = Random(DEFAULT_SEED)
    val setGenerator = generateIndependentSets(
      1000,
      100,
      10,
      random)
    while (setGenerator.hasNext()) {
      val set = setGenerator.next()
      assertThat(set).containsNoDuplicates();
    }
  }

  @Test
  fun set_next_without_elements_throws() {
    val setGenerator = generateIndependentSets(
      100,
      100,
      1)
    setGenerator.next()

    assertFailsWith(NoSuchElementException::class, "SetGenerator has no elements left") {
      setGenerator.next()
    }
  }

  @Test
  fun set_set_size_out_of_bounds_throws() {
    assertFailsWith(IllegalArgumentException::class, "SetSize larger than UniverseSize") {
      generateIndependentSets(
        10,
        100,
        1).next()
    }
  }

  @Test
  fun set_num_sets_out_of_bounds_throws() {
    assertFailsWith(IllegalArgumentException::class, "Number of sets less than 1") {
      generateIndependentSets(
        1,
        1,
        -10).next()
    }
  }

  @Test
  fun set_universe_size_out_of_bounds_throws() {
    assertFailsWith(IllegalArgumentException::class, "Universe size less than 1") {
      generateIndependentSets(
        -10,
        1,
        1).next()
    }
  }
}
