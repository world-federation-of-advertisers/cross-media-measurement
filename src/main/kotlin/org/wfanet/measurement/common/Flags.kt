/**
 * Utility library for command-line flags.
 *
 * Only flags of the format "--flag_name=flag_value" are supported. This does not support positional
 * arguments or spaces between the flag name and value.
 */
package org.wfanet.measurement.common

import java.time.Duration

/**
 * Singleton owner of all [Flag] objects.
 *
 * Typical usage:
 *
 * ```
 * fun main(args: Array<String>) = runBlocking {
 *   Args.parse(args.asIterable())
 *   ... rest of main ...
 * }
 * ```
 */
object Flags {
  internal val flags = mutableMapOf<String, Flag<*>>()
  private val PATTERN = Regex("^--?([a-z_-]+)=(.+)$")

  /**
   * Parses a list of command-line arguments to set flags.
   *
   * Each element of [args] should have the format "--flag_name=flag_value".
   *
   * @param[args] a list of args
   */
  fun parse(args: Iterable<String>) {
    args.forEach(this::parse)
  }

  /**
   * Parses a command-line argument in the form "--flag_name=flag_value".
   *
   * @param[arg] the argument to parse
   */
  fun parse(arg: String) {
    // Group 0 is the full match, 1 is the flag name, 2 is the value.
    val groups = PATTERN.matchEntire(arg)?.groupValues ?: listOf()
    when (groups.size) {
      3 -> setFlag(groups[1], groups[2])
      else -> throw FlagError("Invalid arg: $arg")
    }
  }

  /**
   * Gets a Flag.
   *
   * @param[name] the global name of the flag
   */
  operator fun get(name: String): Flag<*>? = flags[name]

  /**
   * Clears all flags.
   *
   * References to existing flags are still valid, but flags created before [clear] is called will
   * not be set via [parse] and cannot be retried by [get].
   */
  fun clear() {
    flags.clear()
  }

  private fun setFlag(name: String, value: String) {
    when (val maybeFlag = flags[name]) {
      null -> throw FlagError("Trying to set unknown flag: $name")
      else -> maybeFlag.parseFrom(value)
    }
  }
}

/**
 * Wraps a [value] of type [T], along with a string parser for this type.
 *
 * @param[T] the wrapped type
 * @property[name] the unique global name for this value.
 * @property[default] the initial value of the flag.
 * @property[parser] a function to translate a string to a [T]
 */
class Flag<T>(val name: String, val default: T, private val parser: (String) -> T) {
  var value: T = default

  init {
    if (Flags.flags.containsKey(name)) {
      throw IllegalArgumentException("Flag $name already declared")
    }
    Flags.flags[name] = this
  }

  fun parseFrom(s: String) {
    value = parser(s)
  }
}

fun intFlag(name: String, default: Int): Flag<Int> = Flag(name, default, Integer::decode)

fun longFlag(name: String, default: Long): Flag<Long> = Flag(name, default, java.lang.Long::decode)

fun doubleFlag(name: String, default: Double): Flag<Double> = Flag(name, default, String::toDouble)

fun booleanFlag(name: String, default: Boolean): Flag<Boolean> =
  Flag(name, default) {
    when (it) {
      "1" -> true
      "0" -> false
      else -> it.toBoolean()
    }
  }

fun stringFlag(name: String, default: String): Flag<String> = Flag(name, default, String::toString)

internal fun String.parseDuration(): Duration {
  val groups = Regex("^(\\d+)(ms|s|m|h|d)$").matchEntire(this)?.groupValues ?: listOf()
  if (groups.size == 3) {
    val amount = groups[1].toLong()
    when (groups[2].toLowerCase()) {
      "ms" -> return Duration.ofMillis(amount)
      "s" -> return Duration.ofSeconds(amount)
      "m" -> return Duration.ofMinutes(amount)
      "h" -> return Duration.ofHours(amount)
      "d" -> return Duration.ofDays(amount)
    }
  }
  return Duration.parse(this)
}

fun durationFlag(name: String, default: Duration): Flag<Duration> =
  Flag(name, default, String::parseDuration)

class FlagError(message: String) : Exception(message)
