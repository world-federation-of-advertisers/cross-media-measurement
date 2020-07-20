package org.wfanet.measurement.common

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalUnit

enum class DurationFormat {
  /**
   * Human-readable format consisting of a sequence of decimal numbers followed by a unit suffix.
   *
   * The valid suffixes are:
   *   * `h` - hours
   *   * `m` - minutes
   *   * `s` - seconds
   *   * `ms` - milliseconds
   *   * `ns` - nanoseconds
   */
  HUMAN_READABLE,

  /*
   * ISO-8601 duration format.
   */
  ISO_8601
}

/** Parses the string to a [Duration]. */
fun String.toDuration(format: DurationFormat = DurationFormat.HUMAN_READABLE): Duration {
  return when (format) {
    DurationFormat.HUMAN_READABLE -> parseHumanReadableDuration(this)
    DurationFormat.ISO_8601 -> Duration.parse(this)
  }
}

/**
 * Parses a duration string in [HUMAN_READABLE][DurationFormat.HUMAN_READABLE] format to a
 * [Duration].
 */
private fun parseHumanReadableDuration(durationString: String): Duration {
  var duration = Duration.ZERO
  val results = Regex("(\\d+)(ns|ms|s|m|h)").findAll(durationString)
  for (result in results) {
    val values = result.groupValues
    check(values.size == 3) { "Bad duration string" }
    val amount = values[1].toLong()
    val unit: TemporalUnit = when (values[2]) {
      "ns" -> ChronoUnit.NANOS
      "ms" -> ChronoUnit.MILLIS
      "s" -> ChronoUnit.SECONDS
      "m" -> ChronoUnit.MINUTES
      "h" -> ChronoUnit.HOURS
      else -> throw IllegalArgumentException("Unsupported duration unit ${values[2]}")
    }
    duration = duration.plus(amount, unit)
  }

  return duration
}
