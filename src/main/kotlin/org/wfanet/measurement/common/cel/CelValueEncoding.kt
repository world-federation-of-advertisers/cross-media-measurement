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

package org.wfanet.measurement.common.cel

import java.util.Locale

/**
 * Encodes [this] as a CEL double-quoted string literal, escaping backslash, double-quote, and
 * control characters (including 0x00-0x1F and DEL) per the CEL string-literal grammar
 * (https://github.com/google/cel-spec/blob/master/doc/langdef.md#string-literals).
 *
 * Printable non-ASCII characters are passed through as UTF-8 -- CEL accepts them as-is in string
 * literals -- rather than being re-escaped to `\uXXXX`. This keeps generated CEL readable in server
 * logs.
 */
fun String.toCelStringLiteral(): String {
  val out = StringBuilder(length + 2)
  out.append('"')
  for (c in this) {
    when {
      c == '\\' -> out.append("\\\\")
      c == '"' -> out.append("\\\"")
      c == '\n' -> out.append("\\n")
      c == '\r' -> out.append("\\r")
      c == '\t' -> out.append("\\t")
      c == '\b' -> out.append("\\b")
      c == '' -> out.append("\\f")
      c == '' -> out.append("\\u007f")
      c.code < 0x20 -> out.append("\\u").append(String.format(Locale.ROOT, "%04x", c.code))
      else -> out.append(c)
    }
  }
  out.append('"')
  return out.toString()
}

/**
 * Encodes [this] as a CEL numeric literal. Rejects `NaN` and infinities: CEL renders these as bare
 * identifiers (`NaN`, `Infinity`, `-Infinity`) rather than numeric literals, so Kotlin's
 * `Float.toString()` output would produce a filter that fails to compile. Semantically, `NaN ==
 * NaN` is `false` in IEEE 754 -- a comparison against `NaN` is meaningless -- so rejecting upstream
 * is the right shape even if CEL supported the literal.
 *
 * Note: `Float.toString()` yields the shortest decimal that round-trips to `Float`; CEL parses
 * numeric literals as `double`, so bit-exact equality against a proto `float` field is not
 * guaranteed. Continuous-float IMPRESSION_QUALIFICATION fields are discouraged (see
 * `testing_only.proto`); prefer enums or bool buckets.
 */
fun Float.toCelNumericLiteral(): String {
  require(isFinite()) {
    "Cannot encode non-finite float ${this} as a CEL numeric literal; NaN and infinities are " +
      "not meaningful in filter comparisons and have no valid CEL representation."
  }
  return toString()
}
