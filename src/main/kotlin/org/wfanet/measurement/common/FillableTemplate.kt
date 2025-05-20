/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

import java.nio.CharBuffer

/**
 * Simple string templating engine using [PLACEHOLDER_START] and [PLACEHOLDER_END] to indicate
 * placeholders.
 *
 * The implementation attempts to avoid unnecessary string copies.
 */
class FillableTemplate(template: String) {
  private val template = CharBuffer.wrap(template)

  /**
   * Returns [template] with placeholders replaced with values from [values].
   *
   * If a placeholder key is not in [values], it will be replaced with the empty string.
   */
  fun fill(values: Map<String, String>): String {
    return buildString {
      var index = 0
      while (index < template.length) {
        val placeholderStartIndex: Int = template.indexOf(PLACEHOLDER_START, index)

        if (placeholderStartIndex == -1) {
          append(template.subSequence(index, template.length))
          break
        }

        append(template.subSequence(index, placeholderStartIndex))
        val placeholderEndIndex: Int = template.indexOf(PLACEHOLDER_END, placeholderStartIndex)
        check(placeholderEndIndex != -1) {
          "Placeholder at index $placeholderStartIndex has no end"
        }
        val placeholderKey: String =
          template.substring(placeholderStartIndex + PLACEHOLDER_START.length, placeholderEndIndex)
        val value: String = values.getOrDefault(placeholderKey, "")

        append(value)
        index = placeholderEndIndex + PLACEHOLDER_END.length
      }
    }
  }

  companion object {
    const val PLACEHOLDER_START = "{{"
    const val PLACEHOLDER_END = "}}"
  }
}
