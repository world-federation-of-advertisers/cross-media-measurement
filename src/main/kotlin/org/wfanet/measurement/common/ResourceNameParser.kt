// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common

private val LITERAL_SEGMENT_PATTERN = Regex("[a-z][a-zA-Z0-9]*")
private val VARIABLE_NAME_PATTERN = Regex("[a-z][a-z0-9_]*")
private const val ID_SUFFIX = "_id"
private const val SEGMENT_SEPARATOR = "/"

/**
 * A parser for API resource names that follow the specified resource name pattern.
 *
 * See [AIP-122: Resource names](https://google.aip.dev/122)
 *
 * @param pattern a resource name pattern as described in
 * [AIP-123: Resource types](https://google.aip.dev/123), consisting of segments separated by a
 * slash (`/`). Each segment can either be a literal string, or an ID variable name wrapped in
 * braces.
 */
class ResourceNameParser(pattern: String) {
  private val segments: List<Segment> =
    pattern.split(SEGMENT_SEPARATOR).map { part ->
      val content = part.removeSurrounding("{", "}")
      if (part.length > content.length) {
        require(!content.endsWith(ID_SUFFIX)) { "Variable name $content ends in $ID_SUFFIX" }
        require(VARIABLE_NAME_PATTERN.matches(content)) { "Invalid variable name $content" }
        VariableSegment(content)
      } else {
        require(LITERAL_SEGMENT_PATTERN.matches(content)) { "Invalid literal segment $content" }
        LiteralSegment(content)
      }
    }

  /**
   * Parses ID segments from a [relativeResourceName] that matches this parser's pattern.
   *
   * @returns a [Map] of segment variable name to value, or `null` if [relativeResourceName] does
   * not match this parser's pattern.
   */
  fun parseIdSegments(relativeResourceName: String): Map<String, String>? {
    val parts: List<String> = relativeResourceName.split(SEGMENT_SEPARATOR)
    if (parts.size != segments.size) {
      return null
    }

    return parts
      .zip(segments)
      .mapNotNull { (part: String, segment: Segment) ->
        when (segment) {
          is LiteralSegment -> {
            if (part != segment.value) {
              return null
            }
            null
          }
          is VariableSegment -> segment.name to part
        }
      }
      .toMap()
  }

  /**
   * Assembles a relative resource name for this [ResourceNameParser]'s pattern using [idSegments].
   */
  fun assembleName(idSegments: Map<String, String>): String {
    return segments.joinToString(SEGMENT_SEPARATOR) { segment ->
      when (segment) {
        is LiteralSegment -> segment.value
        is VariableSegment ->
          idSegments.getOrDefault(segment.name, "").apply {
            require(isNotEmpty()) { "Missing value for ${segment.name}" }
          }
      }
    }
  }
}

private sealed class Segment

private data class LiteralSegment(val value: String) : Segment()

private data class VariableSegment(val name: String) : Segment()
