// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.vidlabeler

import java.security.MessageDigest
import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.ModelLineKey

/** Deterministic key derivation for VID-labeled impression outputs. */
object LabeledImpressionsBlobKeys {
  /** Returns the absolute output URI for one raw-impression input and model line. */
  fun forInputUri(
    prefix: String,
    inputBlobUri: String,
    modelLine: String,
    eventDate: LocalDate,
  ): String = "${prefix.trimEnd('/')}/${forInput(inputBlobUri, modelLine, eventDate)}"

  /** Returns the absolute done-marker URI for one upload, model line, and event date. */
  fun forDoneUri(
    prefix: String,
    rawImpressionUpload: String,
    modelLine: String,
    eventDate: LocalDate,
  ): String = "${prefix.trimEnd('/')}/${forDone(rawImpressionUpload, modelLine, eventDate)}"

  /** Returns the relative done-marker key for one upload, model line, and event date. */
  fun forDone(rawImpressionUpload: String, modelLine: String, eventDate: LocalDate): String {
    require(rawImpressionUpload.isNotEmpty()) { "rawImpressionUpload must not be empty" }
    return "${forDateDirectory(modelLine, eventDate)}/done-${sha256(rawImpressionUpload)}"
  }

  /** Returns the relative output key for one raw-impression input and model line. */
  fun forInput(inputBlobUri: String, modelLine: String, eventDate: LocalDate): String {
    val dateDirectory = forDateDirectory(modelLine, eventDate)
    return "$dateDirectory/${sha256("$inputBlobUri|$modelLine")}"
  }

  private fun sha256(value: String): String =
    MessageDigest.getInstance("SHA-256").digest(value.toByteArray(Charsets.UTF_8)).joinToString(
      ""
    ) {
      "%02x".format(it)
    }

  private fun forDateDirectory(modelLine: String, eventDate: LocalDate): String {
    val modelLineId =
      requireNotNull(ModelLineKey.fromName(modelLine)) {
          "model line is not a valid ModelLine resource name: $modelLine"
        }
        .modelLineId
    return "model-line/$modelLineId/$eventDate"
  }
}
