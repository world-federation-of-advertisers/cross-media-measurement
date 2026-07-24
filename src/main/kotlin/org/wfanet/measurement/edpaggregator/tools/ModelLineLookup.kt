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

package org.wfanet.measurement.edpaggregator.tools

import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest

/**
 * Returns the single [RawImpressionUploadModelLine] under [uploadName] whose `cmms_model_line`
 * equals [cmmsModelLine], or null if none exists.
 *
 * @throws IllegalArgumentException if more than one model line matches.
 */
internal suspend fun RawImpressionUploadModelLineServiceCoroutineStub.findModelLine(
  uploadName: String,
  cmmsModelLine: String,
): RawImpressionUploadModelLine? {
  var pageToken = ""
  var found: RawImpressionUploadModelLine? = null
  do {
    val response =
      listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          parent = uploadName
          filter =
            ListRawImpressionUploadModelLinesRequestKt.filter { this.cmmsModelLine = cmmsModelLine }
          this.pageToken = pageToken
        }
      )
    for (line in response.rawImpressionUploadModelLinesList) {
      // The service filters by cmms_model_line, but guard against a server that ignores it.
      if (line.cmmsModelLine != cmmsModelLine) continue
      require(found == null) {
        "Duplicate RawImpressionUploadModelLine for $cmmsModelLine under $uploadName"
      }
      found = line
    }
    pageToken = response.nextPageToken
  } while (pageToken.isNotEmpty())
  return found
}

/** Returns all [RawImpressionUploadModelLine]s under [uploadName]. */
internal suspend fun RawImpressionUploadModelLineServiceCoroutineStub.listModelLines(
  uploadName: String
): List<RawImpressionUploadModelLine> {
  val modelLines = mutableListOf<RawImpressionUploadModelLine>()
  var pageToken = ""
  do {
    val response =
      listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          parent = uploadName
          this.pageToken = pageToken
        }
      )
    modelLines.addAll(response.rawImpressionUploadModelLinesList)
    pageToken = response.nextPageToken
  } while (pageToken.isNotEmpty())
  return modelLines
}
