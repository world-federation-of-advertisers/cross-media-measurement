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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class RequestIdsTest {

  @Test
  fun `request ids are stable across calls`() {
    assertThat(RequestIds.forRawImpressionUpload(DONE_BLOB, 1L))
      .isEqualTo(RequestIds.forRawImpressionUpload(DONE_BLOB, 1L))
    assertThat(RequestIds.forRawImpressionUploadFile(UPLOAD, FILE_URI))
      .isEqualTo(RequestIds.forRawImpressionUploadFile(UPLOAD, FILE_URI))
    assertThat(RequestIds.forRawImpressionUploadModelLine(UPLOAD, MODEL_LINE))
      .isEqualTo(RequestIds.forRawImpressionUploadModelLine(UPLOAD, MODEL_LINE))
  }

  @Test
  fun `upload request id differs by generation`() {
    assertThat(RequestIds.forRawImpressionUpload(DONE_BLOB, 1L))
      .isNotEqualTo(RequestIds.forRawImpressionUpload(DONE_BLOB, 2L))
  }

  @Test
  fun `file request id includes upload context so the same blob in two uploads differs`() {
    // Regression for the cross-upload data-integrity collision: the same fileBlobUri landing in two
    // different uploads must produce distinct request IDs (else the second create returns the
    // first upload's cached file).
    assertThat(
        RequestIds.forRawImpressionUploadFile("$DATA_PROVIDER/rawImpressionUploads/a", FILE_URI)
      )
      .isNotEqualTo(
        RequestIds.forRawImpressionUploadFile("$DATA_PROVIDER/rawImpressionUploads/b", FILE_URI)
      )
  }

  @Test
  fun `type prefixes prevent cross-resource collisions for identical args`() {
    // file vs model-line both take (uploadName, string); the type prefix must keep them distinct.
    assertThat(RequestIds.forRawImpressionUploadFile(UPLOAD, SHARED))
      .isNotEqualTo(RequestIds.forRawImpressionUploadModelLine(UPLOAD, SHARED))
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp123"
    private const val UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/upload-1"
    private const val DONE_BLOB = "gs://bucket/edp/2026-01-01/done"
    private const val FILE_URI = "gs://bucket/edp/2026-01-01/impressions_001"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val SHARED = "shared-value"
  }
}
