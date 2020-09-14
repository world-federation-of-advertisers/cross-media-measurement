// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.storage.testing

import com.google.common.truth.FailureMetadata
import com.google.common.truth.Subject
import com.google.common.truth.Truth.assertAbout
import com.google.protobuf.ByteString
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.size
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.measurement.storage.read

class BlobSubject private constructor(failureMetadata: FailureMetadata, subject: Blob) :
  Subject(failureMetadata, subject) {

  private val actual = subject

  fun hasSize(size: Int) {
    check("size").that(actual.size).isEqualTo(size)
  }

  suspend fun contentEqualTo(content: ByteString) {
    val actualContent = actual.read().flatten()

    // First check size to avoid outputting potentially large number of bytes.
    check("read().toByteString().size").that(actualContent.size).isEqualTo(content.size)

    check("read().toByteString()").that(actualContent).isEqualTo(content)
  }

  companion object {
    fun assertThat(actual: Blob): BlobSubject = assertAbout(blobs()).that(actual)

    fun blobs(): (failureMetadata: FailureMetadata, subject: Blob) -> BlobSubject = ::BlobSubject
  }
}
