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

package org.wfanet.panelmatch.client.eventpreprocessing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.PreprocessEventsRequest

@RunWith(JUnit4::class)
class EncryptEventsTest {

  @Test
  fun test() {
    val request =
      PreprocessEventsRequest.newBuilder()
        .apply {
          cryptoKey = ByteString.copyFromUtf8("cryptokey")
          pepper = ByteString.copyFromUtf8("pepper")
          addUnprocessedEventsBuilder().apply {
            id = ByteString.copyFromUtf8("identifier")
            data = ByteString.copyFromUtf8("eventdata")
          }
        }
        .build()
    val encryptEvents = EncryptEvents()

    assertThat(encryptEvents.apply(request)).isNotNull()
  }
}
