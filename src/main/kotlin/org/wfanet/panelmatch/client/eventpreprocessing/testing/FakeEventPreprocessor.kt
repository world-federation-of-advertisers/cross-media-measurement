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

package org.wfanet.panelmatch.client.eventpreprocessing.testing

import org.wfanet.panelmatch.client.eventpreprocessing.EventPreprocessor
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessEventsRequest
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessEventsResponse
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessEventsResponseKt.processedEvent
import org.wfanet.panelmatch.client.eventpreprocessing.preprocessEventsResponse

/** A fake [EventPreprocessor] implementation. */
class FakeEventPreprocessor : EventPreprocessor {

  override fun preprocess(request: PreprocessEventsRequest): PreprocessEventsResponse {
    val suffix = with(request) { identifierHashPepper.concat(hkdfPepper).concat(cryptoKey) }
    return preprocessEventsResponse {
      for (events in request.unprocessedEventsList) {
        processedEvents += processedEvent {
          encryptedId = events.id.toStringUtf8().toLong() + 1
          encryptedData = events.data.concat(suffix)
        }
      }
    }
  }
}
