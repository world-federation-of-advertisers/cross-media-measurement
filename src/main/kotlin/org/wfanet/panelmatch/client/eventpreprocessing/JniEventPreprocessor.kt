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

import org.wfanet.panelmatch.common.loadLibraryFromResource
import org.wfanet.panelmatch.common.wrapJniException
import org.wfanet.panelmatch.protocol.eventpreprocessing.EventPreprocessingSwig

/** A [EventPreprocessor] implementation using the JNI [EventPreprocessor]. */
class JniEventPreprocessor : EventPreprocessor {

  override fun preprocess(request: PreprocessEventsRequest): PreprocessEventsResponse {
    return wrapJniException {
      PreprocessEventsResponse.parseFrom(
        EventPreprocessingSwig.preprocessEventsWrapper(request.toByteArray())
      )
    }
  }

  companion object {
    init {
      loadLibraryFromResource(
        libraryName = "preprocess_events",
        resourcePathPrefix = "/main/swig/wfanet/panelmatch/client/eventpreprocessing",
      )
    }
  }
}
