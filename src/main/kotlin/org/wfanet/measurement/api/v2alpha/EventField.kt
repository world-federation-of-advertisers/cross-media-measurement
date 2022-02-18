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

package org.wfanet.measurement.api.v2alpha

import com.google.protobuf.Descriptors
import java.lang.IllegalArgumentException

data class EventField(val descriptor: Descriptors.Descriptor) {
  init {
    if (!descriptor.options.hasExtension(EventAnnotations.eventField)) {
      throw IllegalArgumentException("Descriptor does not have EventField annotation")
    }
  }
  val displayName: String by lazy {
    descriptor.options.getExtension(EventAnnotations.eventField).displayName
  }

  val description: String by lazy {
    descriptor.options.getExtension(EventAnnotations.eventField).description
  }

  val fieldNames: List<String> by lazy { descriptor.fields.map { field -> field.name } }
}
