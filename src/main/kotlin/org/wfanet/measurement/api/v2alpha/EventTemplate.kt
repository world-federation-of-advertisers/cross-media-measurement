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
import com.google.protobuf.Descriptors.FieldDescriptor.Type
import java.lang.IllegalArgumentException

data class EventTemplate(val descriptor: Descriptors.Descriptor) {
  init {
    if (!descriptor.options.hasExtension(EventAnnotations.eventTemplate)) {
      throw IllegalArgumentException("Descriptor does not have EventTemplate annotation")
    }
  }
  val displayName: String by lazy {
    descriptor.options.getExtension(EventAnnotations.eventTemplate).displayName
  }

  val description: String by lazy {
    descriptor.options.getExtension(EventAnnotations.eventTemplate).description
  }

  val eventFields: List<EventField> by lazy {
    descriptor.fields
      .filter { field ->
        field.type == Type.MESSAGE &&
          field.messageType.options.hasExtension(EventAnnotations.eventField)
      }
      .map { field -> EventField(field.messageType) }
  }
}
