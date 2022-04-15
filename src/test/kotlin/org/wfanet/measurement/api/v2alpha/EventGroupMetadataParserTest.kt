// Copyright 2020 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.Descriptors.FileDescriptor
import com.google.protobuf.Duration
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EventGroupKt.metadata
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage2
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage2
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorKt.details
import org.wfanet.measurement.internal.kingdom.eventGroupMetadataDescriptor

private const val API_VERSION = "v2alpha"
val TEST_MESSAGE = testMetadataMessage2 {
  name = "Alice"
  message = testMetadataMessage {
    name = "Bob"
    value = 1
    duration = Duration.newBuilder().setSeconds(30).build()
  }
}

val TEST_MESSAGE_2 = testMetadataMessage2 {
  name = "Joe"
  message = testMetadataMessage {
    name = "Susan"
    value = 2
    duration = Duration.newBuilder().setSeconds(60).build()
  }
}

@RunWith(JUnit4::class)
class EventGroupMetadataParserTest {
  @Test
  fun `packed 'Any' message returns DynamicMessage`() {
    val eventGroupMetadataDescriptor1 = eventGroupMetadataDescriptor {
      externalDataProviderId = 1L
      externalEventGroupMetadataDescriptorId = 2L
      details = details {
        apiVersion = API_VERSION
        descriptorSet = TEST_MESSAGE.getDescriptorForType().getFileDescriptorSet()
      }
    }
    val eventGroupMetadataDescriptor2 = eventGroupMetadataDescriptor {
      details = details {
        apiVersion = API_VERSION
        descriptorSet = TEST_MESSAGE_2.getDescriptorForType().getFileDescriptorSet()
      }
    }
    val parser =
      EventGroupMetadataParser(listOf(eventGroupMetadataDescriptor1, eventGroupMetadataDescriptor2))

    val eventGroupMetadata1 = metadata {
      eventGroupMetadataDescriptor = "dataProviders/123/eventGroupMetadataDescriptors/abc"
      metadata = Any.pack(TEST_MESSAGE)
    }
    val eventGroupMetadata2 = metadata {
      eventGroupMetadataDescriptor = "dataProviders/234/eventGroupMetadataDescriptors/def"
      metadata = Any.pack(TEST_MESSAGE_2)
    }
    val result = parser.convertToDynamicMessage(eventGroupMetadata1)
    val result2 = parser.convertToDynamicMessage(eventGroupMetadata2)

    assertThat(TestMetadataMessage2.parseFrom(result!!.toByteString())).isEqualTo(TEST_MESSAGE)
    assertThat(TestMetadataMessage2.parseFrom(result2!!.toByteString())).isEqualTo(TEST_MESSAGE_2)
  }

  fun Descriptor.getFileDescriptorSet(): FileDescriptorSet {
    val fileDescriptors = mutableSetOf<FileDescriptor>()
    val toVisit = mutableListOf<FileDescriptor>(file)
    while (toVisit.isNotEmpty()) {
      val fileDescriptor = toVisit.removeLast()
      if (!fileDescriptors.contains(fileDescriptor)) {
        fileDescriptors.add(fileDescriptor)
        fileDescriptor.dependencies.forEach {
          if (!fileDescriptors.contains(it)) {
            toVisit.add(it)
          }
        }
      }
    }
    return FileDescriptorSet.newBuilder().addAllFile(fileDescriptors.map { it.toProto() }).build()
  }
}
