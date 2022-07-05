// Copyright 2022 The Cross-Media Measurement Authors
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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EventGroupKt.metadata
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.age
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.duration
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt.name
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestParentMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testParentMetadataMessage

private val TEST_MESSAGE = testMetadataMessage {
  name = name { value = "Bob" }
  age = age { value = 10 }
  duration = duration { value = 2 }
}

private val TEST_PARENT_MESSAGE = testParentMetadataMessage {
  name = "Joe"
  message = testMetadataMessage {
    name = name { value = "Susan" }
    age = age { value = 20 }
    duration = duration { value = 60 }
  }
}

private const val NAME = "dataProviders/123/eventGroupMetadataDescriptors/abc"

private val EVENT_GROUP_METADATA = metadata {
  eventGroupMetadataDescriptor = NAME
  metadata = Any.pack(TEST_MESSAGE)
}
private val PARENT_EVENT_GROUP_METADATA = metadata {
  eventGroupMetadataDescriptor = NAME
  metadata = Any.pack(TEST_PARENT_MESSAGE)
}

@RunWith(JUnit4::class)
class EventGroupMetadataParserTest {
  @Test
  fun `parser converting eventGroupMetadataDescriptor returns expected DynamicMessage`() {
    val eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
      name = NAME
      descriptorSet = TEST_MESSAGE.getDescriptorForType().getFileDescriptorSet()
    }
    val parentEventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
      name = NAME
      descriptorSet = TEST_PARENT_MESSAGE.getDescriptorForType().getFileDescriptorSet()
    }
    val parser =
      EventGroupMetadataParser(
        listOf(eventGroupMetadataDescriptor, parentEventGroupMetadataDescriptor)
      )

    val result = parser.convertToDynamicMessage(EVENT_GROUP_METADATA)
    val result2 = parser.convertToDynamicMessage(PARENT_EVENT_GROUP_METADATA)

    assertThat(TestMetadataMessage.parseFrom(result!!.toByteString())).isEqualTo(TEST_MESSAGE)
    assertThat(TestParentMetadataMessage.parseFrom(result2!!.toByteString()))
      .isEqualTo(TEST_PARENT_MESSAGE)
  }

  @Test
  fun `parser unable to parse eventGroupMetadataDescriptor throws exception`() {
    val eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
      name = NAME
      descriptorSet = TEST_MESSAGE.getDescriptorForType().getFileDescriptorSet()
    }
    val parser = EventGroupMetadataParser(listOf(eventGroupMetadataDescriptor))

    assertThat(parser.convertToDynamicMessage(PARENT_EVENT_GROUP_METADATA)).isNull()
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
