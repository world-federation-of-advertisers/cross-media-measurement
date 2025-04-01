// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.teeapps.utils

import org.junit.Test
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.common.truth.Truth.assertThat
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person.Gender as PersonGender
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person.AgeGroup
import org.wfanet.virtualpeople.common.demoInfo
import com.google.protobuf.Any
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent

class TeeApplicationUtilsTest {
  @Test
  fun `convertMessage should correctly map fields from source to target message`() {
    val originalMessage = demoBucket {
      gender = Gender.GENDER_MALE
      age = ageRange {
        minAge = 18
        maxAge = 34
      }
    }

    val fieldValueMap = mapOf(
      "gender.GENDER_MALE" to "gender.MALE",
      "gender.GENDER_FEMALE" to "gender.FEMALE",
      "age.min_age.18" to "age_group.YEARS_18_TO_34",
      "age.max_age.34" to "age_group.YEARS_18_TO_34",
    )

    val newMessageDescriptor = Person.getDescriptor()

    val newMessage = TeeApplicationUtils.convertMessage(
      originalMessage,
      newMessageDescriptor,
      fieldValueMap
    )

    val expectedMessage = person {
      gender = PersonGender.MALE
      ageGroup = AgeGroup.YEARS_18_TO_34
    }
    val typeRegistry = TypeRegistry.newBuilder().add(Person.getDescriptor()).build()
    assertThat(newMessage.descriptorForType).isEqualTo(expectedMessage.descriptorForType)
    assertThat(Any.pack(newMessage)).unpackingAnyUsing(typeRegistry, ExtensionRegistry.getEmptyRegistry()).isEqualTo(Any.pack(expectedMessage))
    assertThat(newMessage.toByteString() == expectedMessage.toByteString())
  }

  @Test
  fun `convertMessage should correctly map fields from source to target message with nested original messages`() {
    val originalMessage = demoInfo {
      demoBucket = demoBucket {
        gender = Gender.GENDER_MALE
        age = ageRange {
          minAge = 18
          maxAge = 34
        }
      }
    }

    val fieldValueMap = mapOf(
      "demo_bucket.gender.GENDER_MALE" to "gender.MALE",
      "demo_bucket.gender.GENDER_FEMALE" to "gender.FEMALE",
      "demo_bucket.age.min_age.18" to "age_group.YEARS_18_TO_34",
      "demo_bucket.age.max_age.34" to "age_group.YEARS_18_TO_34",
    )

    val newMessageDescriptor = Person.getDescriptor()

    val newMessage = TeeApplicationUtils.convertMessage(
      originalMessage,
      newMessageDescriptor,
      fieldValueMap
    )

    val expectedMessage = person {
      gender = PersonGender.MALE
      ageGroup = AgeGroup.YEARS_18_TO_34
    }

    val typeRegistry = TypeRegistry.newBuilder().add(Person.getDescriptor()).build()
    assertThat(newMessage.descriptorForType).isEqualTo(expectedMessage.descriptorForType)
    assertThat(Any.pack(newMessage)).unpackingAnyUsing(typeRegistry, ExtensionRegistry.getEmptyRegistry()).isEqualTo(Any.pack(expectedMessage))
    assertThat(newMessage.toByteString() == expectedMessage.toByteString())
  }

  @Test
  fun `convertMessage should correctly map fields from source to target message with nested original and target messages`() {
    val originalMessage = demoInfo {
      demoBucket = demoBucket {
        gender = Gender.GENDER_MALE
        age = ageRange {
          minAge = 18
          maxAge = 34
        }
      }
    }

    val fieldValueMap = mapOf(
      "demo_bucket.gender.GENDER_MALE" to "person.gender.MALE",
      "demo_bucket.gender.GENDER_FEMALE" to "person.gender.FEMALE",
      "demo_bucket.age.min_age.18" to "person.age_group.YEARS_18_TO_34",
      "demo_bucket.age.max_age.34" to "person.age_group.YEARS_18_TO_34",
    )

    val newMessageDescriptor = TestEvent.getDescriptor()

    val newMessage = TeeApplicationUtils.convertMessage(
            originalMessage,
            newMessageDescriptor,
            fieldValueMap
    )

    val expectedMessage = testEvent {
      person = person {
        gender = PersonGender.MALE
        ageGroup = AgeGroup.YEARS_18_TO_34
      }
    }

    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()
    assertThat(newMessage.descriptorForType).isEqualTo(expectedMessage.descriptorForType)
    assertThat(Any.pack(newMessage)).unpackingAnyUsing(typeRegistry, ExtensionRegistry.getEmptyRegistry()).isEqualTo(Any.pack(expectedMessage))
    assertThat(newMessage.toByteString() == expectedMessage.toByteString())
  }
}
