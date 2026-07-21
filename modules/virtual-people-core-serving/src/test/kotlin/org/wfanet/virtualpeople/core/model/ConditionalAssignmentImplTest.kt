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

package org.wfanet.virtualpeople.core.model

import com.google.common.truth.extensions.proto.ProtoTruth
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.*
import org.wfanet.virtualpeople.common.BranchNodeKt.attributesUpdater
import org.wfanet.virtualpeople.common.ConditionalAssignmentKt.assignment
import org.wfanet.virtualpeople.common.FieldFilterProto.Op

@RunWith(JUnit4::class)
class ConditionalAssignmentImplTest {

  @Test
  fun `no condition should throw`() {
    val config = attributesUpdater {
      conditionalAssignment = conditionalAssignment {
        assignments.add(
          assignment {
            sourceField = "acting_demo.gender"
            targetField = "corrected_demo.gender"
          }
        )
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("Condition is not set"))
  }

  @Test
  fun `invalid condition should throw`() {
    val config = attributesUpdater {
      conditionalAssignment = conditionalAssignment {
        condition = fieldFilterProto { op = Op.EQUAL }
        assignments.add(
          assignment {
            sourceField = "acting_demo.gender"
            targetField = "corrected_demo.gender"
          }
        )
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("Name must be set"))
  }

  @Test
  fun `empty assignment should throw`() {
    val config = attributesUpdater {
      conditionalAssignment = conditionalAssignment {
        condition = fieldFilterProto {
          op = Op.HAS
          name = "acting_demo.gender"
        }
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("No assignments"))
  }

  @Test
  fun `no source field should throw`() {
    val config = attributesUpdater {
      conditionalAssignment = conditionalAssignment {
        condition = fieldFilterProto {
          op = Op.HAS
          name = "acting_demo.gender"
        }
        assignments.add(assignment { targetField = "corrected_demo.gender" })
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("must have source_field"))
  }

  @Test
  fun `no target field should throw`() {
    val config = attributesUpdater {
      conditionalAssignment = conditionalAssignment {
        condition = fieldFilterProto {
          op = Op.HAS
          name = "acting_demo.gender"
        }
        assignments.add(assignment { sourceField = "acting_demo.gender" })
      }
    }
    val error = assertFailsWith<IllegalStateException> { AttributesUpdaterInterface.build(config) }
    assertTrue(error.message!!.contains("must have target_field"))
  }

  @Test
  fun `single assignement success`() {
    val config = attributesUpdater {
      conditionalAssignment = conditionalAssignment {
        condition = fieldFilterProto {
          op = Op.HAS
          name = "acting_demo.gender"
        }
        assignments.add(
          assignment {
            sourceField = "acting_demo.gender"
            targetField = "corrected_demo.gender"
          }
        )
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    /** Matches the condition. Updates the event. */
    val eventBuilder1 =
      labelerEvent { actingDemo = demoBucket { gender = Gender.GENDER_FEMALE } }.toBuilder()
    val expectedEvent1 = labelerEvent {
      actingDemo = demoBucket { gender = Gender.GENDER_FEMALE }
      correctedDemo = demoBucket { gender = Gender.GENDER_FEMALE }
    }
    updater.update(eventBuilder1)
    ProtoTruth.assertThat(expectedEvent1).isEqualTo(eventBuilder1.build())

    /** Does not match the condition. Does nothing. */
    val eventBuilder2 = labelerEvent {}.toBuilder()
    val expectedEvent2 = labelerEvent {}
    updater.update(eventBuilder1)
    ProtoTruth.assertThat(expectedEvent2).isEqualTo(eventBuilder2.build())
  }

  @Test
  fun `multiple assignements success`() {
    val config = attributesUpdater {
      conditionalAssignment = conditionalAssignment {
        condition = fieldFilterProto {
          op = Op.AND
          subFilters.add(
            fieldFilterProto {
              op = Op.HAS
              name = "acting_demo.gender"
            }
          )
          subFilters.add(
            fieldFilterProto {
              op = Op.HAS
              name = "acting_demo.age.min_age"
            }
          )
          subFilters.add(
            fieldFilterProto {
              op = Op.HAS
              name = "acting_demo.age.max_age"
            }
          )
        }
        assignments.add(
          assignment {
            sourceField = "acting_demo.gender"
            targetField = "corrected_demo.gender"
          }
        )
        assignments.add(
          assignment {
            sourceField = "acting_demo.age.min_age"
            targetField = "corrected_demo.age.min_age"
          }
        )
        assignments.add(
          assignment {
            sourceField = "acting_demo.age.max_age"
            targetField = "corrected_demo.age.max_age"
          }
        )
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    /** Matches the condition. Updates the event. */
    val eventBuilder1 =
      labelerEvent {
          actingDemo = demoBucket {
            gender = Gender.GENDER_FEMALE
            age = ageRange {
              minAge = 25
              maxAge = 29
            }
          }
        }
        .toBuilder()
    val expectedEvent1 = labelerEvent {
      actingDemo = demoBucket {
        gender = Gender.GENDER_FEMALE
        age = ageRange {
          minAge = 25
          maxAge = 29
        }
      }
      correctedDemo = demoBucket {
        gender = Gender.GENDER_FEMALE
        age = ageRange {
          minAge = 25
          maxAge = 29
        }
      }
    }
    updater.update(eventBuilder1)
    ProtoTruth.assertThat(expectedEvent1).isEqualTo(eventBuilder1.build())

    /** Does not match the condition as acting_demo.gender is not set. Does nothing. */
    val eventBuilder2 =
      labelerEvent {
          actingDemo = demoBucket {
            age = ageRange {
              minAge = 25
              maxAge = 29
            }
          }
        }
        .toBuilder()
    val expectedEvent2 = labelerEvent {
      actingDemo = demoBucket {
        age = ageRange {
          minAge = 25
          maxAge = 29
        }
      }
    }
    updater.update(eventBuilder2)
    ProtoTruth.assertThat(expectedEvent2).isEqualTo(eventBuilder2.build())

    /** Does not match the condition as acting_demo.age.max_age is not set. Does nothing. */
    val eventBuilder3 =
      labelerEvent {
          actingDemo = demoBucket {
            gender = Gender.GENDER_FEMALE
            age = ageRange { minAge = 25 }
          }
        }
        .toBuilder()
    val expectedEvent3 = labelerEvent {
      actingDemo = demoBucket {
        gender = Gender.GENDER_FEMALE
        age = ageRange { minAge = 25 }
      }
    }
    updater.update(eventBuilder3)
    ProtoTruth.assertThat(expectedEvent3).isEqualTo(eventBuilder3.build())

    /** Does not match the condition as acting_demo.age.min_age is not set. Does nothing. */
    val eventBuilder4 =
      labelerEvent {
          actingDemo = demoBucket {
            gender = Gender.GENDER_FEMALE
            age = ageRange { maxAge = 29 }
          }
        }
        .toBuilder()
    val expectedEvent4 = labelerEvent {
      actingDemo = demoBucket {
        gender = Gender.GENDER_FEMALE
        age = ageRange { maxAge = 29 }
      }
    }
    updater.update(eventBuilder4)
    ProtoTruth.assertThat(expectedEvent4).isEqualTo(eventBuilder4.build())
  }

  @Test
  fun `skip assignments for unset field`() {
    val config = attributesUpdater {
      conditionalAssignment = conditionalAssignment {
        condition = fieldFilterProto {
          op = Op.HAS
          name = "acting_demo.age"
        }
        assignments.add(
          assignment {
            sourceField = "acting_demo.age.min_age"
            targetField = "corrected_demo.age.min_age"
          }
        )
        assignments.add(
          assignment {
            sourceField = "acting_demo.age.max_age"
            targetField = "corrected_demo.age.max_age"
          }
        )
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    /** Skip the conditional assignment on min_age as the source field is not set. */
    val eventBuilder1 =
      labelerEvent { actingDemo = demoBucket { age = ageRange { maxAge = 29 } } }.toBuilder()
    val expectedEvent1 = labelerEvent {
      actingDemo = demoBucket { age = ageRange { maxAge = 29 } }
      correctedDemo = demoBucket { age = ageRange { maxAge = 29 } }
    }
    updater.update(eventBuilder1)
    ProtoTruth.assertThat(expectedEvent1).isEqualTo(eventBuilder1.build())
  }
}
