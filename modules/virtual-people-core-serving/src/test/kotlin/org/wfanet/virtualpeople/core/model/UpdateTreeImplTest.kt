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
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.*
import org.wfanet.virtualpeople.common.BranchNodeKt.attributesUpdater
import org.wfanet.virtualpeople.common.BranchNodeKt.attributesUpdaters
import org.wfanet.virtualpeople.common.BranchNodeKt.branch

private const val SEED_NUMBER = 10000

@RunWith(JUnit4::class)
class UpdateTreeImplTest {

  @Test
  fun `empty tree`() {
    val config = attributesUpdater {
      updateTree = updateTree { root = compiledNode { stopNode = stopNode {} } }
    }
    val updater = AttributesUpdaterInterface.build(config)
    val event = labelerEvent {}.toBuilder()
    updater.update(event)

    /** Nothing happened. */
    ProtoTruth.assertThat(event.build()).isEqualTo(labelerEvent {})
  }

  @Test
  fun `single branch`() {
    /**
     * An UpdateTree that updates person_country_code from "COUNTRY_1" to "UPDATED_COUNTRY_1". If
     * person_country_code is not "COUNTRY_1", throw an error.
     */
    val config = attributesUpdater {
      updateTree = updateTree {
        root = compiledNode {
          branchNode = branchNode {
            branches.add(
              branch {
                node = compiledNode { stopNode = stopNode {} }
                condition = fieldFilterProto { op = FieldFilterProto.Op.TRUE }
              }
            )
            updates = attributesUpdaters {
              updates.add(
                attributesUpdater {
                  updateMatrix = updateMatrix {
                    columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
                    rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
                    probabilities.add(1.0f)
                  }
                }
              )
            }
          }
        }
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    val event1 = labelerEvent {}.toBuilder()
    val exception1 = assertFailsWith<IllegalStateException> { updater.update(event1) }
    assertTrue(exception1.message!!.contains("No column matching for event"))

    val event2 = labelerEvent { personCountryCode = "COUNTRY_2" }.toBuilder()
    val exception2 = assertFailsWith<IllegalStateException> { updater.update(event2) }
    assertTrue(exception2.message!!.contains("No column matching for event"))

    val event3 = labelerEvent { personCountryCode = "COUNTRY_1" }.toBuilder()
    updater.update(event3)
    assertEquals("UPDATED_COUNTRY_1", event3.personCountryCode)
  }

  @Test
  fun `two branches`() {
    /**
     * An UpdateTree that updates person_country_code from "COUNTRY_1" to "UPDATED_COUNTRY_1", and
     * from "COUNTRY_2" to "UPDATED_COUNTRY_2". If person_country_code is not "COUNTRY_1" or
     * "COUNTRY_2", return error status.
     */
    val config = attributesUpdater {
      updateTree = updateTree {
        root = compiledNode {
          branchNode = branchNode {
            branches.add(
              branch {
                node = compiledNode {
                  branchNode = branchNode {
                    branches.add(
                      branch {
                        node = compiledNode { stopNode = stopNode {} }
                        condition = fieldFilterProto { op = FieldFilterProto.Op.TRUE }
                      }
                    )
                    updates = attributesUpdaters {
                      updates.add(
                        attributesUpdater {
                          updateMatrix = updateMatrix {
                            columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
                            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
                            probabilities.add(1.0f)
                          }
                        }
                      )
                    }
                  }
                }
                condition = fieldFilterProto {
                  name = "person_country_code"
                  op = FieldFilterProto.Op.EQUAL
                  value = "COUNTRY_1"
                }
              }
            )
            branches.add(
              branch {
                node = compiledNode {
                  branchNode = branchNode {
                    branches.add(
                      branch {
                        node = compiledNode { stopNode = stopNode {} }
                        condition = fieldFilterProto { op = FieldFilterProto.Op.TRUE }
                      }
                    )
                    updates = attributesUpdaters {
                      updates.add(
                        attributesUpdater {
                          updateMatrix = updateMatrix {
                            columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
                            rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
                            probabilities.add(1.0f)
                          }
                        }
                      )
                    }
                  }
                }
                condition = fieldFilterProto {
                  name = "person_country_code"
                  op = FieldFilterProto.Op.EQUAL
                  value = "COUNTRY_2"
                }
              }
            )
          }
        }
      }
    }
    val updater = AttributesUpdaterInterface.build(config)

    val event1 = labelerEvent {}.toBuilder()
    val exception1 = assertFailsWith<IllegalStateException> { updater.update(event1) }
    assertTrue(exception1.message!!.contains("No condition matches the input event"))

    val event2 = labelerEvent { personCountryCode = "COUNTRY_3" }.toBuilder()
    val exception2 = assertFailsWith<IllegalStateException> { updater.update(event2) }
    assertTrue(exception2.message!!.contains("No condition matches the input event"))

    val event3 = labelerEvent { personCountryCode = "COUNTRY_1" }.toBuilder()
    updater.update(event3)
    assertEquals("UPDATED_COUNTRY_1", event3.personCountryCode)

    val event4 = labelerEvent { personCountryCode = "COUNTRY_2" }.toBuilder()
    updater.update(event4)
    assertEquals("UPDATED_COUNTRY_2", event4.personCountryCode)
  }

  @Test
  fun `two branches with indexes`() {
    /**
     * An UpdateTree that updates person_country_code from "COUNTRY_1" to "UPDATED_COUNTRY_1", and
     * from "COUNTRY_2" to "UPDATED_COUNTRY_2". If person_country_code is not "COUNTRY_1" or
     * "COUNTRY_2", return error status.
     */
    val config = attributesUpdater {
      updateTree = updateTree {
        root = compiledNode {
          branchNode = branchNode {
            branches.add(
              branch {
                nodeIndex = 1
                condition = fieldFilterProto {
                  name = "person_country_code"
                  op = FieldFilterProto.Op.EQUAL
                  value = "COUNTRY_1"
                }
              }
            )
            branches.add(
              branch {
                nodeIndex = 2
                condition = fieldFilterProto {
                  name = "person_country_code"
                  op = FieldFilterProto.Op.EQUAL
                  value = "COUNTRY_2"
                }
              }
            )
          }
        }
      }
    }

    val nodeConfig1 = compiledNode {
      branchNode = branchNode {
        branches.add(
          branch {
            node = compiledNode { stopNode = stopNode {} }
            condition = fieldFilterProto { op = FieldFilterProto.Op.TRUE }
          }
        )
        updates = attributesUpdaters {
          updates.add(
            attributesUpdater {
              updateMatrix = updateMatrix {
                columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
                rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_1" })
                probabilities.add(1.0f)
              }
            }
          )
        }
      }
    }
    val nodeConfig2 = compiledNode {
      branchNode = branchNode {
        branches.add(
          branch {
            node = compiledNode { stopNode = stopNode {} }
            condition = fieldFilterProto { op = FieldFilterProto.Op.TRUE }
          }
        )
        updates = attributesUpdaters {
          updates.add(
            attributesUpdater {
              updateMatrix = updateMatrix {
                columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
                rows.add(labelerEvent { personCountryCode = "UPDATED_COUNTRY_2" })
                probabilities.add(1.0f)
              }
            }
          )
        }
      }
    }
    val compiledNode1 = ModelNode.build(nodeConfig1)
    val compiledNode2 = ModelNode.build(nodeConfig2)
    val nodeRefs = mutableMapOf(Pair(1, compiledNode1), Pair(2, compiledNode2))

    val updater = AttributesUpdaterInterface.build(config, nodeRefs)

    val event1 = labelerEvent {}.toBuilder()
    val exception1 = assertFailsWith<IllegalStateException> { updater.update(event1) }
    assertTrue(exception1.message!!.contains("No condition matches the input event"))

    val event2 = labelerEvent { personCountryCode = "COUNTRY_3" }.toBuilder()
    val exception2 = assertFailsWith<IllegalStateException> { updater.update(event2) }
    assertTrue(exception2.message!!.contains("No condition matches the input event"))

    val event3 = labelerEvent { personCountryCode = "COUNTRY_1" }.toBuilder()
    updater.update(event3)
    assertEquals("UPDATED_COUNTRY_1", event3.personCountryCode)

    val event4 = labelerEvent { personCountryCode = "COUNTRY_2" }.toBuilder()
    updater.update(event4)
    assertEquals("UPDATED_COUNTRY_2", event4.personCountryCode)
  }
}
