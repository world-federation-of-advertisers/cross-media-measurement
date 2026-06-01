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
// limitations under the \License.

package org.wfanet.virtualpeople.core.labeler

import com.google.protobuf.TextFormat
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.*
import org.wfanet.virtualpeople.common.BranchNodeKt.branch
import org.wfanet.virtualpeople.common.PopulationNodeKt.virtualPersonPool

private const val EVENT_ID_NUMBER = 10000

@RunWith(JUnit4::class)
class LabelerTest {

  @Test
  fun `build from root`() {
    /**
     * A model with
     * - 40% probability to assign virtual person id 10
     * - 60% probability to assign virtual person id 20
     */
    val root = compiledNode {
      name = "TestNode1"
      branchNode = branchNode {
        branches.add(
          branch {
            node = compiledNode {
              populationNode = populationNode {
                pools.add(
                  virtualPersonPool {
                    populationOffset = 10
                    totalPopulation = 1
                  }
                )
                randomSeed = "TestPopulationNodeSeed1"
              }
            }
            chance = 0.4
          }
        )
        branches.add(
          branch {
            node = compiledNode {
              populationNode = populationNode {
                pools.add(
                  virtualPersonPool {
                    populationOffset = 20
                    totalPopulation = 1
                  }
                )
                randomSeed = "TestPopulationNodeSeed2"
              }
            }
            chance = 0.6
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    val labeler = Labeler.build(root)

    val vPidCounts = mutableMapOf<ULong, Int>()
    (0 until EVENT_ID_NUMBER).forEach {
      val input = labelerInput { eventId = eventId { id = it.toString() } }
      val vPid = labeler.label(input).getPeople(0).virtualPersonId.toULong()
      vPidCounts[vPid] = vPidCounts.getOrDefault(vPid, 0) + 1
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * expected count for 10 is 0.4 * EVENT_ID_NUMBER = 4000, the expected count for 20 is 0.6 *
     * EVENT_ID_NUMBER = 6000
     */
    assertEquals(2, vPidCounts.size)
    assertEquals(4061, vPidCounts[10UL])
    assertEquals(5939, vPidCounts[20UL])
  }

  @Test
  fun `build from nodes root with index`() {
    /**
     * All nodes, including root node, have index set.
     *
     * A model with
     * - 40% probability to assign virtual person id 10
     * - 60% probability to assign virtual person id 20
     */
    val node1 = compiledNode {
      name = "TestNode1"
      index = 1
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 2
            chance = 0.4
          }
        )
        branches.add(
          branch {
            nodeIndex = 3
            chance = 0.6
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    val node2 = compiledNode {
      name = "TestNode2"
      index = 2
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed1"
      }
    }
    val node3 = compiledNode {
      name = "TestNode3"
      index = 3
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 20
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed2"
      }
    }
    /** The root node is put at the end. */
    val nodes = listOf(node2, node3, node1)
    val labeler = Labeler.build(nodes)

    val vPidCounts = mutableMapOf<ULong, Int>()
    (0 until EVENT_ID_NUMBER).forEach {
      val input = labelerInput { eventId = eventId { id = it.toString() } }
      val vPid = labeler.label(input).getPeople(0).virtualPersonId.toULong()
      vPidCounts[vPid] = vPidCounts.getOrDefault(vPid, 0) + 1
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * expected count for 10 is 0.4 * EVENT_ID_NUMBER = 4000, the expected count for 20 is 0.6 *
     * EVENT_ID_NUMBER = 6000
     */
    assertEquals(2, vPidCounts.size)
    assertEquals(4061, vPidCounts[10UL])
    assertEquals(5939, vPidCounts[20UL])
  }

  @Test
  fun `build from nodes root without index`() {
    /**
     * All nodes, except root node, have index set.
     *
     * A model with
     * - 40% probability to assign virtual person id 10
     * - 60% probability to assign virtual person id 20
     */
    val node1 = compiledNode {
      name = "TestNode1"
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 2
            chance = 0.4
          }
        )
        branches.add(
          branch {
            nodeIndex = 3
            chance = 0.6
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    val node2 = compiledNode {
      name = "TestNode2"
      index = 2
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed1"
      }
    }
    val node3 = compiledNode {
      name = "TestNode3"
      index = 3
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 20
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed2"
      }
    }
    /** The root node is put at the end. */
    val nodes = listOf(node2, node3, node1)
    val labeler = Labeler.build(nodes)

    val vPidCounts = mutableMapOf<ULong, Int>()
    (0 until EVENT_ID_NUMBER).forEach {
      val input = labelerInput { eventId = eventId { id = it.toString() } }
      val vPid = labeler.label(input).getPeople(0).virtualPersonId.toULong()
      vPidCounts[vPid] = vPidCounts.getOrDefault(vPid, 0) + 1
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * expected count for 10 is 0.4 * EVENT_ID_NUMBER = 4000, the expected count for 20 is 0.6 *
     * EVENT_ID_NUMBER = 6000
     */
    assertEquals(2, vPidCounts.size)
    assertEquals(4061, vPidCounts[10UL])
    assertEquals(5939, vPidCounts[20UL])
  }

  @Test
  fun `build from list with single node`() {
    /**
     * All nodes, except root node, have index set.
     *
     * A model with
     * - 40% probability to assign virtual person id 10
     * - 60% probability to assign virtual person id 20
     */
    val node = compiledNode {
      name = "TestNode1"
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed1"
      }
    }
    val labeler = Labeler.build(listOf(node))

    (0 until EVENT_ID_NUMBER).forEach {
      val input = labelerInput { eventId = eventId { id = it.toString() } }
      val vPid = labeler.label(input).getPeople(0).virtualPersonId.toULong()
      assertEquals(10UL, vPid)
    }
  }

  @Test
  fun `build from nodes node after root should throw`() {
    val node1 = compiledNode {
      name = "TestNode1"
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 2
            chance = 0.4
          }
        )
        branches.add(
          branch {
            nodeIndex = 3
            chance = 0.6
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    val node2 = compiledNode {
      name = "TestNode2"
      index = 2
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed1"
      }
    }
    val node3 = compiledNode {
      name = "TestNode3"
      index = 3
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 20
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed2"
      }
    }
    /** The root node is not at the end. */
    val nodes = listOf(node2, node1, node3)

    val exception = assertFailsWith<IllegalStateException> { Labeler.build(nodes) }
    assertTrue(
      exception.message!!.contains("The ModelNode object of the child node index 3 is not provided")
    )
  }

  @Test
  fun `build from nodes multiple roots should throw`() {
    /**
     * ```
     * 2 trees exist:
     * 1 -> 3, 4
     * 2 -> 5
     * ```
     */
    val root1 = compiledNode {
      name = "TestNode1"
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 3
            chance = 0.4
          }
        )
        branches.add(
          branch {
            nodeIndex = 4
            chance = 0.6
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    val root2 = compiledNode {
      name = "TestNode2"
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 5
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    val node3 = compiledNode {
      name = "TestNode3"
      index = 3
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed1"
      }
    }
    val node4 = compiledNode {
      name = "TestNode4"
      index = 4
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 20
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed2"
      }
    }
    val node5 = compiledNode {
      name = "TestNode5"
      index = 5
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 20
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed3"
      }
    }

    val nodes = listOf(node3, node4, node5, root1, root2)
    val exception = assertFailsWith<IllegalStateException> { Labeler.build(nodes) }
    assertTrue(exception.message!!.contains("No node is allowed after the root node"))
  }

  @Test
  fun `build from nodes no root should throw`() {
    /** Nodes 1 and 2 reference each other as child node. No root node can be recognized. */
    val node1 = compiledNode {
      name = "TestNode1"
      index = 1
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 2
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed1"
      }
    }
    val node2 = compiledNode {
      name = "TestNode2"
      index = 2
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 1
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed2"
      }
    }

    val exception = assertFailsWith<IllegalStateException> { Labeler.build(listOf(node1, node2)) }
    assertTrue(
      exception.message!!.contains("The ModelNode object of the child node index 2 is not provided")
    )
  }

  @Test
  fun `build from nodes no node for index should throw`() {
    /** Nodes 1 and 2 reference each other as child node. No root node can be recognized. */
    val node1 = compiledNode {
      name = "TestNode1"
      index = 1
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 2
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed1"
      }
    }

    val exception = assertFailsWith<IllegalStateException> { Labeler.build(listOf(node1)) }
    assertTrue(
      exception.message!!.contains("The ModelNode object of the child node index 2 is not provided")
    )
  }

  @Test
  fun `build from nodes duplicated indexes should throw`() {
    val node1 = compiledNode {
      name = "TestNode1"
      index = 1
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 2
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    val node2 = compiledNode {
      name = "TestNode2"
      index = 2
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed1"
      }
    }
    val node3 = compiledNode {
      name = "TestNode3"
      index = 2
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 20
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed2"
      }
    }

    val exception =
      assertFailsWith<IllegalStateException> { Labeler.build(listOf(node2, node3, node1)) }
    assertTrue(exception.message!!.contains("Duplicated indexes"))
  }

  @Test
  fun `build from nodes multiple parents should throw`() {
    val node1 = compiledNode {
      name = "TestNode1"
      index = 1
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 2
            chance = 0.5
          }
        )
        branches.add(
          branch {
            nodeIndex = 3
            chance = 10.5
          }
        )
        randomSeed = "TestBranchNodeSeed1"
      }
    }
    val node2 = compiledNode {
      name = "TestNode2"
      index = 2
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 3
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed2"
      }
    }
    val node3 = compiledNode {
      name = "TestNode3"
      index = 3
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 20
            totalPopulation = 1
          }
        )
        randomSeed = "TestPopulationNodeSeed2"
      }
    }

    val exception =
      assertFailsWith<IllegalStateException> { Labeler.build(listOf(node3, node2, node1)) }
    assertTrue(
      exception.message!!.contains("The ModelNode object of the child node index 3 is not provided")
    )
  }

  /** A minimal single-pool model used by the debug-trace tests below. */
  private fun buildSinglePoolLabeler(): Labeler {
    val root = compiledNode {
      name = "TestNode1"
      branchNode = branchNode {
        branches.add(
          branch {
            node = compiledNode {
              populationNode = populationNode {
                pools.add(
                  virtualPersonPool {
                    populationOffset = 10
                    totalPopulation = 1
                  }
                )
                randomSeed = "TestPopulationNodeSeed1"
              }
            }
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    return Labeler.build(root)
  }

  @Test
  fun `serialized debug trace is empty when enable_debug_trace is unset`() {
    val labeler = buildSinglePoolLabeler()
    // Default: enableDebugTrace is unset (== false).
    val output = labeler.label(labelerInput { eventId = eventId { id = "evt_42" } })
    assertEquals("", output.serializedDebugTrace)
  }

  @Test
  fun `serialized debug trace is empty when enable_debug_trace is explicitly false`() {
    val labeler = buildSinglePoolLabeler()
    val output =
      labeler.label(
        labelerInput {
          eventId = eventId { id = "evt_42" }
          enableDebugTrace = false
        }
      )
    assertEquals("", output.serializedDebugTrace)
  }

  @Test
  fun `serialized debug trace is populated when enable_debug_trace is true`() {
    val labeler = buildSinglePoolLabeler()
    val output =
      labeler.label(
        labelerInput {
          eventId = eventId { id = "evt_42" }
          enableDebugTrace = true
        }
      )
    assertTrue(
      output.serializedDebugTrace.isNotEmpty(),
      "expected non-empty debug trace when enable_debug_trace=true",
    )
    // The trace is the TextFormat dump of the LabelerEvent — the input event id we set
    // should be visible verbatim in the trace.
    assertTrue(
      output.serializedDebugTrace.contains("evt_42"),
      "expected debug trace to contain input event id; got:\n${output.serializedDebugTrace}",
    )
    // And the trace should be re-parseable as a LabelerEvent text format, round-tripping
    // back to the original input event id.
    val rebuilt = LabelerEvent.newBuilder()
    TextFormat.merge(output.serializedDebugTrace, rebuilt)
    assertEquals("evt_42", rebuilt.build().labelerInput.eventId.id)
  }
}
