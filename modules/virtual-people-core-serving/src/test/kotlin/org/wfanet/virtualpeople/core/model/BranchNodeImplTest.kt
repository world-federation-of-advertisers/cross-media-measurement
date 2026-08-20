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
import org.wfanet.virtualpeople.common.FieldFilterProto.Op
import org.wfanet.virtualpeople.common.PopulationNodeKt.virtualPersonPool

private const val FINGERPRINT_NUMBER = 10000L

@RunWith(JUnit4::class)
class BranchNodeImplTest {

  @Test
  fun `apply branch with node by chance`() {
    /**
     * The branch node has 2 branches. One branch is selected with 40% chance, which is a population
     * node always assigns virtual person id 10. The other branch is selected with 60% chance, which
     * is a population node always assigns virtual person id 20.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
                randomSeed = "TestPopulationNodeSeed1"
              }
            }
            chance = 0.6
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    val node = ModelNode.build(config)

    val idCounts = mutableMapOf<ULong, Int>()
    (0 until FINGERPRINT_NUMBER).forEach {
      val input = labelerEvent { actingFingerprint = it }.toBuilder()
      node.apply(input)
      val virtualPersonId = input.getVirtualPersonActivities(0).virtualPersonId.toULong()
      idCounts[virtualPersonId] = idCounts.getOrDefault(virtualPersonId, 0) + 1
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * expected count for 10 is 0.4 * FINGERPRINT_NUMBER = 4000, the expected count for 20 is 0.6 *
     * FINGERPRINT_NUMBER=6000
     */
    assertEquals(2, idCounts.size)
    assertEquals(4014, idCounts[10UL])
    assertEquals(5986, idCounts[20UL])
  }

  @Test
  fun `apply branch with node index by chance`() {
    /**
     * The branch node has 2 branches. One branch is selected with 40% chance, which is a population
     * node always assigns virtual person id 10. The other branch is selected with 60% chance, which
     * is a population node always assigns virtual person id 20.
     */
    val branchNodeConfig = compiledNode {
      name = "TestBranchNode"
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

    val populationNodeConfig1 = compiledNode {
      name = "TestPopulationNode1"
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
    val populationNodeConfig2 = compiledNode {
      name = "TestPopulationNode2"
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

    val nodeRefs =
      mutableMapOf(
        Pair(2, ModelNode.build(populationNodeConfig1)),
        Pair(3, ModelNode.build(populationNodeConfig2))
      )

    val node = ModelNode.build(branchNodeConfig, nodeRefs)

    val idCounts = mutableMapOf<ULong, Int>()
    (0 until FINGERPRINT_NUMBER).forEach {
      val input = labelerEvent { actingFingerprint = it }.toBuilder()
      node.apply(input)
      val virtualPersonId = input.getVirtualPersonActivities(0).virtualPersonId.toULong()
      idCounts[virtualPersonId] = idCounts.getOrDefault(virtualPersonId, 0) + 1
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * expected count for 10 is 0.4*FINGERPRINT_NUMBER = 4000, the expected count for 20 is
     * 0.6*FINGERPRINT_NUMBER=6000
     */
    assertEquals(2, idCounts.size)
    assertEquals(4014, idCounts[10UL])
    assertEquals(5986, idCounts[20UL])
  }

  @Test
  fun `apply branch with node index by chance not normalized should throw`() {
    /** The branch node has 2 branches, but the chances are not normalized. */
    val branchNodeConfig = compiledNode {
      name = "TestBranchNode"
      index = 1
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 2
            chance = 0.8
          }
        )
        branches.add(
          branch {
            nodeIndex = 3
            chance = 0.12
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }

    val populationNodeConfig1 = compiledNode {
      name = "TestPopulationNode1"
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
    val populationNodeConfig2 = compiledNode {
      name = "TestPopulationNode2"
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

    val nodeRefs =
      mutableMapOf(
        Pair(2, ModelNode.build(populationNodeConfig1)),
        Pair(3, ModelNode.build(populationNodeConfig2))
      )

    val exception =
      assertFailsWith<IllegalStateException> { ModelNode.build(branchNodeConfig, nodeRefs) }
    assertTrue(exception.message!!.contains("Probabilities do not sum to 1"))
  }

  @Test
  fun `apply branch with node by condition`() {
    /**
     * The branch node has 2 branches. One branch is selected with 40% chance, which is a population
     * node always assigns virtual person id 10. The other branch is selected with 60% chance, which
     * is a population node always assigns virtual person id 20.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
            condition = fieldFilterProto {
              name = "person_country_code"
              op = Op.EQUAL
              value = "country_code_1"
            }
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
                randomSeed = "TestPopulationNodeSeed1"
              }
            }
            condition = fieldFilterProto {
              name = "person_country_code"
              op = Op.EQUAL
              value = "country_code_2"
            }
          }
        )
        randomSeed = "TestBranchNodeSeed"
      }
    }
    val node = ModelNode.build(config)

    val input1 = labelerEvent { personCountryCode = "country_code_1" }.toBuilder()
    node.apply(input1)
    assertEquals(10UL, input1.getVirtualPersonActivities(0).virtualPersonId.toULong())

    val input2 = labelerEvent { personCountryCode = "country_code_2" }.toBuilder()
    node.apply(input2)
    assertEquals(20UL, input2.getVirtualPersonActivities(0).virtualPersonId.toULong())

    /** No branch matches. Returns error status. */
    val input3 = labelerEvent { personCountryCode = "country_code_3" }.toBuilder()
    val exception = assertFailsWith<IllegalStateException> { node.apply(input3) }
    assertTrue(exception.message!!.contains("No condition matches the input event"))
  }

  @Test
  fun `apply branch with node index resolved recursively`() {
    /**
     * The branch node has 2 branches. One branch is selected with 40% chance, which is a population
     * node always assigns virtual person id 10. The other branch is selected with 60% chance, which
     * is a population node always assigns virtual person id 20.
     */
    val branchNodeConfig1 = compiledNode {
      name = "TestBranchNode"
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
        randomSeed = "TestBranchNodeSeed1"
      }
    }
    val branchNodeConfig2 = compiledNode {
      name = "TestBranchNode"
      index = 2
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 4
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed2"
      }
    }
    val branchNodeConfig3 = compiledNode {
      name = "TestBranchNode"
      index = 3
      branchNode = branchNode {
        branches.add(
          branch {
            nodeIndex = 5
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed3"
      }
    }

    val populationNodeConfig1 = compiledNode {
      name = "TestPopulationNode1"
      index = 4
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
    val populationNodeConfig2 = compiledNode {
      name = "TestPopulationNode2"
      index = 5
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

    val nodeRefs = mutableMapOf<Int, ModelNode>()
    nodeRefs[4] = ModelNode.build(populationNodeConfig1)
    nodeRefs[2] = ModelNode.build(branchNodeConfig2, nodeRefs)
    nodeRefs[5] = ModelNode.build(populationNodeConfig2)
    nodeRefs[3] = ModelNode.build(branchNodeConfig3, nodeRefs)

    val branchNode1 = ModelNode.build(branchNodeConfig1, nodeRefs)

    val idCounts = mutableMapOf<ULong, Int>()
    (0 until FINGERPRINT_NUMBER).forEach {
      val input = labelerEvent { actingFingerprint = it }.toBuilder()
      branchNode1.apply(input)
      val virtualPersonId = input.getVirtualPersonActivities(0).virtualPersonId.toULong()
      idCounts[virtualPersonId] = idCounts.getOrDefault(virtualPersonId, 0) + 1
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. The
     * expected count for 10 is 0.4*FINGERPRINT_NUMBER = 4000, the expected count for 20 is
     * 0.6*FINGERPRINT_NUMBER=6000
     */
    assertEquals(2, idCounts.size)
    assertEquals(4010, idCounts[10UL])
    assertEquals(5990, idCounts[20UL])
  }

  @Test
  fun `no branch should throw`() {
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
      branchNode = branchNode {}
    }

    val exception = assertFailsWith<IllegalStateException> { ModelNode.build(config) }
    assertTrue(exception.message!!.contains("BranchNode must have at least 1 branch"))
  }

  @Test
  fun `no child node should throw`() {
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
      branchNode = branchNode { branches.add(branch { chance = 1.0 }) }
    }

    val exception = assertFailsWith<IllegalStateException> { ModelNode.build(config) }
    assertTrue(exception.message!!.contains("BranchNode must have one of node_index and node"))
  }

  @Test
  fun `no select by should throw`() {
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
          }
        )
      }
    }

    val exception = assertFailsWith<IllegalStateException> { ModelNode.build(config) }
    assertTrue(exception.message!!.contains("BranchNode must have one of chance and condition"))
  }

  @Test
  fun `different select by should throw`() {
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
            chance = 0.5
          }
        )
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
            condition = fieldFilterProto {}
          }
        )
      }
    }

    val exception = assertFailsWith<IllegalStateException> { ModelNode.build(config) }
    assertTrue(exception.message!!.contains("All branches should use the same select_by type"))
  }

  @Test
  fun `resolve child references index not found should throw`() {
    val config = compiledNode {
      name = "TestBranchNode"
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

    val exception = assertFailsWith<IllegalStateException> { ModelNode.build(config) }
    assertTrue(
      exception.message!!.contains("The ModelNode object of the child node index 2 is not provided")
    )
  }

  @Test
  fun `apply update matrix`() {
    /**
     * The branch node has 1 attributes updater and 2 branches. The update matrix is
     *
     * ```
     *                    "RAW_COUNTRY_1" "RAW_COUNTRY_2"
     * "country_code_1"         0.8             0.2
     * "country_code_2"         0.2             0.8
     * ```
     *
     * One branch is selected if person_country_code is "country_code_1", which is a population node
     * always assigns virtual person id 10. The other branch is selected if person_country_code is
     * "country_code_2", which is a population node always assigns virtual person id 20.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
            condition = fieldFilterProto {
              name = "person_country_code"
              op = Op.EQUAL
              value = "country_code_1"
            }
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
                randomSeed = "TestPopulationNodeSeed1"
              }
            }
            condition = fieldFilterProto {
              name = "person_country_code"
              op = Op.EQUAL
              value = "country_code_2"
            }
          }
        )
        updates = attributesUpdaters {
          updates.add(
            attributesUpdater {
              updateMatrix = updateMatrix {
                columns.add(labelerEvent { personCountryCode = "RAW_COUNTRY_1" })
                columns.add(labelerEvent { personCountryCode = "RAW_COUNTRY_2" })
                rows.add(labelerEvent { personCountryCode = "country_code_1" })
                rows.add(labelerEvent { personCountryCode = "country_code_2" })
                probabilities.add(0.8f)
                probabilities.add(0.2f)
                probabilities.add(0.2f)
                probabilities.add(0.8f)
              }
            }
          )
        }
      }
    }
    val node = ModelNode.build(config)

    /** Test for RAW_COUNTRY_1. */
    val idCounts1 = mutableMapOf<ULong, Int>()
    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            personCountryCode = "RAW_COUNTRY_1"
            actingFingerprint = it
          }
          .toBuilder()
      node.apply(input)
      /** Confirms the Fingerprint is not changed. */
      assertEquals(input.actingFingerprint, it)
      val personCountryCode = input.personCountryCode
      val virtualPersonId = input.getVirtualPersonActivities(0).virtualPersonId.toULong()
      assertTrue {
        Pair(personCountryCode, virtualPersonId) in
          listOf(Pair("country_code_1", 10UL), Pair("country_code_2", 20UL))
      }
      idCounts1[virtualPersonId] = idCounts1.getOrDefault(virtualPersonId, 0) + 1
    }

    /**
     * The selected column is
     *
     * ```
     *                    "RAW_COUNTRY_1"
     * "country_code_1"         0.8
     * "country_code_2"         0.2
     * ```
     *
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same.
     */
    assertEquals(2, idCounts1.size)
    assertEquals(8022, idCounts1[10UL])
    assertEquals(1978, idCounts1[20UL])

    /** Test for RAW_COUNTRY_2. */
    val idCounts2 = mutableMapOf<ULong, Int>()
    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            personCountryCode = "RAW_COUNTRY_2"
            actingFingerprint = it
          }
          .toBuilder()
      node.apply(input)
      /** Confirms the Fingerprint is not changed. */
      assertEquals(input.actingFingerprint, it)
      val personCountryCode = input.personCountryCode
      val virtualPersonId = input.getVirtualPersonActivities(0).virtualPersonId.toULong()
      assertTrue {
        Pair(personCountryCode, virtualPersonId) in
          listOf(Pair("country_code_1", 10UL), Pair("country_code_2", 20UL))
      }
      idCounts2[virtualPersonId] = idCounts2.getOrDefault(virtualPersonId, 0) + 1
    }

    /**
     * The selected column is
     *
     * ```
     *                    "RAW_COUNTRY_2"
     * "country_code_1"         0.2
     * "country_code_2"         0.8
     * ```
     *
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same.
     */
    assertEquals(2, idCounts2.size)
    assertEquals(2064, idCounts2[10UL])
    assertEquals(7936, idCounts2[20UL])
  }

  @Test
  fun `apply update matrices in order`() {
    /**
     * The branch node has 2 attributes updater and 1 branches. The 1st update matrix always changes
     * person_country_code from COUNTRY_1 to COUNTRY_2. The 2nd update matrix always changes
     * person_country_code from COUNTRY_2 to COUNTRY_3.
     *
     * One branch is selected if person_country_code is "COUNTRY_3", which is a population node
     * always assigns virtual person id 10.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
            condition = fieldFilterProto {
              name = "person_country_code"
              op = Op.EQUAL
              value = "COUNTRY_3"
            }
          }
        )
        updates = attributesUpdaters {
          updates.add(
            attributesUpdater {
              updateMatrix = updateMatrix {
                columns.add(labelerEvent { personCountryCode = "COUNTRY_1" })
                rows.add(labelerEvent { personCountryCode = "COUNTRY_2" })
                probabilities.add(1.0f)
              }
            }
          )
          updates.add(
            attributesUpdater {
              updateMatrix = updateMatrix {
                columns.add(labelerEvent { personCountryCode = "COUNTRY_2" })
                rows.add(labelerEvent { personCountryCode = "COUNTRY_3" })
                probabilities.add(1.0f)
              }
            }
          )
        }
      }
    }
    val node = ModelNode.build(config)

    /** Test for COUNTRY_1. */
    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            personCountryCode = "COUNTRY_1"
            actingFingerprint = it
          }
          .toBuilder()
      node.apply(input)
      /** Confirms the Fingerprint is not changed. */
      assertEquals(it, input.actingFingerprint)
      assertEquals("COUNTRY_3", input.personCountryCode)
      assertEquals(10UL, input.getVirtualPersonActivities(0).virtualPersonId.toULong())
    }
  }

  @Test
  fun `invalid multiplicity field should throw`() {
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
        multiplicity = multiplicity {}
      }
    }
    val exception = assertFailsWith<IllegalStateException> { ModelNode.build(config) }
    assertTrue(exception.message!!.contains("Multiplicity must set person_index_field"))
  }

  @Test
  fun `apply explicit multiplicity and cap at max is true`() {
    /**
     * The branch node has 1 branch, which is a population node always assigns virtual person id 10.
     * Use explicit multiplicity = 3. When cap_at_max = true, should cap to max_value = 1.2.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
        multiplicity = multiplicity {
          expectedMultiplicity = 3.0
          maxValue = 1.2
          capAtMax = true
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity"
        }
      }
    }
    val node = ModelNode.build(config)

    /** All virtual_person_id = 10. */
    var personTotal = 0
    (0 until FINGERPRINT_NUMBER).forEach {
      val input = labelerEvent { actingFingerprint = it }.toBuilder()
      node.apply(input)
      assertTrue { input.virtualPersonActivitiesCount in listOf(1, 2) }
      personTotal += input.virtualPersonActivitiesCount
      input.virtualPersonActivitiesList.forEach { assertEquals(10UL, it.virtualPersonId.toULong()) }
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. View
     * is around 1.2 * FINGERPRINT_NUMBER = 12000
     */
    assertEquals(11949, personTotal)
  }

  @Test
  fun `apply explicit multiplicity and cap at max is false`() {
    /**
     * The branch node has 1 branch, which is a population node always assigns virtual person id 10.
     * Use explicit multiplicity = 3. When cap_at_max = false, should return error.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
        multiplicity = multiplicity {
          expectedMultiplicity = 3.0
          maxValue = 1.2
          capAtMax = false
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity"
        }
      }
    }
    val node = ModelNode.build(config)

    (0 until FINGERPRINT_NUMBER).forEach {
      val input = labelerEvent { actingFingerprint = it }.toBuilder()
      val exception = assertFailsWith<IllegalStateException> { node.apply(input) }
      assertTrue(exception.message!!.contains("exceeds the specified max value"))
    }
  }

  @Test
  fun `apply multiplicity field and cap at max is true`() {
    /**
     * The branch node has 1 branch, which is a population node always assigns virtual person id 10.
     * Read multiplicity from field. When cap_at_max = true, should cap to max_value = 1.2.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
        multiplicity = multiplicity {
          expectedMultiplicityField = "expected_multiplicity"
          maxValue = 1.2
          capAtMax = true
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity"
        }
      }
    }
    val node = ModelNode.build(config)

    /** multiplicity field is not set, return error. */
    (0 until FINGERPRINT_NUMBER).forEach {
      val input = labelerEvent { actingFingerprint = it }.toBuilder()
      val exception = assertFailsWith<IllegalStateException> { node.apply(input) }
      assertTrue(exception.message!!.contains("multiplicity field is not set"))
    }

    /** multiplicity > max_value, cap at max_value. All virtual_person_id = 10. */
    var personTotal = 0
    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            actingFingerprint = it
            expectedMultiplicity = 2.0
          }
          .toBuilder()
      node.apply(input)
      assertTrue { input.virtualPersonActivitiesCount in listOf(1, 2) }
      personTotal += input.virtualPersonActivitiesCount
      input.virtualPersonActivitiesList.forEach { assertEquals(10UL, it.virtualPersonId.toULong()) }
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. View
     * is around 1.2 * FINGERPRINT_NUMBER = 12000
     */
    assertEquals(11949, personTotal)

    /** multiplicity < max_value, cap at max_value. All virtual_person_id = 10. */
    personTotal = 0
    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            actingFingerprint = it
            expectedMultiplicity = 1.1
          }
          .toBuilder()
      node.apply(input)
      assertTrue { input.virtualPersonActivitiesCount in listOf(1, 2) }
      personTotal += input.virtualPersonActivitiesCount
      input.virtualPersonActivitiesList.forEach { assertEquals(10UL, it.virtualPersonId.toULong()) }
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. View
     * is around 1.1 * FINGERPRINT_NUMBER = 11000
     */
    assertEquals(10959, personTotal)
  }

  @Test
  fun `apply multiplicity field and cap at max is false`() {
    /**
     * The branch node has 1 branch, which is a population node always assigns virtual person id 10.
     * Read multiplicity from field. When cap_at_max = true, should cap to max_value = 1.2.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
        multiplicity = multiplicity {
          expectedMultiplicityField = "expected_multiplicity"
          maxValue = 1.2
          capAtMax = false
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity"
        }
      }
    }
    val node = ModelNode.build(config)

    /** multiplicity field is not set, return error. */
    (0 until FINGERPRINT_NUMBER).forEach {
      val input = labelerEvent { actingFingerprint = it }.toBuilder()
      val exception = assertFailsWith<IllegalStateException> { node.apply(input) }
      assertTrue(exception.message!!.contains("multiplicity field is not set"))
    }

    /** multiplicity > max_value, return error */
    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            actingFingerprint = it
            expectedMultiplicity = 2.0
          }
          .toBuilder()
      val exception = assertFailsWith<IllegalStateException> { node.apply(input) }
      assertTrue(exception.message!!.contains("exceeds the specified max value"))
    }

    /** multiplicity < max_value, cap at max_value. All virtual_person_id = 10. */
    var personTotal = 0
    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            actingFingerprint = it
            expectedMultiplicity = 1.1
          }
          .toBuilder()
      node.apply(input)
      assertTrue { input.virtualPersonActivitiesCount in listOf(1, 2) }
      personTotal += input.virtualPersonActivitiesCount
      input.virtualPersonActivitiesList.forEach { assertEquals(10UL, it.virtualPersonId.toULong()) }
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same. View
     * is around 1.1 * FINGERPRINT_NUMBER = 11000
     */
    assertEquals(10959, personTotal)
  }

  @Test
  fun `apply explicit multiplicity`() {
    /**
     * The branch node has 2 branches. One branch is selected with 20% chance, which is a population
     * node always assigns virtual person id 10. The other branch is selected with 80% chance, which
     * is a population node always assigns virtual person id 20. Multiplicity is explicit value =
     * 1.3.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
            chance = 0.2
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
            chance = 0.8
          }
        )
        randomSeed = "TestBranchNodeSeed"
        multiplicity = multiplicity {
          expectedMultiplicity = 1.3
          maxValue = 2.0
          capAtMax = true
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity"
        }
      }
    }
    val node = ModelNode.build(config)

    val idCountsIndex0 = mutableMapOf<ULong, Int>()
    val idCountsIndex1 = mutableMapOf<ULong, Int>()
    var samePool = 0
    var differentPool = 0

    (0 until FINGERPRINT_NUMBER).forEach {
      val input = labelerEvent { actingFingerprint = it }.toBuilder()
      node.apply(input)
      assertTrue { input.virtualPersonActivitiesCount in listOf(1, 2) }
      val virtualPersonId0 = input.getVirtualPersonActivities(0).virtualPersonId.toULong()
      idCountsIndex0[virtualPersonId0] = idCountsIndex0.getOrDefault(virtualPersonId0, 0) + 1
      if (input.virtualPersonActivitiesCount == 2) {
        val virtualPersonId1 = input.getVirtualPersonActivities(1).virtualPersonId.toULong()
        idCountsIndex1[virtualPersonId1] = idCountsIndex1.getOrDefault(virtualPersonId1, 0) + 1
        if (virtualPersonId1 == virtualPersonId0) {
          ++samePool
        } else {
          ++differentPool
        }
      }
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same.
     *
     * Expect ~70% events have 1 virtual person, ~30% events have 2. The clones are labeled
     * independently, so they have the same probability to go to each pool.
     *
     * Expected values are
     *
     * ```
     *   idCountsIndex0[10UL] ~= FINGERPRINT_NUMBER * 0.8 = 8000
     *   idCountsIndex0[20UL] ~= FINGERPRINT_NUMBER * 0.2 = 2000
     *   idCountsIndex1[10UL] ~= FINGERPRINT_NUMBER * 0.3 * 0.2 = 600
     *   idCountsIndex2[20UL] ~= FINGERPRINT_NUMBER * 0.3 * 0.8 = 2400
     * ```
     */
    assertEquals(2, idCountsIndex0.size)
    assertEquals(1986, idCountsIndex0[10UL])
    assertEquals(8014, idCountsIndex0[20UL])
    assertEquals(2, idCountsIndex1.size)
    assertEquals(570, idCountsIndex1[10UL])
    assertEquals(2377, idCountsIndex1[20UL])

    /**
     * ```
     * Same pool chance: 0.2 * 0.2 + 0.8 * 0.8 = 0.68.
     * Expect value for samePool is around FINGERPRINT_NUMBER * 0.3 * 0.68 = 2040
     * Different pool chance: 0.2 * 0.8 * 2 = 0.32.
     * Expect value for samePool is around FINGERPRINT_NUMBER * 0.3 * 0.32 = 960
     * ```
     */
    assertEquals(2000, samePool)
    assertEquals(947, differentPool)
  }

  @Test
  fun `apply multiplicity less than one`() {
    /**
     * The branch node has 2 branches. One branch is selected with 20% chance, which is a population
     * node always assigns virtual person id 10. The other branch is selected with 80% chance, which
     * is a population node always assigns virtual person id 20. Multiplicity is explicit value =
     * 0.3. Each event will have 0 or 1 virtual person.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
            chance = 0.2
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
            chance = 0.8
          }
        )
        randomSeed = "TestBranchNodeSeed"
        multiplicity = multiplicity {
          expectedMultiplicity = 0.3
          maxValue = 2.0
          capAtMax = true
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity"
        }
      }
    }
    val node = ModelNode.build(config)

    val idCountsIndex = mutableMapOf<ULong, Int>()
    (0 until FINGERPRINT_NUMBER).forEach {
      val input = labelerEvent { actingFingerprint = it }.toBuilder()
      node.apply(input)
      assertTrue { input.virtualPersonActivitiesCount in listOf(0, 1) }
      if (input.virtualPersonActivitiesCount == 1) {
        val virtualPersonId = input.getVirtualPersonActivities(0).virtualPersonId.toULong()
        idCountsIndex[virtualPersonId] = idCountsIndex.getOrDefault(virtualPersonId, 0) + 1
      }
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same.
     *
     * Expect ~70% events have 0 virtual person, ~30% events have 1.
     *
     * ```
     * Expected values are
     *   idCountsIndex[10UL] ~= FINGERPRINT_NUMBER * 0.3 * 0.2 = 600
     *   idCountsIndex[20UL] ~= FINGERPRINT_NUMBER * 0.3 * 0.8 = 2400
     * ```
     */
    assertEquals(2, idCountsIndex.size)
    assertEquals(593, idCountsIndex[10UL])
    assertEquals(2354, idCountsIndex[20UL])
  }

  @Test
  fun `apply multiplicity from field`() {
    /**
     * The branch node has 2 branches. One branch is selected with 20% chance, which is a population
     * node always assigns virtual person id 10. The other branch is selected with 80% chance, which
     * is a population node always assigns virtual person id 20. Multiplicity is read from a field.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
            chance = 0.2
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
            chance = 0.8
          }
        )
        randomSeed = "TestBranchNodeSeed"
        multiplicity = multiplicity {
          expectedMultiplicityField = "expected_multiplicity"
          maxValue = 2.0
          capAtMax = true
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity"
        }
      }
    }
    val node = ModelNode.build(config)

    val idCountsIndex0 = mutableMapOf<ULong, Int>()
    val idCountsIndex1 = mutableMapOf<ULong, Int>()
    var samePool = 0
    var differentPool = 0

    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            actingFingerprint = it
            expectedMultiplicity = 1.3
          }
          .toBuilder()
      node.apply(input)
      assertTrue { input.virtualPersonActivitiesCount in listOf(1, 2) }
      val virtualPersonId0 = input.getVirtualPersonActivities(0).virtualPersonId.toULong()
      idCountsIndex0[virtualPersonId0] = idCountsIndex0.getOrDefault(virtualPersonId0, 0) + 1
      if (input.virtualPersonActivitiesCount == 2) {
        val virtualPersonId1 = input.getVirtualPersonActivities(1).virtualPersonId.toULong()
        idCountsIndex1[virtualPersonId1] = idCountsIndex1.getOrDefault(virtualPersonId1, 0) + 1
        if (virtualPersonId1 == virtualPersonId0) {
          ++samePool
        } else {
          ++differentPool
        }
      }
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same.
     *
     * Expect ~70% events have 1 virtual person, ~30% events have 2. The clones are labeled
     * independently, so they have the same probability to go to each pool.
     *
     * Expected values are
     *
     * ```
     *   idCountsIndex0[10UL] ~= FINGERPRINT_NUMBER * 0.8 = 8000
     *   idCountsIndex0[20UL] ~= FINGERPRINT_NUMBER * 0.2 = 2000
     *   idCountsIndex1[10UL] ~= FINGERPRINT_NUMBER * 0.3 * 0.2 = 600
     *   idCountsIndex1[20UL] ~= FINGERPRINT_NUMBER * 0.3 * 0.8 = 2400
     * ```
     */
    assertEquals(2, idCountsIndex0.size)
    assertEquals(1986, idCountsIndex0[10UL])
    assertEquals(8014, idCountsIndex0[20UL])
    assertEquals(2, idCountsIndex1.size)
    assertEquals(570, idCountsIndex1[10UL])
    assertEquals(2377, idCountsIndex1[20UL])

    /**
     * ```
     * Same pool chance: 0.2 * 0.2 + 0.8 * 0.8 = 0.68.
     * Expect value for samePool is around FINGERPRINT_NUMBER * 0.3 * 0.68 = 2040
     * Different pool chance: 0.2 * 0.8 * 2 = 0.32.
     * Expect value for samePool is around FINGERPRINT_NUMBER * 0.3 * 0.32 = 960
     * ```
     */
    assertEquals(2000, samePool)
    assertEquals(947, differentPool)
  }

  @Test
  fun `use person index`() {
    /**
     * The branch node has 2 branches. The first one is a population node always assigns virtual
     * person id 10. The second one is a population node always assigns virtual person id 20. Select
     * the first one if person_index = 0, else select the second one.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
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
            condition = fieldFilterProto {
              name = "multiplicity_person_index"
              op = Op.EQUAL
              value = "0"
            }
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
            condition = fieldFilterProto { op = Op.TRUE }
          }
        )
        multiplicity = multiplicity {
          expectedMultiplicityField = "expected_multiplicity"
          maxValue = 2.0
          capAtMax = true
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity"
        }
      }
    }
    val node = ModelNode.build(config)

    /** person_index = 0 get id 10, person_index = 1 get id 20. */
    val idCountsIndex = mutableMapOf<ULong, Int>()
    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            actingFingerprint = it
            expectedMultiplicity = 1.3
          }
          .toBuilder()
      node.apply(input)
      assertTrue { input.virtualPersonActivitiesCount in listOf(1, 2) }
      val virtualPersonId0 = input.getVirtualPersonActivities(0).virtualPersonId.toULong()
      assertEquals(10UL, virtualPersonId0)
      idCountsIndex[virtualPersonId0] = idCountsIndex.getOrDefault(virtualPersonId0, 0) + 1
      if (input.virtualPersonActivitiesCount == 2) {
        val virtualPersonId1 = input.getVirtualPersonActivities(1).virtualPersonId.toULong()
        assertEquals(20UL, virtualPersonId1)
        idCountsIndex[virtualPersonId1] = idCountsIndex.getOrDefault(virtualPersonId1, 0) + 1
      }
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same.
     *
     * All events have person_index 0. ~30% events have person_index 1.
     *
     * ```
     * Expected values are
     *   idCountsIndex[10UL] ~= FINGERPRINT_NUMBER = 10000
     *   idCountsIndex[20UL] ~= FINGERPRINT_NUMBER * 0.3 = 3000
     * ```
     */
    assertEquals(2, idCountsIndex.size)
    assertEquals(10000, idCountsIndex[10UL])
    assertEquals(2947, idCountsIndex[20UL])
  }

  @Test
  fun `apply nested multiplicity`() {
    /**
     * There are two levels of multiplicity, attached to the corresponding branch nodes.
     *
     * Note that in this case they should use different expected_multiplicity_field(if read from
     * field) and person_index_field. To avoid conflict, we use expected_multiplicity in this test.
     * But using another person_index_field requires a proto change, and we will only do that when
     * there is an actual use case. So this test uses the same person_index_field because it doesn't
     * depend on person_index.
     *
     * The first level is a branch node with one child, and multiplicity = 1.2. The second level is
     * a branch node with two children, and multiplicity = 1.6. The first child is a population node
     * always assigns virtual person id 10. The second child is a population node always assigns
     * virtual person id 20. Select the first one with chance 0.2, the second one with chance 0.8.
     */
    val config = compiledNode {
      name = "BranchLevel1"
      index = 1
      branchNode = branchNode {
        multiplicity = multiplicity {
          expectedMultiplicity = 1.2
          maxValue = 2.0
          capAtMax = true
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity 1"
        }
        branches.add(
          branch {
            condition = fieldFilterProto { op = Op.TRUE }
            node = compiledNode {
              name = "BranchLevel2"
              branchNode = branchNode {
                multiplicity = multiplicity {
                  expectedMultiplicity = 1.6
                  maxValue = 2.0
                  capAtMax = true
                  personIndexField = "multiplicity_person_index"
                  randomSeed = "test multiplicity 2"
                }
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
                    chance = 0.2
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
                    chance = 0.8
                  }
                )
                randomSeed = "TestBranchNodeSeed"
              }
            }
          }
        )
      }
    }

    val node = ModelNode.build(config)

    val idCounts = mutableMapOf<ULong, Int>()
    val virtualPersonSizeCounts = mutableMapOf<Int, Int>()
    (0 until FINGERPRINT_NUMBER).forEach {
      val input =
        labelerEvent {
            actingFingerprint = it
            expectedMultiplicity = 1.3
          }
          .toBuilder()
      node.apply(input)
      assertTrue { input.virtualPersonActivitiesCount in listOf(1, 2, 3, 4) }
      virtualPersonSizeCounts[input.virtualPersonActivitiesCount] =
        virtualPersonSizeCounts.getOrDefault(input.virtualPersonActivitiesCount, 0) + 1
      input.virtualPersonActivitiesList.forEach {
        val virtualId = it.virtualPersonId.toULong()
        idCounts[virtualId] = idCounts.getOrDefault(virtualId, 0) + 1
      }
    }

    /**
     * Compare to the exact result to make sure C++ and Kotlin implementations behave the same.
     *
     * ```
     * Total multiplicity = 1.2 * 1.6 = 1.92
     *    20% with id 10  (10000 * 1.92 * 0.2 = 3800
     *    80% with id 20  (10000 * 1.92 * 0.8 = 15200
     *
     * There can be 1-4 virtual persons for each event.
     * Level 1: 80% chance 1 clone, 20% chance 2 clones
     * Level 2: 40% chance 1 clones, 60% chance 2 clones
     * Probability for 1-4 virtual persons:
     * 1 VP: first level 1 clone, second level 1 clone: 0.8 * 0.4 = 0.32  (32000)
     * 2 VP: first level 1 clone, second level 2 clones;
     *      or first level 2 clones, second level 1 clone:
     *     0.8 * 0.6 + 0.2 * 0.4 * 0.4 = 0.512  (5120)
     * 3 VP: first level 2 clones, second level 1 clone + 2 clones(or 2 + 1):
     *     0.2 * 0.4 * 0.6 + 0.2 * 0.6 * 0.4 = 0.096  (960)
     * 4 VP: first level 2 clones, second level 2 clones + 2 clones:
     *     0.2 * 0.6 * 0.6 = 0.072  (720)
     * ```
     */
    assertEquals(2, idCounts.size)
    assertEquals(3884, idCounts[10UL])
    assertEquals(15384, idCounts[20UL])
    assertEquals(4, virtualPersonSizeCounts.size)
    assertEquals(3207, virtualPersonSizeCounts[1])
    assertEquals(5083, virtualPersonSizeCounts[2])
    assertEquals(945, virtualPersonSizeCounts[3])
    assertEquals(765, virtualPersonSizeCounts[4])
  }

  @Test
  fun `apply multiplicity in pass-1 mode folds back pool assignments`() {
    /**
     * The branch node has one ranked-population-node branch and uses multiplicity to clone events.
     * In pool-identity (pass-1) mode each clone emits a PoolAssignment instead of a virtual person.
     * Every clone's assignment must be folded back onto the event, so the pass-1 pool-assignment
     * total must equal the full-mode virtual-person total (one of each per clone). Regression test
     * for applyMultiplicity previously dropping pool assignments from clones.
     */
    val config = compiledNode {
      name = "TestBranchNode"
      index = 1
      branchNode = branchNode {
        branches.add(
          branch {
            node = compiledNode {
              rankedPopulationNode = rankedPopulationNode {
                pools.add(
                  virtualPersonPool {
                    populationOffset = 100
                    totalPopulation = 500
                  }
                )
                randomSeed = "TestRankedSeed"
                rankedSize = 200
                unrankedMode = RankedPopulationNode.UnrankedMode.DISJOINT
              }
            }
            chance = 1.0
          }
        )
        randomSeed = "TestBranchNodeSeed"
        multiplicity = multiplicity {
          expectedMultiplicity = 3.0
          maxValue = 1.2
          capAtMax = true
          personIndexField = "multiplicity_person_index"
          randomSeed = "test multiplicity"
        }
      }
    }
    val node = ModelNode.build(config)

    /**
     * cloneCount depends only on actingFingerprint, so it is identical across both modes. Full mode
     * yields one virtual person per clone; pass-1 mode must yield one pool assignment per clone.
     */
    var fullModeTotal = 0
    var pass1Total = 0
    (0 until FINGERPRINT_NUMBER).forEach {
      val fullInput = labelerEvent { actingFingerprint = it }.toBuilder()
      node.apply(fullInput)
      fullModeTotal += fullInput.virtualPersonActivitiesCount

      val pass1Input =
        labelerEvent {
            actingFingerprint = it
            poolIdentityMode = true
          }
          .toBuilder()
      node.apply(pass1Input)
      assertEquals(0, pass1Input.virtualPersonActivitiesCount)
      pass1Total += pass1Input.poolAssignmentsCount
    }

    assertEquals(fullModeTotal, pass1Total)
    assertTrue(pass1Total > FINGERPRINT_NUMBER, "Expected multiplicity to clone some events")
  }
}
