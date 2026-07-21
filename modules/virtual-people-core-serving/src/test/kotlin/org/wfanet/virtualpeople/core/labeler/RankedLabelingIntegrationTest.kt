// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.virtualpeople.core.labeler

import com.google.protobuf.TextFormat
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.copy
import org.wfanet.virtualpeople.common.eventId
import org.wfanet.virtualpeople.common.labelerInput
import org.wfanet.virtualpeople.common.rankAssignment

@RunWith(JUnit4::class)
class RankedLabelingIntegrationTest {

  private fun buildLabeler(protoText: String): Labeler {
    val root = CompiledNode.newBuilder()
    TextFormat.merge(protoText, root)
    return Labeler.build(root.build())
  }

  private fun makeInput(id: String) = labelerInput {
    eventId = eventId {
      publisher = "test"
      this.id = id
    }
  }

  @Test
  fun `ranked events produce collision free VIDs`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "test-seed"
        ranked_size: 200
        unranked_mode: DISJOINT
      }
    """
      )

    val vids = mutableSetOf<ULong>()
    for (rank in 0uL until 200uL) {
      val input = labelerInput {
        eventId = eventId {
          publisher = "test"
          id = rank.toString()
        }
        rankAssignments += rankAssignment {
          poolOffset = 100
          localRank = rank.toLong()
        }
      }

      val output = labeler.label(input)
      assertEquals(1, output.peopleCount)
      val vid = output.peopleList[0].virtualPersonId.toULong()
      assertTrue(vid >= 100uL, "VID $vid below pool_offset")
      assertTrue(vid < 300uL, "VID $vid above ranked range")
      vids.add(vid)
    }

    assertEquals(200, vids.size, "Collision detected in ranked VIDs")
  }

  @Test
  fun `unranked disjoint events get VIDs in unranked range`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "test-seed"
        ranked_size: 200
        unranked_mode: DISJOINT
      }
    """
      )

    for (i in 0 until 50) {
      val output = labeler.label(makeInput("unranked-$i"))
      assertEquals(1, output.peopleCount)
      val vid = output.peopleList[0].virtualPersonId.toULong()
      assertTrue(vid >= 300uL, "VID $vid below unranked range")
      assertTrue(vid < 600uL, "VID $vid above pool range")
    }
  }

  @Test
  fun `unranked full pool events get VIDs in full range`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "test-seed"
        ranked_size: 200
        unranked_mode: FULL_POOL
      }
    """
      )

    for (i in 0 until 50) {
      val output = labeler.label(makeInput("fullpool-$i"))
      assertEquals(1, output.peopleCount)
      val vid = output.peopleList[0].virtualPersonId.toULong()
      assertTrue(vid >= 100uL, "VID $vid below pool range")
      assertTrue(vid < 600uL, "VID $vid above pool range")
    }
  }

  @Test
  fun `rank overflow falls back to unranked path`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "overflow-seed"
        ranked_size: 200
        unranked_mode: DISJOINT
      }
    """
      )

    // local_rank=250 > ranked_size=200 — should fall back to unranked.
    val input = labelerInput {
      eventId = eventId {
        publisher = "test"
        id = "overflow-event"
      }
      rankAssignments += rankAssignment {
        poolOffset = 100
        localRank = 250
      }
    }

    val output = labeler.label(input)
    assertEquals(1, output.peopleCount)
    val vid = output.peopleList[0].virtualPersonId.toULong()
    assertTrue(vid >= 300uL, "Overflow VID $vid should be in unranked range")
    assertTrue(vid < 600uL, "Overflow VID $vid should be in unranked range")
  }

  @Test
  fun `boundary rank equals ranked size falls back to unranked`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "boundary-seed"
        ranked_size: 200
        unranked_mode: DISJOINT
      }
    """
      )

    // local_rank=200 == ranked_size=200 — exactly at boundary, should fall back to unranked.
    val input = labelerInput {
      eventId = eventId {
        publisher = "test"
        id = "boundary-event"
      }
      rankAssignments += rankAssignment {
        poolOffset = 100
        localRank = 200
      }
    }

    val output = labeler.label(input)
    assertEquals(1, output.peopleCount)
    val vid = output.peopleList[0].virtualPersonId.toULong()
    assertTrue(vid >= 300uL, "Boundary VID $vid should be in unranked range")
    assertTrue(vid < 600uL, "Boundary VID $vid should be in unranked range")
  }

  @Test
  fun `multiple rank assignments resolves correct pool`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 500 total_population: 1000 }
        random_seed: "multi-rank-seed"
        ranked_size: 400
        unranked_mode: DISJOINT
      }
    """
      )

    // Event carries rank assignments for multiple pools — only pool_offset=500 should match.
    val input = labelerInput {
      eventId = eventId {
        publisher = "test"
        id = "multi-rank-event"
      }
      rankAssignments += rankAssignment {
        poolOffset = 100
        localRank = 5
      }
      rankAssignments += rankAssignment {
        poolOffset = 500
        localRank = 10
      }
      rankAssignments += rankAssignment {
        poolOffset = 9000
        localRank = 99
      }
    }

    val output = labeler.label(input)
    assertEquals(1, output.peopleCount)
    val vid = output.peopleList[0].virtualPersonId.toULong()
    assertTrue(vid >= 500uL, "VID $vid should be in ranked range for pool 500")
    assertTrue(vid < 900uL, "VID $vid should be in ranked range for pool 500")
  }

  /**
   * Regression test for world-federation-of-advertisers/cross-media-measurement#4073.
   *
   * When this leaf's pool_offset matches no entry in rank_assignments, the leaf must fall back to
   * the hash path instead of failing the job. The dominant case is Phase-1 overflow: when this
   * subpool reached ranked_size and this fingerprint was one of the unranked surplus, the subpool
   * has no rank for it (per design § Retention Rule, overflow-fps-fall-back-to-unranked-path).
   */
  @Test
  fun `rank for non-existent pool falls back to hash`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "overflow-fallback-seed"
        ranked_size: 200
        unranked_mode: DISJOINT
      }
    """
      )

    val input = labelerInput {
      eventId = eventId {
        publisher = "test"
        id = "wrong-pool-event"
      }
      // Rank assignments only carry entries for OTHER pools — none for pool_offset=100.
      rankAssignments += rankAssignment {
        poolOffset = 9000
        localRank = 5
      }
      rankAssignments += rankAssignment {
        poolOffset = 99999
        localRank = 17
      }
    }

    val output = labeler.label(input)
    assertEquals(1, output.peopleCount)
    val activity = output.peopleList[0]
    val vid = activity.virtualPersonId.toULong()
    // DISJOINT mode: VID lands in the unranked sub-range
    // [pool_offset + ranked_size, pool_offset + pool_size) = [300, 600).
    assertTrue(vid >= 300uL, "VID $vid below unranked range")
    assertTrue(vid < 600uL, "VID $vid above pool")
    // The whole point of this path is to surface that the leaf had to hash-fall-back.
    assertTrue(activity.memoizedRankFallback)
  }

  /**
   * The non-matching-rank_assignments path must produce the same VID as the empty-rank_assignments
   * path — both represent the same semantic state (no rank for this fingerprint in this subpool,
   * hash-fall-back), so same input must yield the same VID. Locks in that the fix didn't
   * accidentally introduce a different hash seed for the two no-rank cases.
   */
  @Test
  fun `non-matching rank assignment produces same VID as empty rank assignments`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "overflow-fallback-seed"
        ranked_size: 200
        unranked_mode: DISJOINT
      }
    """
      )

    val baseInput = labelerInput {
      eventId = eventId {
        publisher = "test"
        id = "shared-id"
      }
    }

    // Input A: no rank_assignments at all.
    val activityA = labeler.label(baseInput).peopleList[0]
    val vidA = activityA.virtualPersonId
    // No rank_assignments at all => caller did not attempt memoization, so the
    // fallback signal MUST NOT fire.
    assertFalse(activityA.memoizedRankFallback)

    // Input B: same event id, rank_assignments only for OTHER pools.
    val inputB =
      baseInput.copy {
        rankAssignments += rankAssignment {
          poolOffset = 9000
          localRank = 5
        }
      }
    val activityB = labeler.label(inputB).peopleList[0]
    val vidB = activityB.virtualPersonId
    // Non-matching assignments => caller did attempt memoization but the leaf
    // hash-fell-back, so the signal MUST fire.
    assertTrue(activityB.memoizedRankFallback)

    // Both must produce the same VID — non-matching assignments behave exactly like empty.
    assertEquals(vidA, vidB)
  }

  @Test
  fun `ranked size zero produces same VIDs as PopulationNode`() {
    val rankedLabeler =
      buildLabeler(
        """
      name: "RankedNode"
      ranked_population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "same-seed"
        ranked_size: 0
        unranked_mode: FULL_POOL
      }
    """
      )

    val popLabeler =
      buildLabeler(
        """
      name: "PopNode"
      population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "same-seed"
      }
    """
      )

    for (i in 0 until 100) {
      val input = makeInput("event-$i")
      val rankedVid = rankedLabeler.label(input).peopleList[0].virtualPersonId
      val popVid = popLabeler.label(input).peopleList[0].virtualPersonId
      assertEquals(popVid, rankedVid, "VID mismatch for event-$i")
    }
  }

  @Test
  fun `pass 1 emits pool assignment without VID`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 100 total_population: 500 }
        random_seed: "pass1-seed"
        ranked_size: 200
        unranked_mode: DISJOINT
      }
    """
      )

    val output = labeler.label(makeInput("pass1-event"), LabelingMode.POOL_IDENTITY)

    assertEquals(0, output.peopleCount, "Pass-1 should not assign VIDs")
    assertEquals(1, output.poolAssignmentsCount, "Pass-1 should emit pool assignment")
    assertEquals(100L, output.poolAssignmentsList[0].poolOffset)
    assertEquals(500L, output.poolAssignmentsList[0].poolSize)
    assertEquals(200L, output.poolAssignmentsList[0].rankedSize)
  }

  @Test
  fun `pass 1 population node produces no output`() {
    val labeler =
      buildLabeler(
        """
      name: "PopNode"
      population_node {
        pools { population_offset: 10 total_population: 100 }
        random_seed: "pop-seed"
      }
    """
      )

    val output = labeler.label(makeInput("pop-event"), LabelingMode.POOL_IDENTITY)

    assertEquals(0, output.peopleCount, "Pass-1 PopulationNode should not assign VIDs")
    assertEquals(
      0,
      output.poolAssignmentsCount,
      "Pass-1 PopulationNode should emit no pool assignment",
    )
  }

  @Test
  fun `pass 1 mixed model emits assignments only for ranked leaf`() {
    // A model may mix a RankedPopulationNode leaf (needs ranking) and a plain
    // PopulationNode leaf (does not). In pass-1, events routing to the ranked
    // leaf emit a PoolAssignment; events routing to the vanilla leaf emit
    // nothing. No VIDs are assigned in either case.
    val labeler =
      buildLabeler(
        """
      name: "Root"
      branch_node {
        branches {
          node {
            name: "RankedLeaf"
            ranked_population_node {
              pools { population_offset: 1000 total_population: 500 }
              random_seed: "ranked-seed"
              ranked_size: 200
              unranked_mode: DISJOINT
            }
          }
          chance: 0.5
        }
        branches {
          node {
            name: "VanillaLeaf"
            population_node {
              pools { population_offset: 10 total_population: 100 }
              random_seed: "vanilla-seed"
            }
          }
          chance: 0.5
        }
        random_seed: "branch-seed"
      }
    """
      )

    var rankedCount = 0
    var vanillaCount = 0
    for (i in 0 until 200) {
      val output = labeler.label(makeInput("mixed-$i"), LabelingMode.POOL_IDENTITY)
      assertEquals(0, output.peopleCount, "Pass-1 should never assign VIDs")
      when (output.poolAssignmentsCount) {
        1 -> {
          assertEquals(1000L, output.poolAssignmentsList[0].poolOffset)
          rankedCount++
        }
        0 -> vanillaCount++
        else -> error("Unexpected pool assignment count ${output.poolAssignmentsCount}")
      }
    }

    assertTrue(rankedCount > 0, "No events routed to the ranked leaf")
    assertTrue(vanillaCount > 0, "No events routed to the vanilla PopulationNode leaf")
    assertEquals(200, rankedCount + vanillaCount)
  }

  @Test
  fun `pass 1 branch node routes to correct pools`() {
    val labeler =
      buildLabeler(
        """
      name: "Root"
      branch_node {
        branches {
          node {
            name: "PoolA"
            ranked_population_node {
              pools { population_offset: 1000 total_population: 500 }
              random_seed: "pool-a-seed"
              ranked_size: 200
              unranked_mode: DISJOINT
            }
          }
          chance: 0.5
        }
        branches {
          node {
            name: "PoolB"
            ranked_population_node {
              pools { population_offset: 5000 total_population: 500 }
              random_seed: "pool-b-seed"
              ranked_size: 300
              unranked_mode: FULL_POOL
            }
          }
          chance: 0.5
        }
        random_seed: "branch-seed"
      }
    """
      )

    val poolOffsets = mutableSetOf<Long>()
    for (i in 0 until 200) {
      val output = labeler.label(makeInput("branch-$i"), LabelingMode.POOL_IDENTITY)
      assertEquals(1, output.poolAssignmentsCount)
      assertEquals(0, output.peopleCount)
      poolOffsets.add(output.poolAssignmentsList[0].poolOffset)
    }

    assertTrue(poolOffsets.contains(1000L), "No events routed to pool A")
    assertTrue(poolOffsets.contains(5000L), "No events routed to pool B")
  }

  @Test
  fun `two pass collision free end to end`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 0 total_population: 300 }
        random_seed: "two-pass-seed"
        ranked_size: 100
        unranked_mode: DISJOINT
      }
    """
      )

    val eventCount = 100

    // Pass 1: collect pool assignments.
    val inputs = (0 until eventCount).map { makeInput("event-$it") }

    inputs.forEach { input ->
      val output = labeler.label(input, LabelingMode.POOL_IDENTITY)
      assertEquals(1, output.poolAssignmentsCount)
      assertEquals(0, output.peopleCount)
    }

    // Pass 2: inject ranks and assign VIDs.
    val vids = mutableSetOf<ULong>()
    inputs.forEachIndexed { rank, input ->
      val rankedInput =
        input.copy {
          rankAssignments += rankAssignment {
            poolOffset = 0
            localRank = rank.toLong()
          }
        }

      val output = labeler.label(rankedInput)
      assertEquals(1, output.peopleCount)
      val vid = output.peopleList[0].virtualPersonId.toULong()
      assertTrue(vid < 100uL, "Ranked VID $vid should be in [0, 100)")
      vids.add(vid)
    }

    assertEquals(eventCount, vids.size, "Collision detected in two-pass flow")
  }

  @Test
  fun `two pass is deterministic`() {
    val labeler =
      buildLabeler(
        """
      name: "TestRankedNode"
      ranked_population_node {
        pools { population_offset: 0 total_population: 200 }
        random_seed: "determinism-seed"
        ranked_size: 50
        unranked_mode: DISJOINT
      }
    """
      )

    val inputs = (0 until 50).map { makeInput("det-$it") }

    // Run two-pass twice and verify identical VIDs.
    fun runTwoPass(): List<Long> {
      return inputs.mapIndexed { rank, input ->
        val rankedInput =
          input.copy {
            rankAssignments += rankAssignment {
              poolOffset = 0
              localRank = rank.toLong()
            }
          }
        labeler.label(rankedInput).peopleList[0].virtualPersonId
      }
    }

    val run1 = runTwoPass()
    val run2 = runTwoPass()
    assertEquals(run1, run2, "Two-pass should be deterministic")
  }
}
