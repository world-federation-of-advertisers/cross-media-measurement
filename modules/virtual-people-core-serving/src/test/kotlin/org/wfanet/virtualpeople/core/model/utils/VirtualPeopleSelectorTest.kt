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

package org.wfanet.virtualpeople.core.model.utils

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.PopulationNodeKt.virtualPersonPool
import org.wfanet.virtualpeople.common.populationNode

private const val SEED_NUMBER = 10000

@RunWith(JUnit4::class)
class VirtualPeopleSelectorTest {

  @Test
  fun `get virtual person id successfully`() {
    val populationNode = populationNode {
      pools.add(
        virtualPersonPool {
          populationOffset = 10
          totalPopulation = 3
        }
      )
      pools.add(
        virtualPersonPool {
          populationOffset = (ULong.MAX_VALUE - 100UL).toLong()
          totalPopulation = 3
        }
      )
      pools.add(
        virtualPersonPool {
          populationOffset = 20
          totalPopulation = 4
        }
      )
    }
    val selector = VirtualPeopleSelector.build(populationNode.poolsList)

    val idCounts = mutableMapOf<ULong, Int>()

    (0 until SEED_NUMBER).forEach {
      val id = selector.getVirtualPersonId(it.toULong())
      idCounts[id] = idCounts.getOrDefault(id, 0) + 1
    }

    assertEquals(10, idCounts.size)

    /**
     * The expected count for getting a given virtual person id is 1 / 10 * [SEED_NUMBER]= 1000. We
     * compare to the exact values to make sure the kotlin and c++ implementations behave the same.
     */
    assertEquals(993, idCounts[10UL])
    assertEquals(997, idCounts[11UL])
    assertEquals(994, idCounts[12UL])
    assertEquals(980, idCounts[20UL])
    assertEquals(1027, idCounts[21UL])
    assertEquals(979, idCounts[22UL])
    assertEquals(1020, idCounts[23UL])
    assertEquals(1000, idCounts[ULong.MAX_VALUE - 100UL])
    assertEquals(1015, idCounts[ULong.MAX_VALUE - 99UL])
    assertEquals(995, idCounts[ULong.MAX_VALUE - 98UL])
  }

  @Test
  fun `invalid pools should fail`() {
    val populationNode = populationNode {
      pools.add(
        virtualPersonPool {
          populationOffset = 10
          totalPopulation = 0
        }
      )
      pools.add(
        virtualPersonPool {
          populationOffset = 30
          totalPopulation = 0
        }
      )
      pools.add(
        virtualPersonPool {
          populationOffset = 20
          totalPopulation = 0
        }
      )
    }
    val exception =
      assertFailsWith<IllegalStateException> {
        VirtualPeopleSelector.build(populationNode.poolsList)
      }
    assertTrue(exception.message!!.contains("The total population of the pools is 0"))
  }
}
