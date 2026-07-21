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

import org.wfanet.virtualpeople.common.PopulationNode.VirtualPersonPool

// Selects an id from a set of virtual person pools.
//
// The selection is based on consistent hashing.
//
// The possible id space is the combination of all pools. E.g.
// If the input pools are
// [
//   {
//     population_offset: 100
//     total_population: 10
//   },
//   {
//     population_offset: 200
//     total_population: 20
//   },
//   {
//     population_offset: 300
//     total_population: 30
//   }
// ]
// The possible id space is [100, 109], [200, 219], [300, 329].
//

/**
 * @param totalPopulation The sum of total population of all pools. Required for hashing.
 * @param pools Stores the required information to compute the virtual person id after hashing.
 */
class VirtualPeopleSelector
private constructor(
  private val totalPopulation: ULong,
  private val pools: List<VirtualPersonIdPool>
) {

  /**
   * Selects and returns an id from the virtual person pools, using consistent hashing based on the
   * random_seed.
   */
  fun getVirtualPersonId(randomSeed: ULong): ULong {
    val populationIndex = jumpConsistentHash(randomSeed, totalPopulation.toInt()).toULong()
    /** Gets the first pool with population_index_offset larger than population_index. */
    val index = pools.indexOfFirst { populationIndex < it.populationIndexOffset }
    /** Pick the virtualPeopleId from the previous pool. */
    val preIndex = if (index == -1) pools.size - 1 else index - 1
    return populationIndex - pools[preIndex].populationIndexOffset +
      pools[preIndex].virtualPeopleIdOffset
  }

  companion object {

    // Sets up total_population_ and pools_.
    // For example, if the input pools are
    // [
    //   {
    //     population_offset: 100
    //     total_population: 10
    //   },
    //   {
    //     population_offset: 200
    //     total_population: 20
    //   },
    //   {
    //     population_offset: 300
    //     total_population: 30
    //   }
    // ]
    // The total_population_ will be 60 = 10 + 20 + 30.
    // And the pools_ will be
    // [
    //   {
    //     virtual_people_id_offset: 100
    //     population_index_offset: 0
    //   },
    //   {
    //     virtual_people_id_offset: 200
    //     population_index_offset: 10
    //   },
    //   {
    //     virtual_people_id_offset: 300
    //     population_index_offset: 30 = 10 + 20
    //   }
    // ]
    fun build(pools: List<VirtualPersonPool>): VirtualPeopleSelector {
      var totalPopulation = 0UL
      val compiledPools = mutableListOf<VirtualPersonIdPool>()
      pools.forEach {
        /** Only process non-empty pool. */
        if (it.totalPopulation.toULong() > 0UL) {
          compiledPools.add(VirtualPersonIdPool(it.populationOffset.toULong(), totalPopulation))
          totalPopulation += it.totalPopulation.toULong()
        }
      }
      if (totalPopulation == 0UL) {
        error("The total population of the pools is 0. The model is invalid.")
      }
      return VirtualPeopleSelector(totalPopulation, compiledPools)
    }
  }
}

/**
 * Includes the information of a virtual person pool.
 *
 * The first virtual person id of this pool is [virtualPeopleIdOffset]. The first population index
 * of this pool is [populationIndexOffset]. [populationIndexOffset] equals to the accumulated
 * population of all previous pools.
 */
data class VirtualPersonIdPool(val virtualPeopleIdOffset: ULong, val populationIndexOffset: ULong)
