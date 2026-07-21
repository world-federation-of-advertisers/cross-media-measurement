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
import org.wfanet.virtualpeople.common.PopulationNodeKt.virtualPersonPool

private const val FINGERPRINT_NUMBER = 10000L

@RunWith(JUnit4::class)
class PopulationNodeImplTest {

  @Test
  fun `distribution of the applied virtual people ids should match`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
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
        randomSeed = "TestRandomSeed"
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
     * result should be around 0.1 * FINGERPRINT_NUMBER = 1000
     */
    assertEquals(10, idCounts.size)
    assertEquals(1076, idCounts[10UL])
    assertEquals(990, idCounts[11UL])
    assertEquals(975, idCounts[12UL])
    assertEquals(991, idCounts[20UL])
    assertEquals(1010, idCounts[21UL])
    assertEquals(1005, idCounts[22UL])
    assertEquals(1005, idCounts[23UL])
    assertEquals(985, idCounts[ULong.MAX_VALUE - 100UL])
    assertEquals(982, idCounts[ULong.MAX_VALUE - 99UL])
    assertEquals(981, idCounts[ULong.MAX_VALUE - 98UL])
  }

  @Test
  fun `apply no labels`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }
    val node = ModelNode.build(config)

    val input = labelerEvent { actingFingerprint = FINGERPRINT_NUMBER }.toBuilder()
    node.apply(input)

    val expectEvent = labelerEvent {
      virtualPersonActivities.add(virtualPersonActivity { virtualPersonId = 10 })
      actingFingerprint = 10000
    }

    ProtoTruth.assertThat(expectEvent).isEqualTo(input.build())
  }

  @Test
  fun `apply with label`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }
    val node = ModelNode.build(config)

    val input =
      labelerEvent {
          label = personLabelAttributes {
            demo = demoBucket {
              gender = Gender.GENDER_FEMALE
              age = ageRange {
                minAge = 25
                maxAge = 1000
              }
            }
          }
          actingFingerprint = 10000
        }
        .toBuilder()
    node.apply(input)

    val expectEvent = labelerEvent {
      virtualPersonActivities.add(
        virtualPersonActivity {
          virtualPersonId = 10
          label = personLabelAttributes {
            demo = demoBucket {
              gender = Gender.GENDER_FEMALE
              age = ageRange {
                minAge = 25
                maxAge = 1000
              }
            }
          }
        }
      )
      label = personLabelAttributes {
        demo = demoBucket {
          gender = Gender.GENDER_FEMALE
          age = ageRange {
            minAge = 25
            maxAge = 1000
          }
        }
      }
      actingFingerprint = 10000
    }

    ProtoTruth.assertThat(expectEvent).isEqualTo(input.build())
  }

  @Test
  fun `apply with single quantum label`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }
    val node = ModelNode.build(config)

    val input =
      labelerEvent {
          quantumLabels = independentQuantumLabels {
            quantumLabels.add(
              quantumLabel {
                labels.add(
                  personLabelAttributes {
                    demo = demoBucket {
                      gender = Gender.GENDER_FEMALE
                      age = ageRange {
                        minAge = 25
                        maxAge = 1000
                      }
                    }
                  }
                )
                labels.add(
                  personLabelAttributes {
                    demo = demoBucket {
                      gender = Gender.GENDER_MALE
                      age = ageRange {
                        minAge = 25
                        maxAge = 1000
                      }
                    }
                  }
                )
                probabilities.add(1.0)
                probabilities.add(0.0)
                seed = "CollapseSeed"
              }
            )
          }
          actingFingerprint = 10000
        }
        .toBuilder()
    node.apply(input)

    val expectEvent = labelerEvent {
      virtualPersonActivities.add(
        virtualPersonActivity {
          virtualPersonId = 10
          label = personLabelAttributes {
            demo = demoBucket {
              gender = Gender.GENDER_FEMALE
              age = ageRange {
                minAge = 25
                maxAge = 1000
              }
            }
          }
        }
      )
      quantumLabels = independentQuantumLabels {
        quantumLabels.add(
          quantumLabel {
            labels.add(
              personLabelAttributes {
                demo = demoBucket {
                  gender = Gender.GENDER_FEMALE
                  age = ageRange {
                    minAge = 25
                    maxAge = 1000
                  }
                }
              }
            )
            labels.add(
              personLabelAttributes {
                demo = demoBucket {
                  gender = Gender.GENDER_MALE
                  age = ageRange {
                    minAge = 25
                    maxAge = 1000
                  }
                }
              }
            )
            probabilities.add(1.0)
            probabilities.add(0.0)
            seed = "CollapseSeed"
          }
        )
      }
      actingFingerprint = 10000
    }

    ProtoTruth.assertThat(expectEvent).isEqualTo(input.build())
  }

  @Test
  fun `apply with multiple quantum labels`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }
    val node = ModelNode.build(config)

    val input =
      labelerEvent {
          quantumLabels = independentQuantumLabels {
            quantumLabels.add(
              quantumLabel {
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
                )
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } }
                )
                probabilities.add(1.0)
                probabilities.add(0.0)
                seed = "CollapseSeed"
              }
            )
            quantumLabels.add(
              quantumLabel {
                labels.add(
                  personLabelAttributes {
                    demo = demoBucket {
                      age = ageRange {
                        minAge = 25
                        maxAge = 1000
                      }
                    }
                  }
                )
                labels.add(
                  personLabelAttributes {
                    demo = demoBucket {
                      age = ageRange {
                        minAge = 1
                        maxAge = 24
                      }
                    }
                  }
                )
                probabilities.add(1.0)
                probabilities.add(0.0)
                seed = "CollapseSeed"
              }
            )
          }
          actingFingerprint = 10000
        }
        .toBuilder()
    node.apply(input)

    val expectEvent = labelerEvent {
      virtualPersonActivities.add(
        virtualPersonActivity {
          virtualPersonId = 10
          label = personLabelAttributes {
            demo = demoBucket {
              gender = Gender.GENDER_FEMALE
              age = ageRange {
                minAge = 25
                maxAge = 1000
              }
            }
          }
        }
      )
      quantumLabels = independentQuantumLabels {
        quantumLabels.add(
          quantumLabel {
            labels.add(
              personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
            )
            labels.add(personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } })
            probabilities.add(1.0)
            probabilities.add(0.0)
            seed = "CollapseSeed"
          }
        )
        quantumLabels.add(
          quantumLabel {
            labels.add(
              personLabelAttributes {
                demo = demoBucket {
                  age = ageRange {
                    minAge = 25
                    maxAge = 1000
                  }
                }
              }
            )
            labels.add(
              personLabelAttributes {
                demo = demoBucket {
                  age = ageRange {
                    minAge = 1
                    maxAge = 24
                  }
                }
              }
            )
            probabilities.add(1.0)
            probabilities.add(0.0)
            seed = "CollapseSeed"
          }
        )
      }
      actingFingerprint = 10000
    }

    ProtoTruth.assertThat(expectEvent).isEqualTo(input.build())
  }

  @Test
  fun `apply with multiple quantum labels override`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }
    val node = ModelNode.build(config)

    /** The collapsed quantum label can override existing collapsed quantum labels. */
    val input =
      labelerEvent {
          quantumLabels = independentQuantumLabels {
            quantumLabels.add(
              quantumLabel {
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
                )
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } }
                )
                probabilities.add(1.0)
                probabilities.add(0.0)
                seed = "CollapseSeed"
              }
            )
            quantumLabels.add(
              quantumLabel {
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
                )
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } }
                )
                probabilities.add(0.0)
                probabilities.add(1.0)
                seed = "CollapseSeed"
              }
            )
          }
          actingFingerprint = 10000
        }
        .toBuilder()
    node.apply(input)

    val expectEvent = labelerEvent {
      virtualPersonActivities.add(
        virtualPersonActivity {
          virtualPersonId = 10
          label = personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } }
        }
      )
      quantumLabels = independentQuantumLabels {
        quantumLabels.add(
          quantumLabel {
            labels.add(
              personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
            )
            labels.add(personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } })
            probabilities.add(1.0)
            probabilities.add(0.0)
            seed = "CollapseSeed"
          }
        )
        quantumLabels.add(
          quantumLabel {
            labels.add(
              personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
            )
            labels.add(personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } })
            probabilities.add(0.0)
            probabilities.add(1.0)
            seed = "CollapseSeed"
          }
        )
      }
      actingFingerprint = 10000
    }

    ProtoTruth.assertThat(expectEvent).isEqualTo(input.build())
  }

  @Test
  fun `apply with quantum label and classic label`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }
    val node = ModelNode.build(config)

    val input =
      labelerEvent {
          quantumLabels = independentQuantumLabels {
            quantumLabels.add(
              quantumLabel {
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
                )
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } }
                )
                probabilities.add(1.0)
                probabilities.add(0.0)
                seed = "CollapseSeed"
              }
            )
          }
          label = personLabelAttributes {
            demo = demoBucket {
              age = ageRange {
                minAge = 25
                maxAge = 1000
              }
            }
          }
          actingFingerprint = 10000
        }
        .toBuilder()
    node.apply(input)

    val expectEvent = labelerEvent {
      virtualPersonActivities.add(
        virtualPersonActivity {
          virtualPersonId = 10
          label = personLabelAttributes {
            demo = demoBucket {
              gender = Gender.GENDER_FEMALE
              age = ageRange {
                minAge = 25
                maxAge = 1000
              }
            }
          }
        }
      )
      quantumLabels = independentQuantumLabels {
        quantumLabels.add(
          quantumLabel {
            labels.add(
              personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
            )
            labels.add(personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } })
            probabilities.add(1.0)
            probabilities.add(0.0)
            seed = "CollapseSeed"
          }
        )
      }
      label = personLabelAttributes {
        demo = demoBucket {
          age = ageRange {
            minAge = 25
            maxAge = 1000
          }
        }
      }
      actingFingerprint = 10000
    }

    ProtoTruth.assertThat(expectEvent).isEqualTo(input.build())
  }

  @Test
  fun `apply with quantum label and classic label override`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }
    val node = ModelNode.build(config)

    /** The classic label can override existing collapsed quantum labels. */
    val input =
      labelerEvent {
          quantumLabels = independentQuantumLabels {
            quantumLabels.add(
              quantumLabel {
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
                )
                labels.add(
                  personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } }
                )
                probabilities.add(1.0)
                probabilities.add(0.0)
                seed = "CollapseSeed"
              }
            )
          }
          label = personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } }
          actingFingerprint = 10000
        }
        .toBuilder()
    node.apply(input)

    val expectEvent = labelerEvent {
      virtualPersonActivities.add(
        virtualPersonActivity {
          virtualPersonId = 10
          label = personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } }
        }
      )
      quantumLabels = independentQuantumLabels {
        quantumLabels.add(
          quantumLabel {
            labels.add(
              personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_FEMALE } }
            )
            labels.add(personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } })
            probabilities.add(1.0)
            probabilities.add(0.0)
            seed = "CollapseSeed"
          }
        )
      }
      label = personLabelAttributes { demo = demoBucket { gender = Gender.GENDER_MALE } }
      actingFingerprint = 10000
    }

    ProtoTruth.assertThat(expectEvent).isEqualTo(input.build())
  }

  @Test
  fun `apply existing virtual person should throw`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 10
            totalPopulation = 1
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }
    val node = ModelNode.build(config)

    val input =
      labelerEvent {
          virtualPersonActivities.add(virtualPersonActivity { virtualPersonId = 10 })
          actingFingerprint = 10000
        }
        .toBuilder()
    val exception = assertFailsWith<IllegalStateException> { node.apply(input) }
    assertTrue(
      exception.message!!.contains("virtual_person_activities should only be created in leaf node")
    )
  }

  @Test
  fun `empty population pool should assign no id`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
        pools.add(
          virtualPersonPool {
            populationOffset = 0
            totalPopulation = 0
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }
    val node = ModelNode.build(config)

    val input =
      labelerEvent {
          label = personLabelAttributes {
            demo = demoBucket {
              gender = Gender.GENDER_MALE
              age = ageRange {
                minAge = 25
                maxAge = 1000
              }
            }
          }
          actingFingerprint = 10000
        }
        .toBuilder()
    node.apply(input)

    val expectEvent = labelerEvent {
      virtualPersonActivities.add(
        virtualPersonActivity {
          label = personLabelAttributes {
            demo = demoBucket {
              gender = Gender.GENDER_MALE
              age = ageRange {
                minAge = 25
                maxAge = 1000
              }
            }
          }
        }
      )
      label = personLabelAttributes {
        demo = demoBucket {
          gender = Gender.GENDER_MALE
          age = ageRange {
            minAge = 25
            maxAge = 1000
          }
        }
      }
      actingFingerprint = 10000
    }

    ProtoTruth.assertThat(expectEvent).isEqualTo(input.build())
  }

  @Test
  fun `invalid pool should throw`() {
    val config = compiledNode {
      name = "TestPopulationNode"
      index = 1
      populationNode = populationNode {
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
            populationOffset = 29
            totalPopulation = 0
          }
        )
        randomSeed = "TestRandomSeed"
      }
    }

    val exception = assertFailsWith<IllegalStateException> { ModelNode.build(config) }
    assertTrue(exception.message!!.contains("The model is invalid"))
  }
}
