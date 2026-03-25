/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.service.api.v2alpha

import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.ProtocolConfig.NoiseMechanism as InternalNoiseMechanism
import org.wfanet.measurement.internal.kingdom.dataProviderCapabilities as internalDataProviderCapabilities

@RunWith(JUnit4::class)
class SelectNoiseMechanismsTest {

  @Test
  fun `throws when EDP has empty allowed noise mechanisms`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(internalDataProviderCapabilities {}, internalDataProviderCapabilities {})

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    }
  }

  @Test
  fun `throws when capabilities list is empty`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, emptyList())
    }
  }

  @Test
  fun `intersects server mechanisms with single EDP capabilities`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        }
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(
      setOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN),
      result.toSet(),
    )
  }

  @Test
  fun `intersects across multiple EDPs`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `intersects across three EDPs to single common mechanism`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `throws when one EDP has empty allowed list among explicit EDPs`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderCapabilities {},
      )

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    }
  }

  @Test
  fun `throws when all EDPs have empty allowed lists`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val capabilities =
      listOf(
        internalDataProviderCapabilities {},
        internalDataProviderCapabilities {},
        internalDataProviderCapabilities {},
      )

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    }
  }

  @Test
  fun `server has single mechanism that all EDPs support`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_GAUSSIAN), result)
  }

  @Test
  fun `throws when intersection is empty`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.CONTINUOUS_LAPLACE)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
      }
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.status.code)
  }

  @Test
  fun `throws when two EDPs have disjoint capabilities`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val capabilities =
      listOf(
        internalDataProviderCapabilities { allowedNoiseMechanisms += InternalNoiseMechanism.NONE },
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    assertFailsWith<StatusRuntimeException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    }
  }

  @Test
  fun `throws when EDP capability not in server mechanisms`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.NONE)
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        }
      )

    assertFailsWith<StatusRuntimeException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    }
  }

  @Test
  fun `throws when empty list EDP with server only having NONE`() {
    val serverMechanisms = listOf(InternalNoiseMechanism.NONE)
    val capabilities = listOf(internalDataProviderCapabilities {})

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    }
  }

  @Test
  fun `throws when empty list EDP mixed with explicit EDP`() {
    val serverMechanisms =
      listOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN)
    val capabilities =
      listOf(
        internalDataProviderCapabilities { allowedNoiseMechanisms += InternalNoiseMechanism.NONE },
        internalDataProviderCapabilities {},
      )

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    }
  }

  @Test
  fun `returns both NONE and CONTINUOUS_GAUSSIAN when both are common`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(
      setOf(InternalNoiseMechanism.NONE, InternalNoiseMechanism.CONTINUOUS_GAUSSIAN),
      result.toSet(),
    )
  }

  @Test
  fun `throws when server noise mechanisms list is empty`() {
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        }
      )

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(emptyList(), capabilities)
    }
  }

  @Test
  fun `intersects when EDP specifies CONTINUOUS_LAPLACE`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val capabilities =
      listOf(
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_LAPLACE
        },
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_LAPLACE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
      )

    val result = MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    assertEquals(listOf(InternalNoiseMechanism.CONTINUOUS_LAPLACE), result)
  }

  @Test
  fun `throws when mixed empty and restrictive capabilities`() {
    val serverMechanisms =
      listOf(
        InternalNoiseMechanism.NONE,
        InternalNoiseMechanism.CONTINUOUS_LAPLACE,
        InternalNoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    val capabilities =
      listOf(
        internalDataProviderCapabilities {},
        internalDataProviderCapabilities {
          allowedNoiseMechanisms += InternalNoiseMechanism.NONE
          allowedNoiseMechanisms += InternalNoiseMechanism.CONTINUOUS_GAUSSIAN
        },
        internalDataProviderCapabilities {},
      )

    assertFailsWith<IllegalArgumentException> {
      MeasurementsService.selectNoiseMechanisms(serverMechanisms, capabilities)
    }
  }
}
