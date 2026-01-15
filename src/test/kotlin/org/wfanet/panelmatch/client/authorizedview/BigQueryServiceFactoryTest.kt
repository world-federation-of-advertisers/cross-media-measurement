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

package org.wfanet.panelmatch.client.authorizedview

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class BigQueryServiceFactoryTest {
  private val factory = BigQueryServiceFactory()

  @Test
  fun `getService returns non-null service`() {
    val service = factory.getService("test-project")

    assertThat(service).isNotNull()
  }

  @Test
  fun `getService returns same instance for same project ID`() {
    val projectId = "test-project"

    val service1 = factory.getService(projectId)
    val service2 = factory.getService(projectId)

    assertThat(service2).isSameInstanceAs(service1)
  }

  @Test
  fun `getService returns different instances for different project IDs`() {
    val service1 = factory.getService("project-1")
    val service2 = factory.getService("project-2")

    assertThat(service1).isNotSameInstanceAs(service2)
  }
}
