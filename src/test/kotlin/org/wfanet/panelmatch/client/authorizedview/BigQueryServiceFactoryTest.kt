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
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class BigQueryServiceFactoryTest {
  private val factory = BigQueryServiceFactory()

  @Test
  fun `getService creates new service for new project ID`() {
    // Given
    val projectId = "test-project-1"

    // When
    val service = factory.getService(projectId)

    // Then
    assertThat(service).isNotNull()
    assertThat(factory.cachedServiceCount).isEqualTo(1)
    assertThat(factory.cachedProjectIds).containsExactly(projectId)
  }

  @Test
  fun `getService returns cached service for same project ID`() {
    // Given
    val projectId = "test-project-2"

    // When - get service twice for same project
    val service1 = factory.getService(projectId)
    val service2 = factory.getService(projectId)

    // Then - should be the same instance
    assertThat(service2).isSameInstanceAs(service1)
    assertThat(factory.cachedServiceCount).isEqualTo(1)
    assertThat(factory.cachedProjectIds).containsExactly(projectId)
  }

  @Test
  fun `getService creates different services for different projects`() {
    // Given
    val projectId1 = "test-project-3"
    val projectId2 = "test-project-4"
    val projectId3 = "test-project-5"

    // When
    val service1 = factory.getService(projectId1)
    val service2 = factory.getService(projectId2)
    val service3 = factory.getService(projectId3)

    // Then - different services for different projects
    assertThat(service1).isNotSameInstanceAs(service2)
    assertThat(service2).isNotSameInstanceAs(service3)
    assertThat(service1).isNotSameInstanceAs(service3)
    assertThat(factory.cachedServiceCount).isEqualTo(3)
    assertThat(factory.cachedProjectIds).containsExactly(projectId1, projectId2, projectId3)
  }

  @Test
  fun `cachedProjectIds returns correct set of project IDs`() {
    // Given
    val projectIds = setOf("project-a", "project-b", "project-c", "project-d")

    // When - create services for multiple projects
    projectIds.forEach { factory.getService(it) }

    // Then
    assertThat(factory.cachedProjectIds).isEqualTo(projectIds)
    assertThat(factory.cachedServiceCount).isEqualTo(projectIds.size)
  }

  @Test
  fun `concurrent access creates only one service per project`() = runBlockingTest {
    // Given
    val projectId = "concurrent-test-project"
    val numberOfConcurrentRequests = 100

    // When - make concurrent requests for the same project
    val services =
      (1..numberOfConcurrentRequests).map { async { factory.getService(projectId) } }.awaitAll()

    // Then - all should be the same instance
    val firstService = services.first()
    assertThat(services).isNotEmpty()
    services.forEach { service -> assertThat(service).isSameInstanceAs(firstService) }
    assertThat(factory.cachedServiceCount).isEqualTo(1)
    assertThat(factory.cachedProjectIds).containsExactly(projectId)
  }

  @Test
  fun `concurrent access to different projects creates correct number of services`() =
    runBlockingTest {
      // Given
      val projectIds = (1..10).map { "concurrent-project-$it" }
      val requestsPerProject = 10

      // When - make concurrent requests for different projects
      val services =
        projectIds
          .flatMap { projectId ->
            (1..requestsPerProject).map { async { projectId to factory.getService(projectId) } }
          }
          .awaitAll()

      // Then - verify each project has exactly one service
      val servicesByProject = services.groupBy({ it.first }, { it.second })
      servicesByProject.forEach { (projectId, projectServices) ->
        // All services for the same project should be the same instance
        val firstService = projectServices.first()
        projectServices.forEach { service -> assertThat(service).isSameInstanceAs(firstService) }
      }

      assertThat(factory.cachedServiceCount).isEqualTo(projectIds.size)
      assertThat(factory.cachedProjectIds).isEqualTo(projectIds.toSet())
    }

  @Test
  fun `mixed sequential and concurrent access maintains cache consistency`() = runBlockingTest {
    // Given
    val sequentialProjects = listOf("seq-1", "seq-2", "seq-3")
    val concurrentProjects = listOf("conc-1", "conc-2", "conc-3")

    // When - mix sequential and concurrent access
    // First, sequential access
    val sequentialServices =
      sequentialProjects.map { projectId -> projectId to factory.getService(projectId) }

    // Then concurrent access
    val concurrentServices =
      concurrentProjects
        .flatMap { projectId ->
          (1..5).map { async { projectId to factory.getService(projectId) } }
        }
        .awaitAll()

    // Finally, access all projects again
    val allProjectIds = sequentialProjects + concurrentProjects
    val verificationServices =
      allProjectIds.map { projectId -> projectId to factory.getService(projectId) }

    // Then - verify consistency
    assertThat(factory.cachedServiceCount).isEqualTo(allProjectIds.size)
    assertThat(factory.cachedProjectIds).isEqualTo(allProjectIds.toSet())

    // Verify sequential services are still the same
    sequentialServices.forEach { (projectId, originalService) ->
      val currentService = factory.getService(projectId)
      assertThat(currentService).isSameInstanceAs(originalService)
    }

    // Verify concurrent services are consistent
    val concurrentServiceMap = concurrentServices.groupBy({ it.first }, { it.second })
    concurrentServiceMap.forEach { (projectId, services) ->
      val firstService = services.first()
      services.forEach { service -> assertThat(service).isSameInstanceAs(firstService) }
    }
  }

  @Test
  fun `getService handles empty project ID`() {
    // Given
    val emptyProjectId = ""

    // When
    val service = factory.getService(emptyProjectId)

    // Then - should still create a service (though it might not be functional)
    assertThat(service).isNotNull()
    assertThat(factory.cachedServiceCount).isEqualTo(1)
    assertThat(factory.cachedProjectIds).containsExactly(emptyProjectId)
  }

  @Test
  fun `getService handles project ID with special characters`() {
    // Given
    val specialProjectIds =
      listOf(
        "project-with-dashes",
        "project_with_underscores",
        "project.with.dots",
        "project123",
        "PROJECT-UPPERCASE",
      )

    // When
    val services = specialProjectIds.map { projectId -> projectId to factory.getService(projectId) }

    // Then
    assertThat(factory.cachedServiceCount).isEqualTo(specialProjectIds.size)
    assertThat(factory.cachedProjectIds).isEqualTo(specialProjectIds.toSet())

    // Verify each project has a unique service
    val uniqueServices = services.map { it.second }.toSet()
    assertThat(uniqueServices).hasSize(specialProjectIds.size)
  }

  @Test
  fun `factory maintains separate cache across instances`() {
    // Given
    val factory1 = BigQueryServiceFactory()
    val factory2 = BigQueryServiceFactory()
    val projectId = "shared-project"

    // When
    val service1 = factory1.getService(projectId)
    val service2 = factory2.getService(projectId)

    // Then - different factory instances should have separate caches
    assertThat(service1).isNotSameInstanceAs(service2)
    assertThat(factory1.cachedServiceCount).isEqualTo(1)
    assertThat(factory2.cachedServiceCount).isEqualTo(1)
    assertThat(factory1.cachedProjectIds).isEqualTo(factory2.cachedProjectIds)
  }
}
