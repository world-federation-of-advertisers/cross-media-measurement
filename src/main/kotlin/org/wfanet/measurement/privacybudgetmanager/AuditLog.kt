/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.privacybudgetmanager

/** Manages interaction with EDP owned persistent audit layer that captures charged Pbm [Query]s. */
interface AuditLog {
  /**
   * Writes the list of Queries and the groupId to the audit log .
   *
   * @param queries: List of [Query] that is charged to the PBM to be logged.
   * @param groupId: Identifier used to id this list of [Query]s.
   * @returns the identifier in the audit log that can be used to retrieve this group. e.g. Blob URL
   *   of the written file for a blob storage based audit log.
   * @throws PrivacyBudgetManager exception if the write operation was unsuccessful.
   */
  fun write(queries: List<Query>, groupId: String): String
}
