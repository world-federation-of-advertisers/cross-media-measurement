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

/**
 * This is the default value for the total amount that can be charged to a single privacy bucket.
 */
private const val MAXIMUM_PRIVACY_USAGE_PER_BUCKET = 1.0f
private const val MAXIMUM_DELTA_PER_BUCKET = 1.0e-9f

class PrivacyBudgetManager(
        val auditLog: AuditLog,
        val activePrivacyLandscape: PrivacyLandscape,
        val inactivePrivacyLandscapes: List<PrivacyLandscape>,
        val privacyLandscapeMappings: List<PrivacyLandscapeMapping>,
        private val backingStore: BackingStore,
        private val maximumPrivacyBudget: Float = MAXIMUM_PRIVACY_USAGE_PER_BUCKET,
        private val maximumTotalDelta: Float = MAXIMUM_DELTA_PER_BUCKET,
) {

    init {
        val activeLandScapeName = activePrivacyLandscape.name
        for (inactivePrivacyLandscape in inactivePrivacyLandscapes) {
            val mapping =
                    privacyLandscapeMappings
                            .filter { it.fromLandscape == inactivePrivacyLandscape.name }
                            .filter { it.toLandscape == activeLandScapeName }
            require(mapping.length == 1) {
                "There must be exactly 1 mapping from each inactive landscape to an active landscape"
            }
        }
    }

    /**
     * Charges the PBM in batch with the charges resulting from the given queries and writes the
     * successful charge operation to the audit log.
     *
     * @throws PrivacyBudgetManager exception if either the charging or audit log write operations
     * are unsuccessful.
     */
    suspend fun charge(queries: List<Query>, groupId: String): String {

        // Declare newQueries outside the use block
        val queriesWithCommitTime: List<Query>

        backingStore.startTransaction().use { context: TransactionContext ->
            val alreadyCommitedQueries = context.read(queries)
            // Filter out the queries that were already committed before.
            val queriesToCommit =
                    queries.filter { query ->
                        !alreadyCommitedQueries.map { it.referenceId }.contains(query.referenceId)
                    }

            // Get slice intended to be committed to the PBM, defined by queriesToCommit.
            val delta: Slice = getDelta(queriesToCommit)

            // Read the backing store for the rows that slice targets.
            val targettedSlice = context.read(delta.getRowKeys())

            // Check if any of the updated buckets exceed the budget and aggregate to find the slice
            // to commit.
            val sliceToCommit = checkAndAggregate(delta, targettedSlice)

            // Write aggregated slice and the selected Queries to the Backing Store.
            queriesWithCommitTime =
                    context.write(sliceToCommit, queriesToCommit) + alreadyCommitedQueries

            // Commit the transaction
            context.commit()
        }

        // Write all the given queries to the EDP owned audit log and return the audit reference.
        // The detail of writing all the given queries and not the queriesToCommit is important
        // due to the following scenario:
        //  1. PBM is charged with queries A,B,C -> charges are committed successfully but
        //     the audit log write failed - so the audit log doesn't contain A,B,C. The caller
        // shouldn't
        //     fulfill requisitions for A,B,C.
        //  2. PBM is then called at a later time with queries B,C,D -> charges are committed
        // succesfully
        //     for only D, PBM did not commit charges from B,C because they are already in the
        // backing store.
        //     In this case, all B,C,D should be written to the audit log because if you only write
        // D, then
        //     the auditor will conclude less queries are wrritten to the audit log than in the PBM.
        //
        // This brings about the following scenario:
        //  1. PBM is charged with A,B,C all commited, all successfully written to audit log.
        //  2. Then, PBM is charged with B,C,D. Again, D is committed, all B,C,D written
        // successfully
        //     to audit log.
        // In this scenario, auditor will find duplicate entries for B and C. This won't be a
        // problem
        // because in the replay scenario, the PBM code will check the references and conclude they
        // were
        // already written to the DB.
        return auditLog.write(queriesWithCommitTime, groupId)
    }

    private suspend fun getDelta(queries: List<Query>): Slice {

        val delta = Slice()

        for (query in queries) {

            // Check if the query's landscape mask is current.
            if (query.privacyLandscapeName == activePrivacyLandscape.name) {
                delta.add(
                        Filter.getBuckets(query.event_group_landscape_mask, activePrivacyLandscape),
                        query.charge
                )
            }

            // If the query's landscape is not active, then get the mapping to old landscape and
            // convert the charges
            else {
                val inactivePrivacyLandscape =
                        inactivePrivacyLandscapes.find { it.name == query.privacyLandscapeName }
                                ?: requireNotNull(null) {
                                    "Privacy landscape with name '${query.privacyLandscapeName}' not found."
                                }

                val privacyLandscapeMapping =
                        privacyLandscapeMappings.find {
                            it.fromLandscape == query.privacyLandscapeName &&
                                    it.toLandscape == activePrivacyLandscape.name
                        }
                                ?: requireNotNull(null) {
                                    "Privacy landscape mapping with not found."
                                }

                val mappedBuckets =
                        Filter.getBuckets(
                                query.event_group_landscape_mask,
                                privacyLandscapeMapping,
                                inactivePrivacyLandscape,
                                activePrivacyLandscape
                        )

                delta.add(mappedBuckets, query.charge)
            }

            return delta
        }
    }

    private suspend fun checkAndAggregate(delta: Slice, targettedSlice: Slice): Slice =
            TODO("uakyol: implement this")
}
