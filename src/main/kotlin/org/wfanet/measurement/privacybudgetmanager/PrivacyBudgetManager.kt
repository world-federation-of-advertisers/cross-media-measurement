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

import org.wfanet.measurement.eventdataprovider.noiser.DpParams

/**
 * This is the default value for the total amount that can be charged to a single privacy bucket.
 */
private const val MAXIMUM_PRIVACY_USAGE_PER_BUCKET = 1.0f
private const val MAXIMUM_DELTA_PER_BUCKET = 1.0e-9f

class PrivacyBudgetManager(
  val filter: PrivacyBucketFilter,
  val auditLog: AuditLog,
  val activePrivacyLandscape: PrivacyLandscape,
  val privacyLandscapeMappings: Map<PrivacyLandscape, PrivacyLandscapeMapping>,
  private val backingStore: PrivacyBudgetBackingStore,
  private val maximumPrivacyBudget: Float = MAXIMUM_PRIVACY_USAGE_PER_BUCKET,
  private val maximumTotalDelta: Float = MAXIMUM_DELTA_PER_BUCKET,
) {

    suspend fun getDelta(queries : List<Query>): Delta {

        val delta = Delta()
        
        for(query in queries){
            // Check if the query's landscape mask is current.
            if(query.privacyLandscapeName == activePrivacyLandscape.name){
                delta.add(filter.getBuckets(query.event_group_landscape_mask, activePrivacyLandscape), query.charge)
            }
            // If the query's landscape is not current, then get the mapping to old landscape and convert the charges
            else{
                val privacyLandscapeMapping = privacyLandscapeMappings.getOrElse(query.privacyLandscapeName) {
                    throw IllegalStateException("PrivacyLandscapeMappings do not contain a mappping for ${query.privacyLandscapeName}")
                }

                val mappedBuckets = filter.getBuckets(query.event_group_landscape_mask, privacyLandscapeMapping, activePrivacyLandscape)

                delta.add(mappedBuckets, query.charge)
            }
            
            return delta
        }
    }

    suspend fun charge(queries : List<Query>, groupId: String) : String {

        // Get delta that is intended to be committed to the PBM, defined by queries.
        val delta: Delta = getDelta(queries) 

        backingStore.startTransaction().use { context: TransactionContext ->

            // Read the backing store for the rows those buckets target.
            val targettedRows = context.read(delta.getRowKeys())
            
            // Extract the state for the actual buckets from those rows.
            val targettedBucketsState = getDeltaToCommit(targettedRows, delta.getBuckets())

            // Check if any of the updated buckets exceed the budget and aggregate to find the rows to commit. 
            val (chargesToCommit, queriesToCommit) = checkAndAggregate(context, delta, targettedBucketsState)

            // Write Delta and the selected Queries to the Backing Store.
            context.write(rowsToCommit, queriesToCommit)

            // Commit the transaction
            context.commit()
        }

        
        // Write the commited queries to the EDP owned audit log and return the audit reference.
        return auditLog.write(queriesToCommit, groupId)
    }  
}
