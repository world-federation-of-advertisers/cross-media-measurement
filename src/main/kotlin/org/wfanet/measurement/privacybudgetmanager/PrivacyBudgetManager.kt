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
  val privacyLandscapeMappings: Map<String, Pair<PrivacyLandscape, PrivacyLandscapeMapping>>,
  private val backingStore: BackingStore,
  private val maximumPrivacyBudget: Float = MAXIMUM_PRIVACY_USAGE_PER_BUCKET,
  private val maximumTotalDelta: Float = MAXIMUM_DELTA_PER_BUCKET,
) {

    suspend fun getDelta(queries : List<Query>): Slice {

        val delta = Slice()
        
        for(query in queries){
            // Check if the query's landscape mask is current.
            if(query.privacyLandscapeName == activePrivacyLandscape.name){
                delta.add(Filter.getBuckets(query.event_group_landscape_mask, activePrivacyLandscape), query.charge)
            }
            // If the query's landscape is not active, then get the mapping to old landscape and convert the charges
            else{
                val (inactivePrivacyLandscape, privacyLandscapeMapping) = privacyLandscapeMappings.getOrElse(query.privacyLandscapeName) {
                    throw IllegalStateException("PrivacyLandscapeMappings does not contain a mappping for ${query.privacyLandscapeName}")
                }

                val mappedBuckets = Filter.getBuckets(query.event_group_landscape_mask, privacyLandscapeMapping, inactivePrivacyLandscape, activePrivacyLandscape)

                delta.add(mappedBuckets, query.charge)
            }

            return delta
        }
    }

    suspend fun checkAndAggregate(delta: Slice,  targettedSlice: Slice): Slice = TODO("uakyol: implement this")
    
    suspend fun charge(queries : List<Query>, groupId: String) : String {

        backingStore.startTransaction().use { context: TransactionContext ->
            
            // Filter out the queries that were already committed before.
            val queriesToCommit = queries.filter{ query -> !context.read(queries).contains(query)  }

            // Get slice intended to be committed to the PBM, defined by queriesToCommit.
            val delta: Slice = getDelta(queriesToCommit) 

            // Read the backing store for the rows those buckets target.
            val targettedSlice = context.read(delta.getRowKeys())

            // Check if any of the updated buckets exceed the budget and aggregate to find the rows to commit. 
            val sliceToCommit = checkAndAggregate(delta, targettedSlice)

            // Write Delta and the selected Queries to the Backing Store.
            context.write(sliceToCommit, queriesToCommit)

            // Commit the transaction
            context.commit()
        }
        
        // Write the commited queries to the EDP owned audit log and return the audit reference.
        return auditLog.write(queriesToCommit, groupId)
    }  
}
