// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.db

/**
 * Wrapper interface for the Kingdom's relational database.
 *
 * TODO: this interface is in a transitional state. As part of a larger process to split this
 * interface into several smaller, more focused ones, functionality has been pulled into the
 * super-interfaces. The next step will be to transition clients to use the super-interfaces, then
 * to delete this interface.
 */
interface KingdomRelationalDatabase : ReportDatabase, RequisitionDatabase, LegacySchedulingDatabase
