-- Copyright 2022 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE LedgerEntries (
   -- A unique key associated with this row to allow for subsequent updates and deletions.
   LedgerEntryId serial PRIMARY KEY,
   -- Which Measurement Consumer this PrivacyBucket belongs to.
   MeasurementConsumerId integer NOT NULL,
   -- Day for this PrivacyBucket. DD-MM-YYYY.
   Date varchar(10) NOT NULL,
   -- Age for this PrivacyBucket.
   Age integer NOT NULL,
   -- Gender for this PrivacyBucket.
   Gender varchar(10) NOT NULL,
   -- Vid of this PrivacyBucket.
   Vid real NOT NULL,
   -- Delta for the charge of this ledger entry.
   Delta real NOT NULL,
   -- Epsilon for the charge of this ledger entry.
   Epsilon real NOT NULL,
   -- How many times this charge is applied to this Privacy Bucket.
   RepetitionCount integer NOT NULL
);
-- Used to query a privacy bucket efficiently.
CREATE UNIQUE INDEX LedgerEntriesByBucket ON LedgerEntries (MeasurementConsumerId, Date, Age, Gender, Vid);
-- Used to update the RepetitionCount for a Delta and Epsilon pair efficiently.
CREATE UNIQUE INDEX LedgerEntriesByCharge ON LedgerEntries (Delta, Epsilon);
