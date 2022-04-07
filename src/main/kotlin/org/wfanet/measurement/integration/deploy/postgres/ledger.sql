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
   id serial PRIMARY KEY,
   -- Which Measurement Consumer this PrivacyBucket belongs to.
   measurementConsumerId integer NOT NULL,
   -- Day for this PrivacyBucket. DD-MM-YYYY.
   date varchar(10) NOT NULL,
   -- Age for this PrivacyBucket.
   age integer NOT NULL,
   -- Gender for this PrivacyBucket.
   gender varchar(10) NOT NULL,
   -- Vid of this PrivacyBucket.
   vid real NOT NULL,
   -- Delta for the charge of this ledger entry.
   delta real NOT NULL,
   -- Epsilon for the charge of this ledger entry.
   epsilon real NOT NULL,
   -- How many times this charge is applied to this Privacy Bucket.
   repetition_count integer NOT NULL
);
-- Used to query a privacy bucket efficiently.
CREATE UNIQUE INDEX by_bucket ON LedgerEntries (measurementConsumerId, date, age, gender, vid);
-- Used to update the repetition_count for a delta and epsilon pair efficiently.
CREATE UNIQUE INDEX by_charge ON LedgerEntries (delta, epsilon);
