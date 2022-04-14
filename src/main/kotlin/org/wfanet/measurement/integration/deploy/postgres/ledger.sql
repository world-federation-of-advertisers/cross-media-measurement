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

CREATE TYPE Gender AS ENUM ('M', 'F');
CREATE TYPE AgeGroup AS ENUM ('18_34', '35_54', '55+');

-- This sequence will be referenced as `TransactionId` field at `LedgerEntries` table
-- being incremented only at the start of any transaction.
-- This will allow for transaction rollback functionality after a commit.
-- The special value of 0 is reserved for merged transactions.
CREATE SEQUENCE LedgerEntriesTransactionIdSeq AS bigint START WITH 100;

CREATE TABLE LedgerEntries (
    -- A unique key associated with this row to allow for subsequent updates and deletions.
    LedgerEntryId bigserial PRIMARY KEY,
    -- Which Measurement Consumer this PrivacyBucket belongs to.
    MeasurementConsumerId text NOT NULL,
    -- ID for the transaction this entry belongs to.
    TransactionId bigint NOT NULL,
    -- Day for this PrivacyBucket. DD-MM-YYYY.
    Date Date NOT NULL,
    -- Age for this PrivacyBucket.
    AgeGroup AgeGroup NOT NULL,
    -- Gender for this PrivacyBucket.
    Gender Gender NOT NULL,
    -- Start of the Vid range for this PrivacyBucket. Bucket vid's ranges from VidStart to VidStart + 0.1.
    VidStart real NOT NULL,
    -- Delta for the charge of this ledger entry.
    Delta real NOT NULL,
    -- Epsilon for the charge of this ledger entry.
    Epsilon real NOT NULL,
    -- How many times this charge is applied to this Privacy Bucket.
    RepetitionCount integer NOT NULL
);
-- Used to query entries efficiently to update RepetitionCount
CREATE INDEX LedgerEntriesByCharge
  ON LedgerEntries
  (MeasurementConsumerId, Date, AgeGroup, Gender, VidStart, Delta, Epsilon);
-- Used to rollback transactions
CREATE INDEX LedgerEntriesByTransaction ON LedgerEntries (TransactionId);
