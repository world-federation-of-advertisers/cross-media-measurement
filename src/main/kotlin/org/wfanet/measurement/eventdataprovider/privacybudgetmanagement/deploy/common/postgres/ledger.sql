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

CREATE TYPE Gender
AS ENUM('M', 'F');

CREATE TYPE AgeGroup
AS ENUM('18_34', '35_54', '55+');

-- TODO(@uakyol): consider normalizing this table by splitting (Delta, Epsilon) pair to other table
-- TODO(@uakyol): migrate this to Liquibase changelog format.
CREATE TABLE PrivacyBucketAcdpCharges(
  -- Which Measurement Consumer this PrivacyBucket belongs to.
  MeasurementConsumerId text NOT NULL,
  -- Day for this PrivacyBucket. DD-MM-YYYY.
  Date Date NOT NULL,
  -- Age for this PrivacyBucket.
  AgeGroup AgeGroup NOT NULL,
  -- Gender for this PrivacyBucket.
  Gender Gender NOT NULL,
  -- Start of the Vid range for this PrivacyBucket. Bucket vid's ranges from VidStart to VidStart + 0.1.
  VidStart real NOT NULL,
  -- Rho for the AcdpCharge of this ledger entry.
  Rho real NOT NULL,
  -- Theta for the AcdpCharge of this ledger entry.
  Theta real NOT NULL,
  -- Used to query entries efficiently to update rho and theta.
  PRIMARY KEY (MeasurementConsumerId, Date, AgeGroup, Gender, VidStart)
  );

CREATE TABLE LedgerEntries(
  -- Which Measurement Consumer this Ledger Entry belongs to.
  MeasurementConsumerId text NOT NULL,
  -- ID from an external system that uniquely identifies the source all charges in a transaction
  -- for a given MeasurementConsumer.
  ReferenceId text NOT NULL,
  -- Whether or not the charge is a refund.
  IsRefund Boolean NOT NULL,
  -- Time when the row was inserted.
  CreateTime TIMESTAMP NOT NULL);

-- Used to query references quickly
CREATE
  INDEX LedgerEntriesByReferenceId
ON LedgerEntries(MeasurementConsumerId, ReferenceId);


-- TODO(@uakyol): consider adding a table that links LedgerEntries to BalanceEntries for
-- ad hoc queries
