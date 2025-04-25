-- Copyright 2025 The Cross-Media Measurement Authors
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



CREATE TYPE ChargesTableState
AS ENUM('NOT_READY', 'BACKFILLING', 'READY');

-- Holds the state and privacyLandscapeName for all the previous (deleted) and current PrivacyCharges tables.
CREATE TABLE PrivacyChargesMetadata (
    id SERIAL PRIMARY KEY,
    state ChargesTableState,
    privacyLandscapeName TEXT,
    CreateTime TIMESTAMP NOT NULL,
    DeleteTime TIMESTAMP
);

-- Holds all the charges for an EDP.
CREATE TABLE PrivacyCharges(
  -- The Measurement Consumer these charges belong to.
  MeasurementConsumerId text NOT NULL,
  -- The Event Group these charges belong to.
  EventGroupId text NOT NULL,
  -- Day for this PrivacyBucket. DD-MM-YYYY.
  Date Date NOT NULL,
  -- A wfa.measurement.privacybudgetmanager.Charges proto capturing charges for each bucket.
  Charges BYTEA,
  -- There can be only one entry per MeasurementConsumerId, EventGroupId, Date triplet.
  PRIMARY KEY (MeasurementConsumerId, EventGroupId, Date));

-- Holds all the ledger Entries for an EDP.
CREATE TABLE LedgerEntries(
  -- The Measurement Consumer this Ledger Entry belongs to.
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