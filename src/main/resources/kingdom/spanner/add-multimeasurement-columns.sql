-- liquibase formatted sql

-- Copyright 2026 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset raimundoltdf:37 dbms:cloudspanner
-- comment: Add VectorIndex to Requisitions, so that a multimeasurement may hold one Requisition per vector, and PlaintextResult to Measurements.

START BATCH DDL;

-- The index of the multimeasurement base vector the Requisition fulfills, or NULL for any other
-- Measurement.
ALTER TABLE Requisitions ADD COLUMN VectorIndex INT64;

-- Not NULL_FILTERED, so that NULL VectorIndex values collide and a Measurement that is not a
-- multimeasurement still has at most one Requisition per DataProvider.
CREATE UNIQUE INDEX RequisitionsByDataProviderIdAndVectorIndex
  ON Requisitions(MeasurementConsumerId, MeasurementId, DataProviderId, VectorIndex);

-- Serialized wfa.measurement.internal.kingdom.Measurement.ResultInfo of a succeeded multimeasurement,
-- whose Result is neither signed nor encrypted, or NULL for any other Measurement. It has no
-- aggregator certificate, so it is not in DuchyMeasurementResults, and is kept out of
-- MeasurementDetails, which every Measurement update rewrites. BYTES rather than a PROTO column,
-- because Measurement.ResultInfo belongs to the internal API and is not in the Details proto
-- bundle.
ALTER TABLE Measurements ADD COLUMN PlaintextResult BYTES(MAX);

DROP INDEX RequisitionsByDataProviderId;

RUN BATCH;
