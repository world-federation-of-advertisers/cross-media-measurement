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

-- changeset raimundoltdf:17 dbms:cloudspanner
-- comment: Create ReportComputations table for multimeasurement BasicReports

START BATCH DDL;

-- One multimeasurement a BasicReport is computed by: one per impression
-- qualification filter the BasicReport names.
CREATE TABLE ReportComputations (
  MeasurementConsumerId INT64 NOT NULL,
  BasicReportId INT64 NOT NULL,
  -- The ID of the ImpressionQualificationFilter every base vector of the
  -- multimeasurement qualifies under, or -1 for the custom filter.
  ImpressionQualificationFilterId INT64 NOT NULL,
  -- Request ID of the CMMS CreateMeasurement call, set before the call so that
  -- a retry returns the existing Measurement.
  CmmsCreateMeasurementRequestId STRING(MAX) NOT NULL,
  -- ID of the Measurement from the CMMS public API. NULL until created.
  CmmsMeasurementId STRING(MAX),
  -- Serialized wfa.measurement.api.v2alpha.Measurement.Multimeasurement whose
  -- RequisitionSpecs have no measurement_public_key.
  Multimeasurement BYTES(MAX) NOT NULL,
  -- Serialized wfa.measurement.internal.reporting.v2.ResultGroupQueryMapping
  -- messages, one per Query of the multimeasurement.
  --
  -- TODO(world-federation-of-advertisers/cross-media-measurement#4532): Switch
  -- to an ARRAY of PROTO column type once the proto bundle includes
  -- ResultGroupQueryMapping.
  ResultGroupQueryMappings ARRAY<BYTES(MAX)> NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (MeasurementConsumerId, BasicReportId, ImpressionQualificationFilterId),
  INTERLEAVE IN PARENT BasicReports ON DELETE CASCADE;

CREATE UNIQUE INDEX ReportComputationsByCmmsCreateMeasurementRequestId
  ON ReportComputations(MeasurementConsumerId, CmmsCreateMeasurementRequestId);

RUN BATCH;
