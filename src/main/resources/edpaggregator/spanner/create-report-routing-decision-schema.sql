-- liquibase formatted sql

-- Copyright 2026 The Cross-Media Measurement Authors
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

-- changeset marcopremier:23 dbms:cloudspanner

START BATCH DDL;

CREATE TABLE ReportRoutingDecision (
  -- DataProvider that owns the Report routing decision.
  DataProviderResourceId STRING(63) NOT NULL,
  -- Full Reporting Report resource name.
  Report STRING(512) NOT NULL,
  -- Immutable original closed-open bounding interval of the Report.
  ReportIntervalStartTime TIMESTAMP NOT NULL,
  ReportIntervalEndTime TIMESTAMP NOT NULL,
  -- Advertiser-visible model line in the Report's Requisitions.
  ExternalModelLine STRING(512) NOT NULL,
  -- Internal model line selected for every WorkItem of the Report.
  SelectedModelLine STRING(512) NOT NULL,
  -- UTC cutover instant used when the decision was created. Null for identity or legacy mapping.
  CutoverTime TIMESTAMP,
  -- Time when the decision was first persisted.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (DataProviderResourceId, Report);

RUN BATCH;
