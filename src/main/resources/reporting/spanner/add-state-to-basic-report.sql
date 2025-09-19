-- liquibase formatted sql

-- Copyright 2025 The Cross-Media Measurement Authors
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

-- changeset tristanvuong2021:2 dbms:cloudspanner
-- comment: Add State and Report ID columns to BasicReport

ALTER TABLE BasicReports
-- org.wfanet.measurement.internal.reporting.v2.BasicReport.State protobuf enum
-- encoded as an integer.
ADD COLUMN State INT64 NOT NULL DEFAULT (4);
