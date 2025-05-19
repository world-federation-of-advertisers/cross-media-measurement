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

-- changeset tristanvuong2021:add-model-line-to-metric-calculation-specs-and-metrics dbms:postgresql
-- comment: Add 3 columns for different components of the ModelLine name to the MetricCalculationSpecs table and the Metrics table.

ALTER TABLE MetricCalculationSpecs
  ADD COLUMN CmmsModelLineName text;

ALTER TABLE Metrics
  ADD COLUMN CmmsModelLineName text;
