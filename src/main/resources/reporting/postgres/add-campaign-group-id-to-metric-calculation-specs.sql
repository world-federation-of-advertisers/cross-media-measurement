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

-- changeset tristanvuong2021:add-campaign-group-to-metric-calculation-specs dbms:postgresql
-- comment: Add a CampaignGroupId column to the MetricCalculationSpecs table.
ALTER TABLE MetricCalculationSpecs
  ADD COLUMN CampaignGroupId bigint,
  ADD FOREIGN KEY (MeasurementConsumerId, CampaignGroupId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId);

-- changeset tristanvuong2021:add-metric-calculation-specs-campaign-group-id-index dbms:postgresl
-- comment: For finding metric calculation specs by campaign group id
CREATE INDEX metric_calculation_specs_campaign_group_id_index
  ON MetricCalculationSpecs (MeasurementConsumerId, CampaignGroupId);
