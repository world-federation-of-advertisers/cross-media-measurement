-- liquibase formatted sql

-- Copyright 2024 The Cross-Media Measurement Authors
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

-- changeset renjijez:16 dbms:cloudspanner
-- comment: update RequisitionsByDataProviderId to remove UNIQUE constraint.
--          Besides, FulfillingDuchyId in Requisitions would be set during
--          creation, thus it won't indicate whether the Requisition is
--          fulfilled.

DROP INDEX RequisitionsByDataProviderId;

CREATE INDEX RequisitionsByDataProviderId
    ON Requisitions(MeasurementConsumerId, MeasurementId, DataProviderId);
