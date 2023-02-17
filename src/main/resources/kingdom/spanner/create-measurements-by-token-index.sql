-- liquibase formatted sql

-- Copyright 2023 The Cross-Media Measurement Authors
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

-- changeset sanjayvas:5 dbms:cloudspanner
DROP INDEX MeasurementsByState;

-- For querying "active" measurements after a given
-- StreamActiveComputationsContinuationToken.
-- changeset sanjayvas:6 dbms:cloudspanner
CREATE INDEX MeasurementsByContinuationToken
ON Measurements(State, UpdateTime ASC, ExternalComputationId ASC);
