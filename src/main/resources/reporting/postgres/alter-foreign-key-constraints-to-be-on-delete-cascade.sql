-- liquibase formatted sql

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

-- Postgres database schema for the Reporting server.
--
-- Table hierarchy:
--   Root
--   ├── Reports
--   │   ├── TimeIntervals
--   │   ├── PeriodicTimeIntervals
--   │   ├── Metrics
--   │       └── NamedSetOperations
--   │           ├── SetOperations
--   │           └── MeasurementCalculations
--   │               └── WeightedMeasurements
--   │   └── ReportMeasurements
--   ├── Measurements
--   └── ReportingSets
--       └── ReportingSetEventGroups

-- changeset tristanvuong2021:alter-time-intervals-table-foreign-key-reports dbms:postgresql
ALTER TABLE TimeIntervals
  DROP CONSTRAINT timeintervals_measurementconsumerreferenceid_reportid_fkey,
  ADD CONSTRAINT timeintervals_measurementconsumerreferenceid_reportid_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId)
    REFERENCES Reports(MeasurementConsumerReferenceId, ReportId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-periodic-time-intervals-table-foreign-key-reports dbms:postgresql
ALTER TABLE PeriodicTimeIntervals
  DROP CONSTRAINT periodictimeintervals_measurementconsumerreferenceid_repor_fkey,
  ADD CONSTRAINT periodictimeintervals_measurementconsumerreferenceid_repor_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId)
    REFERENCES Reports(MeasurementConsumerReferenceId, ReportId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-metrics-table-foreign-key-reports dbms:postgresql
ALTER TABLE Metrics
  DROP CONSTRAINT metrics_measurementconsumerreferenceid_reportid_fkey,
  ADD CONSTRAINT metrics_measurementconsumerreferenceid_reportid_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId)
    REFERENCES Reports(MeasurementConsumerReferenceId, ReportId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-report-measurements-table-foreign-key-reports dbms:postgresql
ALTER TABLE ReportMeasurements
  DROP CONSTRAINT reportmeasurements_measurementconsumerreferenceid_reportid_fkey,
  ADD CONSTRAINT reportmeasurements_measurementconsumerreferenceid_reportid_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId)
    REFERENCES Reports(MeasurementConsumerReferenceId, ReportId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-report-measurements-table-foreign-key-measurements dbms:postgresql
ALTER TABLE ReportMeasurements
  DROP CONSTRAINT reportmeasurements_measurementconsumerreferenceid_measurem_fkey,
  ADD CONSTRAINT reportmeasurements_measurementconsumerreferenceid_measurem_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, MeasurementReferenceId)
    REFERENCES Measurements(MeasurementConsumerReferenceId, MeasurementReferenceId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-reporting-set-event-groups-table-foreign-key-reporting-sets dbms:postgresql
ALTER TABLE ReportingSetEventGroups
  DROP CONSTRAINT reportingseteventgroups_measurementconsumerreferenceid_rep_fkey,
  ADD CONSTRAINT reportingseteventgroups_measurementconsumerreferenceid_rep_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerReferenceId, ReportingSetId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-set-operations-table-foreign-key-reporting-sets dbms:postgresql
ALTER TABLE SetOperations
  DROP CONSTRAINT setoperations_measurementconsumerreferenceid_lefthandrepor_fkey,
  DROP CONSTRAINT setoperations_measurementconsumerreferenceid_righthandrepo_fkey,
  ADD CONSTRAINT setoperations_measurementconsumerreferenceid_lefthandrepor_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, LeftHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerReferenceId, ReportingSetId)
    ON DELETE CASCADE,
  ADD CONSTRAINT setoperations_measurementconsumerreferenceid_righthandrepo_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, RightHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerReferenceId, ReportingSetId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-set-operations-table-foreign-key-metrics dbms:postgresql
ALTER TABLE SetOperations
  DROP CONSTRAINT setoperations_measurementconsumerreferenceid_reportid_metr_fkey,
  ADD CONSTRAINT setoperations_measurementconsumerreferenceid_reportid_metr_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId, MetricId)
    REFERENCES Metrics(MeasurementConsumerReferenceId, ReportId, MetricId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-named-set-operations-table-foreign-key-metrics dbms:postgresql
ALTER TABLE NamedSetOperations
  DROP CONSTRAINT namedsetoperations_measurementconsumerreferenceid_reportid_fkey,
  ADD CONSTRAINT namedsetoperations_measurementconsumerreferenceid_reportid_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId, MetricId)
    REFERENCES Metrics(MeasurementConsumerReferenceId, ReportId, MetricId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-named-set-operations-table-foreign-key-set-operations dbms:postgresql
ALTER TABLE NamedSetOperations
  DROP CONSTRAINT namedsetoperations_measurementconsumerreferenceid_reporti_fkey1,
  ADD CONSTRAINT namedsetoperations_measurementconsumerreferenceid_reporti_fkey1
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId, MetricId, SetOperationId)
    REFERENCES SetOperations(MeasurementConsumerReferenceId, ReportId, MetricId, SetOperationId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-measurement-calculations-table-foreign-key-time-intervals dbms:postgresql
ALTER TABLE MeasurementCalculations
  DROP CONSTRAINT measurementcalculations_measurementconsumerreferenceid_rep_fkey,
  ADD CONSTRAINT measurementcalculations_measurementconsumerreferenceid_rep_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerReferenceId, ReportId, TimeIntervalId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-measurement-calculations-table-foreign-key-named-set-operations dbms:postgresql
ALTER TABLE MeasurementCalculations
  DROP CONSTRAINT measurementcalculations_measurementconsumerreferenceid_re_fkey1,
  ADD CONSTRAINT measurementcalculations_measurementconsumerreferenceid_re_fkey1
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId, MetricId, NamedSetOperationId)
    REFERENCES NamedSetOperations(MeasurementConsumerReferenceId, ReportId, MetricId, NamedSetOperationId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-weighted-measurements-table-foreign-key-measurement-calculations dbms:postgresql
ALTER TABLE WeightedMeasurements
  DROP CONSTRAINT weightedmeasurements_measurementconsumerreferenceid_report_fkey,
  ADD CONSTRAINT weightedmeasurements_measurementconsumerreferenceid_report_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, ReportId, MetricId, NamedSetOperationId, MeasurementCalculationId)
    REFERENCES MeasurementCalculations(MeasurementConsumerReferenceId, ReportId, MetricId, NamedSetOperationId, MeasurementCalculationId)
    ON DELETE CASCADE;

-- changeset tristanvuong2021:alter-weighted-measurements-table-foreign-key-measurements dbms:postgresql
ALTER TABLE WeightedMeasurements
  DROP CONSTRAINT weightedmeasurements_measurementconsumerreferenceid_measur_fkey,
  ADD CONSTRAINT weightedmeasurements_measurementconsumerreferenceid_measur_fkey
    FOREIGN KEY(MeasurementConsumerReferenceId, MeasurementReferenceId)
    REFERENCES Measurements(MeasurementConsumerReferenceId, MeasurementReferenceId)
    ON DELETE CASCADE;
