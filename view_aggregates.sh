#!/bin/bash

# Spanner Database Aggregates Report
# Shows comprehensive statistics about Measurements, Requisitions, DataProviders, and EventGroups

set -e

INSTANCE="dev-instance"
DATABASE="kingdom"
PROJECT="halo-cmm-dev"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Helper function to run SQL queries
run_query() {
    local sql="$1"
    gcloud spanner databases execute-sql "$DATABASE" \
        --instance="$INSTANCE" \
        --project="$PROJECT" \
        --sql="$sql"
}

echo "============================================================================="
echo -e "${CYAN}SPANNER DATABASE AGGREGATES REPORT${NC}"
echo "Instance: $INSTANCE | Database: $DATABASE | Project: $PROJECT"
echo "Generated: $(date)"
echo "============================================================================="
echo ""

# 1. MEASUREMENTS OVERVIEW
echo -e "${YELLOW}[1] MEASUREMENTS OVERVIEW${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    COUNT(*) as TotalMeasurements,
    COUNT(DISTINCT MeasurementConsumerId) as UniqueMeasurementConsumers,
    MIN(UpdateTime) as OldestMeasurement,
    MAX(UpdateTime) as NewestMeasurement
FROM Measurements"
echo ""

# 2. MEASUREMENTS BY TIME PERIOD
echo -e "${YELLOW}[2] MEASUREMENTS BY TIME PERIOD${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    'Last 1 Hour' as Period,
    COUNT(*) as MeasurementCount
FROM Measurements
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
UNION ALL
SELECT
    'Last 6 Hours' as Period,
    COUNT(*) as MeasurementCount
FROM Measurements
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
UNION ALL
SELECT
    'Last 24 Hours' as Period,
    COUNT(*) as MeasurementCount
FROM Measurements
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
UNION ALL
SELECT
    'Last 48 Hours' as Period,
    COUNT(*) as MeasurementCount
FROM Measurements
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
UNION ALL
SELECT
    'Last 7 Days' as Period,
    COUNT(*) as MeasurementCount
FROM Measurements
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY MeasurementCount"
echo ""

# 3. MEASUREMENTS BY STATE
echo -e "${YELLOW}[3] MEASUREMENTS BY STATE${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    State,
    COUNT(*) as Count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Measurements), 2) as Percentage
FROM Measurements
GROUP BY State
ORDER BY Count DESC"
echo ""

# 4. REQUISITIONS OVERVIEW
echo -e "${YELLOW}[4] REQUISITIONS OVERVIEW${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    COUNT(*) as TotalRequisitions,
    COUNT(DISTINCT MeasurementId) as UniqueMeasurements,
    COUNT(DISTINCT DataProviderId) as UniqueDataProviders,
    MIN(UpdateTime) as OldestRequisition,
    MAX(UpdateTime) as NewestRequisition
FROM Requisitions"
echo ""

# 5. REQUISITIONS BY STATE
echo -e "${YELLOW}[5] REQUISITIONS BY STATE${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    State,
    COUNT(*) as Count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Requisitions), 2) as Percentage
FROM Requisitions
GROUP BY State
ORDER BY Count DESC"
echo ""

# 6. REQUISITIONS BY TIME PERIOD
echo -e "${YELLOW}[6] REQUISITIONS BY TIME PERIOD${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    'Last 1 Hour' as Period,
    COUNT(*) as RequisitionCount
FROM Requisitions
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
UNION ALL
SELECT
    'Last 6 Hours' as Period,
    COUNT(*) as RequisitionCount
FROM Requisitions
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
UNION ALL
SELECT
    'Last 24 Hours' as Period,
    COUNT(*) as RequisitionCount
FROM Requisitions
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
UNION ALL
SELECT
    'Last 48 Hours' as Period,
    COUNT(*) as RequisitionCount
FROM Requisitions
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
UNION ALL
SELECT
    'Last 7 Days' as Period,
    COUNT(*) as RequisitionCount
FROM Requisitions
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY RequisitionCount"
echo ""

# 7. DATA PROVIDERS ACTIVITY (LAST 48 HOURS)
echo -e "${YELLOW}[7] DATA PROVIDER ACTIVITY (LAST 48 HOURS)${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    dp.DataProviderId,
    dp.ExternalDataProviderId,
    COUNT(DISTINCT r.MeasurementId) as MeasurementCount,
    COUNT(*) as RequisitionCount,
    MIN(r.UpdateTime) as FirstRequisition,
    MAX(r.UpdateTime) as LastRequisition
FROM Requisitions r
JOIN DataProviders dp ON r.DataProviderId = dp.DataProviderId
WHERE r.UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
GROUP BY dp.DataProviderId, dp.ExternalDataProviderId
ORDER BY MeasurementCount DESC"
echo ""

# 8. DATA PROVIDERS WITH EVENT GROUPS
echo -e "${YELLOW}[8] DATA PROVIDERS WITH EVENT GROUPS${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    dp.DataProviderId,
    dp.ExternalDataProviderId,
    COUNT(DISTINCT eg.EventGroupId) as EventGroupCount,
    COUNT(DISTINCT eg.ProvidedEventGroupId) as UniqueProvidedIds,
    MIN(eg.ProvidedEventGroupId) as FirstProvidedId,
    MAX(eg.ProvidedEventGroupId) as LastProvidedId
FROM DataProviders dp
LEFT JOIN EventGroups eg ON dp.DataProviderId = eg.DataProviderId
GROUP BY dp.DataProviderId, dp.ExternalDataProviderId
ORDER BY EventGroupCount DESC"
echo ""

# 9. FULFILLING DUCHY ACTIVITY (LAST 48 HOURS)
echo -e "${YELLOW}[9] FULFILLING DUCHY ACTIVITY (LAST 48 HOURS)${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    FulfillingDuchyId,
    COUNT(*) as RequisitionCount,
    COUNT(DISTINCT MeasurementId) as UniqueMeasurements,
    COUNT(DISTINCT DataProviderId) as UniqueDataProviders
FROM Requisitions
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
GROUP BY FulfillingDuchyId
ORDER BY RequisitionCount DESC"
echo ""

# 10. MEASUREMENT CONSUMER ACTIVITY (LAST 48 HOURS)
echo -e "${YELLOW}[10] MEASUREMENT CONSUMER ACTIVITY (LAST 48 HOURS)${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    MeasurementConsumerId,
    COUNT(*) as MeasurementCount,
    MIN(UpdateTime) as FirstMeasurement,
    MAX(UpdateTime) as LastMeasurement
FROM Measurements
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
GROUP BY MeasurementConsumerId
ORDER BY MeasurementCount DESC"
echo ""

# 11. REQUISITIONS PER MEASUREMENT (VALIDATION)
echo -e "${YELLOW}[11] REQUISITIONS PER MEASUREMENT (VALIDATION)${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    RequisitionsPerMeasurement,
    COUNT(*) as MeasurementCount
FROM (
    SELECT
        MeasurementId,
        COUNT(*) as RequisitionsPerMeasurement
    FROM Requisitions
    WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
    GROUP BY MeasurementId
)
GROUP BY RequisitionsPerMeasurement
ORDER BY RequisitionsPerMeasurement"
echo ""

# 12. EVENT GROUPS BY DATA PROVIDER
echo -e "${YELLOW}[12] EVENT GROUPS BY DATA PROVIDER${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    eg.DataProviderId,
    dp.ExternalDataProviderId,
    eg.ProvidedEventGroupId,
    eg.ExternalEventGroupId,
    eg.DataAvailabilityStartTime,
    eg.DataAvailabilityEndTime
FROM EventGroups eg
JOIN DataProviders dp ON eg.DataProviderId = dp.DataProviderId
WHERE eg.ProvidedEventGroupId NOT LIKE 'load-test-%'
  AND eg.ProvidedEventGroupId NOT LIKE '%-%-%-%-%'
ORDER BY eg.DataProviderId, eg.ProvidedEventGroupId
LIMIT 20"
echo ""

# 13. RECENT MEASUREMENT ACTIVITY (HOURLY BREAKDOWN)
echo -e "${YELLOW}[13] RECENT MEASUREMENT ACTIVITY (HOURLY, LAST 24H)${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    TIMESTAMP_TRUNC(UpdateTime, HOUR) as Hour,
    COUNT(*) as MeasurementCount,
    COUNT(DISTINCT MeasurementConsumerId) as UniqueConsumers
FROM Measurements
WHERE UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY Hour
ORDER BY Hour DESC"
echo ""

# 14. DATA PROVIDER SUMMARY
echo -e "${YELLOW}[14] ALL DATA PROVIDERS SUMMARY${NC}"
echo "-----------------------------------------------------------------------------"
run_query "SELECT
    dp.DataProviderId,
    dp.ExternalDataProviderId,
    COUNT(DISTINCT eg.EventGroupId) as EventGroups,
    (SELECT COUNT(*) FROM Requisitions r WHERE r.DataProviderId = dp.DataProviderId AND r.UpdateTime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)) as RecentRequisitions
FROM DataProviders dp
LEFT JOIN EventGroups eg ON dp.DataProviderId = eg.DataProviderId
GROUP BY dp.DataProviderId, dp.ExternalDataProviderId
ORDER BY RecentRequisitions DESC, EventGroups DESC"
echo ""

echo "============================================================================="
echo -e "${GREEN}REPORT COMPLETE${NC}"
echo "============================================================================="
