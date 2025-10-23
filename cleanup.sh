#!/bin/bash

# Requisition Cleanup Script
# This script cleans up requisitions and related data from the halo-cmm-dev environment

set -e

# Configuration
PROJECT="halo-cmm-dev"
INSTANCE="dev-instance"
DATA_PROVIDER_ID_1="8635770568665047672"
DATA_PROVIDER_ID_2="8868812534684912454"
DATA_PROVIDER_RESOURCE_1="T5RryPMNong"
DATA_PROVIDER_RESOURCE_2="J3-pzhqS9Lo"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Function to drain a Pub/Sub subscription
drain_subscription() {
    local subscription=$1
    local total_drained=0

    print_info "Draining subscription: $subscription"

    while true; do
        # Pull messages and count them
        local result=$(gcloud pubsub subscriptions pull "$subscription" \
            --project="$PROJECT" \
            --auto-ack \
            --limit=1000 \
            --format="value(MESSAGE_ID)" 2>/dev/null | wc -l)

        if [ "$result" -eq 0 ]; then
            if [ "$total_drained" -eq 0 ]; then
                print_success "Subscription is already empty"
            else
                print_success "Finished draining. Total messages: $total_drained"
            fi
            break
        fi

        total_drained=$((total_drained + result))
        print_info "  Drained $result messages (total: $total_drained)"
    done

    return 0
}

# Function to delete requisitions
delete_requisitions() {
    local data_provider_id=$1

    print_info "Deleting requisitions for DataProviderId: $data_provider_id"

    local output=$(gcloud spanner databases execute-sql kingdom \
        --instance="$INSTANCE" \
        --project="$PROJECT" \
        --sql="delete from Requisitions WHERE DataProviderId = $data_provider_id" 2>&1)

    local rows=$(echo "$output" | grep -oP 'Statement modified \K\d+' || echo "0")

    if [ "$rows" -eq 0 ]; then
        print_warning "No requisitions found to delete"
    else
        print_success "Deleted $rows requisitions"
    fi

    return 0
}

# Function to delete requisition metadata
delete_metadata() {
    local resource_id=$1

    print_info "Deleting metadata for DataProviderResourceId: $resource_id"

    local output=$(gcloud spanner databases execute-sql edp-aggregator \
        --instance="$INSTANCE" \
        --project="$PROJECT" \
        --sql="delete from RequisitionMetadata WHERE DataProviderResourceId = '$resource_id'" 2>&1)

    local rows=$(echo "$output" | grep -oP 'Statement modified \K\d+' || echo "0")

    if [ "$rows" -eq 0 ]; then
        print_warning "No metadata found to delete"
    else
        print_success "Deleted $rows metadata records"
    fi

    return 0
}

# Main execution
main() {
    print_header "Starting Cleanup Process"
    print_info "Project: $PROJECT"
    print_info "Spanner Instance: $INSTANCE"
    echo ""

    # Step 1: Drain Pub/Sub Queues
    print_header "Step 1: Draining Pub/Sub Queues"
    drain_subscription "results-fulfiller-subscription"
    echo ""
    drain_subscription "results-fulfiller-queue-dlq-sub"
    echo ""

    # Step 2: Delete Requisitions from Kingdom Database
    print_header "Step 2: Deleting Requisitions from Kingdom Database"
    delete_requisitions "$DATA_PROVIDER_ID_1"
    echo ""
    delete_requisitions "$DATA_PROVIDER_ID_2"
    echo ""

    # Step 3: Delete Requisition Metadata from EDP Aggregator Database
    print_header "Step 3: Deleting Metadata from EDP Aggregator Database"
    delete_metadata "$DATA_PROVIDER_RESOURCE_1"
    echo ""
    delete_metadata "$DATA_PROVIDER_RESOURCE_2"
    echo ""

    # Summary
    print_header "Cleanup Process Complete"
    print_success "All cleanup operations completed successfully!"
    echo ""

    # Verification reminder
    print_info "To verify the cleanup, run:"
    echo "  ./cleanup.sh --verify"
}

# Verification function
verify() {
    print_header "Verification"

    # Check Pub/Sub queues
    print_info "Checking Pub/Sub queues..."

    local main_count=$(gcloud pubsub subscriptions pull results-fulfiller-subscription \
        --project="$PROJECT" \
        --limit=1 \
        --format="value(MESSAGE_ID)" 2>/dev/null | wc -l)

    local dlq_count=$(gcloud pubsub subscriptions pull results-fulfiller-queue-dlq-sub \
        --project="$PROJECT" \
        --limit=1 \
        --format="value(MESSAGE_ID)" 2>/dev/null | wc -l)

    if [ "$main_count" -eq 0 ] && [ "$dlq_count" -eq 0 ]; then
        print_success "Pub/Sub queues are empty"
    else
        print_warning "Pub/Sub queues still have messages (main: $main_count, dlq: $dlq_count)"
    fi
    echo ""

    # Check requisitions
    print_info "Checking requisitions in Kingdom database..."

    local req_output=$(gcloud spanner databases execute-sql kingdom \
        --instance="$INSTANCE" \
        --project="$PROJECT" \
        --sql="SELECT COUNT(*) as count FROM Requisitions WHERE DataProviderId IN ($DATA_PROVIDER_ID_1, $DATA_PROVIDER_ID_2)" 2>&1)

    local req_count=$(echo "$req_output" | grep -oP '\d+' | tail -n 1 || echo "unknown")

    if [ "$req_count" = "0" ]; then
        print_success "No requisitions found in Kingdom database"
    else
        print_warning "Found $req_count requisitions still in Kingdom database"
    fi
    echo ""

    # Check metadata
    print_info "Checking metadata in EDP Aggregator database..."

    local meta_output=$(gcloud spanner databases execute-sql edp-aggregator \
        --instance="$INSTANCE" \
        --project="$PROJECT" \
        --sql="SELECT COUNT(*) as count FROM RequisitionMetadata WHERE DataProviderResourceId IN ('$DATA_PROVIDER_RESOURCE_1', '$DATA_PROVIDER_RESOURCE_2')" 2>&1)

    local meta_count=$(echo "$meta_output" | grep -oP '\d+' | tail -n 1 || echo "unknown")

    if [ "$meta_count" = "0" ]; then
        print_success "No metadata found in EDP Aggregator database"
    else
        print_warning "Found $meta_count metadata records still in EDP Aggregator database"
    fi
    echo ""

    print_header "Verification Complete"
}

# Show usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --verify    Run verification checks only"
    echo "  --help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0              # Run full cleanup"
    echo "  $0 --verify     # Verify cleanup results"
}

# Parse command line arguments
case "${1:-}" in
    --verify)
        verify
        ;;
    --help)
        usage
        ;;
    "")
        main
        ;;
    *)
        print_error "Unknown option: $1"
        usage
        exit 1
        ;;
esac
